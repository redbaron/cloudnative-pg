/*
Copyright © contributors to CloudNativePG, established as
CloudNativePG a Series of LF Projects, LLC.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

SPDX-License-Identifier: Apache-2.0
*/

package majorupgrade

import (
	"context"
	"fmt"
	"time"

	"github.com/cloudnative-pg/cloudnative-pg/pkg/resources"
	"github.com/cloudnative-pg/machinery/pkg/log"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	apiv1 "github.com/cloudnative-pg/cloudnative-pg/api/v1"
	"github.com/cloudnative-pg/cloudnative-pg/internal/configuration"
	"github.com/cloudnative-pg/cloudnative-pg/pkg/resources/status"
	"github.com/cloudnative-pg/cloudnative-pg/pkg/specs"
	"github.com/cloudnative-pg/cloudnative-pg/pkg/utils"
	"github.com/cloudnative-pg/cloudnative-pg/pkg/versions"
)

// ErrIncoherentMajorUpgradeJob is raised when the major upgrade job
// is missing the target image
var ErrIncoherentMajorUpgradeJob = fmt.Errorf("major upgrade job is missing the target image")

// ErrNoPrimaryPVCFound is raised when the list of PVCs doesn't
// include any primary instance.
var ErrNoPrimaryPVCFound = fmt.Errorf("no primary PVC found")

// Reconcile implements the major version upgrade logic.
func Reconcile(
	ctx context.Context,
	c client.Client,
	cluster *apiv1.Cluster,
	instances []corev1.Pod,
	pvcs []corev1.PersistentVolumeClaim,
	jobs []batchv1.Job,
	noInstanceIsAlive bool,
) (*ctrl.Result, error) {
	contextLogger := log.FromContext(ctx)

	if majorUpgradeJob := getMajorUpdateJob(jobs); majorUpgradeJob != nil {
		return majorVersionUpgradeHandleCompletion(ctx, c, cluster, majorUpgradeJob, pvcs)
	}

	if err := postUpgradeBackupCompletion(ctx, c, cluster, noInstanceIsAlive); err != nil {
		return nil, err
	}

	requestedMajor, err := cluster.GetPostgresqlMajorVersion()
	if err != nil {
		contextLogger.Error(err, "Unable to retrieve the requested PostgreSQL version")
		return nil, err
	}

	if cluster.Status.PGDataImageInfo == nil || requestedMajor <= cluster.Status.PGDataImageInfo.MajorVersion {
		return nil, nil
	}
	// If we are here, upgrade job didn't complete yet

	primaryNodeSerial, err := getPrimarySerial(pvcs)
	if err != nil || primaryNodeSerial == 0 {
		contextLogger.Error(err, "Unable to retrieve the primary node serial")
		return nil, err
	}

	contextLogger.Info("Reconciling in-place major version upgrades",
		"primaryNodeSerial", primaryNodeSerial, "requestedMajor", requestedMajor)

	err = registerPhase(ctx, c, cluster, apiv1.PhaseMajorUpgrade,
		fmt.Sprintf("Upgrading cluster to major version %v", requestedMajor))
	if err != nil {
		return nil, err
	}

	if result, err := deleteAllPodsInMajorUpgradePreparation(ctx, c, instances, jobs); err != nil {
		contextLogger.Error(err, "Unable to delete pods and jobs in preparation for major upgrade")
		return nil, err
	} else if result != nil {
		return result, err
	}

	if result, err := registerPostUpgradeBackup(ctx, c, cluster, primaryNodeSerial); err != nil {
		contextLogger.Error(err, "Unable to create post-upgrade backup")
		return nil, err
	} else if result != nil {
		return result, err
	}

	if result, err := createMajorUpgradeJob(ctx, c, cluster, primaryNodeSerial); err != nil {
		contextLogger.Error(err, "Unable to create major upgrade job")
		return nil, err
	} else if result != nil {
		return result, err
	}

	return &ctrl.Result{RequeueAfter: 30 * time.Second}, nil
}

func getMajorUpdateJob(items []batchv1.Job) *batchv1.Job {
	for _, job := range items {
		if isMajorUpgradeJob(&job) {
			return &job
		}
	}

	return nil
}

func getPostUpgradeBackup(ctx context.Context, c client.Client, cluster *apiv1.Cluster) (*apiv1.Backup, error) {
	backupName := cluster.Status.MajorUpgradeStatus.PostUpgradeBackupName
	if backupName == "" {
		return nil, nil
	}

	var backup apiv1.Backup
	backupObjectKey := client.ObjectKey{
		Namespace: cluster.Namespace,
		Name:      backupName,
	}
	err := c.Get(ctx, backupObjectKey, &backup)
	if err != nil {
		return nil, err
	}
	return &backup, nil
}

func deleteAllPodsInMajorUpgradePreparation(
	ctx context.Context,
	c client.Client,
	instances []corev1.Pod,
	jobs []batchv1.Job,
) (*ctrl.Result, error) {
	foundSomethingToDelete := false

	for _, pod := range instances {
		if pod.GetDeletionTimestamp() != nil {
			continue
		}

		foundSomethingToDelete = true
		if err := c.Delete(ctx, &pod); err != nil {
			return nil, err
		}
	}

	for _, job := range jobs {
		if job.GetDeletionTimestamp() != nil {
			continue
		}

		foundSomethingToDelete = true
		if err := c.Delete(ctx, &job, &client.DeleteOptions{
			PropagationPolicy: ptr.To(metav1.DeletePropagationForeground),
		}); err != nil {
			return nil, err
		}
	}

	if foundSomethingToDelete {
		return &ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	return nil, nil
}

func registerPostUpgradeBackup(ctx context.Context, c client.Client, cluster *apiv1.Cluster, nodeSerial int) (*ctrl.Result, error) {
	contextLogger := log.FromContext(ctx)
	_, ok := chooseBackupMethod(cluster)
	if !ok {
		contextLogger.Debug("Skipping configuring post-upgrade backup, because unable to determine backup method")
		return nil, nil
	}

	var backupName string
	// Write status once, being careful not to erase original instances count if we happen to retry
	// registerPostUpgradeBackup call
	if cluster.Status.MajorUpgradeStatus.PostUpgradeBackupName == "" {
		instanceName := specs.GetInstanceName(cluster.Name, nodeSerial)
		backupName = fmt.Sprintf("%s-%s", instanceName, "post-upgrade-backup")

		if err := status.PatchWithOptimisticLock(ctx, c, cluster, func(c *apiv1.Cluster) {
			c.Status.MajorUpgradeStatus.PreUpgradeInstanceCount = c.Spec.Instances
			c.Status.MajorUpgradeStatus.PostUpgradeBackupName = backupName
		}); err != nil {
			contextLogger.Error(err, "Unable to update Cluster's status with a MajorUpgradeStatus")
			return nil, err
		}
	}

	//TODO: should we skip backup if instance count was 1 in the beginning?
	if cluster.Spec.Instances != 1 {
		if err := resources.RetryWithRefreshedResource(ctx, c, cluster, func() error {
			return c.Update(ctx, cluster)
		}); err != nil {
			contextLogger.Error(err, "Unable to update Cluster instance count")
			return nil, err
		}
	}

	// NOTE: we can't create backup here yet, because creating it too early will often makes backup fail
	// when it attempts to backup freshly started instance
	return nil, nil
}

// Possible return values:
// - nil, err:  unexpected error
// - nil, nil: no backup exists and no backup is expected to exist yet
// - backup, nil: we either created or found existing backup
func createOrGetPostUpgradeBackup(ctx context.Context, c client.Client, cluster *apiv1.Cluster, noInstancesIsAlive bool) (*apiv1.Backup, error) {
	contextLogger := log.FromContext(ctx)
	if backup, err := getPostUpgradeBackup(ctx, c, cluster); err == nil {
		return backup, err
	} else if !errors.IsNotFound(err) {
		return nil, err
	}

	// At this moment know that post-upgrade backup was registered in the status, but it doesn't exist in the cluster.
	// We should create it, but not before first instance comes alive, otherwise backup fails if unrecoverable error
	if noInstancesIsAlive {
		return nil, nil
	}

	backupMethod, ok := chooseBackupMethod(cluster)
	if !ok { // Should not happen, because we checked it before registering backup
		return nil, nil
	}

	backupName := cluster.Status.MajorUpgradeStatus.PostUpgradeBackupName

	backup := apiv1.Backup{
		ObjectMeta: metav1.ObjectMeta{Name: backupName, Namespace: cluster.Namespace},
		Spec: apiv1.BackupSpec{
			Cluster: apiv1.LocalObjectReference{Name: cluster.Name},
			Method:  backupMethod,
		},
	}
	utils.SetOperatorVersion(&backup.ObjectMeta, versions.Version)
	utils.InheritAnnotations(&backup.ObjectMeta, cluster.Annotations,
		cluster.GetFixedInheritedAnnotations(), configuration.Current)
	utils.InheritLabels(&backup.ObjectMeta, cluster.Labels,
		cluster.GetFixedInheritedLabels(), configuration.Current)
	utils.SetAsOwnedBy(&backup.ObjectMeta, cluster.ObjectMeta, cluster.TypeMeta)

	if err := c.Create(ctx, &backup); err != nil {
		contextLogger.Error(err, "Unable to create post-upgrade backup")
		return nil, err
	}
	return nil, nil
}

func createMajorUpgradeJob(
	ctx context.Context,
	c client.Client,
	cluster *apiv1.Cluster,
	primaryNodeSerial int,
) (*ctrl.Result, error) {
	contextLogger := log.FromContext(ctx)

	job := createMajorUpgradeJobDefinition(cluster, primaryNodeSerial)

	if err := ctrl.SetControllerReference(cluster, job, c.Scheme()); err != nil {
		contextLogger.Error(err, "Unable to set the owner reference for major upgrade job")
		return nil, err
	}

	utils.SetOperatorVersion(&job.ObjectMeta, versions.Version)
	utils.InheritAnnotations(&job.ObjectMeta, cluster.Annotations,
		cluster.GetFixedInheritedAnnotations(), configuration.Current)
	utils.InheritAnnotations(&job.Spec.Template.ObjectMeta, cluster.Annotations,
		cluster.GetFixedInheritedAnnotations(), configuration.Current)
	utils.InheritLabels(&job.ObjectMeta, cluster.Labels,
		cluster.GetFixedInheritedLabels(), configuration.Current)
	utils.InheritLabels(&job.Spec.Template.ObjectMeta, cluster.Labels,
		cluster.GetFixedInheritedLabels(), configuration.Current)
	utils.SetInstanceRole(job.Spec.Template.ObjectMeta, specs.ClusterRoleLabelPrimary)

	contextLogger.Info("Creating new major upgrade Job",
		"jobName", job.Name,
		"primary", true)

	if err := c.Create(ctx, job); err != nil {
		if errors.IsAlreadyExists(err) {
			// This Job was already created, maybe the cache is stale.
			return &ctrl.Result{RequeueAfter: 30 * time.Second}, nil
		}
		return nil, err
	}

	return nil, nil
}

func majorVersionUpgradeHandleCompletion(
	ctx context.Context,
	c client.Client,
	cluster *apiv1.Cluster,
	job *batchv1.Job,
	pvcs []corev1.PersistentVolumeClaim,
) (*ctrl.Result, error) {
	contextLogger := log.FromContext(ctx)

	if !utils.JobHasOneCompletion(*job) {
		contextLogger.Info("Major upgrade job not completed.")
		return &ctrl.Result{RequeueAfter: 30 * time.Second}, nil
	}

	for _, pvc := range pvcs {
		if pvc.GetDeletionTimestamp() != nil {
			continue
		}

		if specs.IsPrimary(pvc.ObjectMeta) {
			continue
		}

		if err := c.Delete(ctx, &pvc); err != nil {
			// Ignore if NotFound, otherwise report the error
			if !errors.IsNotFound(err) {
				return nil, err
			}
		}
	}

	jobImage, ok := getTargetImageFromMajorUpgradeJob(job)
	if !ok {
		return nil, ErrIncoherentMajorUpgradeJob
	}

	requestedMajor, err := cluster.GetPostgresqlMajorVersion()
	if err != nil {
		contextLogger.Error(err, "Unable to retrieve the requested PostgreSQL version")
		return nil, err
	}

	if err := status.PatchWithOptimisticLock(
		ctx,
		c,
		cluster,
		status.SetPGDataImageInfo(&apiv1.ImageInfo{
			Image:        jobImage,
			MajorVersion: requestedMajor,
		}),
	); err != nil {
		contextLogger.Error(err, "Unable to update cluster status after major upgrade completed.")
		return nil, err
	}

	if err := c.Delete(ctx, job, &client.DeleteOptions{
		PropagationPolicy: ptr.To(metav1.DeletePropagationForeground),
	}); err != nil {
		contextLogger.Error(err, "Unable to delete major upgrade job.")
		return nil, err
	}

	return &ctrl.Result{Requeue: true}, nil
}

func postUpgradeBackupCompletion(ctx context.Context, c client.Client, cluster *apiv1.Cluster, noInstancesIsAlive bool) error {
	contextLogger := log.FromContext(ctx)
	backup, err := createOrGetPostUpgradeBackup(ctx, c, cluster, noInstancesIsAlive)
	if err != nil {
		contextLogger.Error(err, "Unable to create or get post-upgrade backup")
		return err
	}
	if backup == nil {
		return nil
	}

	if !backup.Status.IsDone() {
		contextLogger.Debug("Waiting for the post-upgrade backup completion", "backup_name", backup.Name, "backup_phase", backup.Status.Phase)
		return nil
	}
	contextLogger.Info("Post-upgrade backup completed, restoring original instance count and finalizing status updates")

	cluster.Spec.Instances = cluster.Status.MajorUpgradeStatus.PreUpgradeInstanceCount
	if err := c.Update(ctx, cluster); err != nil {
		contextLogger.Error(err, "Unable to scale up cluster status after major upgrade completed.")
		return err
	}

	if err := status.PatchWithOptimisticLock(ctx, c, cluster, func(cluster *apiv1.Cluster) {
		cluster.Status.MajorUpgradeStatus = apiv1.MajorUpgradeStatus{}
	}); err != nil {
		contextLogger.Error(err, "Unable to update cluster status after major upgrade completed.")
		return err
	}
	return nil
}

// registerPhase sets a phase into the cluster
func registerPhase(
	ctx context.Context,
	c client.Client,
	cluster *apiv1.Cluster,
	phase string,
	reason string,
) error {
	return status.PatchWithOptimisticLock(
		ctx,
		c,
		cluster,
		status.SetPhase(phase, reason),
		status.SetClusterReadyCondition,
	)
}

// getPrimarySerial tries to obtain the primary serial from a group of PVCs
func getPrimarySerial(
	pvcs []corev1.PersistentVolumeClaim,
) (int, error) {
	for _, pvc := range pvcs {
		instanceRole, _ := utils.GetInstanceRole(pvc.Labels)
		if instanceRole != specs.ClusterRoleLabelPrimary {
			continue
		}

		return specs.GetNodeSerial(pvc.ObjectMeta)
	}

	return 0, ErrNoPrimaryPVCFound
}

func chooseBackupMethod(cluster *apiv1.Cluster) (apiv1.BackupMethod, bool) {
	walArchivingActive := (cluster.Spec.Backup != nil && cluster.Spec.Backup.BarmanObjectStore != nil) ||
		cluster.GetEnabledWALArchivePluginName() != ""

	// This backup won't be usable for replica initialization, so don't take it.
	if !walArchivingActive {
		return "", false
	}

	bSpec := cluster.Spec.Backup
	if bSpec == nil {
		return "", false
	} else if bSpec.VolumeSnapshot != nil {
		return apiv1.BackupMethodVolumeSnapshot, true
	} else if bSpec.IsBarmanBackupConfigured() {
		return apiv1.BackupMethodBarmanObjectStore, true
	} else {
		// It probably should be 'plugin', but this method is not set in a Cluster CR, but in Backup object
		// FIXME: we should probably lookup latest Backup objects? Or even explicitly requires to specify backup config?
		return "", false
	}
}
