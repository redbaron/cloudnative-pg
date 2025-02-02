/*
Copyright The CloudNativePG Contributors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package report

import (
	"archive/zip"
	"context"
	"fmt"
	"io"
	"path/filepath"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/cloudnative-pg/cloudnative-pg/internal/cmd/plugin"
	"github.com/cloudnative-pg/cloudnative-pg/pkg/utils"
)

const jobMatcherLabel = "job-name"

// streamPodLogs streams the pod logs via REST to an io.Writer
// in JSON-lines format
//
// NOTE: streaming to a writer is advantageous, as logs may take up a lot of
// memory and blow up RAM if read/written in full to a buffer
func streamPodLogs(ctx context.Context, pod corev1.Pod, writer io.Writer) (err error) {
	pods := kubernetes.NewForConfigOrDie(plugin.Config).CoreV1().Pods(pod.Namespace)
	logsRequest := pods.GetLogs(pod.Name, &corev1.PodLogOptions{})
	logStream, err := logsRequest.Stream(ctx)
	if err != nil {
		return fmt.Errorf("could not stream the logs: %w", err)
	}
	defer func() {
		innerErr := logStream.Close()
		if err == nil && innerErr != nil {
			err = innerErr
		}
	}()

	_, err = io.Copy(writer, logStream)
	if err != nil {
		err = fmt.Errorf("could not send logs to writer: %w", err)
	}
	return err
}

// streamPodLogsToZip streams the pod logs to a new section in the ZIP
func streamPodLogsToZip(ctx context.Context, pods []corev1.Pod,
	dirname, name string, zipper *zip.Writer,
) error {
	logsdir := filepath.Join(dirname, name)
	if _, err := zipper.Create(logsdir + "/"); err != nil {
		return fmt.Errorf("could not add '%s' to zip: %w", logsdir, err)
	}

	for i := range pods {
		pod := pods[i]
		path := filepath.Join(logsdir, fmt.Sprintf("%s-logs.jsonl", pod.Name))
		writer, zipperErr := zipper.Create(path)
		if zipperErr != nil {
			return fmt.Errorf("could not add '%s' to zip: %w", path, zipperErr)
		}
		if err := streamPodLogs(ctx, pod, writer); err != nil {
			return err
		}
	}

	return nil
}

// streamClusterLogsToZip streams the logs from the pods in the cluster, one by
// one, each in a new file, within  a folder
func streamClusterLogsToZip(ctx context.Context, clusterName, namespace string,
	dirname string, zipper *zip.Writer,
) error {
	logsdir := filepath.Join(dirname, "logs")
	_, err := zipper.Create(logsdir + "/")
	if err != nil {
		return fmt.Errorf("could not add '%s' to zip: %w", logsdir, err)
	}

	matchClusterName := client.MatchingLabels{
		utils.ClusterLabelName: clusterName,
	}

	var podList corev1.PodList
	err = plugin.Client.List(ctx, &podList, matchClusterName, client.InNamespace(namespace))
	if err != nil {
		return fmt.Errorf("could not get cluster pods: %w", err)
	}

	for _, pod := range podList.Items {
		writer, err := zipper.Create(filepath.Join(logsdir, pod.Name) + ".jsonl")
		if err != nil {
			return fmt.Errorf("could not add '%s' to zip: %w",
				filepath.Join(logsdir, pod.Name), err)
		}

		err = streamPodLogs(ctx, pod, writer)
		if err != nil {
			return err
		}
	}

	return nil
}

// streamClusterJobLogsToZip checks for jobs in the cluster, and streams
// the logs from the pods created by those jobs, one by one, each in a new file
func streamClusterJobLogsToZip(ctx context.Context, clusterName, namespace string,
	dirname string, zipper *zip.Writer,
) error {
	logsdir := filepath.Join(dirname, "job-logs")
	_, err := zipper.Create(logsdir + "/")
	if err != nil {
		return fmt.Errorf("could not add '%s' to zip: %w", logsdir, err)
	}

	matchClusterName := client.MatchingLabels{
		utils.ClusterLabelName: clusterName,
	}

	var jobList batchv1.JobList
	err = plugin.Client.List(ctx, &jobList, matchClusterName, client.InNamespace(namespace))
	if err != nil {
		return fmt.Errorf("could not get cluster jobs: %w", err)
	}

	for _, job := range jobList.Items {
		matchJobName := client.MatchingLabels{
			jobMatcherLabel: job.Name,
		}
		var podList corev1.PodList
		err = plugin.Client.List(ctx, &podList, matchJobName, client.InNamespace(namespace))
		if err != nil {
			return fmt.Errorf("could not get pods for job '%s': %w", job.Name, err)
		}

		for _, pod := range podList.Items {
			writer, err := zipper.Create(filepath.Join(logsdir, pod.Name) + ".jsonl")
			if err != nil {
				return fmt.Errorf("could not add '%s' to zip: %w",
					filepath.Join(logsdir, pod.Name), err)
			}

			err = streamPodLogs(ctx, pod, writer)
			if err != nil {
				return err
			}
		}
	}

	return nil
}
