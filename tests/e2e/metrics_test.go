/*
This file is part of Cloud Native PostgreSQL.

Copyright (C) 2019-2021 EnterpriseDB Corporation.
*/

package e2e

import (
	"fmt"
	"regexp"

	"github.com/EnterpriseDB/cloud-native-postgresql/tests"
	"github.com/EnterpriseDB/cloud-native-postgresql/tests/utils"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Metrics", func() {
	const (
		targetDBOne             = "test"
		targetDBTwo             = "test1"
		targetDBSecret          = "secret_test"
		testTableName           = "test_table"
		clusterMetricsFile      = fixturesDir + "/metrics/cluster-metrics.yaml"
		clusterMetricsDBFile    = fixturesDir + "/metrics/cluster-metrics-with-target-databases.yaml"
		customQueriesSampleFile = fixturesDir + "/metrics/custom-queries-with-target-databases.yaml"
		level                   = tests.Low
	)

	BeforeEach(func() {
		if testLevelEnv.Depth < int(level) {
			Skip("Test depth is lower than the amount requested for this test")
		}
	})

	// Cluster identifiers
	var namespace, metricsClusterName string

	// We define a few metrics in the tests. We check that all of them exist and
	// there are no errors during the collection.
	metricsRegexp := regexp.MustCompile(
		`(?m:^(` +
			`cnp_pg_postmaster_start_time_seconds \d+\.\d+|` + // wokeignore:rule=master
			`cnp_pg_wal_files_total \d+|` +
			`cnp_pg_database_size_bytes{datname="app"} [0-9e\+\.]+|` +
			`cnp_pg_replication_slots_inactive 0|` +
			`cnp_pg_stat_archiver_archived_count \d+|` +
			`cnp_pg_stat_archiver_failed_count \d+|` +
			`cnp_pg_locks_blocked_queries 0|` +
			`cnp_runonserver_match 42|` +
			`cnp_collector_last_collection_error 0)` +
			`$)`)

	JustAfterEach(func() {
		if CurrentSpecReport().Failed() {
			env.DumpClusterEnv(namespace, clusterMetricsFile,
				"out/"+CurrentSpecReport().LeafNodeText+".log")
		}
	})
	AfterEach(func() {
		err := env.DeleteNamespace(namespace)
		Expect(err).ToNot(HaveOccurred())
	})
	It("can gather metrics", func() {
		// Create the cluster namespace
		namespace = "cluster-metrics-e2e"
		metricsClusterName = "postgresql-metrics"
		err := env.CreateNamespace(namespace)
		Expect(err).ToNot(HaveOccurred())

		AssertCustomMetricsResourcesExist(namespace, fixturesDir+"/metrics/custom-queries.yaml", 2, 1)

		// Create the cluster
		AssertCreateCluster(namespace, metricsClusterName, clusterMetricsFile, env)

		By("collecting metrics on each pod", func() {
			podList, err := env.GetClusterPodList(namespace, metricsClusterName)
			Expect(err).ToNot(HaveOccurred())
			// Gather metrics in each pod
			for _, pod := range podList.Items {
				out, _, err := utils.Run(fmt.Sprintf(
					"kubectl exec -n %v %v -- %v",
					namespace,
					pod.GetName(),
					"sh -c 'curl -s 127.0.0.1:9187/metrics'"))
				matches := metricsRegexp.FindAllString(out, -1)
				Expect(matches, err).To(HaveLen(8), "Metric collection issues on %v.\nCollected metrics:\n%v", pod.GetName(), out)
			}
		})
	})

	It("can gather metrics with multiple target databases", func() {
		namespace = "metrics-target-databases-e2e"
		metricsClusterName = "metrics-target-databases"
		// Create the cluster namespace
		err := env.CreateNamespace(namespace)
		Expect(err).ToNot(HaveOccurred())
		AssertCustomMetricsResourcesExist(namespace, customQueriesSampleFile, 1, 1)
		// Create the cluster
		AssertCreateCluster(namespace, metricsClusterName, clusterMetricsDBFile, env)
		AssertCreationOfTestDataForTargetDB(namespace, metricsClusterName, targetDBOne, testTableName)
		AssertCreationOfTestDataForTargetDB(namespace, metricsClusterName, targetDBTwo, testTableName)
		AssertCreationOfTestDataForTargetDB(namespace, metricsClusterName, targetDBSecret, testTableName)
		AssertMetricsData(namespace, metricsClusterName, targetDBOne, targetDBTwo, targetDBSecret)
	})
})
