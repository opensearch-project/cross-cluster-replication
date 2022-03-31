# Copyright OpenSearch Contributors
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.

import glob
import json
import logging
import os
import random
import shutil
import string
import sys

import requests
from git.git_repository import GitRepository
from requests.auth import HTTPBasicAuth
from system.temporary_directory import TemporaryDirectory
from system.working_directory import WorkingDirectory
from test_workflow.perf_test.perf_multi_node_cluster import PerfMultiNodeCluster
from test_workflow.perf_test.perf_single_node_cluster import PerfSingleNodeCluster
from test_workflow.perf_test.perf_test_cluster_config import PerfTestClusterConfig
from test_workflow.perf_test.perf_test_suite import PerfTestSuite


class CcrPerfTest:
    CONNECTION_ALIAS = "leaderAlias"
    CCR_SCENARIO = "CROSS_CLUSTER_REPLICATION"
    TEST_OWNER = "cross-cluster-replication"
    MAX_ALLOWED_REPLICATION_LAG_SECONDS = 60
    # TODO: Make credentials configurable/randomized
    CREDS = HTTPBasicAuth("admin", "admin")

    def __init__(
        self, test_config: dict, config, bundle_manifest, tests_result_dir, security
    ):
        self.description = test_config["Description"]
        self.test_id = "ccr-" + "".join(
            random.choices(string.ascii_uppercase + string.digits, k=5)
        )
        self.bundle_manifest = bundle_manifest
        self.test_config = test_config
        self.config = config
        self.security = security
        self.tests_result_dir = tests_result_dir
        self.test_num = self.test_config.get("Number", "0")
        self.execution_id = None
        self.test_succeeded = False
        self.failure_reason = None

    def get_infra_repo_url(self):
        if "GITHUB_TOKEN" in os.environ:
            return "https://${GITHUB_TOKEN}@github.com/opensearch-project/opensearch-infra.git"
        else:
            return "https://github.com/opensearch-project/opensearch-infra.git"

    def run(self):
        self.setup_logging()
        if self.tests_result_dir is not None:
            tests_dir = self.tests_result_dir + "/" + str(self.test_num)
        else:
            tests_dir = os.path.join(
                str(os.getcwd()).split("cross-cluster-replication")[0]
                + "cross-cluster-replication/build/test-results/perf-test/"
                + str(self.test_num)
            )
            if os.path.exists(tests_dir):
                shutil.rmtree(tests_dir)
        os.makedirs(tests_dir, exist_ok=True)
        with TemporaryDirectory(keep=False, chdir=True) as work_dir:
            current_workspace = os.path.join(work_dir.name, "infra")
            with GitRepository(self.get_infra_repo_url(), "main", current_workspace):
                with WorkingDirectory(current_workspace):
                    cluster_config = PerfTestClusterConfig(
                        self.security,
                        self.test_config.get("DataNodes", 1),
                        self.test_config.get("MasterNodes", 0),
                    )
                    # TODO: Add support for configurable instance type. Default is m5.2xlarge
                    with self.create_cluster(
                        self.bundle_manifest,
                        self.config,
                        self.test_id + "-l",
                        cluster_config,
                        current_workspace,
                    ) as leaderCluster, self.create_cluster(
                        self.bundle_manifest,
                        self.config,
                        self.test_id + "-f",
                        cluster_config,
                        current_workspace,
                    ) as followerCluster:
                        # Wait for cluster to be up.
                        leaderCluster.wait_for_processing(tries=5, delay=30, backoff=2)
                        followerCluster.wait_for_processing(
                            tries=5, delay=30, backoff=2
                        )
                        self.setup_replication(
                            leaderCluster, followerCluster, self.test_config["Workload"]
                        )
                        # Setup args for Perf Suite.
                        target_hosts = {
                            "default": [leaderCluster.endpoint_with_port],
                            "follower": [followerCluster.endpoint_with_port],
                        }
                        workload_options = self.workload_options(
                            leaderCluster, followerCluster
                        )
                        args = PerfSuiteArgs(
                            self.test_config["Workload"], workload_options, 1, 0
                        )
                        try:
                            logging.info(
                                f"Starting test {self.description}, Leader: {leaderCluster.endpoint}, Follower: {followerCluster.endpoint}"
                            )
                            # Execute the suite.
                            perf_test_suite = PerfTestSuite(
                                self.bundle_manifest,
                                target_hosts,
                                self.security,
                                current_workspace,
                                tests_dir,
                                args,
                                self.TEST_OWNER,
                                self.CCR_SCENARIO,
                            )
                            perf_test_suite.execute()

                            # Get test results and validate the metrics.
                            result_json_file_path = max(
                                glob.glob(tests_dir + "/*.json"), key=os.path.getmtime
                            )
                            with open(result_json_file_path, "r") as file:
                                self.verify_result(json.load(file), leaderCluster, followerCluster)
                        except Exception as e:
                            self.failure_reason = str(e)
                            logging.error(f"Test failed for {self.description}: {e}")

    def create_cluster(self, bundle_manifest, config, stack_name, cluster_config, current_workspace):
        if cluster_config.is_single_node_cluster():
            return PerfSingleNodeCluster.create(bundle_manifest, config, stack_name, cluster_config, current_workspace)
        else:
            return PerfMultiNodeCluster.create(bundle_manifest, config, stack_name, cluster_config, current_workspace)

    def verify_result(self, result, leaderCluster, followerCluster):
        self.verify_checkpoints(followerCluster)
        self.verify_doc_count(leaderCluster, followerCluster)
        self.execution_id = result["testExecutionId"]
        for telemetry_result in result["testResults"]["customTelemetryData"]["overall"]:
            if (
                telemetry_result["telemetryDevice"] == "ccr-stats"
                and telemetry_result["metric"] == "replication_lag"
            ):
                # TODO: Add support for comparison with benchmark run
                assert (
                    telemetry_result["p100"] > 0
                ), "Replication lag should be non-zero"
                assert (
                    telemetry_result["p100"] < self.MAX_ALLOWED_REPLICATION_LAG_SECONDS
                ), "Replication lag p100 was greater than a minute"
                self.test_succeeded = True
                logging.info(f"Test succeeded for {self.description}")

    def verify_doc_count(self, leaderCluster, followerCluster):
        index_name = self.test_config["Workload"]
        path = f"/{index_name}/_count?format=json"

        leader_url = "".join([leaderCluster.endpoint_with_port, path])
        leader_resp = requests.get(url=leader_url, auth=self.CREDS, verify=False)
        leader_doc_count = leader_resp.json()['count']

        follower_url = "".join([followerCluster.endpoint_with_port, path])
        follower_resp = requests.get(url=follower_url, auth=self.CREDS, verify=False)
        follower_doc_count = follower_resp.json()['count']

        assert leader_doc_count == follower_doc_count, "Doc count on leader doesnt match with follower"

    def verify_checkpoints(self, followerCluster):
        index_name = self.test_config["Workload"]
        path = f"/_plugins/_replication/{index_name}/_status?pretty"

        url = "".join([followerCluster.endpoint_with_port, path])
        follower_resp = requests.get(url=url, auth=self.CREDS, verify=False)
        leader_checkpoint = follower_resp.json()['syncing_details']['leader_checkpoint']
        follower_checkpoint = follower_resp.json()['syncing_details']['follower_checkpoint']

        assert leader_checkpoint == follower_checkpoint, "Follower is not at same checkpoint as leader"

    def setup_logging(self):
        root = logging.getLogger()
        root.setLevel(logging.INFO)
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.INFO)
        root.addHandler(handler)

    def workload_options(self, leaderCluster, followerCluster):
        telemetry_options = {
            "ccr-stats-sample-interval": 1,
            "ccr-stats-indices": {"follower": [self.test_config["Workload"]]},
        }
        workload_options = {
            "telemetry-params": json.dumps(telemetry_options),
            "workload-params": "number_of_shards:{},number_of_replicas:{}".format(
                self.test_config.get("Shards", 1), self.test_config.get("Replica", 0)
            ),
        }
        if self.security:
            # TODO: Move to configurable auth username and pwd.
            client_options = {
                "default": {
                    "use_ssl": True,
                    "basic_auth_user": "admin",
                    "basic_auth_password": "admin",
                    "verify_certs": False,
                },
                "follower": {
                    "use_ssl": True,
                    "basic_auth_user": "admin",
                    "basic_auth_password": "admin",
                    "verify_certs": False,
                },
            }
            workload_options["client-options"] = json.dumps(client_options)
        return json.dumps(workload_options)

    def setup_replication(self, leaderCluster, followerCluster, workload_name):
        self.setup_seed_nodes(leaderCluster, followerCluster)
        self.setup_autofollow(followerCluster, workload_name)

    def setup_autofollow(self, followerCluster, workload_name):
        url = "".join(
            [followerCluster.endpoint_with_port, "/_plugins/_replication/_autofollow"]
        )
        body = {
            "leader_alias": self.CONNECTION_ALIAS,
            "name": "all",
            "pattern": workload_name,
        }
        if self.security:
            body["use_roles"] = {
                "leader_cluster_role": "all_access",
                "follower_cluster_role": "all_access",
            }

        headers = {"Content-Type": "application/json"}
        resp = requests.post(
            url=url,
            json=body,
            headers=headers,
            auth=self.CREDS,
            verify=False,
        )
        if resp.status_code != 200:
            raise RuntimeError(
                "Unable to update trigger autofollow replication: {}".format(
                    resp.reason
                )
            )

    def setup_seed_nodes(self, leaderCluster, followerCluster):
        requests.packages.urllib3.disable_warnings()
        # Fetch seed nodes from leader cluster
        url = "".join([leaderCluster.endpoint_with_port, "/_cat/nodes?format=json"])
        resp = requests.get(url=url, auth=self.CREDS, verify=False)
        data = resp.json()
        seed_nodes = []
        for node in data:
            if node["node.role"] == "dmr" or node["node.role"] == "dir":
                seed_nodes.append("".join([node["ip"], ":9300"]))
        if len(seed_nodes) == 0:
            raise RuntimeError(
                "Unable to get seed nodes from leader: {}".format(resp.data)
            )

        # Configure seed nodes on follower
        headers = {"Content-Type": "application/json"}
        url = "".join([followerCluster.endpoint_with_port, "/_cluster/settings"])
        request_body = {
            "persistent": {
                "cluster": {"remote": {self.CONNECTION_ALIAS: {"seeds": seed_nodes}}}
            }
        }
        resp = requests.put(
            url=url,
            json=request_body,
            headers=headers,
            auth=self.CREDS,
            verify=False,
        )
        if resp.status_code != 200:
            raise RuntimeError("Unable to update seed nodes: {}".format(resp.reason))


class PerfSuiteArgs:
    def __init__(self, workload, workload_options, test_iters, warmup_iters):
        self.workload = workload
        self.workload_options = workload_options
        self.test_iters = test_iters
        self.warmup_iters = warmup_iters
