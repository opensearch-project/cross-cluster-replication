# Copyright OpenSearch Contributors
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.

import argparse
import logging
import sys
from multiprocessing import Pool

import yaml
from ccr_perf_test import CcrPerfTest
from manifests.bundle_manifest import BundleManifest
from perf_runner import PerfRunner

def main():
    """
    Entry point for CCR Performance Test with bundle manifest, config file containing the required arguments for running
    opensearch-benchmark test and the stack name for the cluster.
    """
    parser = argparse.ArgumentParser(description="Test CCR on OpenSearch Bundle")
    parser.add_argument(
        "--bundle-manifest",
        type=argparse.FileType("r"),
        help="Bundle Manifest file.",
        required=True,
    )
    parser.add_argument(
        "--config",
        type=argparse.FileType("r"),
        help="Config file with account where the clusters will be created.",
        required=True,
    )
    parser.add_argument(
        "--test-result-dir", help="Path for storing test results.", required=False
    )
    parser.add_argument(
        "--without-security",
        help="Force the security of the cluster to be disabled.",
        required=False,
        dest="insecure",
        action="store_true",
        default=False,
    )
    args = parser.parse_args()

    # Setup logging
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)
    root.addHandler(handler)

    test_suite_config = yaml.safe_load(open("run_perf_suite/test_suite.yaml", "r"))
    config = yaml.safe_load(args.config)
    if "PublicIp" not in config["Constants"]:
        config["Constants"]["PublicIp"] = "enable"

    bundle_manifest = BundleManifest.from_file(args.bundle_manifest)
    security = "security" in bundle_manifest.components and not args.insecure

    test_suite = []
    for ccr_test_config in test_suite_config:
        test = CcrPerfTest(
            ccr_test_config, config, bundle_manifest, args.test_result_dir, security
        )
        test_suite.append(test)

    results = []
    failed_test = []
    try:
        with Pool(len(test_suite)) as p:
            results = p.map(PerfRunner.run, test_suite)
        failed_test = list(filter((lambda t: not t.success), results))
        assert len(failed_test) == 0, "One or more tests failed!!"
    finally:
        logging.info("TEST SUMMARY:")
        for result in results:
            logging.info(
                f"{result.test_num}: {result.test_description}, Success:{result.success}, Execution ID: {result.execution_id}"
            )

        if len(failed_test) != 0:
            logging.info("FAILURES:")
            for test in failed_test:
                logging.info(
                    f"{test.test_num} : {test.test_description}, Execution ID: {test.execution_id}, Failure: {test.failure_reason}"
                )


if __name__ == "__main__":
    sys.exit(main())
