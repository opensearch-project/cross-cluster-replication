# Copyright OpenSearch Contributors
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
import logging
import sys
import traceback

from ccr_perf_test import CcrPerfTest

class PerfRunner:
    @classmethod
    def run(cls, test: CcrPerfTest):
        cls.setup_logging()
        try:
            test.run()
        except Exception as e:
            logging.error(f"Test failed for {test.description}: {e}")
            test.failure_reason = e
            traceback.print_exc()
            # Not raising the exception as we're already logging the failure in the test result.
            pass
        finally:
            return PerfTestResult(
                test.test_num,
                test.description,
                test.test_succeeded,
                test.execution_id,
                test.failure_reason,
            )

    @classmethod
    def setup_logging(cls):
        root = logging.getLogger()
        root.setLevel(logging.INFO)
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.INFO)
        root.addHandler(handler)

class PerfTestResult:
    def __init__(self, test_num, test_description, success, execution_id, failure_reason):
        self.test_num = test_num
        self.test_description = test_description
        self.success = success
        self.execution_id = execution_id
        self.failure_reason = failure_reason
