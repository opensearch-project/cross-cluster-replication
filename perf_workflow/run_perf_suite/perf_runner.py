# Copyright OpenSearch Contributors
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.

import traceback

from ccr_perf_test import CcrPerfTest

class PerfRunner:
    @classmethod
    def run(cls, test: CcrPerfTest):
        try:
            test.run()
        except Exception:
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


class PerfTestResult:
    def __init__(self, test_num, test_description, success, execution_id, failure_reason):
        self.test_num = test_num
        self.test_description = test_description
        self.success = success
        self.execution_id = execution_id
        self.failure_reason = failure_reason
