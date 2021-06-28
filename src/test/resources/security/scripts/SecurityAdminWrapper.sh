#   Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
#   Licensed under the Apache License, Version 2.0 (the "License").
#   You may not use this file except in compliance with the License.
#   A copy of the License is located at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   or in the "license" file accompanying this file. This file is distributed
#   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
#   express or implied. See the License for the specific language governing
#   permissions and limitations under the License.

#!/bin/bash

BUILD_DIR=$1
LEADER_CONFIG_DIR=$BUILD_DIR/testclusters/leaderCluster-0/config
LEADER_PLUGIN_DIR=$BUILD_DIR/testclusters/leaderCluster-0/distro/1.0.0-INTEG_TEST/plugins/opensearch-security/
FOLLOWER_CONFIG_DIR=$BUILD_DIR/testclusters/followCluster-0/config
FOLLOWER_PLUGIN_DIR=$BUILD_DIR/testclusters/followCluster-0/distro/1.0.0-INTEG_TEST/plugins/opensearch-security/

"$LEADER_PLUGIN_DIR/tools/securityadmin.sh" -p 9300 \
-cd "$LEADER_PLUGIN_DIR/securityconfig" \
-icl -key "$LEADER_CONFIG_DIR/kirk-key.pem" \
-cert "$LEADER_CONFIG_DIR/kirk.pem" \
-cacert "$LEADER_CONFIG_DIR/root-ca.pem" -nhnv

"$FOLLOWER_PLUGIN_DIR/tools/securityadmin.sh" -p 9301 \
-cd "$FOLLOWER_PLUGIN_DIR/securityconfig" \
-icl -key "$FOLLOWER_CONFIG_DIR/kirk-key.pem" \
-cert "$FOLLOWER_CONFIG_DIR/kirk.pem" \
-cacert "$FOLLOWER_CONFIG_DIR/root-ca.pem" -nhnv
