#
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
#
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.
#

#!/bin/bash

BUILD_DIR=$1
LEADER_CONFIG_DIR=$BUILD_DIR/testclusters/leaderCluster-0/config
LEADER_PLUGIN_DIR=$BUILD_DIR/testclusters/leaderCluster-0/distro/1.2.3-INTEG_TEST/plugins/opensearch-security/
FOLLOWER_CONFIG_DIR=$BUILD_DIR/testclusters/followCluster-0/config
FOLLOWER_PLUGIN_DIR=$BUILD_DIR/testclusters/followCluster-0/distro/1.2.3-INTEG_TEST/plugins/opensearch-security/

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
