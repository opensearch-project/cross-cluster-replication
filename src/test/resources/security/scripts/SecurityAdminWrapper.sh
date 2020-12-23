#!/bin/bash

BUILD_DIR=$1
LEADER_CONFIG_DIR=$BUILD_DIR/testclusters/leaderCluster-0/config
LEADER_PLUGIN_DIR=$BUILD_DIR/testclusters/leaderCluster-0/distro/7.10.2-INTEG_TEST/plugins/opendistro_security/
FOLLOWER_CONFIG_DIR=$BUILD_DIR/testclusters/followCluster-0/config
FOLLOWER_PLUGIN_DIR=$BUILD_DIR/testclusters/followCluster-0/distro/7.10.2-INTEG_TEST/plugins/opendistro_security/

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
