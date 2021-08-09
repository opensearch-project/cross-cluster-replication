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

admin='admin:admin'
testuser="testuser"

if [ -z "$1" ]; then
   echo "Please provide endpoint hostname:port"
   exit 1
fi

endpoint="$1"

echo "Creating user '${testuser}' and associating with replication_backend role"
curl -ks -u $admin -XPUT "https://${endpoint}/_opendistro/_security/api/internalusers/${testuser}?pretty" -H 'Content-Type: application/json' -d'
{
  "password": "testuser",
  "backend_roles": ["replication_backend"]
}
'
echo
echo "-----"

echo "Creating actiongroup 'follower-replication-action-group' and associating index level permissions to start/stop replication"
curl -ks -u $admin -XPUT "https://${endpoint}/_opendistro/_security/api/actiongroups/follower-replication-action-group" -H 'Content-Type: application/json' -d'
{
  "allowed_actions": [
    "indices:admin/close",
    "indices:admin/close[s]",
    "indices:admin/create",
    "indices:admin/mapping/put",
    "indices:admin/open",
    "indices:admin/plugins/replication/index/start",
    "indices:admin/plugins/replication/index/stop",
    "indices:data/read/plugins/replication/file_metadata",
    "indices:data/write/index",
    "indices:data/write/plugins/replication/changes",
    "indices:data/write/replication",
    "indices:monitor/stats"
  ]
}
'
echo
echo "-----"

echo "Creating actiongroup 'follower-replication-cluster-action-group' and associating cluster level permissions for replication."
curl -ks -u $admin -XPUT "https://${endpoint}/_opendistro/_security/api/actiongroups/follower-replication-cluster-action-group" -H 'Content-Type: application/json' -d'
{
  "allowed_actions": [
    "cluster:monitor/state",
    "cluster:admin/snapshot/restore",
    "cluster:admin/plugins/replication/autofollow/update"
  ]
}
'
echo
echo "-----"


echo "Creating actiongroup 'leader-replication-action-group' and associating index level permissions for replication."
curl -ks -u $admin -XPUT "https://${endpoint}/_opendistro/_security/api/actiongroups/leader-replication-action-group" -H 'Content-Type: application/json' -d'
{
  "allowed_actions": [
    "indices:data/read/plugins/replication/file_chunk",
    "indices:data/read/plugins/replication/file_metadata",
    "indices:admin/plugins/replication/resources/release",
    "indices:data/read/plugins/replication/changes",
    "indices:admin/mappings/get",
    "indices:monitor/stats"
  ]
}
'
echo
echo "-----"


echo "Creating actiongroup 'leader-replication-cluster-action-group' and associating cluster level permissions for replication."
curl -ks -u $admin -XPUT "https://${endpoint}/_opendistro/_security/api/actiongroups/leader-replication-cluster-action-group" -H 'Content-Type: application/json' -d'
{
  "allowed_actions": [
    "cluster:monitor/state"
  ]
}
'
echo
echo "-----"

echo "Creating role 'replication_follower_role' and associating for index pattern '*' and actiongroups ['follower-replication-action-group', 'follower-replication-cluster-action-group']"
curl -ks -u $admin -XPUT "https://${endpoint}/_opendistro/_security/api/roles/replication_follower_role" -H 'Content-Type: application/json' -d'
{
  "cluster_permissions": [
    "follower-replication-cluster-action-group"
  ],
  "index_permissions": [{
    "index_patterns": [
      "*"
    ],
    "allowed_actions": [
      "follower-replication-action-group"
    ]
  }]
}
'
echo
echo "-----"

echo "Creating role 'replication_leader_role' and associating for index pattern '*' and actiongroup ['leader-replication-action-group', 'leader-replication-cluster-action-group']"
curl -ks -u $admin -XPUT "https://${endpoint}/_opendistro/_security/api/roles/replication_leader_role" -H 'Content-Type: application/json' -d'
{
  "cluster_permissions": [
    "leader-replication-cluster-action-group"
  ],
  "index_permissions": [{
    "index_patterns": [
      "*"
    ],
    "allowed_actions": [
      "leader-replication-action-group"
    ]
  }]
}
'
echo
echo "-----"


echo "Mapping role 'replication_follower_role' to 'replication_backend' backend role"
curl -ks -u $admin -XPUT "https://${endpoint}/_opendistro/_security/api/rolesmapping/replication_follower_role?pretty" -H 'Content-Type: application/json' -d'
{
  "backend_roles" : [
    "replication_backend"
  ]
}
'
echo
echo "-----"

echo "Mapping role 'replication_leader_role' to 'replication_backend' backend role"
curl -ks -u $admin -XPUT "https://${endpoint}/_opendistro/_security/api/rolesmapping/replication_leader_role?pretty" -H 'Content-Type: application/json' -d'
{
  "backend_roles" : [
    "replication_backend"
  ]
}
'
echo
echo "-----"

echo

