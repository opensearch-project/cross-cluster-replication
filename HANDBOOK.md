# Handbook for commands/API’s while using replication plugin

- [Handbook for commands/API’s while using replication plugin](#handbook-for-commandsapis-while-using-replication-plugin)
    - [Setup](#setup)
        - [Spin up sample clusters from the packaged example](#spin-up-sample-clusters-from-the-packaged-example)
        - [Setup for custom OpenSearch clusters](#setup-for-opensearch-clusters)
            - [Install cross-cluster-replication plugin](#install-cross-cluster-replication-plugin)
            - [Ensure all follower nodes can act as remote_cluster_client](#ensure-all-follower-nodes-can-act-as-remote_cluster_client)
            - [OpenSearch security plugin specific configuration](#opensearch-security-plugin-specific-configuration)
                - [Ensure user_injection is set to true](#ensure-user_injection-is-set-to-true)
                - [Ensure nodes_dn setting on the leader cluster allows connections from follower cluster](#ensure-nodes_dn-setting-on-the-leader-cluster-allows-connections-from-follower-cluster)
            - [Configure variables](#configure-variables)
    - [Setup cross-cluster connectivity](#setup-cross-cluster-connectivity)
    - [Security](#security)
        - [Required permissions on follower cluster](#required-permissions-on-follower-cluster)
        - [Required permissions on leader cluster](#required-permissions-on-leader-cluster)
        - [Populate it on test clusters](#populate-it-on-test-clusters)
    - [Start replication](#start-replication)
    - [Stop replication](#stop-replication)
    - [Start replication via Autofollow pattern](#start-replication-via-autofollow-pattern)
    - [Stop AutoFollow](#stop-autofollow)
    - [Check ongoing replication tasks](#check-ongoing-replication-tasks)
    - [Check completed and failed replication tasks](#check-completed-and-failed-replication-tasks)

This document helps you with sample commands/api’s to run, for the various scenarios supported by replication plugin.

## Setup

### Setup for custom OpenSearch clusters

#### Install cross-cluster-replication plugin

Install cross-cluster-replication plugin on all nodes of both leader and follower clusters

```bash
#TODO: Update repo
sudo bin/opensearch-plugin install \
https://github.com/opensearch-project/cross-cluster-replication/releases/download/v1.1.0.0/opensearch-cross-cluster-replication-1.1.0.0.zip
```
#### Ensure all follower nodes can act as remote_cluster_client

By default, all nodes can act as remote_cluster_client. If you have overridden `nodes.roles`, then ensure it also includes `remote_cluster_client` role as shown below

```yml
node.roles: [<other_roles>, remote_cluster_client]
```

#### OpenSearch security plugin specific configuration

If you have [disabled](https://opensearch.org/docs/security-plugin/configuration/disable) security plugin, you can skip this step. Ensure that either security plugin is **enabled on both clusters** OR **disabled on both clusters**.

##### Ensure user_injection is set to true

```yml
plugins.security.unsupported.inject_user.enabled: true
```

##### Ensure nodes_dn setting on the leader cluster allows connections from follower cluster

Ensure nodesdn dynamic API is enabled in opensearch.yml

```yml
plugins.security.nodes_dn_dynamic_config_enabled: true
```

Allow connections from follower cluster on the leader as follows

```bash
curl -k -u admin:admin -XPUT "https://${LEADER}/_plugins/_security/api/nodesdn/follower" \
-H 'Content-type: application/json' \
-d'{"nodes_dn": ["CN=follower.example.com"]}'
```

#### Configure variables

```bash
export LEADER=<leader-endpoint>
export FOLLOWER=<follower-endpoint>
export LEADER_NODES_IPS='["<node1:9300>", "<node2:9300>"]'
```

## Setup cross-cluster connectivity

Setup remote cluster connection from follower cluster to the leader cluster. The OpenSearch security plugin ensures the cross-cluster traffic is encrypted.

```bash
curl -k -u admin:admin -XPUT "https://${FOLLOWER}/_cluster/settings?pretty" \
-H 'Content-Type: application/json' -d"
{
  \"persistent\": {
    \"cluster\": {
      \"remote\": {
        \"leader-cluster\": {
          \"seeds\": ${LEADER_NODES_IPS}
        }
      }
    }
  }
}
"
```

## Security

### Required permissions on follower cluster

```
# Index Level Permissions

indices:admin/close
indices:admin/close[s]
indices:admin/create
indices:admin/mapping/put
indices:admin/open
indices:admin/plugins/replication/index/start
indices:admin/plugins/replication/index/stop
indices:data/read/plugins/replication/file_metadata
indices:data/write/index
indices:data/write/plugins/replication/changes
indices:data/write/replication
indices:monitor/stats

# Cluster Level Permissions

cluster:monitor/state
cluster:admin/snapshot/restore
cluster:admin/plugins/replication/autofollow/update
```

### Required permissions on leader cluster

```
# Index Level Permissions
indices:data/read/plugins/replication/file_chunk
indices:data/read/plugins/replication/file_metadata
indices:admin/plugins/replication/resources/release
indices:data/read/plugins/replication/changes
indices:admin/mappings/get
indices:monitor/stats

# Cluster Level Permissions
cluster:monitor/state
```

### Populate it on test clusters

You can run the [example script](https://github.com/opensearch-project/cross-cluster-replication/tree/main/examples/sample/setup_permissions.sh) to setup the required permissions on the test clusters.

```bash
sh ./setup_permissions.sh "${LEADER}"
sh ./setup_permissions.sh "${FOLLOWER}"
```

## Start replication

This API is used to initiate replication of an index from the leader cluster onto the follower cluster. The API is invoked on the follower and the desired remote index from leader cluster is provided as parameters.

**Signature**

```bash
# REQUEST

PUT localhost:{{foll_port}}/_plugins/_replication/<index>/_start
Content-Type: application/json

{  "leader_alias" : "leader-cluster",  "leader_index": "<index>"}


# RESPONSE

{
    "acknowledged": true
}

```

**Example** 
```bash
curl -k -u testuser:testuser -XPUT \
"https://${FOLLOWER}/_plugins/_replication/follower-01/_start?pretty" \
-H 'Content-type: application/json' \
-d'{"leader_alias":"leader-cluster", "leader_index": "leader-01"}'

# Make sure to create an index with name leader-01 on the leader before starting replication on top of this index.
# Now there should be a ReadOnly index named 'follower-01' on the follower cluster that should continuously stay updated with changes to 'leader-01' index on the leader cluster.
```

## Stop replication

Replication can be stopped anytime by invocation of Stop API on the follower cluster. Stopping replication opens up the index for writes.

Note that the follower index is NOT deleted on stopping replication.

**Signature**

```bash
# REQUEST

POST localhost:{{foll_port}}/_plugins/<index>/replicate/_stop
Content-Type: application/json
{}

# RESPONSE

{
    "acknowledged": true
}
```

**Example**
```bash
curl -k -u testuser:testuser -XPOST \
"https://${FOLLOWER}/_plugins/_replication/follower-01/_stop?pretty" \
-H 'Content-type: application/json' -d'{}'

# You can confirm data isn't replicated any more by making modifications to
# leader-01 index on $LEADER cluster 
```

## Start replication via Autofollow pattern

AutoFollow API helps to automatically start replication on indices matching a pattern.

**Signature**

```bash
# REQUEST

POST localhost:{{foll_port}}/_plugins/_replication/_autofollow
Content-Type: application/json

{"leader_alias" : "<remote cluster connection name>",  "pattern": "<index pattern>", "name": "<name to identify autofollow task>"}

# RESPONSE

{
    "acknowledged": true
}
```

**Example**
```bash
curl -k -u testuser:testuser -XPOST \
"https://${FOLLOWER}/_plugins/_replication/_autofollow?pretty" \
-H 'Content-type: application/json' \
-d'{"leader_alias":"leader-cluster","pattern":"leader-*", "name":"my-replication"}'
```

## Stop AutoFollow

AutoFollow can be removed by invoking API on the follower as follows. Invocation of the API is only to stop any new auto-follow activity and does NOT stop replication already initiated by the auto-follow.

**Signature**

```bash
DELETE localhost:{{foll_port}}/_plugins/_replication/_autofollow
Content-Type: application/json

{
  "leader_alias": "leader-cluster",
  "name": "test"
}
```

**Example**

```bash
curl -k -u testuser:testuser -XDELETE \
"https://${FOLLOWER}/_plugins/_replication/_autofollow?pretty" \
-H 'Content-type: application/json' \
-d'{"leader_alias":"leader-cluster", "name":"my-replication"}'
```

## Check ongoing replication tasks

Until a status API is added, you can check ongoing replication via the tasks API.

```bash
curl -k -u admin:admin -XGET "https://${FOLLOWER}/_cat/tasks?v&actions=*replication*&detailed" 

action                                task_id                    parent_task_id type       start_time    timestamp running_time ip          node           description
cluster:indices/admin/replication[c]  ltIs84uTRLOYnOr8Giu0VQ:118 cluster:1      persistent 1613651479438 12:31:19  17.7s        172.18.0.20 odfe-follower1 replication:leader-cluster:[leader-01/g3d9ddwZQHeuvEGEouQxDQ] -> follower-01
cluster:indices/shards/replication[c] ltIs84uTRLOYnOr8Giu0VQ:147 cluster:2      persistent 1613651480095 12:31:20  17.1s        172.18.0.20 odfe-follower1 replication:leader-cluster:[leader-01][0] -> [follower-01][0]
```

## Check completed and failed replication tasks

Failed and completed tasks are captured in '.tasks' index. For failed tasks, the failure reason is also captured. You can look for the replicated index name to identify the tasks corresponding to the index.

```bash
curl -k -u admin:admin -XGET "https://${FOLLOWER}/.tasks/_search?pretty"

{
  "took" : 5,
  "timed_out" : false,
  "_shards" : {
    "total" : 1,
    "successful" : 1,
    "skipped" : 0,
    "failed" : 0
  },
  "hits" : {
    "total" : {
      "value" : 2,
      "relation" : "eq"
    },
    "max_score" : 1.0,
    "hits" : [
      {
        "_index" : ".tasks",
        "_type" : "task",
        "_id" : "ltIs84uTRLOYnOr8Giu0VQ:118",
        "_score" : 1.0,
        "_source" : {
          "completed" : true,
          "task" : {
            "node" : "ltIs84uTRLOYnOr8Giu0VQ",
            "id" : 118,
            "type" : "persistent",
            "action" : "cluster:indices/admin/replication[c]",
            "status" : {
              "state" : "STARTED"
            },
            "description" : "replication:leader-cluster:[leader-01/g3d9ddwZQHeuvEGEouQxDQ] -> follower-01",
            "start_time_in_millis" : 1613651479438,
            "running_time_in_nanos" : 79627167100,
            "cancellable" : true,
            "parent_task_id" : "cluster:1",
            "headers" : { }
          },
          "response" : {
            "index_task_status" : "COMPLETED",
            "following_tasks" : {
              "state" : "MONITORING"
            }
          }
        }
      },
      {
        "_index" : ".tasks",
        "_type" : "task",
        "_id" : "ltIs84uTRLOYnOr8Giu0VQ:147",
        "_score" : 1.0,
        "_source" : {
          "completed" : true,
          "task" : {
            "node" : "ltIs84uTRLOYnOr8Giu0VQ",
            "id" : 147,
            "type" : "persistent",
            "action" : "cluster:indices/shards/replication[c]",
            "status" : {
              "state" : "STARTED"
            },
            "description" : "replication:leader-cluster:[leader-01][0] -> [follower-01][0]",
            "start_time_in_millis" : 1613651480095,
            "running_time_in_nanos" : 78969894200,
            "cancellable" : true,
            "parent_task_id" : "cluster:2",
            "headers" : { }
          },
          "response" : {
            "status" : "COMPLETED"
          }
        }
      }
    ]
  }
}
```

