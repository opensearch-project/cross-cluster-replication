# Handbook for commands/API’s while using replication plugin

- [Handbook for commands/API’s while using replication plugin](#handbook-for-commandsapis-while-using-replication-plugin)
  - [Spin up two test clusters with Open Distro for Elasticsearch and install replication plugin](#spin-up-two-test-clusters-with-open-distro-for-elasticsearch-and-install-replication-plugin)
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

The example uses docker based setup to spin up the clusters with OpenDistro for Elasticsearch security plugin.

## Spin up two test clusters with Open Distro for Elasticsearch and install replication plugin

Clone the cross-cluster-replication repository and spin up the clusters from the [packaged example](https://github.com/opendistro-for-elasticsearch/cross-cluster-replication/tree/main/examples/sample).

```bash

# 1. Clone the cross-cluster-replication repo
git clone https://github.com/opendistro-for-elasticsearch/cross-cluster-replication.git 

# 2. Navigate to example directory
cd cross-cluster-replication/examples/sample

# 3. Build local image with replication plugin
docker build -t odfe-with-replication ./odfe-with-replication

# 4. Bring up 2 clusters with replication plugin installed
docker-compose up

# 5. Set variables for readability (in different terminal window/tab where you will run rest of the steps)
export LEADER=localhost:9200
export FOLLOWER=localhost:9201
export LEADER_IP=172.16.0.10
```

If you are setting this up on your own Open Distro for Elasticsearch 1.13 clusters, you can install cross-cluster-replication plugin on every node of leader and follower clusters as follows

```bash
sudo bin/elasticsearch-plugin install \
https://github.com/opendistro-for-elasticsearch/cross-cluster-replication/releases/download/experimental-opendistro-1.9.0.2/replication-7.8.0.zip
```

Further you need to ensure *user_injection* is enabled in the elasticsearch.yml as follows in addition to other [security configuration](https://opendistro.github.io/for-elasticsearch-docs/docs/security/configuration/) to use it with Open Distro for Elasticsearch security plugin.

```yml
opendistro_security.unsupported.inject_user.enabled: true
```

## Setup cross-cluster connectivity

Setup remote cluster connection from follower cluster to the leader cluster. The Open Distro for Elasticsearch security plugin ensures the cross-cluster traffic is encrypted.

```bash
curl -k -u admin:admin -XPUT "https://${FOLLOWER}/_cluster/settings?pretty" \
-H 'Content-Type: application/json' -d"
{
  \"persistent\": {
    \"cluster\": {
      \"remote\": {
        \"leader-cluster\": {
          \"seeds\": [ \"${LEADER_IP}:9300\" ]
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
indices:admin/opendistro/replication/index/start
indices:admin/opendistro/replication/index/stop
indices:data/read/opendistro/replication/file_metadata
indices:data/write/index
indices:data/write/opendistro/replication/changes
indices:data/write/replication
indices:monitor/stats

# Cluster Level Permissions

cluster:monitor/state
cluster:admin/snapshot/restore
cluster:admin/opendistro/replication/autofollow/update
```

### Required permissions on leader cluster

```
# Index Level Permissions
indices:data/read/opendistro/replication/file_chunk
indices:data/read/opendistro/replication/file_metadata
indices:admin/opendistro/replication/resources/release
indices:data/read/opendistro/replication/changes
indices:admin/mappings/get
indices:monitor/stats

# Cluster Level Permissions
cluster:monitor/state
```

### Populate it on test clusters

You can run the [example script](https://github.com/opendistro-for-elasticsearch/cross-cluster-replication/tree/main/examples/sample/setup_permissions.sh) to setup the required permissions on the test clusters.

```bash
sh ./setup_permissions.sh "${LEADER}"
sh ./setup_permissions.sh "${FOLLOWER}"
```

## Start replication

This API is used to initiate replication of an index from the leader cluster onto the follower cluster. The API is invoked on the follower and the desired remote index from leader cluster is provided as parameters.

**Signature**

```bash
# REQUEST

PUT localhost:{{foll_port}}/_opendistro/_replication/<index>/_start
Content-Type: application/json

{  "remote_cluster" : "leader-cluster",  "remote_index": "<index>"}


# RESPONSE

{
    "acknowledged": true
}

```

**Example**
```bash
curl -k -u testuser:testuser -XPUT \
"https://${FOLLOWER}/_opendistro/_replication/follower-01/_start?pretty" \
-H 'Content-type: application/json' \
-d'{"remote_cluster":"leader-cluster", "remote_index": "leader-01"}'

# Now there should be a ReadOnly index named 'follower-01' on the follower cluster that should continuously stay updated with changes to 'leader-01' index on the leader cluster.
```

## Stop replication

Replication can be stopped anytime by invocation of Stop API on the follower cluster. Stopping replication opens up the index for writes.

Note that the follower index is NOT deleted on stopping replication.

**Signature**

```bash
# REQUEST

POST localhost:{{foll_port}}/_opendistro/<index>/replicate/_stop
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
"https://${FOLLOWER}/_opendistro/_replication/follower-01/_stop?pretty" \
-H 'Content-type: application/json' -d'{}'

# You can confirm data isn't replicated any more by making modifications to
# leader-01 index on $LEADER cluster 
```

## Start replication via Autofollow pattern

AutoFollow API helps to automatically start replication on indices matching a pattern.

**Signature**

```bash
# REQUEST

POST localhost:{{foll_port}}/_opendistro/_replication/_autofollow
Content-Type: application/json

{"connection" : "<remote cluster connection name>",  "pattern": "<index pattern>", "name": "<name to identify autofollow task>"}

# RESPONSE

{
    "acknowledged": true
}
```

**Example**
```bash
curl -k -u testuser:testuser -XPOST \
"https://${FOLLOWER}/_opendistro/_replication/_autofollow?pretty" \
-H 'Content-type: application/json' \
-d'{"connection":"leader-cluster","pattern":"leader-*", "name":"my-replication"}
```

## Stop AutoFollow

AutoFollow can be removed by invoking API on the follower as follows. Invocation of the API is only to stop any new auto-follow activity and does NOT stop replication already initiated by the auto-follow.

**Signature**

```bash
DELETE localhost:{{foll_port}}/_opendistro/_replication/_autofollow
Content-Type: application/json

{
  "connection": "leader-cluster",
  "name": "test"
}
```

**Example**

```bash
curl -k -u testuser:testuser -XDELETE \
"https://${FOLLOWER}/_opendistro/_replication/_autofollow?pretty" \
-H 'Content-type: application/json' \
-d'{"connection":"leader-cluster", "name":"my-replication"}
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

