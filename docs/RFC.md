# Request for Comments for Cross-Cluster Replication
- [Request for Comments for Cross-Cluster Replication](#request-for-comments-for-cross-cluster-replication)
    - [Problem Statement](#problem-statement)
    - [Proposal](#proposal)
    - [Architecture](#architecture)
        - [Overview](#overview)
        - [Details](#details)
    - [APIs](#apis)
        - [Configure Remote Connection on Follower](#configure-remote-connection-on-follower)
        - [Start Replication](#start-replication)
        - [Stop Replication](#stop-replication)
        - [Start replication via Autofollow pattern](#start-replication-via-autofollow-pattern)
        - [Remove AutoFollow](#remove-autofollow)
    - [Component Level Design](#component-level-design)
        - [Restore Leader Service](#restore-leader-service)
    - [Replication Tasks](#replication-tasks)
        - [Index level replication tasks](#index-level-replication-tasks)
            - [Index replication task](#index-replication-task)
            - [Shard replication task](#shard-replication-task)
        - [Cluster level replication tasks](#cluster-level-replication-tasks)
        - [Autofollow replication task](#autofollow-replication-task)
    - [Security Considerations](#security-considerations)
        - [Permission model](#permission-model)
            - [Access to Replication REST APIs](#access-to-replication-rest-apis)
            - [Access to Transport actions on leader/follower](#access-to-transport-actions-on-leaderfollower)
            - [Authorization for the transport actions](#authorization-for-the-transport-actions)
    - [Feature sets being actively worked on](#feature-sets-being-actively-worked-on)
    - [Comments/feedback](#commentsfeedback)
## Problem Statement 

Today OpenSearch users don’t have a native solution to replicate data across multiple clusters. Replication allows users to achieve the following

- **High Availability (HA)**: Cross-cluster replication ensures uninterrupted service availability with the ability to failover to an alternate cluster in case of failure or outages on the primary cluster.
- **Reduced Latency**: Replicating data to a cluster that is closer to the application users minimizes the query latency. 
- **Horizontal scalability**: Splitting a query heavy workload across multiple replica clusters improves application availability.
- **Aggregated reports**: Enterprise customers can roll up reports continually from smaller clusters belonging to different lines of business into a central cluster for consolidated reports, dashboards or visualizations.


## Proposal

Cross-cluster replication project is intended to incubate and deliver a replication feature for OpenSearch.  Cross-cluster replication continuously replicates indices from one OpenSearch cluster to another. Basic feature set proposed are:

1. APIs to start and stop replication for indices with support for auto follow patterns.
2. Implement active-passive mode of replication at index level that involves:
    1. Bootstrap a replica index from leader cluster. 
    2. Once bootstrapped, continuously replicate data changes from leader to follower. 
    3. Replicate index metadata automatically from leader to follower. This includes creating new indices as well as replicating metadata changes to replicated indices, mappings and other internal bookkeeping information. 
    4. The delay between an update on the leader cluster becoming visible on the follower is under a few seconds during normal operation. 


## Architecture

### Overview

The replication machinery is implemented as an OpenSearch plugin that exposes APIs to control replication, spawns background persistent tasks to asynchronously replicate indices and utilizes snapshot repository abstraction to facilitate bootstrap. Replication relies on [cross cluster connection setup](https://github.com/opensearch-project/cross-cluster-replication/blob/main/HANDBOOK.md#setup-cross-cluster-connectivity) from the follower cluster to the leader cluster for connectivity. Once replication is initiated on an index, a background persistent task per primary shard on the follower cluster continuously polls corresponding shards from the leader index and applies the changes on to the corresponding follower shards.

![Architecture](/docs/images/rfc0.png?raw=true "Architecture")

### Details

The Start Replication API performs basic validations (such as existence checks on the remote cluster & index) and then spawns a persistent background task named `IndexReplicationTask` in the follower cluster to coordinate the replication process.  This task does not replicate any data directly, but rather is responsible for initiating subtasks and monitoring the overall replication process.  Hence, it can run on any node on the cluster (including Cluster manager nodes) and we chose the node with the least number of tasks at the time.  Each step in the workflow is checkpointed so that it can be resumed safely if interrupted.


![Details](/docs/images/rfc1.png?raw=true "Details")

1. It adds a **retention lease** to the shards on the leader cluster which ensures that the existing soft deletes are not merged away and that the translog is not truncated.  This preserves the history of operations that occurred in the leader. 
2. **Bootstrap phase**: It then initiates copying over the current index contents from the leader cluster using the snapshot restore machinery in ES. The replication plugin exposes the leader cluster as an internal snapshot repository in the follower cluster, translating requests to the repository to requests to the leader cluster. The snapshot recovery process takes care of creating the index on the follower with the same settings and configuration as on the leader. If the recovery fails then the `IndexReplicationTask` transitions to the FAILED state. 
3. **Replication Phase**: Once the bootstrap phase is completed `IndexReplicationTask` spawns a persistent background `ShardReplicationTask` sub task for each shard on the follower cluster.  This task starts out running on the same node which hosts that primary shard.  The `ShardReplicationTask` starts one or more reader and writer threads that perform the actual replication:
    1. **Replication Reader**: The reader process initiates a long poll to a leader shard copy to fetch a batch of translog operations to be replayed in sequence.  If there are no operations to be replayed the request will wait for a configurable timeout (currently 5 mins) before returning a response and having the follower ShardReplicationTask re-initiate the long poll request.  There may be more than one concurrent long poll request in flight at any time as required to match the indexing throughput on the leader.  The replicated operations are written to a queue on the node.
    2. **Replication Writer**: The writer process reads operations in-order from the queue and replays them on the follower shard. In case an operation requires a mapping update the writer synchronously fetches the updated mapping from the leader cluster and updates the mapping on the follower cluster. Once the operations are written to the primary shard they are automatically replicated to the follower replica shards just like any normal write operation to the shard.
    3. Updates the retention lease on the leader cluster after new operations have been successfully written to the shard. This allows the translog history to be safely truncated on the leader shard. 



## APIs

### Configure Remote Connection on Follower

Cross-cluster replication requires the follower cluster to be connected to the leader cluster via a remote-cluster connection. This is not a new API, but rather a pre-requisite for APIs to work.

```bash

PUT $FOLLOWER/_cluster/settings?pretty
Content-Type: application/json

{
  "persistent": {
    "cluster": {
      "remote": {
        "leader-cluster": {
          "seeds": [ "<leader-node1-ip>:9300", "<leader-node2-ip>:9300".. ]
        }
      }
    }
  }
}

```

### Start Replication

This API is used to initiate replication of an index from the leader cluster onto the follower cluster. The API is invoked on the follower and the desired remote index from leader cluster is provided as parameters.

```bash
Request

PUT $FOLLOWER/_plugins/_replication/<index>/_start
Content-Type: application/json

{  "leader_alias" : "leader-cluster",  "leader_index": "<index>"}

## Response

{
  "acknowledged" : true
}
```

### Stop Replication

Replication can be stopped anytime by invocation of Stop API on the follower cluster. Stopping replication opens up the index for writes.

Note that the follower index is NOT deleted on stopping replication.

```bash

Request
POST $FOLLOWER/_plugins/_replication/<index>/_stop
Content-Type: application/json

{}

Response

{
  "acknowledged" : true
}

```

### Start replication via Autofollow pattern

AutoFollow makes it easy to automatically replicate multiple indices matching a pattern. Replication is also automatically started for any new indices created on leader that matches one of the auto-follow patterns.

```bash
Request
POST $FOLLOWER/_plugins/_replication/_autofollow
Content-Type: application/json

{
  "leader_alias": "leader-cluster",
  "name": "test",
  "pattern": "*customer*"
}
```

### Remove AutoFollow

AutoFollow can be removed by invoking API on the follower as follows. Invocation of the API is only to stop any new auto-follow activity and does NOT stop replication of indices already initiated by the auto-follow.

```bash
DELETE $FOLLOWER/_plugins/_replication/_autofollow
Content-Type: application/json

{
  "leader_alias": "leader-cluster",
  "name": "test"
}
```

## Component Level Design 

### Restore Leader Service 

This component is responsible for co-ordinating bootstrap process at the leader cluster node. It maintains the current commit point that the follower shard is restoring from, restore start time and other resources aiding the actual segment transfer from the leader shard. This service cleans up resources at the leader end once the restore is completed.

![Restore Leader Service](/docs/images/rfc2.jpg?raw=true "Restore Leader Service")

## Replication Tasks

### Index level replication tasks

Replication at index level is managed by spinning-up persistent tasks. These tasks persist current task state information in cluster state and are tolerant to node restarts by continuing execution from the last known state. OpenSearch internally manages task lifecycle.

![Index level replication tasks](/docs/images/rfc3.jpg?raw=true "Index level replication tasks")


Above figure shows the index level replication tasks in action. All the tasks are executed on the follower cluster nodes.

#### Index replication task

![State Transition](/docs/images/rfc4.jpeg?raw=true "State transition")

- Index replication task is spun up for each follower index and co-ordinates with all of the shard level replication tasks. 
- It is responsible for starting the bootstrap phase via restore workflow and spins-up shard replication tasks after the bootstrap is completed. 
- After starting shard replication tasks, index replication task monitors, takes corrective actions and performs graceful handling of failures at the index level
- It executes under the same user context invoking start replication action.


#### Shard replication task

- Each follower index primary shard has an associated shard replication task through out the life cycle of replication.
- Shard replication task is co-located on the same node as the primary shard on the follower cluster and this task is responsible for replaying the changes and take corrective actions at the shard level. 
- Upon failures the index task is notified to take appropriate actions.
- It executes under the same user context invoking start replication action.

### Cluster level replication tasks

Cluster level tasks are persistent tasks and are not associated with any specific indices. These tasks are responsible to orchestrate cluster level changes from the leader onto the follower cluster. Following tasks are supported under cluster level replication tasks

### Autofollow replication task

- This task is responsible for initiating replication on the follower for matching index patterns on the leader cluster.
- This task is spun as a part of invoking  `_replication/_autofollow`  API and it polls leader cluster periodically for any new indices matching the configured index pattern. 
- It executes under the same user context invoking autofollow action.
- Each start replication action matching the index pattern is triggered under the same user context. 

## Security Considerations 

Cross-cluster replication supports security controls via integration with OpenSearch Security plugin.

Node-to-node encryption can be turned on both leader and follower cluster to ensure the replication traffic between the clusters are encrypted. Note that either both clusters need to have it turned on OR both clusters need to have it turned off.

Cross-cluster replication asynchronous tasks uses the security context of the user making the start replication API or autofollow API which is captured under cluster state. This means users can perform access control for replication actions via API/kibana interface provided by security plugin.

Given that there are multiple clusters involved, it introduces possibilities where clusters could differ in security configurations. Following configurations are supported

- Security plugin absent/disabled on both clusters
- Security plugin fully enabled on both clusters
- Security plugin enabled only for TLS on both clusters (plugins.security.ssl_only)

Since the replication steps are implemented using standard OpenSearch action framework, audit logging works seamlessly as well.

### Permission model

#### Access to Replication REST APIs

- All the REST APIs for replication are backed by corresponding transport actions. Access control mechanisms can be employed on these transport actions via security plugin and can be restricted to index patterns and user/backend role.

#### Access to Transport actions on leader/follower

- Replication tasks underneath invoke various transport actions to fetch changes from leader index and apply it onto the follower index. Access to these transport actions can be restricted to index patterns and user/backend role.

#### Authorization for the transport actions
It happens for every transport request at both the follower and leader cluster invoked via the background persistent tasks.

- If user/backend role is updated to remove access to certain actions after the replication is started, the subsequent operations should fail with unauthorized requests.

## Feature sets being actively worked on

We are actively working on the following features and will release it in the repo soon:

- Improve resiliency of index replication tasks in different failure modes
- Task relocation and restarts due to cluster configuration changes
- Detailed Status API for ongoing replication 

## Comments/feedback 

- For any feedback on the problem statement or design, please leave your comments on the [github issue](https://github.com/opensearch-project/cross-cluster-replication/issues/1).
- For any issues or specific feature requests, please report by following the [guidelines](../CONTRIBUTING.md).



