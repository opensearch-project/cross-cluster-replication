package com.amazon.elasticsearch.replication.metadata

/**
 * This indicates the overall state for
 * the replication job visible to the customer.
 * This doesn't cover task level states
 * over the lifecycle of replication.
 */
enum class ReplicationOverallState {
    /**
     * Indicates that the replication job is
     * running. Internally, it can be in bootstrap or
     * following phase
     */
    RUNNING,

    /**
     * Indicates that the replication job is stopped
     * and index is opened for writes at the follower
     * cluster
     */
    STOPPED
}
