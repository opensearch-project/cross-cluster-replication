package com.amazon.elasticsearch.replication.action.status

import org.apache.logging.log4j.LogManager
import org.elasticsearch.action.ActionRequestValidationException
import org.elasticsearch.action.support.IndicesOptions
import org.elasticsearch.action.support.single.shard.SingleShardRequest
import org.elasticsearch.cluster.node.DiscoveryNode
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.xcontent.*
import org.elasticsearch.transport.RemoteClusterAwareRequest

class IndexReplicationStatusRequest : SingleShardRequest<IndexReplicationStatusRequest>, RemoteClusterAwareRequest {

    lateinit var indexName: String

    constructor(indexName: String?, remoteNode: DiscoveryNode?) {
        if (indexName != null) {
            this.indexName = indexName
        }
    }

    constructor(inp: StreamInput): super(inp) {
        indexName = inp.readString()
    }

    companion object {
        private val log = LogManager.getLogger(IndexReplicationStatusRequest::class.java)
        private val PARSER = ObjectParser<IndexReplicationStatusRequest, Void>("IndexReplicationStatusRequestParser") {
            IndexReplicationStatusRequest()
        }

        private fun IndexReplicationStatusRequest(): IndexReplicationStatusRequest? {
            return IndexReplicationStatusRequest(null,null)
        }

        fun fromXContent(parser: XContentParser, followerIndex: String): IndexReplicationStatusRequest {
            val indexReplicationStatusRequest = PARSER.parse(parser, null)
            indexReplicationStatusRequest.indexName = followerIndex
            return indexReplicationStatusRequest
        }
    }

    override fun validate(): ActionRequestValidationException? {
        return null
    }

    override fun indices(): Array<String> {
        return arrayOf(indexName)
    }

    override fun indicesOptions(): IndicesOptions {
        return IndicesOptions.strictSingleIndexNoExpandForbidClosed()
    }

    fun toXContent(builder: XContentBuilder, params: ToXContent.Params?): XContentBuilder {
        builder.startObject()
        builder.field("indexName", indexName)
        builder.endObject()
        return builder
    }

    override fun getPreferredTargetNode(): DiscoveryNode {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}