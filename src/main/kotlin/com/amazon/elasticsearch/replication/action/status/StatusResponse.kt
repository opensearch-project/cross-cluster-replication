package com.amazon.elasticsearch.replication.action.status

import org.elasticsearch.action.ActionResponse
import org.elasticsearch.common.ParseField
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.ToXContentObject
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.index.shard.ShardId

class StatusResponse(val acknowledged: String,val restoreDetailsList: List<RestoreDetails>,val replayDetailsList: List<ReplayDetails>): ActionResponse(), ToXContentObject {
    constructor(inp: StreamInput) : this(inp.readString(), inp.readGenericValue() as List<RestoreDetails> ,inp.readGenericValue() as List<ReplayDetails>)


    private val STATE = ParseField("state")
    private val RESTOREDETAILS = ParseField("restore_task_details")
    private val REPLAYDETAILS = ParseField("replay_task_details")

    override fun writeTo(out: StreamOutput) {
        out.writeString(acknowledged)
        out.writeGenericValue(restoreDetailsList)
        out.writeGenericValue(replayDetailsList)
    }

    override fun toXContent(builder: XContentBuilder?, params: ToXContent.Params?): XContentBuilder {
        builder!!.startObject()
        builder.field(STATE.preferredName, acknowledged)
        builder.field(RESTOREDETAILS.preferredName, restoreDetailsList)
        builder.field(REPLAYDETAILS.preferredName, replayDetailsList)
        builder.endObject()
        return builder
    }
}

class RestoreDetails(val totalBytes : Long, val recoveredBytes : Long, val recovereyPercentage : Float,
                     val totalFiles : Int, val recoveredFiles : Int, val fileRecovereyPercentage : Float,
                     val startTime : Long, val time : Long, val shardId: ShardId) : ActionResponse(), ToXContentObject {
    constructor(inp: StreamInput) : this(inp.readLong(),inp.readLong(),inp.readFloat(),
        inp.readInt(),inp.readInt(),inp.readFloat(),inp.readLong() ,inp.readLong(), inp.readGenericValue() as ShardId)

    private val TOTALBYTES = ParseField("bytes_total")
    private val BYTESRECOVERED = ParseField("bytes_recovered")
    private val BYTESRECOVEREDPERCENTAGE = ParseField("bytes_percent")
    private val TOTALFILES = ParseField("files_total")
    private val FILESRECOVERED = ParseField("files_recovered")
    private val FILESRECOVEREDPERCENTAGE = ParseField("files_percent")
    private val STARTTIME = ParseField("start_time")
    private val RUNNINGTIME = ParseField("running_time")
    private val SHARDID = ParseField("shard_id")

    override fun writeTo(out: StreamOutput) {
        out.writeLong(totalBytes)
        out.writeLong(recoveredBytes)
        out.writeFloat(recovereyPercentage)
        out.writeInt(totalFiles)
        out.writeInt(recoveredFiles)
        out.writeFloat(fileRecovereyPercentage)
        out.writeLong(startTime)
        out.writeLong(time)
    }

    override fun toXContent(builder: XContentBuilder?, params: ToXContent.Params?): XContentBuilder {
        builder!!.startObject()
        builder.field(TOTALBYTES.preferredName, totalBytes)
        builder.field(BYTESRECOVERED.preferredName, recoveredBytes)
        builder.field(BYTESRECOVEREDPERCENTAGE.preferredName, recovereyPercentage)
        builder.field(TOTALFILES.preferredName, totalFiles)
        builder.field(FILESRECOVERED.preferredName,recoveredFiles)
        builder.field(FILESRECOVEREDPERCENTAGE.preferredName, fileRecovereyPercentage)
        builder.field(STARTTIME.preferredName, startTime)
        builder.field(RUNNINGTIME.preferredName, time)
        builder.field(SHARDID.preferredName, shardId)
        builder.endObject()
        return builder
    }
}

class ReplayDetails(val lastSyncedGlobalCheckpoint: Long,val lastKnownGlobalCheckpoint : Long,val seqNo : Long,
                    val shardId: ShardId): ActionResponse(), ToXContentObject {
    constructor(inp: StreamInput) : this(inp.readLong(),inp.readLong(),inp.readLong(), inp.readGenericValue() as ShardId)

    private val GLOBALCHECKPOINT = ParseField("leader_global_checkpoint")
    private val LOCALCHECKPOINT = ParseField("follower_global_checkpoint")
    private val SEQUENCENUMBER = ParseField("seq_no")
    private val SHARDID = ParseField("shard_id")


    fun lastSyncedGlobalCheckpoint(): Long {
        return lastSyncedGlobalCheckpoint
    }

    fun lastKnownGlobalCheckpoint(): Long {
        return lastKnownGlobalCheckpoint
    }

    fun seqNo(): Long {
        return seqNo
    }

    fun shardId(): ShardId {
        return shardId
    }

    override fun writeTo(out: StreamOutput) {
        out.writeLong(lastSyncedGlobalCheckpoint)
        out.writeLong(lastKnownGlobalCheckpoint)
        out.writeLong(lastKnownGlobalCheckpoint)
    }

    override fun toXContent(builder: XContentBuilder?, params: ToXContent.Params?): XContentBuilder {
        builder!!.startObject()
        builder.field(GLOBALCHECKPOINT.preferredName, lastSyncedGlobalCheckpoint())
        builder.field(LOCALCHECKPOINT.preferredName, lastKnownGlobalCheckpoint())
        builder.field(SEQUENCENUMBER.preferredName, seqNo())
        builder.field(SHARDID.preferredName, shardId())
        builder.endObject()
        return builder
    }
}


