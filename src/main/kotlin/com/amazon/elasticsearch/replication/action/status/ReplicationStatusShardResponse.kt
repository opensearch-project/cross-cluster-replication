package com.amazon.elasticsearch.replication.action.status

import com.amazon.elasticsearch.replication.action.index.ReplicateIndexRequest
import org.elasticsearch.action.ActionResponse
import org.elasticsearch.action.support.broadcast.BroadcastResponse
import org.elasticsearch.action.support.broadcast.BroadcastShardResponse
import org.elasticsearch.common.ParseField
import org.elasticsearch.common.io.stream.StreamInput
import org.elasticsearch.common.io.stream.StreamOutput
import org.elasticsearch.common.xcontent.ToXContent
import org.elasticsearch.common.xcontent.ToXContentObject
import org.elasticsearch.common.xcontent.XContentBuilder
import org.elasticsearch.index.shard.ShardId
import java.io.IOException

class ReplicationStatusShardResponse : BroadcastShardResponse, ToXContentObject {

    var replayDetails: ReplayDetails
    var restoreDetails: RestoreDetails

    constructor(si: StreamInput) : super(si) {
        this.replayDetails = ReplayDetails(si)
        this.restoreDetails = RestoreDetails(si)
    }

    constructor(shardId: ShardId,replayDetails : ReplayDetails,restoreDetails : RestoreDetails) : super(shardId) {
        this.replayDetails = replayDetails
        this.restoreDetails = restoreDetails
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        replayDetails.writeTo(out)
        restoreDetails.writeTo(out)
    }

    private val STATE = ParseField("state")
    private val SHARDID = ParseField("shard_id")
    private val REPLAYDETAILS = ParseField("replay_task_details")
    private val RESTOREDETAILS = ParseField("restore_task_details")


    @Throws(IOException::class)
    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params?): XContentBuilder? {
        builder!!.startObject()
        builder.field(SHARDID.preferredName, shardId)
        builder.field(REPLAYDETAILS.preferredName, replayDetails)
        builder.field(RESTOREDETAILS.preferredName, restoreDetails)
        builder.endObject()
        return builder
    }

}


class RestoreDetails :  BroadcastResponse, ToXContentObject {

    var totalBytes : Long
    var recoveredBytes : Long
    var recovereyPercentage : Float
    var totalFiles : Int
    var recoveredFiles : Int
    var fileRecovereyPercentage : Float
    var startTime : Long
    var time : Long

    constructor(si: StreamInput) {
        this.totalBytes = si.readLong()
        this.recoveredBytes = si.readLong()
        this.recovereyPercentage = si.readFloat()
        this.totalFiles =  si.readInt()
        this.recoveredFiles =  si.readInt()
        this.fileRecovereyPercentage = si.readFloat()
        this.startTime = si.readLong()
        this.time = si.readLong()
    }

    constructor(totalBytes : Long, recoveredBytes : Long, recovereyPercentage : Float, totalFiles : Int,
                recoveredFiles : Int, fileRecovereyPercentage : Float, startTime : Long, time : Long)  {
        this.totalBytes = totalBytes
        this.recoveredBytes = recoveredBytes
        this.recovereyPercentage = recovereyPercentage
        this.totalFiles = totalFiles
        this.recoveredFiles = recoveredFiles
        this.fileRecovereyPercentage = fileRecovereyPercentage
        this.startTime = startTime
        this.time = time
    }

    private val TOTALBYTES = ParseField("bytes_total")
    private val BYTESRECOVERED = ParseField("bytes_recovered")
    private val BYTESRECOVEREDPERCENTAGE = ParseField("bytes_percent")
    private val TOTALFILES = ParseField("files_total")
    private val FILESRECOVERED = ParseField("files_recovered")
    private val FILESRECOVEREDPERCENTAGE = ParseField("files_percent")
    private val STARTTIME = ParseField("start_time")
    private val RUNNINGTIME = ParseField("running_time")

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
        builder.endObject()
        return builder
    }

    override fun toString(): String {
        return "RestoreDetails(totalBytes=$totalBytes, recoveredBytes=$recoveredBytes, recovereyPercentage=$recovereyPercentage, totalFiles=$totalFiles, recoveredFiles=$recoveredFiles, fileRecovereyPercentage=$fileRecovereyPercentage, startTime=$startTime, time=$time, TOTALBYTES=$TOTALBYTES, BYTESRECOVERED=$BYTESRECOVERED, BYTESRECOVEREDPERCENTAGE=$BYTESRECOVEREDPERCENTAGE, TOTALFILES=$TOTALFILES, FILESRECOVERED=$FILESRECOVERED, FILESRECOVEREDPERCENTAGE=$FILESRECOVEREDPERCENTAGE, STARTTIME=$STARTTIME, RUNNINGTIME=$RUNNINGTIME)"
    }

}

class ReplayDetails:  BroadcastResponse, ToXContentObject {

    var lastSyncedGlobalCheckpoint: Long
    var lastKnownGlobalCheckpoint: Long
    var seqNo: Long

    constructor(si: StreamInput) {
        this.lastSyncedGlobalCheckpoint = si.readLong()
        this.lastKnownGlobalCheckpoint = si.readLong()
        this.seqNo = si.readLong()
    }

    constructor(lastSyncedGlobalCheckpoint: Long,lastKnownGlobalCheckpoint : Long,
                seqNo : Long)  {
        this.lastSyncedGlobalCheckpoint = lastSyncedGlobalCheckpoint
        this.lastKnownGlobalCheckpoint = lastKnownGlobalCheckpoint
        this.seqNo = seqNo
    }


    private val GLOBALCHECKPOINT = ParseField("leader_checkpoint")
    private val LOCALCHECKPOINT = ParseField("follower_checkpoint")
    private val SEQUENCENUMBER = ParseField("seq_no")



    fun lastSyncedGlobalCheckpoint(): Long {
        return lastSyncedGlobalCheckpoint
    }

    fun lastKnownGlobalCheckpoint(): Long {
        return lastKnownGlobalCheckpoint
    }

    fun seqNo(): Long {
        return seqNo
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
        builder.endObject()
        return builder
    }

    override fun toString(): String {
        return "ReplayDetails(lastSyncedGlobalCheckpoint=$lastSyncedGlobalCheckpoint, lastKnownGlobalCheckpoint=$lastKnownGlobalCheckpoint, seqNo=$seqNo, GLOBALCHECKPOINT=$GLOBALCHECKPOINT, LOCALCHECKPOINT=$LOCALCHECKPOINT, SEQUENCENUMBER=$SEQUENCENUMBER)"
    }
}