/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.replication.action.status

import org.apache.logging.log4j.LogManager
import org.opensearch.action.support.broadcast.BroadcastResponse
import org.opensearch.action.support.broadcast.BroadcastShardResponse
import org.opensearch.common.ParseField
import org.opensearch.common.io.stream.StreamInput
import org.opensearch.common.io.stream.StreamOutput
import org.opensearch.common.xcontent.ToXContent
import org.opensearch.common.xcontent.ToXContentObject
import org.opensearch.common.xcontent.XContentBuilder
import org.opensearch.index.shard.ShardId
import java.io.IOException

class ShardInfoResponse : BroadcastShardResponse, ToXContentObject {

    val status: String
    lateinit var replayDetails: ReplayDetails
    lateinit var restoreDetails: RestoreDetails

    constructor(si: StreamInput) : super(si) {
        this.status = si.readString()
        if (status.equals("SYNCING"))
            this.replayDetails = ReplayDetails(si)
        if (status.equals("BOOTSTRAPPING"))
            this.restoreDetails = RestoreDetails(si)
    }

    constructor(shardId: ShardId, status :String, restoreDetailsShard : RestoreDetails) : super(shardId) {
        this.status = status
        this.restoreDetails = restoreDetailsShard
    }

    constructor(shardId: ShardId, status :String, replayDetailsShard : ReplayDetails) : super(shardId) {
        this.status = status
        this.replayDetails = replayDetailsShard
    }

    constructor(shardId: ShardId, status :String, replayDetailsShard : ReplayDetails, restoreDetailsShard : RestoreDetails) : super(shardId) {
        this.status = status
        this.replayDetails = replayDetailsShard
        this.restoreDetails = restoreDetailsShard
    }

    @Throws(IOException::class)
    override fun writeTo(out: StreamOutput) {
        super.writeTo(out)
        out.writeString(status)
        if (::replayDetails.isInitialized)
            replayDetails.writeTo(out)
        if (::restoreDetails.isInitialized)
            restoreDetails.writeTo(out)
    }

    private val SHARDID = ParseField("shard_id")
    private val REPLAYDETAILS = ParseField("syncing_task_details")
    private val RESTOREDETAILS = ParseField("bootstrap_task_details")


    @Throws(IOException::class)
    override fun toXContent(builder: XContentBuilder, params: ToXContent.Params?): XContentBuilder? {
        builder!!.startObject()
        builder.field(SHARDID.preferredName, shardId)
        if (::replayDetails.isInitialized)
            builder.field(REPLAYDETAILS.preferredName, replayDetails)
        if (::restoreDetails.isInitialized)
            builder.field(RESTOREDETAILS.preferredName, restoreDetails)
        builder.endObject()
        return builder
    }

    fun isReplayDetailsInitialized(): Boolean {
        return ::replayDetails.isInitialized
    }
    fun isRestoreDetailsInitialized(): Boolean {
        return ::restoreDetails.isInitialized
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

    var remoteCheckpoint: Long = -1
    var localCheckpoint: Long
    var seqNo: Long

    constructor(si: StreamInput) {
        this.remoteCheckpoint = si.readLong()
        this.localCheckpoint = si.readLong()
        this.seqNo = si.readLong()
    }

    constructor(remoteCheckpoint: Long,localCheckpoint : Long,
                seqNo : Long)  {
        this.remoteCheckpoint = remoteCheckpoint
        this.localCheckpoint = localCheckpoint
        this.seqNo = seqNo
    }

    private val REMOTECHECKPOINT = ParseField("leader_checkpoint")
    private val LOCALCHECKPOINT = ParseField("follower_checkpoint")
    private val SEQUENCENUMBER = ParseField("seq_no")

    fun remoteCheckpoint(): Long {
        return remoteCheckpoint
    }

    fun localCheckpoint(): Long {
        return localCheckpoint
    }

    fun seqNo(): Long {
        return seqNo
    }

    override fun writeTo(out: StreamOutput) {
        out.writeLong(remoteCheckpoint)
        out.writeLong(localCheckpoint)
        out.writeLong(localCheckpoint)
    }

    override fun toXContent(builder: XContentBuilder?, params: ToXContent.Params?): XContentBuilder {
        builder!!.startObject()
        builder.field(REMOTECHECKPOINT.preferredName, remoteCheckpoint())
        builder.field(LOCALCHECKPOINT.preferredName, localCheckpoint())
        builder.field(SEQUENCENUMBER.preferredName, seqNo())
        builder.endObject()
        return builder
    }

    override fun toString(): String {
        return "ReplayDetails(remoteCheckpoint=$remoteCheckpoint, localCheckpoint=$localCheckpoint, seqNo=$seqNo, REMOTECHECKPOINT=$REMOTECHECKPOINT, LOCALCHECKPOINT=$LOCALCHECKPOINT, SEQUENCENUMBER=$SEQUENCENUMBER)"
    }


}
