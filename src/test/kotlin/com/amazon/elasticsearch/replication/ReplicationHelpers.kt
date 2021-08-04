/*
 *   Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
 */

package com.amazon.elasticsearch.replication

import com.amazon.elasticsearch.replication.task.index.IndexReplicationExecutor
import com.amazon.elasticsearch.replication.task.shard.ShardReplicationExecutor
import org.apache.http.message.BasicHeader
import org.assertj.core.api.Assertions.assertThat
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksRequest
import org.elasticsearch.action.support.master.AcknowledgedResponse
import org.elasticsearch.client.HttpAsyncResponseConsumerFactory.HeapBufferedResponseConsumerFactory
import org.elasticsearch.client.Request
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.client.Response
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.common.xcontent.DeprecationHandler
import org.elasticsearch.common.xcontent.NamedXContentRegistry
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.test.ESTestCase.assertBusy
import org.elasticsearch.test.rest.ESRestTestCase
import org.junit.Assert
import java.nio.charset.StandardCharsets
import java.util.*
import java.util.concurrent.TimeUnit

data class AssumeRoles(val remoteClusterRole: String = "leader_role", val localClusterRole: String = "follower_role")

data class StartReplicationRequest(val remoteClusterAlias: String, val remoteIndex: String, val toIndex: String,
                                   val settings: Settings = Settings.EMPTY, val assumeRoles: AssumeRoles = AssumeRoles())

const val REST_REPLICATION_PREFIX = "/_plugins/_replication/"
const val REST_REPLICATION_START = "$REST_REPLICATION_PREFIX{index}/_start"
const val REST_REPLICATION_STOP = "$REST_REPLICATION_PREFIX{index}/_stop"
const val REST_REPLICATION_PAUSE = "$REST_REPLICATION_PREFIX{index}/_pause"
const val REST_REPLICATION_RESUME = "$REST_REPLICATION_PREFIX{index}/_resume"
const val REST_REPLICATION_UPDATE = "$REST_REPLICATION_PREFIX{index}/_update"
const val REST_REPLICATION_STATUS_VERBOSE = "$REST_REPLICATION_PREFIX{index}/_status?verbose=true"
const val REST_REPLICATION_STATUS = "$REST_REPLICATION_PREFIX{index}/_status"
const val REST_AUTO_FOLLOW_PATTERN = "${REST_REPLICATION_PREFIX}_autofollow"

const val STATUS_REASON_USER_INITIATED = "User initiated"
const val STATUS_REASON_INDEX_NOT_FOUND = "no such index"

fun RestHighLevelClient.startReplication(request: StartReplicationRequest,
                                         waitFor: TimeValue = TimeValue.timeValueSeconds(10),
                                         waitForShardsInit: Boolean = true,
                                         waitForRestore: Boolean = false,
                                         requestOptions: RequestOptions = RequestOptions.DEFAULT) {
    val lowLevelRequest = Request("PUT", REST_REPLICATION_START.replace("{index}", request.toIndex, true)
            + "?wait_for_restore=${waitForRestore}")
    if (request.settings == Settings.EMPTY) {
        lowLevelRequest.setJsonEntity("""{
                                       "remote_cluster" : "${request.remoteClusterAlias}",
                                       "remote_index": "${request.remoteIndex}",
                                       "assume_roles": {
                                        "remote_cluster_role": "${request.assumeRoles.remoteClusterRole}",
                                        "local_cluster_role": "${request.assumeRoles.localClusterRole}"
                                       }
                                     }            
                                  """)
    } else {
        lowLevelRequest.setJsonEntity("""{
                                       "remote_cluster" : "${request.remoteClusterAlias}",
                                       "remote_index": "${request.remoteIndex}",
                                       "assume_roles": {
                                        "remote_cluster_role": "${request.assumeRoles.remoteClusterRole}",
                                        "local_cluster_role": "${request.assumeRoles.localClusterRole}"
                                       },
                                       "settings": ${request.settings}
                                     }            
                                  """)
    }

    lowLevelRequest.setOptions(requestOptions)
    val lowLevelResponse = lowLevelClient.performRequest(lowLevelRequest)
    val response = getAckResponse(lowLevelResponse)
    assertThat(response.isAcknowledged).withFailMessage("Replication not started.").isTrue()
    waitForReplicationStart(request.toIndex, waitFor)
    if (waitForShardsInit)
        waitForNoInitializingShards()
}
fun getAckResponse(lowLevelResponse: Response): AcknowledgedResponse {
    val xContentType = XContentType.fromMediaTypeOrFormat(lowLevelResponse.entity.contentType.value)
    val xcp = xContentType.xContent().createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.IGNORE_DEPRECATIONS,
            lowLevelResponse.entity.content)
    return AcknowledgedResponse.fromXContent(xcp)
}

fun RestHighLevelClient.replicationStatus(index: String,verbose: Boolean = true, requestOptions: RequestOptions = RequestOptions.DEFAULT) : Map<String, Any> {
    var lowLevelReplStatusRequest = if(!verbose)  Request("GET", REST_REPLICATION_STATUS.replace("{index}", index,true)) else Request("GET", REST_REPLICATION_STATUS_VERBOSE.replace("{index}", index,true))
    lowLevelReplStatusRequest.setJsonEntity("{}")
    lowLevelReplStatusRequest.setOptions(requestOptions)
    val lowLevelStatusResponse = lowLevelClient.performRequest(lowLevelReplStatusRequest)
    val statusResponse: Map<String, Any> = ESRestTestCase.entityAsMap(lowLevelStatusResponse)
    return statusResponse
}

fun `validate status syncing resposne`(statusResp: Map<String, Any>) {
    Assert.assertEquals(statusResp.getValue("status"),"SYNCING")
    Assert.assertEquals(statusResp.getValue("reason"), STATUS_REASON_USER_INITIATED)
    Assert.assertTrue((statusResp.getValue("shard_replication_details")).toString().contains("syncing_task_details"))
    Assert.assertTrue((statusResp.getValue("shard_replication_details")).toString().contains("local_checkpoint"))
    Assert.assertTrue((statusResp.getValue("shard_replication_details")).toString().contains("remote_checkpoint"))
}

fun `validate status syncing aggregated resposne`(statusResp: Map<String, Any>) {
    Assert.assertEquals(statusResp.getValue("status"),"SYNCING")
    Assert.assertEquals(statusResp.getValue("reason"), STATUS_REASON_USER_INITIATED)
    Assert.assertTrue((statusResp.getValue("syncing_details")).toString().contains("local_checkpoint"))
    Assert.assertTrue((statusResp.getValue("syncing_details")).toString().contains("remote_checkpoint"))
}

fun `validate not paused status resposne`(statusResp: Map<String, Any>) {
    Assert.assertNotEquals(statusResp.getValue("status"),"PAUSED")
    Assert.assertEquals(statusResp.getValue("reason"), STATUS_REASON_USER_INITIATED)
    Assert.assertTrue((statusResp.getValue("shard_replication_details")).toString().contains("syncing_task_details"))
    Assert.assertTrue((statusResp.getValue("shard_replication_details")).toString().contains("local_checkpoint"))
    Assert.assertTrue((statusResp.getValue("shard_replication_details")).toString().contains("remote_checkpoint"))
}

fun `validate not paused status aggregated resposne`(statusResp: Map<String, Any>) {
    Assert.assertNotEquals(statusResp.getValue("status"),"PAUSED")
    Assert.assertEquals(statusResp.getValue("reason"), STATUS_REASON_USER_INITIATED)
    Assert.assertTrue((statusResp.getValue("syncing_details")).toString().contains("local_checkpoint"))
    Assert.assertTrue((statusResp.getValue("syncing_details")).toString().contains("remote_checkpoint"))
}

fun `validate paused status resposne`(statusResp: Map<String, Any>) {
    Assert.assertEquals(statusResp.getValue("status"),"PAUSED")
    Assert.assertEquals(statusResp.getValue("reason"), STATUS_REASON_USER_INITIATED)
    Assert.assertFalse(statusResp.containsKey("shard_replication_details"))
    Assert.assertFalse(statusResp.containsKey("local_checkpoint"))
    Assert.assertFalse(statusResp.containsKey("remote_checkpoint"))
}

fun `validate paused status on closed index`(statusResp: Map<String, Any>) {
    Assert.assertEquals(statusResp.getValue("status"), "PAUSED")
    assertThat(statusResp.getValue("reason").toString()).contains("org.elasticsearch.indices.IndexClosedException")
    assertThat(statusResp).doesNotContainKeys("shard_replication_details","follower_checkpoint","leader_checkpoint")
}

fun `validate aggregated paused status resposne`(statusResp: Map<String, Any>) {
    Assert.assertEquals(statusResp.getValue("status"),"PAUSED")
    Assert.assertEquals(statusResp.getValue("reason"), STATUS_REASON_USER_INITIATED)
    Assert.assertTrue(!(statusResp.containsKey("syncing_details")))
}

fun `validate paused status response due to leader index deleted`(statusResp: Map<String, Any>) {
    Assert.assertEquals("PAUSED", statusResp.getValue("status"))
    Assert.assertTrue(statusResp.getValue("reason").toString().contains(STATUS_REASON_INDEX_NOT_FOUND))
}

fun RestHighLevelClient.stopReplication(index: String, shouldWait: Boolean = true, requestOptions: RequestOptions = RequestOptions.DEFAULT) {
    val lowLevelStopRequest = Request("POST", REST_REPLICATION_STOP.replace("{index}", index,true))
    lowLevelStopRequest.setJsonEntity("{}")
    lowLevelStopRequest.setOptions(requestOptions)
    val lowLevelStopResponse = lowLevelClient.performRequest(lowLevelStopRequest)
    val response = getAckResponse(lowLevelStopResponse)
    assertThat(response.isAcknowledged).withFailMessage("Replication could not be stopped").isTrue()
    if (shouldWait) waitForReplicationStop(index)
}

fun RestHighLevelClient.pauseReplication(index: String, requestOptions: RequestOptions = RequestOptions.DEFAULT) {
    val lowLevelPauseRequest = Request("POST", REST_REPLICATION_PAUSE.replace("{index}", index,true))
    lowLevelPauseRequest.setJsonEntity("{}")
    lowLevelPauseRequest.setOptions(requestOptions)
    val lowLevelPauseResponse = lowLevelClient.performRequest(lowLevelPauseRequest)
    val response = getAckResponse(lowLevelPauseResponse)
    assertThat(response.isAcknowledged).withFailMessage("Replication could not be paused").isTrue()
    waitForReplicationStop(index)
}

fun RestHighLevelClient.resumeReplication(index: String, requestOptions: RequestOptions = RequestOptions.DEFAULT) {
    val lowLevelResumeRequest = Request("POST", REST_REPLICATION_RESUME.replace("{index}", index, true))
    lowLevelResumeRequest.setJsonEntity("{}")
    lowLevelResumeRequest.setOptions(requestOptions)
    val lowLevelResumeResponse = lowLevelClient.performRequest(lowLevelResumeRequest)
    val response = getAckResponse(lowLevelResumeResponse)
    assertThat(response.isAcknowledged).withFailMessage("Replication could not be Resumed").isTrue()
    waitForReplicationStart(index, TimeValue.timeValueSeconds(10))
}

fun RestHighLevelClient.updateReplication(index: String, settings: Settings, requestOptions: RequestOptions = RequestOptions.DEFAULT) {
    val lowLevelRequest = Request("PUT", REST_REPLICATION_UPDATE.replace("{index}", index,true))
    lowLevelRequest.setJsonEntity(settings.toString())
    lowLevelRequest.setOptions(requestOptions)
    val lowLevelResponse = lowLevelClient.performRequest(lowLevelRequest)
    val response = getAckResponse(lowLevelResponse)
    assertThat(response.isAcknowledged).isTrue()
}

fun RestHighLevelClient.waitForReplicationStart(index: String, waitFor : TimeValue = TimeValue.timeValueSeconds(10)) {
    assertBusy(
        {
            // Persistent tasks service appends identifiers like '[c]' to indicate child task hence the '*' wildcard
            val request = ListTasksRequest().setDetailed(true).setActions(ShardReplicationExecutor.TASK_NAME + "*",
                                                                          IndexReplicationExecutor.TASK_NAME + "*")
            val response = tasks().list(request,RequestOptions.DEFAULT)
            assertThat(response.tasks)
                .withFailMessage("replication tasks not started")
                .isNotEmpty
        }, waitFor.seconds, TimeUnit.SECONDS)
}

fun RestHighLevelClient.waitForNoInitializingShards() {
    val request = ClusterHealthRequest().waitForNoInitializingShards(true)
        .timeout(TimeValue.timeValueSeconds(70))
    request.level(ClusterHealthRequest.Level.SHARDS)
    this.cluster().health(request, RequestOptions.DEFAULT)
}

fun RestHighLevelClient.waitForNoRelocatingShards() {
    val request = ClusterHealthRequest().waitForNoRelocatingShards(true)
        .timeout(TimeValue.timeValueSeconds(70))
    request.level(ClusterHealthRequest.Level.SHARDS)
    this.cluster().health(request, RequestOptions.DEFAULT)
}

fun RestHighLevelClient.waitForReplicationStop(index: String, waitFor : TimeValue = TimeValue.timeValueSeconds(10)) {
    assertBusy(
        {
            // Persistent tasks service appends modifiers to task action hence the '*'
            val request = ListTasksRequest().setDetailed(true).setActions(ShardReplicationExecutor.TASK_NAME + "*",
                                                                          IndexReplicationExecutor.TASK_NAME + "*")

            val response = tasks().list(request,RequestOptions.DEFAULT)
            assertThat(response.tasks)
                .withFailMessage("replication tasks not stopped.")
                .isEmpty()
        }, waitFor.seconds, TimeUnit.SECONDS)
}

fun RestHighLevelClient.updateAutoFollowPattern(connection: String, patternName: String, pattern: String,
                                                settings: Settings = Settings.EMPTY,
                                                assumeRoles: AssumeRoles = AssumeRoles()) {
    val lowLevelRequest = Request("POST", REST_AUTO_FOLLOW_PATTERN)
    if (settings == Settings.EMPTY) {
        lowLevelRequest.setJsonEntity("""{
                                       "connection" : "${connection}",
                                       "name" : "${patternName}",
                                       "pattern": "${pattern}",
                                       "assume_roles": {
                                        "remote_cluster_role": "${assumeRoles.remoteClusterRole}",
                                        "local_cluster_role": "${assumeRoles.localClusterRole}"
                                       }
                                     }""")
    } else {
        lowLevelRequest.setJsonEntity("""{
                                       "connection" : "${connection}",
                                       "name" : "${patternName}",
                                       "pattern": "${pattern}",
                                       "assume_roles": {
                                        "remote_cluster_role": "${assumeRoles.remoteClusterRole}",
                                        "local_cluster_role": "${assumeRoles.localClusterRole}"
                                       },
                                       "settings": $settings
                                     }""")
    }
    val lowLevelResponse = lowLevelClient.performRequest(lowLevelRequest)
    val response = getAckResponse(lowLevelResponse)
    assertThat(response.isAcknowledged).isTrue()
}

fun RestHighLevelClient.deleteAutoFollowPattern(connection: String, patternName: String) {
    val lowLevelRequest = Request("DELETE", REST_AUTO_FOLLOW_PATTERN)
    lowLevelRequest.setJsonEntity("""{
                                       "connection" : "${connection}",
                                       "name" : "${patternName}"
                                     }""")
    val lowLevelResponse = lowLevelClient.performRequest(lowLevelRequest)
    val response = getAckResponse(lowLevelResponse)
    assertThat(response.isAcknowledged).isTrue()
}
