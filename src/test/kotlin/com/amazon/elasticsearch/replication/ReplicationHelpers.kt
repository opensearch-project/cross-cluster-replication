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
import org.assertj.core.api.Assertions.assertThat
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksRequest
import org.elasticsearch.action.support.master.AcknowledgedResponse
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
import java.util.concurrent.TimeUnit

data class StartReplicationRequest(val remoteClusterAlias: String, val remoteIndex: String, val toIndex: String)

const val REST_REPLICATION_PREFIX = "/_plugins/_replication/"
const val REST_REPLICATION_START = "$REST_REPLICATION_PREFIX{index}/_start"
const val REST_REPLICATION_STOP = "$REST_REPLICATION_PREFIX{index}/_stop"
const val REST_REPLICATION_PAUSE = "$REST_REPLICATION_PREFIX{index}/_pause"
const val REST_REPLICATION_RESUME = "$REST_REPLICATION_PREFIX{index}/_resume"
const val REST_REPLICATION_UPDATE = "$REST_REPLICATION_PREFIX{index}/_update"
const val REST_REPLICATION_STATUS = "$REST_REPLICATION_PREFIX{index}/_status"
const val REST_AUTO_FOLLOW_PATTERN = "${REST_REPLICATION_PREFIX}_autofollow"

fun RestHighLevelClient.startReplication(request: StartReplicationRequest,
                                         waitFor: TimeValue = TimeValue.timeValueSeconds(10),
                                         waitForShardsInit: Boolean = true,
                                         waitForRestore: Boolean = false) {
    val lowLevelRequest = Request("PUT", REST_REPLICATION_START.replace("{index}", request.toIndex, true)
            + "?wait_for_restore=${waitForRestore}")
    lowLevelRequest.setJsonEntity("""{
                                       "remote_cluster" : "${request.remoteClusterAlias}",
                                       "remote_index": "${request.remoteIndex}"
                                     }            
                                  """)
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

fun RestHighLevelClient.replicationStatus(index: String) : Map<String, Any> {
    val lowLevelStopRequest = Request("GET", REST_REPLICATION_STATUS.replace("{index}", index,true))
    lowLevelStopRequest.setJsonEntity("{}")
    val lowLevelStatusResponse = lowLevelClient.performRequest(lowLevelStopRequest)
    val statusResponse: Map<String, Any> = ESRestTestCase.entityAsMap(lowLevelStatusResponse)
    return statusResponse
}

fun `validate status syncing resposne`(statusResp: Map<String, Any>) {
    Assert.assertEquals(statusResp.getValue("status"),"SYNCING")
    Assert.assertTrue((statusResp.getValue("replication_data")).toString().contains("syncing_task_details"))
    Assert.assertTrue((statusResp.getValue("replication_data")).toString().contains("local_checkpoint"))
    Assert.assertTrue((statusResp.getValue("replication_data")).toString().contains("remote_checkpoint"))
}

fun `validate not paused status resposne`(statusResp: Map<String, Any>) {
    Assert.assertNotEquals(statusResp.getValue("status"),"PAUSED")
    Assert.assertTrue((statusResp.getValue("replication_data")).toString().contains("syncing_task_details"))
    Assert.assertTrue((statusResp.getValue("replication_data")).toString().contains("local_checkpoint"))
    Assert.assertTrue((statusResp.getValue("replication_data")).toString().contains("remote_checkpoint"))
}

fun `validate paused status resposne`(statusResp: Map<String, Any>) {
    Assert.assertEquals(statusResp.getValue("status"),"PAUSED")
    Assert.assertTrue((statusResp.getValue("replication_data")).toString().contains("syncing_task_details"))
    Assert.assertTrue((statusResp.getValue("replication_data")).toString().contains("local_checkpoint"))
    Assert.assertTrue((statusResp.getValue("replication_data")).toString().contains("remote_checkpoint"))
}

fun RestHighLevelClient.stopReplication(index: String, shouldWait: Boolean = true) {
    val lowLevelStopRequest = Request("POST", REST_REPLICATION_STOP.replace("{index}", index,true))
    lowLevelStopRequest.setJsonEntity("{}")
    val lowLevelStopResponse = lowLevelClient.performRequest(lowLevelStopRequest)
    val response = getAckResponse(lowLevelStopResponse)
    assertThat(response.isAcknowledged).withFailMessage("Replication could not be stopped").isTrue()
    if (shouldWait) waitForReplicationStop(index)
}

fun RestHighLevelClient.pauseReplication(index: String) {
    val lowLevelStopRequest = Request("POST", REST_REPLICATION_PAUSE.replace("{index}", index,true))
    lowLevelStopRequest.setJsonEntity("{}")
    val lowLevelStopResponse = lowLevelClient.performRequest(lowLevelStopRequest)
    val response = getAckResponse(lowLevelStopResponse)
    assertThat(response.isAcknowledged).withFailMessage("Replication could not be stopped").isTrue()
    waitForReplicationStop(index)
}

fun RestHighLevelClient.resumeReplication(index: String) {
    val lowLevelStopRequest = Request("POST", REST_REPLICATION_RESUME.replace("{index}", index, true))
    lowLevelStopRequest.setJsonEntity("{}")
    val lowLevelStopResponse = lowLevelClient.performRequest(lowLevelStopRequest)
    val response = getAckResponse(lowLevelStopResponse)
    assertThat(response.isAcknowledged).withFailMessage("Replication could not be Resumed").isTrue()
    waitForReplicationStart(index, TimeValue.timeValueSeconds(10))
}

fun RestHighLevelClient.updateReplication(index: String, settings: Settings) {
    val lowLevelRequest = Request("PUT", REST_REPLICATION_UPDATE.replace("{index}", index,true))
    lowLevelRequest.setJsonEntity(settings.toString())
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

fun RestHighLevelClient.updateAutoFollowPattern(connection: String, patternName: String, pattern: String) {
    val lowLevelRequest = Request("POST", REST_AUTO_FOLLOW_PATTERN)
    lowLevelRequest.setJsonEntity("""{
                                       "connection" : "${connection}",
                                       "name" : "${patternName}",
                                       "pattern": "${pattern}"
                                     }""")
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


