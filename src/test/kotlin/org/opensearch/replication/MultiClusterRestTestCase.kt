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

package org.opensearch.replication

import org.opensearch.replication.MultiClusterAnnotations.ClusterConfiguration
import org.opensearch.replication.MultiClusterAnnotations.ClusterConfigurations
import org.opensearch.replication.MultiClusterAnnotations.getAnnotationsFromClass
import org.apache.http.Header
import org.apache.http.HttpHost
import org.apache.http.HttpStatus
import org.apache.http.client.config.RequestConfig
import org.apache.http.entity.ContentType
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder
import org.apache.http.message.BasicHeader
import org.apache.http.nio.conn.ssl.SSLIOSessionStrategy
import org.apache.http.nio.entity.NStringEntity
import org.apache.http.ssl.SSLContexts
import org.apache.lucene.util.SetOnce
import org.opensearch.action.admin.cluster.node.tasks.list.ListTasksRequest
import org.opensearch.bootstrap.BootstrapInfo
import org.opensearch.client.*
import org.opensearch.common.Strings
import org.opensearch.common.io.PathUtils
import org.opensearch.common.settings.Settings
import org.opensearch.common.unit.TimeValue
import org.opensearch.common.util.concurrent.ThreadContext
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentType
import org.opensearch.common.xcontent.json.JsonXContent
import org.opensearch.snapshots.SnapshotState
import org.opensearch.tasks.TaskInfo
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.test.OpenSearchTestCase.assertBusy
import org.opensearch.test.rest.OpenSearchRestTestCase
import org.hamcrest.Matchers
import org.junit.After
import org.junit.AfterClass
import org.junit.BeforeClass
import java.nio.file.Files
import java.security.KeyManagementException
import java.security.KeyStore
import java.security.KeyStoreException
import java.security.NoSuchAlgorithmException
import java.security.cert.CertificateException
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

/**
 * This class provides basic support of managing life-cyle of
 * multiple clusters defined as part of ES build.
 */
abstract class MultiClusterRestTestCase : OpenSearchTestCase() {

    class TestCluster(clusterName: String, val httpHosts: List<HttpHost>, val transportPorts: List<String>,
                      val preserveSnapshots: Boolean, val preserveIndices: Boolean,
                      val preserveClusterSettings: Boolean) {
        val restClient : RestHighLevelClient
        init {
            val builder = RestClient.builder(*httpHosts.toTypedArray())
            configureClient(builder, getClusterSettings(clusterName))
            builder.setStrictDeprecationMode(true)
            restClient = RestHighLevelClient(builder)
        }
        val lowLevelClient = restClient.lowLevelClient!!
    }

    companion object {
        protected lateinit var testClusters : Map<String, TestCluster>

        private fun createTestCluster(configuration: ClusterConfiguration) : TestCluster {
            val cluster = configuration.clusterName
            val systemProperties = BootstrapInfo.getSystemProperties()
            val httpHostsProp = systemProperties.get("tests.cluster.${cluster}.http_hosts") as String?
            val transportHostsProp = systemProperties.get("tests.cluster.${cluster}.transport_hosts") as String?

            requireNotNull(httpHostsProp) { "Missing http hosts property for cluster: $cluster."}
            requireNotNull(transportHostsProp) { "Missing transport hosts property for cluster: $cluster."}

            val httpHosts = httpHostsProp.split(',').map { HttpHost.create("http://$it") }
            val transportPorts = transportHostsProp.split(',')
            return TestCluster(cluster, httpHosts, transportPorts, configuration.preserveSnapshots,
                               configuration.preserveIndices, configuration.preserveClusterSettings)
        }

        private fun getClusterConfigurations(): List<ClusterConfiguration> {
            val repeatedAnnotation = (getAnnotationsFromClass(getTestClass(),ClusterConfiguration::class.java))
            if (repeatedAnnotation.isNotEmpty()) {
                return repeatedAnnotation
            }

            // Kotlin classes don't support repeatable annotations yet
            val groupedAnnotation = getTestClass().getAnnotationsByType(ClusterConfigurations::class.java)
            return if (groupedAnnotation.isNotEmpty()) {
                groupedAnnotation[0].value.toList()
            } else {
                emptyList()
            }
        }

        @BeforeClass @JvmStatic
        fun setupTestClustersForSuite() {
            testClusters = getClusterConfigurations().associate { it.clusterName to createTestCluster(it) }
        }

        @AfterClass @JvmStatic
        fun cleanUpRestClients() {
            testClusters.values.forEach {
                it.restClient.close()
            }
        }

        protected fun getClusterSettings(clusterName: String): Settings {
            /* The default implementation is to return default settings from [OpenSearchRestTestCase].
            * This method can be overridden in base classes to allow different settings
            * for specific cluster. */
            val builder = Settings.builder()
            if (System.getProperty("tests.rest.client_path_prefix") != null) {
                builder.put(OpenSearchRestTestCase.CLIENT_PATH_PREFIX, System.getProperty("tests.rest.client_path_prefix"))
            }
            return builder.build()
        }

        /* Copied this method from [ESRestCase] */
        protected fun configureClient(builder: RestClientBuilder, settings: Settings) {
            val keystorePath = settings[OpenSearchRestTestCase.TRUSTSTORE_PATH]
            if (keystorePath != null) {
                val keystorePass = settings[OpenSearchRestTestCase.TRUSTSTORE_PASSWORD]
                    ?: throw IllegalStateException(OpenSearchRestTestCase.TRUSTSTORE_PATH
                                                       + " is provided but not " + OpenSearchRestTestCase.TRUSTSTORE_PASSWORD)
                val path = PathUtils.get(keystorePath)
                check(
                    Files.exists(path)) { OpenSearchRestTestCase.TRUSTSTORE_PATH + " is set but points to a non-existing file" }
                try {
                    val keyStoreType = if (keystorePath.endsWith(".p12")) "PKCS12" else "jks"
                    val keyStore = KeyStore.getInstance(keyStoreType)
                    Files.newInputStream(path).use { `is` -> keyStore.load(`is`, keystorePass.toCharArray()) }
                    val sslcontext = SSLContexts.custom().loadTrustMaterial(keyStore, null).build()
                    val sessionStrategy = SSLIOSessionStrategy(sslcontext)
                    builder.setHttpClientConfigCallback { httpClientBuilder: HttpAsyncClientBuilder ->
                        httpClientBuilder.setSSLStrategy(sessionStrategy)
                    }
                } catch (e: KeyStoreException) {
                    throw RuntimeException("Error setting up ssl", e)
                } catch (e: NoSuchAlgorithmException) {
                    throw RuntimeException("Error setting up ssl", e)
                } catch (e: KeyManagementException) {
                    throw RuntimeException("Error setting up ssl", e)
                } catch (e: CertificateException) {
                    throw RuntimeException("Error setting up ssl", e)
                }
            }
            val headers = ThreadContext.buildDefaultHeaders(settings)
            val defaultHeaders = arrayOfNulls<Header>(headers.size)
            var i = 0
            for ((key, value) in headers) {
                defaultHeaders[i++] = BasicHeader(key, value)
            }
            builder.setDefaultHeaders(defaultHeaders)
            val socketTimeoutString = settings[OpenSearchRestTestCase.CLIENT_SOCKET_TIMEOUT]
            val socketTimeout = TimeValue.parseTimeValue(socketTimeoutString ?: "60s",
                                                         OpenSearchRestTestCase.CLIENT_SOCKET_TIMEOUT)
            builder.setRequestConfigCallback { conf: RequestConfig.Builder ->
                conf.setSocketTimeout(
                    Math.toIntExact(socketTimeout.millis))
            }
            if (settings.hasValue(OpenSearchRestTestCase.CLIENT_PATH_PREFIX)) {
                builder.setPathPrefix(settings[OpenSearchRestTestCase.CLIENT_PATH_PREFIX])
            }
        }
    }

    @After
    fun wipeClusters() {
        testClusters.values.forEach { wipeCluster(it) }
    }

    private fun wipeCluster(testCluster: TestCluster) {
        if (!testCluster.preserveSnapshots) waitForSnapshotWiping(testCluster)
        if (!testCluster.preserveIndices) wipeIndicesFromCluster(testCluster)
        if (!testCluster.preserveClusterSettings) wipeClusterSettings(testCluster)
    }

    private fun waitForSnapshotWiping(testCluster: TestCluster) {
        val inProgressSnapshots = SetOnce<Map<String, List<Map<*, *>>>>()
        val snapshots = AtomicReference<Map<String, List<Map<*, *>>>>()
        try {
            // Repeatedly delete the snapshots until there aren't any
            assertBusy({
                           snapshots.set(_wipeSnapshots(testCluster))
                           assertThat(snapshots.get(), Matchers.anEmptyMap())
                       }, 2, TimeUnit.MINUTES)
            // At this point there should be no snaphots
            inProgressSnapshots.set(snapshots.get())
        } catch (e: AssertionError) {
            // This will cause an error at the end of this method, but do the rest of the cleanup first
            inProgressSnapshots.set(snapshots.get())
        } catch (e: Exception) {
            inProgressSnapshots.set(snapshots.get())
        }
    }

    protected fun wipeClusterSettings(testCluster: TestCluster) {
        val getResponse: Map<String, Any> = OpenSearchRestTestCase.entityAsMap(testCluster.lowLevelClient.performRequest(
            Request("GET", "/_cluster/settings")))
        var mustClear = false
        val clearCommand = JsonXContent.contentBuilder()
        clearCommand.startObject()
        for ((key1, value) in getResponse) {
            val type = key1
            val settings = value as Map<*, *>
            if (settings.isEmpty()) {
                continue
            }
            mustClear = true
            clearCommand.startObject(type)
            for (key in settings.keys) {
                clearCommand.field("$key.*").nullValue()
            }
            clearCommand.endObject()
        }
        clearCommand.endObject()
        if (mustClear) {
            val request = Request("PUT", "/_cluster/settings")
            request.setJsonEntity(Strings.toString(clearCommand))
            testCluster.lowLevelClient.performRequest(request)
        }
    }

    protected fun wipeIndicesFromCluster(testCluster: TestCluster) {
        try {
            val deleteRequest = Request("DELETE", "*,-.*") // All except system indices
            val response = testCluster.lowLevelClient.performRequest(deleteRequest)
            response.entity.content.use { `is` ->
                assertTrue(
                    XContentHelper.convertToMap(XContentType.JSON.xContent(), `is`, true)["acknowledged"] as Boolean)
            }
        } catch (e: ResponseException) {
            // 404 here just means we had no indexes
            if (e.response.statusLine.statusCode != 404) {
                throw e
            }
        }
    }

    protected fun _wipeSnapshots(testCluster: TestCluster): Map<String, List<Map<String, Any>>> {
        val inProgressSnapshots: MutableMap<String, MutableList<Map<String, Any>>> = mutableMapOf()
        for ((repoName, value) in OpenSearchRestTestCase.entityAsMap(
            testCluster.lowLevelClient.performRequest(Request("GET", "/_snapshot/_all")))) {
            val repoSpec = value as Map<*, *>
            val repoType = repoSpec["type"] as String
            if (repoType == "fs") {
                // All other repo types we really don't have a chance of being able to iterate properly, sadly.
                val listRequest = Request("GET", "/_snapshot/$repoName/_all")
                listRequest.addParameter("ignore_unavailable", "true")
                val snapshots = OpenSearchRestTestCase.entityAsMap(
                    testCluster.lowLevelClient.performRequest(listRequest))["snapshots"] as List<Map<String, Any>>
                for (snapshot in snapshots) {
                    val snapshotInfo = snapshot
                    val name = snapshotInfo["snapshot"] as String?
                    if (!SnapshotState.valueOf((snapshotInfo["state"] as String?)!!).completed()) {
                        inProgressSnapshots.computeIfAbsent(repoName) { mutableListOf() }
                            .add(snapshotInfo)
                    }
                    logger.debug("wiping snapshot [{}/{}]", repoName, name)
                    testCluster.lowLevelClient.performRequest(Request(
                        "DELETE", "/_snapshot/$repoName/$name"))
                }
            }
            deleteRepository(testCluster, repoName)
        }
        return inProgressSnapshots
    }

    protected fun deleteRepository(testCluster: TestCluster, repoName: String) {
        testCluster.lowLevelClient.performRequest(Request("DELETE", "_snapshot/$repoName"))
    }

    fun getNamedCluster(clusterName: String): TestCluster {
        return testClusters[clusterName] ?: error("""Given clusterName:$clusterName was not found.
               |Please confirm if it is defined in build.gradle file and included in clusterConfiguration 
               |annotation in test class.""".trimMargin())
    }

    fun getClientForCluster(clusterName: String): RestHighLevelClient {
        return getNamedCluster(clusterName).restClient
    }

    fun getAsMap(client: RestClient, endpoint: String): Map<String, Any> {
        return OpenSearchRestTestCase.entityAsMap(client.performRequest(Request("GET", endpoint)))
    }

    protected fun createConnectionBetweenClusters(fromClusterName: String, toClusterName: String, connectionName: String="source") {
        val toCluster = getNamedCluster(toClusterName)
        val fromCluster = getNamedCluster(fromClusterName)
        val persistentConnectionRequest = Request("PUT", "_cluster/settings")
        val toClusterHostSeed = toCluster.transportPorts[0]
        val entityAsString = """
                        {
                          "persistent": {
                             "cluster": {
                               "remote": {
                                 "$connectionName": {
                                   "seeds": [ "$toClusterHostSeed" ]
                                 }
                               }
                             }
                          }
                        }""".trimMargin()

        persistentConnectionRequest.entity = NStringEntity(entityAsString, ContentType.APPLICATION_JSON)
        val persistentConnectionResponse = fromCluster.lowLevelClient.performRequest(persistentConnectionRequest)
        assertEquals(HttpStatus.SC_OK.toLong(), persistentConnectionResponse.statusLine.statusCode.toLong())
    }

    fun getReplicationTaskList(clusterName: String, action: String="*replication*"): List<TaskInfo> {
        val client = getClientForCluster(clusterName)
        val request = ListTasksRequest().setDetailed(true).setActions(action)
        val response = client.tasks().list(request,RequestOptions.DEFAULT)
        return response.tasks
    }
}