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

package org.opensearch.replication

import org.opensearch.commons.replication.action.ReplicationActions.STOP_REPLICATION_ACTION_NAME
import org.opensearch.replication.MultiClusterAnnotations.ClusterConfiguration
import org.opensearch.replication.MultiClusterAnnotations.ClusterConfigurations
import org.opensearch.replication.MultiClusterAnnotations.getAnnotationsFromClass
import org.opensearch.replication.integ.rest.FOLLOWER
import org.apache.hc.core5.http.Header
import org.apache.hc.core5.http.HttpHost
import org.apache.hc.core5.http.HttpStatus
import org.apache.hc.client5.http.config.RequestConfig
import org.apache.hc.core5.http.ContentType
import org.apache.hc.client5.http.impl.async.HttpAsyncClientBuilder
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManagerBuilder
import org.apache.hc.client5.http.ssl.ClientTlsStrategyBuilder
import org.apache.hc.core5.http.message.BasicHeader
import org.apache.hc.core5.http.io.entity.StringEntity
import org.apache.hc.core5.ssl.SSLContexts
import org.apache.hc.core5.http.io.entity.EntityUtils
import org.apache.hc.core5.http2.HttpVersionPolicy
import org.apache.hc.core5.util.Timeout
import org.apache.lucene.util.SetOnce
import org.opensearch.action.admin.cluster.node.tasks.list.ListTasksRequest
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest
import org.opensearch.bootstrap.BootstrapInfo
import org.opensearch.client.Request
import org.opensearch.client.RequestOptions
import org.opensearch.client.ResponseException
import org.opensearch.client.RestClient
import org.opensearch.client.RestClientBuilder
import org.opensearch.client.RestHighLevelClient
import org.opensearch.common.io.PathUtils
import org.opensearch.common.settings.Settings
import org.opensearch.common.unit.TimeValue
import org.opensearch.common.util.concurrent.ThreadContext
import org.opensearch.core.xcontent.DeprecationHandler
import org.opensearch.core.xcontent.NamedXContentRegistry
import org.opensearch.common.xcontent.XContentHelper
import org.opensearch.common.xcontent.XContentType
import org.opensearch.common.xcontent.json.JsonXContent
import org.opensearch.snapshots.SnapshotState
import org.opensearch.tasks.TaskInfo
import org.opensearch.test.OpenSearchTestCase
import org.opensearch.test.rest.OpenSearchRestTestCase
import org.hamcrest.Matchers
import org.junit.After
import org.junit.AfterClass
import org.junit.Before
import org.junit.BeforeClass
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.security.KeyManagementException
import java.security.KeyStore
import java.security.KeyStoreException
import java.security.NoSuchAlgorithmException
import java.security.cert.CertificateException
import java.util.Base64
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import java.util.Collections
import javax.net.ssl.SSLContext
import javax.net.ssl.TrustManager
import javax.net.ssl.X509TrustManager

/**
 * This class provides basic support of managing life-cyle of
 * multiple clusters defined as part of ES build.
 */
abstract class MultiClusterRestTestCase : OpenSearchTestCase() {

    class TestCluster(clusterName: String, val httpHosts: List<HttpHost>, val transportPorts: List<String>,
                      val preserveSnapshots: Boolean, val preserveIndices: Boolean,
                      val preserveClusterSettings: Boolean,
                      val securityEnabled: Boolean) {
        val restClient : RestHighLevelClient
        private val connectionManager: org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManager
        init {
            val trustCerts = arrayOf<TrustManager>(object: X509TrustManager {
                override fun checkClientTrusted(chain: Array<out java.security.cert.X509Certificate>?, authType: String?) {
                }

                override fun checkServerTrusted(chain: Array<out java.security.cert.X509Certificate>?, authType: String?) {
                }

                override fun getAcceptedIssuers(): Array<out java.security.cert.X509Certificate>? {
                    return null
                }

            })
            val sslContext = SSLContext.getInstance("SSL")
            sslContext.init(null, trustCerts, java.security.SecureRandom())
            val tlsStrategy = ClientTlsStrategyBuilder.create().setSslContext(sslContext)
                .setHostnameVerifier { _, _ -> true } // Disable hostname verification for local cluster
                .build()
            connectionManager = PoolingAsyncClientConnectionManagerBuilder.create().setTlsStrategy(tlsStrategy).build()

            val builder = RestClient.builder(*httpHosts.toTypedArray()).setHttpClientConfigCallback { httpAsyncClientBuilder ->
                httpAsyncClientBuilder.setConnectionManager(connectionManager)
                httpAsyncClientBuilder.setVersionPolicy(HttpVersionPolicy.FORCE_HTTP_1)
            }
            configureClient(builder, getClusterSettings(clusterName), securityEnabled)
            builder.setStrictDeprecationMode(false)
            restClient = RestHighLevelClient(builder)
        }
        val lowLevelClient = restClient.lowLevelClient!!

        fun close() {
            restClient.close()
            connectionManager.close()
        }

        var defaultSecuritySetupCompleted = false
        companion object {
            const val FS_SNAPSHOT_REPO = "repl_repo"
        }
    }

    companion object {
        lateinit var testClusters : Map<String, TestCluster>
        var isSecurityPropertyEnabled = false
        var forceInitSecurityConfiguration = false

        internal fun createTestCluster(configuration: ClusterConfiguration) : TestCluster {
            return createTestCluster(configuration.clusterName, configuration.preserveSnapshots, configuration.preserveIndices,
                configuration.preserveClusterSettings, configuration.forceInitSecurityConfiguration)
        }

        internal fun createTestCluster(cluster: String, preserveSnapshots: Boolean, preserveIndices: Boolean,
                                       preserveClusterSettings: Boolean, initSecurityConfiguration: Boolean) : TestCluster {
            val systemProperties = BootstrapInfo.getSystemProperties()
            val httpHostsProp = systemProperties.get("tests.cluster.${cluster}.http_hosts") as String?
            val transportHostsProp = systemProperties.get("tests.cluster.${cluster}.transport_hosts") as String?
            val securityEnabled = systemProperties.get("tests.cluster.${cluster}.security_enabled") as String?

            requireNotNull(httpHostsProp) { "Missing http hosts property for cluster: $cluster."}
            requireNotNull(transportHostsProp) { "Missing transport hosts property for cluster: $cluster."}
            requireNotNull(securityEnabled) { "Missing security enabled property for cluster: $cluster."}

            var protocol = "http"
            if(securityEnabled.equals("true", true)) {
                protocol = "https"
                isSecurityPropertyEnabled = true
            }


            forceInitSecurityConfiguration = isSecurityPropertyEnabled && initSecurityConfiguration

            val httpHosts = httpHostsProp.split(',').map { HttpHost.create("$protocol://$it") }
            val transportPorts = transportHostsProp.split(',')
            return TestCluster(cluster, httpHosts, transportPorts, preserveSnapshots,
                               preserveIndices, preserveClusterSettings, securityEnabled.equals("true", true))
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
                it.close()
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
        protected fun configureClient(builder: RestClientBuilder, settings: Settings, securityEnabled: Boolean) {
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
                    val tlsStrategy = ClientTlsStrategyBuilder.create().setSslContext(sslcontext).build()
                    val connManager = PoolingAsyncClientConnectionManagerBuilder.create().setTlsStrategy(tlsStrategy).build()

                    builder.setHttpClientConfigCallback { httpAsyncClientBuilder: HttpAsyncClientBuilder ->
                        httpAsyncClientBuilder.setConnectionManager(connManager)
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
            var headerSize = headers.size
            if(securityEnabled) {
                headerSize = headers.size + 1
            }
            val defaultHeaders = arrayOfNulls<Header>(headerSize)
            var i = 0
            for ((key, value) in headers) {
                defaultHeaders[i++] = BasicHeader(key, value)
            }

            val creds = System.getProperty("user", "admin") + ":" + System.getProperty("password", "myStrongPassword123!")
            if(securityEnabled) {
                defaultHeaders[i++] = BasicHeader("Authorization", "Basic " + Base64.getEncoder().encodeToString(creds.toByteArray(StandardCharsets.UTF_8)))
            }

            builder.setDefaultHeaders(defaultHeaders)
            builder.setStrictDeprecationMode(false)
            val socketTimeoutString = settings[OpenSearchRestTestCase.CLIENT_SOCKET_TIMEOUT]
            val socketTimeout = TimeValue.parseTimeValue(socketTimeoutString ?: "60s",
                                                         OpenSearchRestTestCase.CLIENT_SOCKET_TIMEOUT)
            builder.setRequestConfigCallback { conf: RequestConfig.Builder ->
                conf.setResponseTimeout(Timeout.ofMilliseconds(socketTimeout.millis))
            }
            if (settings.hasValue(OpenSearchRestTestCase.CLIENT_PATH_PREFIX)) {
                builder.setPathPrefix(settings[OpenSearchRestTestCase.CLIENT_PATH_PREFIX])
            }
        }
    }

    /**
     * Setup for the tests
     */
    @Before
    fun setup() {
        testClusters.values.forEach {
            registerSnapshotRepository(it)
            if(it.securityEnabled && !it.defaultSecuritySetupCompleted)
                setupDefaultSecurityRoles(it)
        }
    }

    /**
     * Register snapshot repo - "fs" type on all the clusters
     */
    private fun registerSnapshotRepository(testCluster: TestCluster) {
        val getResponse: Map<String, Any> = OpenSearchRestTestCase.entityAsMap(testCluster.lowLevelClient.performRequest(
                Request("GET", "/_cluster/settings?include_defaults=true&flat_settings=true")))
        val configuredRepositories = (getResponse["defaults"] as Map<*, *>)["path.repo"] as List<*>
        if(configuredRepositories.isEmpty()) {
            return
        }
        val repo = configuredRepositories[0] as String
        val repoConfig = """
            {
              "type": "fs",
              "settings": {
                "location": "$repo"
              }
            }
        """.trimIndent()
        triggerRequest(testCluster.lowLevelClient, "PUT", "_snapshot/${TestCluster.FS_SNAPSHOT_REPO}", repoConfig)
    }

    /**
     * Setup for default security roles is performed once.
     */
    protected fun setupDefaultSecurityRoles(testCluster: TestCluster) {
        val leaderRoleConfig = """
                {
                    "index_permissions": [
                        {
                            "index_patterns": [
                                "*"
                            ],
                            "allowed_actions": [
                                "indices:admin/plugins/replication/index/setup/validate",
                                "indices:data/read/plugins/replication/changes",
                                "indices:data/read/plugins/replication/file_chunk"
                            ]
                        }
                    ]
                }
            """.trimMargin()

        triggerRequest(testCluster.lowLevelClient, "PUT",
                "_plugins/_security/api/roles/leader_role", leaderRoleConfig)

        val followerRoleConfig = """
                {
                    "cluster_permissions": [
                        "cluster:admin/plugins/replication/autofollow/update"
                    ],
                    "index_permissions": [
                        {
                            "index_patterns": [
                                "*"
                            ],
                            "allowed_actions": [
                                "indices:admin/plugins/replication/index/setup/validate",
                                "indices:data/write/plugins/replication/changes",
                                "indices:admin/plugins/replication/index/start",
                                "indices:admin/plugins/replication/index/pause",
                                "indices:admin/plugins/replication/index/resume",
                                "$STOP_REPLICATION_ACTION_NAME",
                                "indices:admin/plugins/replication/index/update",
                                "indices:admin/plugins/replication/index/status_check"
                            ]
                        }
                    ]
                }
            """.trimMargin()

        triggerRequest(testCluster.lowLevelClient, "PUT",
                "_plugins/_security/api/roles/follower_role", followerRoleConfig)

        val userMapping = """
            {
                "users": [
                    "admin"
                ]
            }
            """.trimMargin()

        triggerRequest(testCluster.lowLevelClient, "PUT",
                "_plugins/_security/api/rolesmapping/leader_role", userMapping)

        triggerRequest(testCluster.lowLevelClient, "PUT",
                "_plugins/_security/api/rolesmapping/follower_role", userMapping)

        testCluster.defaultSecuritySetupCompleted = true
    }

    private fun triggerRequest(client: RestClient, method: String, endpoint: String, reqBody: String) {
        val req = Request(method, endpoint)
        req.entity = StringEntity(reqBody, ContentType.APPLICATION_JSON)
        val res = client.performRequest(req)

        assertTrue(HttpStatus.SC_CREATED.toLong() == res.statusLine.statusCode.toLong() ||
                HttpStatus.SC_OK.toLong() == res.statusLine.statusCode.toLong())
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
            request.setJsonEntity(clearCommand.toString())
            testCluster.lowLevelClient.performRequest(request)
        }
    }
    private fun stopAllReplicationJobs(testCluster: TestCluster) {
        val indicesResponse = testCluster.lowLevelClient.performRequest((Request("GET","/_cat/indices/*,-.*?format=json&pretty")))
        val indicesResponseEntity = EntityUtils.toString(indicesResponse.entity)
        var parser = XContentType.JSON.xContent().createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, indicesResponseEntity)
        parser.list().forEach{ item->
            val str = item.toString()
            val map = str.subSequence(1,str.length-1).split(",").associate {
                val (key, value) = it.trim().split("=")
                key to value
            }
            val ind = map.get("index")
            try {
                val stopRequest = Request("POST","/_plugins/_replication/" + ind.toString() + "/_stop")
                stopRequest.setJsonEntity("{}")
                stopRequest.setOptions(RequestOptions.DEFAULT)
                val response=testCluster.lowLevelClient.performRequest(stopRequest)
            }
            catch (e:ResponseException){
                // 400 = index not being replicated, 500 = internal error (e.g., missing synonym files)
                // Both are acceptable during cleanup - we just want to ensure indices can be deleted
                if(e.response.statusLine.statusCode != 400 && e.response.statusLine.statusCode != 500) {
                    throw e
                }
            }
        }
    }
    protected fun wipeIndicesFromCluster(testCluster: TestCluster) {
        stopAllReplicationJobs(testCluster)
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

    fun getAsList(client: RestClient, endpoint: String): List<Any> {
        return OpenSearchRestTestCase.entityAsList(client.performRequest(Request("GET", endpoint)))
    }

    protected fun deleteConnection(fromClusterName: String, connectionName: String="source") {
        val fromCluster = getNamedCluster(fromClusterName)
        val persistentConnectionRequest = Request("PUT", "_cluster/settings")

        val entityAsString = """
                        {
                          "persistent": {
                             "cluster": {
                               "remote": {
                                 "$connectionName": {
                                   "seeds": null
                                 }
                               }
                             }
                          }
                        }""".trimMargin()

        persistentConnectionRequest.entity = StringEntity(entityAsString, ContentType.APPLICATION_JSON)
        val persistentConnectionResponse = fromCluster.lowLevelClient.performRequest(persistentConnectionRequest)
        assertEquals(HttpStatus.SC_OK.toLong(), persistentConnectionResponse.statusLine.statusCode.toLong())
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

        persistentConnectionRequest.entity = StringEntity(entityAsString, ContentType.APPLICATION_JSON)
        val persistentConnectionResponse = fromCluster.lowLevelClient.performRequest(persistentConnectionRequest)
        assertEquals(HttpStatus.SC_OK.toLong(), persistentConnectionResponse.statusLine.statusCode.toLong())
    }

    protected fun getPrimaryNodeForShard(clusterName: String,indexname: String, shardNumber: String) :String {
        val cluster = getNamedCluster(clusterName)
        val persistentConnectionRequest = Request("GET", "/_cat/shards?format=json")
        val persistentConnectionResponse = cluster.lowLevelClient.performRequest(persistentConnectionRequest)
        val resp = EntityUtils.toString(persistentConnectionResponse.entity);

        var parser = XContentType.JSON.xContent().createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, resp)
        var primaryNode:String = ""
        parser.list().forEach{ item  ->
            val entryValue = item.toString()

            val map = entryValue.subSequence(1,entryValue.length-1).split(",").associate {
                val (key, value) = it.trim().split("=")
                key to value
            }
            if(map.get("shard").equals(shardNumber)
                    && map.get("index").equals(indexname)
                    && map.get("prirep").equals("p")) {
                primaryNode = map.get("node").orEmpty()
            }
        }

        return primaryNode
    }

    protected fun getNodesInCluster(clusterName: String) : List<String>{
        val cluster = getNamedCluster(clusterName)
        val persistentConnectionRequest = Request("GET", "/_cat/nodes?format=json")

        val persistentConnectionResponse = cluster.lowLevelClient.performRequest(persistentConnectionRequest)
        val resp = EntityUtils.toString(persistentConnectionResponse.entity);

        var parser = XContentType.JSON.xContent().createParser(NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, resp)
        var nodes = mutableListOf<String>()
        parser.list().forEach{ item  -> nodes.add((item as HashMap<String, String>)["name"].orEmpty())}
        return nodes
    }

    protected fun rerouteShard(clusterName: String, shardNumber: String, indexName: String, fromNode: String, toNode : String) {
        val cluster = getNamedCluster(clusterName)
        val persistentConnectionRequest = Request("POST", "_cluster/reroute")
        val entityAsString = """
                        {
                          "commands": [{
                             "move": {
                               "index": "$indexName", "shard": $shardNumber,
                               "from_node": "$fromNode", "to_node": "$toNode"
                             }
                          }]
                        }""".trimMargin()

        persistentConnectionRequest.entity = StringEntity(entityAsString, ContentType.APPLICATION_JSON)
        val persistentConnectionResponse = cluster.lowLevelClient.performRequest(persistentConnectionRequest)
        assertEquals(HttpStatus.SC_OK.toLong(), persistentConnectionResponse.statusLine.statusCode.toLong())
    }

    fun getReplicationTaskList(clusterName: String, action: String="*replication*"): List<TaskInfo> {
        val client = getClientForCluster(clusterName)
        val request = ListTasksRequest().setDetailed(true).setActions(action)
        val response = client.tasks().list(request,RequestOptions.DEFAULT)
        return response.tasks
    }

    protected fun insertDocToIndex(clusterName: String, docCount: String, docValue: String, indexName: String) {
        val cluster = getNamedCluster(clusterName)
        val persistentConnectionRequest = Request("PUT", indexName + "/_doc/"+ docCount)
        val entityAsString = """
                        {"value" : "$docValue"}""".trimMargin()

        persistentConnectionRequest.entity = StringEntity(entityAsString, ContentType.APPLICATION_JSON)
        val persistentConnectionResponse = cluster.lowLevelClient.performRequest(persistentConnectionRequest)
        assertEquals(HttpStatus.SC_CREATED.toLong(), persistentConnectionResponse.statusLine.statusCode.toLong())
    }

    protected fun docs(clusterName: String,indexName : String) : String{
        val cluster = getNamedCluster(clusterName)
        val persistentConnectionRequest = Request("GET", "/$indexName/_search?pretty&q=*")

        val persistentConnectionResponse = cluster.lowLevelClient.performRequest(persistentConnectionRequest)
        val resp = EntityUtils.toString(persistentConnectionResponse.entity);
        return resp
    }

    protected fun changeTemplate(clusterName: String) {
        val cluster = getNamedCluster(clusterName)
        val persistentConnectionRequest = Request("PUT", "_template/all")
        val entityAsString = """
                        {"template": "*", "settings": {"number_of_shards": 1, "number_of_replicas": 0}}""".trimMargin()

        persistentConnectionRequest.entity = StringEntity(entityAsString, ContentType.APPLICATION_JSON)
        cluster.lowLevelClient.performRequest(persistentConnectionRequest)
    }

    protected fun setMetadataSyncDelay() {
        val followerClient = getClientForCluster(FOLLOWER)
        val updateSettingsRequest = ClusterUpdateSettingsRequest()
        updateSettingsRequest.transientSettings(Collections.singletonMap<String, String?>(ReplicationPlugin.REPLICATION_METADATA_SYNC_INTERVAL.key, "5s"))
        followerClient.cluster().putSettings(updateSettingsRequest, RequestOptions.DEFAULT)
    }

//    TODO Find a way to skip tests when tests are run for remote clusters
    protected  fun checkifIntegTestRemote(): Boolean {
        val systemProperties = BootstrapInfo.getSystemProperties()
        val integTestRemote = systemProperties.get("tests.integTestRemote") as String?
        return integTestRemote.equals("true")
    }

    protected fun isMultiNodeClusterConfiguration(leaderCluster: String, followerCluster: String): Boolean{
        val systemProperties = BootstrapInfo.getSystemProperties()
        val totalLeaderNodes = systemProperties.get("tests.cluster.${leaderCluster}.total_nodes") as String
        val totalFollowerNodes = systemProperties.get("tests.cluster.${followerCluster}.total_nodes") as String

        assertNotNull(totalLeaderNodes)
        assertNotNull(totalFollowerNodes)
        if(totalLeaderNodes < "2" ||  totalFollowerNodes < "2" ) {
            return false
        }
        return true
    }

    protected fun clusterNodes(clusterName: String): Int {
        val systemProperties = BootstrapInfo.getSystemProperties()
        val totalNodes = systemProperties.get("tests.cluster.${clusterName}.total_nodes") as String?
        return totalNodes?.toIntOrNull() ?: 1
    }

    protected fun docCount(cluster: RestHighLevelClient, indexName: String) : Int {
        val persistentConnectionRequest = Request("GET", "/$indexName/_search?pretty&q=*")

        val persistentConnectionResponse = cluster.lowLevelClient.performRequest(persistentConnectionRequest)
        val statusResponse: Map<String, Map<String, Map<String, Any>>> = OpenSearchRestTestCase.entityAsMap(persistentConnectionResponse) as Map<String, Map<String, Map<String, String>>>
        return statusResponse["hits"]?.get("total")?.get("value") as Int
    }

    protected fun deleteIndex(testCluster: RestHighLevelClient, indexName: String) {
        testCluster.lowLevelClient.performRequest(Request("DELETE", indexName))
    }

}
