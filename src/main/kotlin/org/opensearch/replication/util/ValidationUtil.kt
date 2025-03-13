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

package org.opensearch.replication.util

import org.apache.logging.log4j.LogManager
import org.opensearch.ResourceNotFoundException
import org.opensearch.Version
import org.opensearch.action.admin.cluster.node.info.NodeInfo
import org.opensearch.action.admin.cluster.node.info.NodesInfoResponse
import org.opensearch.action.admin.cluster.node.info.PluginsAndModules
import org.opensearch.cluster.ClusterState
import org.opensearch.cluster.metadata.IndexMetadata
import org.opensearch.cluster.metadata.MetadataCreateIndexService
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.ValidationException
import org.opensearch.common.settings.Settings
import org.opensearch.core.common.Strings
import org.opensearch.env.Environment
import org.opensearch.index.IndexNotFoundException
import org.opensearch.node.Node
import org.opensearch.node.remotestore.RemoteStoreNodeAttribute
import org.opensearch.node.remotestore.RemoteStoreNodeService
import org.opensearch.plugins.PluginInfo
import java.io.UnsupportedEncodingException
import java.nio.file.Files
import java.nio.file.Path
import java.util.*
import java.util.function.Predicate

object ValidationUtil {

    private val log = LogManager.getLogger(ValidationUtil::class.java)

    fun validateIndexSettings(
        environment: Environment,
        followerIndex: String,
        leaderSettings: Settings,
        overriddenSettings: Settings,
        metadataCreateIndexService: MetadataCreateIndexService
    ) {
        val settingsList = arrayOf(leaderSettings, overriddenSettings)
        val desiredSettingsBuilder = Settings.builder()
        // Desired settings are taking leader Settings and then overriding them with desired settings
        for (settings in settingsList) {
            for (key in settings.keySet()) {
                desiredSettingsBuilder.copy(key, settings);
            }
        }
        val desiredSettings = desiredSettingsBuilder.build()

        metadataCreateIndexService.validateIndexSettings(followerIndex,desiredSettings, false)
        validateAnalyzerSettings(environment, leaderSettings, overriddenSettings)
    }

    fun validateAnalyzerSettings(environment: Environment, leaderSettings: Settings, overriddenSettings: Settings) {
        val analyserSettings = leaderSettings.filter { k: String? -> k!!.matches(Regex("index.analysis.*path")) }
        for (analyserSetting in analyserSettings.keySet()) {
            val settingValue = if (overriddenSettings.hasValue(analyserSetting)) overriddenSettings.get(analyserSetting) else analyserSettings.get(analyserSetting)
            val path: Path = environment.configDir().resolve(settingValue)
            if (!Files.exists(path)) {
                val message = "IOException while reading ${analyserSetting}: ${path.toString()}"
                log.error(message)
                throw ResourceNotFoundException(message)
            }
        }
    }

    /**
     * Validate the name against the rules that we have for index name.
     */
    fun validateName(name: String, validationException: ValidationException) {
        if (name.toLowerCase(Locale.ROOT) != name)
            validationException.addValidationError("Value $name must be lowercase")

        if (!Strings.validFileName(name))
            validationException.addValidationError("Value $name must not contain the following characters ${Strings.INVALID_FILENAME_CHARS}")

        if (name.contains("#") || name.contains(":"))
            validationException.addValidationError("Value $name must not contain '#' or ':'")

        if (name == "." || name == "..")
            validationException.addValidationError("Value $name must not be '.' or '..'")

        if (name.startsWith('_') || name.startsWith('-') || name.startsWith('+'))
            validationException.addValidationError("Value $name must not start with '_' or '-' or '+'")

        try {
            var byteCount = name.toByteArray(charset("UTF-8")).size
            if (byteCount > MetadataCreateIndexService.MAX_INDEX_NAME_BYTES) {
                validationException.addValidationError("Value $name must not be longer than ${MetadataCreateIndexService.MAX_INDEX_NAME_BYTES} bytes")
            }
        } catch (e: UnsupportedEncodingException) {
            // UTF-8 should always be supported, but rethrow this if it is not for some reason
            validationException.addValidationError("Unable to determine length of $name")
        }

        // Additionally we don't allow replication for system indices i.e. starts with '.'
        if(name.startsWith("."))
            validationException.addValidationError("Value $name must not start with '.'")
    }

    /**
     * Validate the pattern against the rules that we have for indexPattern name.
     */

    fun validatePattern(pattern: String?, validationException: ValidationException) {

        if (!Strings.validFileNameExcludingAstrix(pattern))
            validationException.addValidationError("Autofollow pattern: $pattern must not contain the following characters ${Strings.INVALID_FILENAME_CHARS}")

        if (pattern.isNullOrEmpty() == true)
            validationException.addValidationError("Autofollow pattern: $pattern must not be empty")

        if ((pattern?.contains("#") ?: false)|| (pattern?.contains(":") ?: false))
            validationException.addValidationError("Autofollow pattern: $pattern must not contain '#' or ':'")

        if ((pattern?.startsWith('_') ?: false) || (pattern?.startsWith('-') ?: false))
            validationException.addValidationError("Autofollow pattern: $pattern must not start with '_' or '-'")

    }

    /**
     * validate leader index version for compatibility
     * If on higher version - Replication will not be allowed
     *  - Upgrade path - Upgrade Follower cluster to higher version
     *    and then upgrade leader
     * Most of the settings are backward compatible and not forward compatible
     * This ensures that clusters upgrade in the above order for the existing cross cluster
     * connections
     */
    private fun validateLeaderIndexMetadata(leaderIndexMetadata: IndexMetadata) {
        if(Version.CURRENT.before(leaderIndexMetadata.creationVersion)) {
            val err = "Leader index[${leaderIndexMetadata.index.name}] is on " +
                    "higher version [${leaderIndexMetadata.creationVersion}] than follower [${Version.CURRENT}]"
            log.error(err)
            throw IllegalArgumentException(err)
        }
        if(Version.CURRENT.before(leaderIndexMetadata.upgradedVersion)) {
            val err = "Leader index[${leaderIndexMetadata.index.name}] is upgraded with " +
                    "higher version [${leaderIndexMetadata.upgradedVersion}] than follower [${Version.CURRENT}]"
            log.error(err)
            throw IllegalArgumentException(err)
        }
    }

    /**
     * validate leader index state - version and shard routing, based on leader cluster state
     */
    fun validateLeaderIndexState(leaderAlias: String, leaderIndex: String, leaderClusterState: ClusterState) {
        val leaderIndexMetadata = leaderClusterState.metadata.index(leaderIndex) ?: throw IndexNotFoundException("${leaderAlias}:${leaderIndex}")
        // validate index metadata
        validateLeaderIndexMetadata(leaderIndexMetadata)

        // validate index shard state - All primary shards should be active
        if(!leaderClusterState.routingTable.index(leaderIndex).allPrimaryShardsActive()) {
            val validationException = ValidationException()
            validationException.addValidationError("Primary shards in the Index[${leaderAlias}:${leaderIndex}] are not active")
            throw validationException
        }
    }

    /**
     * Throw exception if leader index is knn a knn is not installed
     */
    fun checkKNNEligibility(nodesInfoResponse: NodesInfoResponse, leaderIndex: String) {
        if(!(nodesInfoResponse.getNodes().stream().flatMap {
                nodeInfo: NodeInfo ->
            nodeInfo.getInfo(
                PluginsAndModules::class.java
            ).pluginInfos.stream()
        }.anyMatch( { pluginInfo: PluginInfo -> pluginInfo.classname == "org.opensearch.knn.plugin.KNNPlugin" }))) {
            throw IllegalStateException("Cannot proceed with replication for k-NN enabled index ${leaderIndex} as knn plugin is not installed.")
        }
    }

    fun isRemoteStoreEnabledCluster(clusterService: ClusterService): Boolean {
        return clusterService.settings.getByPrefix(Node.NODE_ATTRIBUTES.key + RemoteStoreNodeAttribute.REMOTE_STORE_NODE_ATTRIBUTE_KEY_PREFIX).isEmpty == false
    }

    fun isRemoteEnabledOrMigrating(clusterService: ClusterService): Boolean {
        return isRemoteStoreEnabledCluster(clusterService) ||
                clusterService.clusterSettings.get(RemoteStoreNodeService.REMOTE_STORE_COMPATIBILITY_MODE_SETTING).equals(RemoteStoreNodeService.CompatibilityMode.MIXED)
    }
}
