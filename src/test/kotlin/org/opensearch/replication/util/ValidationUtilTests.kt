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

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.whenever
import org.assertj.core.api.Assertions.assertThat
import org.opensearch.cluster.service.ClusterService
import org.opensearch.common.settings.ClusterSettings
import org.opensearch.common.settings.Settings
import org.opensearch.common.ValidationException
import org.opensearch.test.OpenSearchTestCase

@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
class ValidationUtilTests : OpenSearchTestCase() {

    fun testIsRemoteEnabledOrMigrating_returnsTrueWhenRemoteStoreDataAttributesPresent() {
        val settings = Settings.builder()
            .put("node.attr.remote_store.segment.repository", "my-segment-repo")
            .put("node.attr.remote_store.translog.repository", "my-translog-repo")
            .build()

        val clusterService = mockClusterService(settings)

        assertThat(ValidationUtil.isRemoteEnabledOrMigrating(clusterService)).isTrue()
    }

    fun testIsRemoteEnabledOrMigrating_returnsFalseWhenNoRemoteStoreAttributes() {
        val settings = Settings.builder()
            .put("node.attr.zone", "us-east-1a")
            .build()

        val clusterService = mockClusterService(settings)

        assertThat(ValidationUtil.isRemoteEnabledOrMigrating(clusterService)).isFalse()
    }

    fun testIsRemoteEnabledOrMigrating_returnsTrueWhenOnlySegmentRepoPresent() {
        val settings = Settings.builder()
            .put("node.attr.remote_store.segment.repository", "my-segment-repo")
            .build()

        val clusterService = mockClusterService(settings)

        assertThat(ValidationUtil.isRemoteEnabledOrMigrating(clusterService)).isTrue()
    }

    fun testIsRemoteEnabledOrMigrating_returnsTrueWhenOnlyTranslogRepoPresent() {
        val settings = Settings.builder()
            .put("node.attr.remote_store.translog.repository", "my-translog-repo")
            .build()

        val clusterService = mockClusterService(settings)

        assertThat(ValidationUtil.isRemoteEnabledOrMigrating(clusterService)).isTrue()
    }

    private fun mockClusterService(settings: Settings): ClusterService {
        val clusterSettings = ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        val clusterService: ClusterService = mock()
        whenever(clusterService.settings).thenReturn(settings)
        whenever(clusterService.clusterSettings).thenReturn(clusterSettings)
        return clusterService
    }

    fun testValidateFollowerIndexPattern_nullIsValid() {
        val validationException = ValidationException()
        ValidationUtil.validateFollowerIndexPattern(null, validationException)
        assertThat(validationException.validationErrors()).isEmpty()
    }

    fun testValidateFollowerIndexPattern_validPatternPasses() {
        val validationException = ValidationException()
        ValidationUtil.validateFollowerIndexPattern("{{leader_index}}-replica", validationException)
        assertThat(validationException.validationErrors()).isEmpty()
    }

    fun testValidateFollowerIndexPattern_missingPlaceholderIsInvalid() {
        val validationException = ValidationException()
        ValidationUtil.validateFollowerIndexPattern("static-name", validationException)
        assertThat(validationException.validationErrors()).isNotEmpty()
        assertThat(validationException.validationErrors()[0]).contains("{{leader_index}}")
    }

    fun testValidateFollowerIndexPattern_invalidCharsInStaticPortion() {
        val validationException = ValidationException()
        ValidationUtil.validateFollowerIndexPattern("{{leader_index}}:invalid", validationException)
        assertThat(validationException.validationErrors()).isNotEmpty()
    }

    fun testValidateFollowerIndexPattern_prefixWithDotIsInvalid() {
        val validationException = ValidationException()
        ValidationUtil.validateFollowerIndexPattern(".{{leader_index}}-replica", validationException)
        assertThat(validationException.validationErrors()).isNotEmpty()
    }
}
