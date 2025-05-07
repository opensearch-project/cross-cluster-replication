/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.replication

import org.apache.logging.log4j.LogManager
import org.assertj.core.api.Assertions
import org.opensearch.action.DocWriteResponse
import org.opensearch.action.admin.indices.flush.FlushRequest
import org.opensearch.action.index.IndexRequest
import org.opensearch.client.RequestOptions
import org.opensearch.client.RestHighLevelClient
import org.opensearch.test.OpenSearchTestCase

object IndexUtil {
    private val log = LogManager.getLogger(IndexUtil::class.java)

    fun fillIndex(
        clusterClient: RestHighLevelClient,
        indexName: String,
        nFields: Int,
        fieldLength: Int,
        stepSize: Int,
    ) {
        for (i in nFields downTo 1 step stepSize) {
            val sourceMap: MutableMap<String, String> = HashMap()
            for (j in stepSize downTo 1)
                sourceMap[(i - j).toString()] = OpenSearchTestCase.randomAlphaOfLength(fieldLength)
            log.info("Updating index with map of size:${sourceMap.size}")
            val indexResponse = clusterClient.index(IndexRequest(indexName).id(i.toString()).source(sourceMap), RequestOptions.DEFAULT)
            Assertions.assertThat(indexResponse.result).isIn(DocWriteResponse.Result.CREATED, DocWriteResponse.Result.UPDATED)
        }
        // flush the index
        clusterClient.indices().flush(FlushRequest(indexName), RequestOptions.DEFAULT)
    }
}
