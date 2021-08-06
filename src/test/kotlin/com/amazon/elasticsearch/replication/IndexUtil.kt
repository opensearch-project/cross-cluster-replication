package com.amazon.elasticsearch.replication

import org.apache.logging.log4j.LogManager
import org.assertj.core.api.Assertions
import org.elasticsearch.action.DocWriteResponse
import org.elasticsearch.action.admin.indices.flush.FlushRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.test.ESTestCase

object IndexUtil {
    private val log = LogManager.getLogger(IndexUtil::class.java)

    fun fillIndex(clusterClient: RestHighLevelClient,
                          indexName : String,
                          nFields: Int,
                          fieldLength: Int,
                          stepSize: Int) {
        for (i in nFields downTo 1 step stepSize) {
            val sourceMap : MutableMap<String, String> = HashMap()
            for (j in stepSize downTo 1)
                sourceMap[(i-j).toString()] = ESTestCase.randomAlphaOfLength(fieldLength)
            log.info("Updating index with map of size:${sourceMap.size}")
            val indexResponse = clusterClient.index(IndexRequest(indexName).id(i.toString()).source(sourceMap), RequestOptions.DEFAULT)
            Assertions.assertThat(indexResponse.result).isIn(DocWriteResponse.Result.CREATED, DocWriteResponse.Result.UPDATED)
        }
        //flush the index
        clusterClient.indices().flush(FlushRequest(indexName), RequestOptions.DEFAULT)
    }
}
