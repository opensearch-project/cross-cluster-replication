package com.amazon.elasticsearch.replication.util

import org.apache.http.message.BasicHeader
import org.elasticsearch.client.RequestOptions
import java.nio.charset.StandardCharsets
import java.util.Base64

// To set for individual requests
// RequestOptions.DEFAULT.addBasicAuthHeader("admin", "admin")
fun RequestOptions.addBasicAuthHeader(user: String, password: String) {
    this.headers.add(BasicHeader("Authorization",
            "Basic " + Base64.getEncoder().encodeToString("$user:$password".toByteArray(StandardCharsets.UTF_8))))
}

