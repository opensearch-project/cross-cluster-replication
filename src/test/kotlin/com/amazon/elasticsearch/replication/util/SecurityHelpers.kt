package com.amazon.elasticsearch.replication.util

import org.elasticsearch.client.RequestOptions
import java.nio.charset.StandardCharsets
import java.util.Base64

// To set for individual requests
// RequestOptions.DEFAULT.addBasicAuthHeader("admin", "admin")
fun RequestOptions.addBasicAuthHeader(user: String, password: String): RequestOptions {
    return this.toBuilder().addHeader("Authorization",
            "Basic " + Base64.getEncoder().encodeToString("$user:$password".toByteArray(StandardCharsets.UTF_8)))
            .build()
}
