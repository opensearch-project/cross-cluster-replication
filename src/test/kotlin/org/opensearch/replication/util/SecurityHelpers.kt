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

import org.opensearch.client.RequestOptions
import java.nio.charset.StandardCharsets
import java.util.Base64

// To set for individual requests
// RequestOptions.DEFAULT.addBasicAuthHeader("admin", "admin")
fun RequestOptions.addBasicAuthHeader(
    user: String,
    password: String,
): RequestOptions =
    this
        .toBuilder()
        .addHeader(
            "Authorization",
            "Basic " + Base64.getEncoder().encodeToString("$user:$password".toByteArray(StandardCharsets.UTF_8)),
        ).build()
