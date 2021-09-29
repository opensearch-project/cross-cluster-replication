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

package com.amazon.elasticsearch.replication;

import org.elasticsearch.ElasticsearchException

public class MappingNotAvailableException: ElasticsearchException {

    constructor(message: String, vararg args: Any) : super(message, *args)

    constructor(message: String, cause: Throwable, vararg args: Any) : super(message,  cause, *args)
}
