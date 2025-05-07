/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.replication

import org.opensearch.OpenSearchException

public class MappingNotAvailableException : OpenSearchException {

    constructor(message: String, vararg args: Any) : super(message, *args)

    constructor(message: String, cause: Throwable, vararg args: Any) : super(message, cause, *args)
}
