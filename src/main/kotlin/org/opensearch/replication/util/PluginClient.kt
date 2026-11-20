/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.replication.util

import org.opensearch.transport.client.Client

/** The replication plugin's client that runs actions as its system subject. */
class PluginClient(delegate: Client) : org.opensearch.commons.client.PluginClient(delegate)
