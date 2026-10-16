/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.replication.util

import org.apache.logging.log4j.LogManager
import org.opensearch.action.ActionRequest
import org.opensearch.action.ActionType
import org.opensearch.action.support.ContextPreservingActionListener
import org.opensearch.core.action.ActionListener
import org.opensearch.core.action.ActionResponse
import org.opensearch.identity.PluginSubject
import org.opensearch.transport.client.Client
import org.opensearch.transport.client.FilterClient
import java.util.concurrent.atomic.AtomicReference

/**
 * Executes transport actions as this plugin's assigned system subject rather than as the
 * authenticated user, which is how the plugin reaches the system indices it owns.
 */
class PluginClient(delegate: Client) : FilterClient(delegate) {

    companion object {
        private val log = LogManager.getLogger(PluginClient::class.java)
    }

    // Set once, from assignSubject, on a different thread from the actions that read it.
    private val subject = AtomicReference<PluginSubject?>()

    /** Assigns the subject every action runs as. It can be assigned once; assigning the same subject again is a no-op. */
    fun setSubject(subject: PluginSubject) {
        if (!this.subject.compareAndSet(null, subject) && this.subject.get() !== subject) {
            throw IllegalStateException("PluginClient already has a subject assigned.")
        }
    }

    /** Not supported, because a plugin subject has no meaning on another cluster. */
    override fun getRemoteClusterClient(clusterAlias: String): Client =
        throw UnsupportedOperationException("PluginClient cannot run actions on a remote cluster.")

    /** Runs the action as the assigned subject, notifying the listener with the caller's thread context in place. */
    override fun <Request : ActionRequest, Response : ActionResponse> doExecute(
        action: ActionType<Response>,
        request: Request,
        listener: ActionListener<Response>
    ) {
        val currentSubject = subject.get() ?: throw IllegalStateException("PluginClient is not initialized with a subject.")
        // Captured before runAs stashes the context; core restores it around the callback, then puts
        // the responding thread's own context back.
        val callerContext = ContextPreservingActionListener.wrapPreservingContext(listener, threadPool().threadContext)
        currentSubject.runAs<RuntimeException> {
            log.debug("Running transport action as subject: {}", currentSubject.principal.name)
            super.doExecute(action, request, callerContext)
        }
    }
}
