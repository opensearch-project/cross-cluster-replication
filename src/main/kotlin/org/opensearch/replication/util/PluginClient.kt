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
import org.opensearch.core.action.ActionListener
import org.opensearch.core.action.ActionResponse
import org.opensearch.identity.Subject
import org.opensearch.transport.client.Client
import org.opensearch.transport.client.FilterClient

/**
 * Executes transport actions as this plugin's assigned system subject rather than as the
 * authenticated user, which is how the plugin reaches the system indices it owns.
 */
class PluginClient(delegate: Client) : FilterClient(delegate) {

    companion object {
        private val log = LogManager.getLogger(PluginClient::class.java)
    }

    // Assigned from IdentityAwarePlugin.assignSubject, which runs on a different thread than the
    // transport actions that read it.
    @Volatile
    private var subject: Subject? = null

    fun setSubject(subject: Subject) {
        this.subject = subject
    }

    override fun <Request : ActionRequest, Response : ActionResponse> doExecute(
        action: ActionType<Response>,
        request: Request,
        listener: ActionListener<Response>
    ) {
        val currentSubject = this.subject
            ?: throw IllegalStateException("PluginClient is not initialized with a subject.")

        // Saves the caller's context so it can be restored once the action completes. runAs performs
        // the switch itself, so stashing here would clear the context before it gets the chance.
        // Held in a local rather than a use block because the action is asynchronous: a use block
        // would restore on exit, long before the listener fires.
        val storedContext = threadPool().threadContext.newStoredContext(false)

        try {
            currentSubject.runAs<Exception> {
                log.debug("Running transport action as subject: {}", currentSubject.principal.name)
                super.doExecute(action, request, ActionListener.runBefore(listener, storedContext::restore))
            }
        } catch (e: Exception) {
            // Reported through the listener rather than thrown, so an async caller is not left waiting.
            storedContext.close()
            listener.onFailure(e)
        }
    }
}
