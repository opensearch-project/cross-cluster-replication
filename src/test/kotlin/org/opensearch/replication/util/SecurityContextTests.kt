package org.opensearch.replication.util

import org.junit.Assert
import org.junit.Before
import org.opensearch.common.settings.Settings
import org.opensearch.common.util.concurrent.ThreadContext
import org.opensearch.commons.authuser.User
import org.opensearch.test.OpenSearchTestCase

class SecurityContextTests : OpenSearchTestCase() {
    companion object {
        var threadContext: ThreadContext? = null
    }

    @Before
    fun setupContext() {
        threadContext = ThreadContext(Settings.EMPTY)
    }

    fun `test security context from ThreadContext with user Info`() {
        threadContext!!.putTransient("_opendistro_security_user_info", "admin||all_access")
        val expectedUser = User("admin", emptyList<String>(), listOf("all_access"), emptyList<String>())
        val returnedUser = SecurityContext.fromSecurityThreadContext(threadContext!!)
        Assert.assertEquals(expectedUser, returnedUser)
    }

    fun `test security context from ThreadContext with user Info not present and user obj present`() {
        threadContext!!.putTransient("_opendistro_security_user_info", null)
        threadContext!!.putTransient("_opendistro_security_user", "")
        val expectedUser = User("adminDN", emptyList<String>(), emptyList<String>(), emptyList<String>())
        val returnedUser = SecurityContext.fromSecurityThreadContext(threadContext!!)
        Assert.assertEquals(expectedUser, returnedUser)
    }

    fun `test security context from ThreadContext with user Info and user obj not present`() {
        val returnedUser = SecurityContext.fromSecurityThreadContext(threadContext!!)
        Assert.assertNull(returnedUser)
    }
}
