package com.amazon.elasticsearch.replication.util

import com.amazon.opendistroforelasticsearch.commons.authuser.User
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.util.concurrent.ThreadContext
import org.elasticsearch.test.ESTestCase
import org.junit.Assert
import org.junit.Before

class SecurityContextTests: ESTestCase() {

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