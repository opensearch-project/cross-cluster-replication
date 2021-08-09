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

package org.opensearch.replication

import java.util.ArrayList

/**
 * This class defines annotations to help configure multi-cluster
 * settings for integration tests
 */
object MultiClusterAnnotations {
    @JvmStatic
    fun <T : Annotation?> getAnnotationsFromClass(declaringClass: Class<*>,
                                                  annotationClass: Class<T>): List<T> {
        val annotationList: MutableList<T> = ArrayList()
        for (annotation in declaringClass.getAnnotationsByType(annotationClass)) {
            annotationList.add(annotationClass.cast(annotation))
        }
        return annotationList
    }

    @Retention(AnnotationRetention.RUNTIME)
    @Target(AnnotationTarget.ANNOTATION_CLASS, AnnotationTarget.CLASS)
    @Repeatable
    annotation class ClusterConfiguration( /* The name of cluster which is being configured in this configuration
         */
        val clusterName: String,  /* This setting controls whether indices created by one tests are to be
        * preserved in other tests in a single test class.*/
        val preserveIndices: Boolean = false,  /* This setting controls whether snapshots created by one tests are to be
         * preserved in other tests in a single test class.*/
        val preserveSnapshots: Boolean = false,  /* This setting controls whether cluster settings setup by one tests are to be
         * preserved in other tests in a single test class.*/
        val preserveClusterSettings: Boolean = false)

    @Retention(AnnotationRetention.RUNTIME)
    @Target(AnnotationTarget.ANNOTATION_CLASS, AnnotationTarget.CLASS)
    annotation class ClusterConfigurations(vararg val value: ClusterConfiguration)
}