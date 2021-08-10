/*
 *   Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License").
 *   You may not use this file except in compliance with the License.
 *   A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   or in the "license" file accompanying this file. This file is distributed
 *   on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *   express or implied. See the License for the specific language governing
 *   permissions and limitations under the License.
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