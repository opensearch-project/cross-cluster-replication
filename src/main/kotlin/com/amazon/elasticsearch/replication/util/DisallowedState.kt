/*
 *   Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.elasticsearch.replication.util

import com.amazon.elasticsearch.replication.ReplicationSettings
import org.elasticsearch.common.settings.Settings
import java.util.Locale
import kotlin.math.max

public fun getDisallowedSettings(leaderSettings: Settings, replicationSettings: ReplicationSettings): List<Pair<String, String>> {
    val disallowedSettings = mutableListOf<Pair<String, String>>()

    val maxLen = max(replicationSettings.disallowedKeys.size, replicationSettings.disallowedValues.size)
    val disallowedKeys = fillToLength(replicationSettings.disallowedKeys, maxLen)
    val disallowedValues = fillToLength(replicationSettings.disallowedValues, maxLen)
    for (i in 0 until maxLen) {
        val key = disallowedKeys[i]
        val value = disallowedValues[i]
        val fetchedValue = leaderSettings.get(key) ?: continue
        if (fetchedValue.toLowerCase(Locale.ROOT) == value.toLowerCase(Locale.ROOT)) {
            disallowedSettings.add(Pair(key, value))
        }
    }
    return disallowedSettings
}

private fun fillToLength(list: List<String>, len: Int): List<String> {
    val newList = list.toMutableList()
    if (list.size < len) {
        for (i in 0 until len - list.size) {
            newList += list.last()
        }
    }
    return newList
}
