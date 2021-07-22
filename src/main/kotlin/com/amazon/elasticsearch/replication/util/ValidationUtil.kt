package com.amazon.elasticsearch.replication.util

import org.apache.logging.log4j.LogManager
import org.elasticsearch.ResourceNotFoundException
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.env.Environment
import java.nio.file.Files
import java.nio.file.Path

object ValidationUtil {

    private val log = LogManager.getLogger(ValidationUtil::class.java)

    fun validateAnalyzerSettings(environment: Environment, leaderSettings: Settings, overriddenSettings: Settings) {
        val analyserSettings = leaderSettings.filter { k: String? -> k!!.matches(Regex("index.analysis.*path")) }
        for (analyserSetting in analyserSettings.keySet()) {
            val settingValue = if (overriddenSettings.hasValue(analyserSetting)) overriddenSettings.get(analyserSetting) else analyserSettings.get(analyserSetting)
            val path: Path = environment.configFile().resolve(settingValue)
            if (!Files.exists(path)) {
                val message = "IOException while reading ${analyserSetting}: ${path.toString()}"
                log.error(message)
                throw ResourceNotFoundException(message)
            }
        }
    }
}