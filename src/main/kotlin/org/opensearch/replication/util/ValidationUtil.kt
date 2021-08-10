package org.opensearch.replication.util

import org.apache.logging.log4j.LogManager
import org.opensearch.ResourceNotFoundException
import org.opensearch.cluster.metadata.MetadataCreateIndexService
import org.opensearch.common.Strings
import org.opensearch.common.ValidationException
import org.opensearch.common.settings.Settings
import org.opensearch.env.Environment
import java.io.UnsupportedEncodingException
import java.nio.file.Files
import java.nio.file.Path
import java.util.Locale

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

    /**
     * Validate the name against the rules that we have for index name.
     */
    fun validateName(name: String, validationException: ValidationException) {
        if (name.toLowerCase(Locale.ROOT) != name)
            validationException.addValidationError("Value $name must be lowercase")

        if (!Strings.validFileName(name))
            validationException.addValidationError("Value $name must not contain the following characters ${Strings.INVALID_FILENAME_CHARS}")

        if (name.contains("#") || name.contains(":"))
            validationException.addValidationError("Value $name must not contain '#' or ':'")

        if (name == "." || name == "..")
            validationException.addValidationError("Value $name must not be '.' or '..'")

        if (name.startsWith('_') || name.startsWith('-') || name.startsWith('+'))
            validationException.addValidationError("Value $name must not start with '_' or '-' or '+'")

        try {
            var byteCount = name.toByteArray(charset("UTF-8")).size
            if (byteCount > MetadataCreateIndexService.MAX_INDEX_NAME_BYTES) {
                validationException.addValidationError("Value $name must not be longer than ${MetadataCreateIndexService.MAX_INDEX_NAME_BYTES} bytes")
            }
        } catch (e: UnsupportedEncodingException) {
            // UTF-8 should always be supported, but rethrow this if it is not for some reason
            validationException.addValidationError("Unable to determine length of $name")
        }

        // Additionally we don't allow replication for system indices i.e. starts with '.'
        if(name.startsWith("."))
            validationException.addValidationError("Value $name must not start with '.'")
    }
}