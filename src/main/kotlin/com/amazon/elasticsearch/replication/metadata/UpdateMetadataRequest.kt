package com.amazon.elasticsearch.replication.metadata

import org.elasticsearch.action.ActionRequestValidationException
import org.elasticsearch.action.ValidateActions.addValidationError
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest
import org.elasticsearch.action.support.master.AcknowledgedRequest
import org.elasticsearch.common.io.stream.StreamInput

class UpdateMetadataRequest : AcknowledgedRequest<UpdateMetadataRequest> {
    var indexName: String
    var type: Type
    lateinit var request: AcknowledgedRequest<*>

    constructor(indexName: String, type: Type, request: AcknowledgedRequest<*>) : super() {
        this.indexName = indexName
        this.type = type
        this.request = request
    }

    enum class Type {
        MAPPING, SETTING, ALIAS
    }

    constructor(inp: StreamInput): super(inp) {
        indexName = inp.readString()
        type = inp.readEnum(Type::class.java)
        when (type) {
            Type.MAPPING -> PutMappingRequest(inp)
            Type.SETTING -> UpdateSettingsRequest(inp)
            Type.ALIAS -> IndicesAliasesRequest(inp)
        }
    }

    override fun validate(): ActionRequestValidationException? {
        var validationException: ActionRequestValidationException? = request.validate()
        if (indexName == null) validationException = addValidationError("index name is missing", validationException)
        if (type == null) validationException = addValidationError("operation types is missing", validationException)
        return validationException
    }
}