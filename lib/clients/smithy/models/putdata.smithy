$version: "2.0"

namespace com.scality.backbeat

@idempotent
@http(method: "PUT", uri: "/_/backbeat/data/{Bucket}/{Key}?v2")
operation PutData {
    input: PutDataInput,
    output: PutDataOutput
}

structure PutDataInput {
    @required
    @httpLabel
    Bucket: String,
    @required
    @httpLabel
    Key: String,
    @httpHeader("Content-MD5")
    ContentMD5: String,
    @httpHeader("X-Scal-Canonical-Id")
    CanonicalID: String,
    @httpHeader("x-scal-versioning-required")
    VersioningRequired: Boolean,
    @httpPayload
    Body: Blob
}

structure PutDataOutput {
    @httpPayload
    Location: Document
}