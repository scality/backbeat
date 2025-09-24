$version: "2.0"

namespace com.scality.backbeat

@readonly
@http(method: "GET", uri: "/{Bucket}/{Key+}", code: 200)
operation GetObject {
    input: GetObjectInput,
    output: GetObjectOutput,
}

structure GetObjectInput {
    @required
    @httpLabel
    Bucket: String,
    @required
    @httpLabel
    Key: String,
    @httpQuery("versionId")
    VersionId: String,
    @httpHeader("X-Scal-Canonical-Id")
    CanonicalID: String,
}

structure GetObjectOutput {
    @httpHeader("Content-Type")
    ContentType: String,
    @httpHeader("ETag")
    ETag: String,
    @httpHeader("Last-Modified")
    LastModified: Timestamp,
    @httpHeader("x-amz-version-id")
    VersionId: String,
    @httpPayload
    Body: Blob
}
