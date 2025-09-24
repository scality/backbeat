namespace com.scality.backbeat

use aws.protocols#restJson1

@idempotent
@http(method: "DELETE", uri: "/_/backbeat/multiplebackenddata/{Bucket}/{Key+}?operation=deleteobject", code: 200)
operation MultipleBackendDeleteObject {
    input: MultipleBackendDeleteObjectInput,
    output: MultipleBackendDeleteObjectOutput
}

structure MultipleBackendDeleteObjectInput {
    @httpLabel
    @required
    Bucket: String,

    @httpLabel
    @required
    Key: String,

    @httpHeader("X-Scal-Storage-Type")
    StorageType: String,

    @httpHeader("X-Scal-Storage-Class")
    @required
    StorageClass: String,
}

structure MultipleBackendDeleteObjectOutput {
    versionId: String,
}
