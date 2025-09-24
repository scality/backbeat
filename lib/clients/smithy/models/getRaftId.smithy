$version: "2.0"

namespace com.scality.backbeat

@readonly
@http(method: "GET", uri: "/_/metadata/admin/buckets/{Bucket}/id")
operation GetRaftId {
    input: GetRaftIdInput,
    output: GetRaftIdOutput
}

structure GetRaftIdInput {
    @required
    @httpLabel
    Bucket: String
}

structure GetRaftIdOutput {
    @httpPayload
    RaftId: String
}
