$version: "2.0"

namespace com.scality.backbeat

@readonly
@http(method: "GET", uri: "/_/backbeat/index/{Bucket}")
operation GetBucketIndexes {
    input: GetBucketIndexesInput,
    output: GetBucketIndexesOutput
}

structure GetBucketIndexesInput {
    @required
    @httpLabel
    Bucket: String
}

structure GetBucketIndexesOutput {
    Indexes: IndexList
}

list IndexList {
    member: Index
}

structure Index {
    name: String,
    keys: IndexKeyList
}

list IndexKeyList {
    member: IndexKey
}

structure IndexKey {
    order: Integer,
    key: String
}
