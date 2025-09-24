$version: "2.0"

namespace com.scality.backbeat

@readonly
@http(method: "GET", uri: "/_/metadata/default/bucket/{Bucket}")
operation GetObjectList {
    input: GetObjectListInput,
    output: GetObjectListOutput
}

structure GetObjectListInput {
    @required
    @httpLabel
    Bucket: String
}

structure GetObjectListOutput {
    Contents: ObjectMDList,
    CommonPrefixes: CommonPrefixList,
    IsTruncated: Boolean,
    Delimiter: String
}

list ObjectMDList {
    member: ObjectMD
}

structure ObjectMD {
    key: String,
    value: String
}

list CommonPrefixList {
    member: String
}
