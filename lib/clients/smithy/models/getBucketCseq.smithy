namespace com.scality.backbeat

use aws.protocols#restJson1

/// Retrieves bucket sequence information
@readonly
@http(method: "GET", uri: "/_/metadata/default/informations/{Bucket}")
operation GetBucketCseq {
    input: GetBucketCseqInput,
    output: GetBucketCseqOutput,
}

@input
structure GetBucketCseqInput {
    @httpLabel
    @required
    Bucket: String,
}

@output
structure GetBucketCseqOutput {
    /// List of sequence information
    CseqInfo: CseqInfoList,
}

list CseqInfoList {
    member: CseqInfo,
}

structure CseqInfo {
    /// Current sequence number
    cseq: Integer,
}
