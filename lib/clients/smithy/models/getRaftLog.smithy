namespace com.scality.backbeat

use aws.protocols#restJson1

/// Retrieves Raft log entries for a specific log ID
@readonly
@http(method: "GET", uri: "/_/metadata/admin/raft_sessions/{LogId}/log")
operation GetRaftLog {
    input: GetRaftLogInput,
    output: GetRaftLogOutput,
}

@input
structure GetRaftLogInput {
    @httpLabel
    @required
    LogId: String,
    
    @httpQuery("begin")
    Begin: Integer,
    
    @httpQuery("limit")
    Limit: Integer,
    
    @httpQuery("targetLeader")
    TargetLeader: Boolean,
}

@output
structure GetRaftLogOutput {
    /// Information about the Raft log
    info: RaftLogInfo,
    
    /// Log entries
    log: RaftLogEntries,
}

structure RaftLogInfo {
    /// Starting sequence number
    start: Integer,
    
    /// Current sequence number
    cseq: Integer,
    
    /// Prune sequence number
    prune: Integer,
}

list RaftLogEntries {
    member: RaftLogEntry,
}

structure RaftLogEntry {
    /// Database name
    db: String,
    
    /// List of key-value entries
    entries: LogEntryList,
}

list LogEntryList {
    member: LogEntryKeyValue,
}

structure LogEntryKeyValue {
    /// Entry key
    key: String,
    
    /// Entry value
    value: String,
}
