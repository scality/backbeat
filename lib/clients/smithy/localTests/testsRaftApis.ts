import { 
    BackbeatClientConfig,
    BackbeatClient,
    GetRaftIdInput,
    GetRaftIdCommand,
    GetRaftBucketsInput,
    GetRaftBucketsCommand,
    GetRaftLogInput,
    GetRaftLogCommand,
    GetBucketCseqInput,
    GetBucketCseqCommand
} from '@backbeat-service/client';

const bucketName = 'aaaa1';

const config: BackbeatClientConfig = {
    endpoint: 'http://localhost:8000',
    region: 'us-east-1',
    credentials: {
        accessKeyId: 'accessKey1',
        secretAccessKey: 'verySecretKey1',
    },
    maxAttempts: 1,
};

const client = new BackbeatClient(config);

console.log("\n=== Testing GetRaftId ===");
try {
    const getRaftIdInput: GetRaftIdInput = {
        Bucket: bucketName,
    };
    const getRaftIdCommand = new GetRaftIdCommand(getRaftIdInput);
    const raftIdData = await client.send(getRaftIdCommand);
    console.log('GetRaftId succeeded:', raftIdData);
    console.log('Raft ID:', raftIdData.RaftId);
} catch (err) {
    console.log('GetRaftId failed:', err);
}

console.log("\n=== Testing GetRaftBuckets ===");
try {
    const getRaftBucketsInput: GetRaftBucketsInput = {
        LogId: "0",
    };
    const getRaftBucketsCommand = new GetRaftBucketsCommand(getRaftBucketsInput);
    const raftBucketsData = await client.send(getRaftBucketsCommand);
    console.log('GetRaftBuckets succeeded:', raftBucketsData);
    console.log('Buckets:', raftBucketsData.Buckets);
} catch (err) {
    console.log('GetRaftBuckets failed:', err);
}

console.log("\n=== Testing GetRaftLog ===");
try {
    const getRaftLogInput: GetRaftLogInput = {
        LogId: "0",
        Begin: 0,
        Limit: 10,
    };
    const getRaftLogCommand = new GetRaftLogCommand(getRaftLogInput);
    const raftLogData = await client.send(getRaftLogCommand);
    console.log('GetRaftLog succeeded:', raftLogData);
    console.log('Log info:', raftLogData.info);
    console.log('Log entries count:', raftLogData.log?.length || 0);
} catch (err) {
    console.log('GetRaftLog failed:', err);
}

console.log("\n=== Testing GetBucketCseq ===");
try {
    const getBucketCseqInput: GetBucketCseqInput = {
        Bucket: bucketName,
    };
    const getBucketCseqCommand = new GetBucketCseqCommand(getBucketCseqInput);
    const bucketCseqData = await client.send(getBucketCseqCommand);
    console.log('GetBucketCseq succeeded:', bucketCseqData);
    console.log('Cseq info:', bucketCseqData.CseqInfo);
} catch (err) {
    console.log('GetBucketCseq failed:', err);
}
