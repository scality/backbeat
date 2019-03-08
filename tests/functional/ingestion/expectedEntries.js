const expectedNewIngestionEntry = {
    type: 'put',
    bucket: 'zenkobucket',
    key: 'object1',
    value: '{"owner-display-name":"test_1518720219","owner-id":' +
    '"94224c921648ada653f584f3caf42654ccf3f1cbd2e569a24e88eb460f2f84d8",' +
    '"content-length":17303,"content-md5":"42070968aa8ae79704befe77afc6935b",' +
    '"x-amz-version-id":"null","x-amz-server-version-id":"",' +
    '"x-amz-storage-class":"STANDARD","x-amz-server-side-encryption":"",' +
    '"x-amz-server-side-encryption-aws-kms-key-id":"",' +
    '"x-amz-server-side-encryption-customer-algorithm":"",' +
    '"x-amz-website-redirect-location":"","acl":{"Canned":"private",' +
    '"FULL_CONTROL":[],"WRITE_ACP":[],"READ":[],"READ_ACP":[]},"key":"",' +
    '"location":[],"isDeleteMarker":false,"tags":{},"replicationInfo":' +
    '{"status":"","backends":[],"content":[],"destination":"","storageClass":' +
    '"","role":"","storageType":"","dataStoreVersionId":""},"dataStoreName":' +
    '"us-east-1","last-modified":"2018-02-16T22:43:37.174Z",' +
    '"md-model-version":3}',
};

const expectedZeroByteObj = {
    type: 'put',
    bucket: 'zenkobucket',
    key: 'zerobyteobject',
    value: '{"owner-display-name":"test_1518720219","owner-id":' +
    '"94224c921648ada653f584f3caf42654ccf3f1cbd2e569a24e88eb460f2f84d8",' +
    '"content-length":0,"content-md5":"d41d8cd98f00b204e9800998ecf8427e",' +
    '"x-amz-version-id":"null","x-amz-server-version-id":"",' +
    '"x-amz-storage-class":"STANDARD","x-amz-server-side-encryption":"",' +
    '"x-amz-server-side-encryption-aws-kms-key-id":"",' +
    '"x-amz-server-side-encryption-customer-algorithm":"",' +
    '"x-amz-website-redirect-location":"","acl":{"Canned":"private",' +
    '"FULL_CONTROL":[],"WRITE_ACP":[],"READ":[],"READ_ACP":[]},"key":"",' +
    '"location":[],"isDeleteMarker":false,"tags":{},"replicationInfo":' +
    '{"status":"","backends":[],"content":[],"destination":"","storageClass":' +
    '"","role":"","storageType":"","dataStoreVersionId":""},"dataStoreName":' +
    '"us-east-1","last-modified":"2018-02-16T22:43:37.174Z",' +
    '"md-model-version":3}',
};

const expectedUTF8Obj = {
    type: 'put',
    bucket: 'zenkobucket',
    key: '䆩鈁櫨㟔罳',
    value: '{"owner-display-name":"test_1518720219","owner-id":' +
    '"94224c921648ada653f584f3caf42654ccf3f1cbd2e569a24e88eb460f2f84d8",' +
    '"content-length":0,"content-md5":"d41d8cd98f00b204e9800998ecf8427e",' +
    '"x-amz-version-id":"null","x-amz-server-version-id":"",' +
    '"x-amz-storage-class":"STANDARD","x-amz-server-side-encryption":"",' +
    '"x-amz-server-side-encryption-aws-kms-key-id":"",' +
    '"x-amz-server-side-encryption-customer-algorithm":"",' +
    '"x-amz-website-redirect-location":"","acl":{"Canned":"private",' +
    '"FULL_CONTROL":[],"WRITE_ACP":[],"READ":[],"READ_ACP":[]},"key":"",' +
    '"location":[],"isDeleteMarker":false,"tags":{},"replicationInfo":' +
    '{"status":"","backends":[],"content":[],"destination":"","storageClass":' +
    '"","role":"","storageType":"","dataStoreVersionId":""},"dataStoreName":' +
    '"us-east-1","last-modified":"2018-02-16T22:43:37.174Z",' +
    '"md-model-version":3}',
};

const expectedVersionIdObj = {
    type: 'put',
    bucket: 'zenkobucket',
    key: 'versionedobject',
    value: '{"owner-display-name":"test_1518720219","owner-id":' +
    '"94224c921648ada653f584f3caf42654ccf3f1cbd2e569a24e88eb460f2f84d8",' +
    '"content-length":0,"content-md5":"d41d8cd98f00b204e9800998ecf8427e",' +
    '"x-amz-version-id":"null","x-amz-server-version-id":"3/L4kqtJlcpXroDTDmJ' +
    '+rmSpXd3dIbrHY+MTRCxf3vjVBH40Nr8X8gdRQBpUMLUo",' +
    '"x-amz-storage-class":"STANDARD","x-amz-server-side-encryption":"",' +
    '"x-amz-server-side-encryption-aws-kms-key-id":"",' +
    '"x-amz-server-side-encryption-customer-algorithm":"",' +
    '"x-amz-website-redirect-location":"","acl":{"Canned":"private",' +
    '"FULL_CONTROL":[],"WRITE_ACP":[],"READ":[],"READ_ACP":[]},"key":"",' +
    '"location":[],"isDeleteMarker":false,"tags":{},"replicationInfo":' +
    '{"status":"","backends":[],"content":[],"destination":"","storageClass":' +
    '"","role":"","storageType":"","dataStoreVersionId":""},"dataStoreName":' +
    '"us-east-1","last-modified":"2018-02-16T22:43:37.174Z",' +
    '"md-model-version":3}',
};

const expectedTagsObj = {
    type: 'put',
    bucket: 'zenkobucket',
    key: 'taggedobject',
    value: '{"owner-display-name":"test_1518720219","owner-id":' +
    '"94224c921648ada653f584f3caf42654ccf3f1cbd2e569a24e88eb460f2f84d8",' +
    '"content-length":0,"content-md5":"d41d8cd98f00b204e9800998ecf8427e",' +
    '"x-amz-version-id":"null","x-amz-server-version-id":"",' +
    '"x-amz-storage-class":"STANDARD","x-amz-server-side-encryption":"",' +
    '"x-amz-server-side-encryption-aws-kms-key-id":"",' +
    '"x-amz-server-side-encryption-customer-algorithm":"",' +
    '"x-amz-website-redirect-location":"","acl":{"Canned":"private",' +
    '"FULL_CONTROL":[],"WRITE_ACP":[],"READ":[],"READ_ACP":[]},"key":"",' +
    '"location":[],"isDeleteMarker":false,"tags":{"Dogs":"areTheBest"},' +
    '"replicationInfo":{"status":"","backends":[],"content":[],"destination":' +
    '"","storageClass":"","role":"","storageType":"","dataStoreVersionId":' +
    '""},"dataStoreName":"us-east-1","last-modified":' +
    '"2018-02-16T22:43:37.174Z","md-model-version":3}',
};

module.exports = {
    expectedNewIngestionEntry,
    expectedUTF8Obj,
    expectedZeroByteObj,
    expectedVersionIdObj,
    expectedTagsObj,
};
