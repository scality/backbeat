/* eslint-disable */

const AWS = require('aws-sdk');
const async = require('async');


const testConfig = {
    s3: {
        transport: 'http',
        host: '127.0.0.1',
        port: 8000,
        accessKey: 'accessKey1',
        secretKey: 'verySecretKey1',
    },
};

const S3 = AWS.S3;
const s3config = {
    endpoint: `${testConfig.s3.transport}://` +
        `${testConfig.s3.host}:${testConfig.s3.port}`,
    s3ForcePathStyle: true,
    credentials: new AWS.Credentials(testConfig.s3.accessKey,
                                     testConfig.s3.secretKey),
};
const s3 = new S3(s3config);

// ==============
const MAX_KEYS = 2;
const bucket = 'my-bucket';
const key = 'my-key-';

// ==============

// const
//
// function createMPU(scenarioNumber, cb) {
//     const scenarioKeys = this._scenario[scenarioNumber].keyNames;
//     async.timesSeries(scenarioKeys.length, (n, next) => {
//         this.s3.createMultipartUpload({
//             Bucket: this.bucket,
//             Key: scenarioKeys[n],
//         }, next);
//     }, cb);
// }

function printResponse(e, r) {
    if (e) {
        console.log('E:', e);
    } else {
        console.log('R:', r);
    }
}

s3.createBucket({
    Bucket: bucket,
}, printResponse)

for (let i = 0; i < 6; i++) {
    s3.createMultipartUpload({
        Bucket: bucket,
        Key: 'helloworld',
        // Key: `${key}${i}`,
    }, printResponse)
}

s3.createMultipartUpload({
    Bucket: bucket,
    Key: 'helloworld2'
}, printResponse)

// s3.putObject({
//     Bucket: bucket,
//     Key: 'hello2',
// }, printResponse)

// s3.getObject({
//     Bucket: bucket,
//     Key: 'hello2',
// }, printResponse)


// s3.listMultipartUploads({
//     Bucket: bucket,
//     MaxUploads: 2,
// }, (e, r) => {
//     printResponse(e, r)
//     s3.listMultipartUploads({
//         Bucket: bucket,
//         MaxUploads: 2,
//         KeyMarker: r.NextKeyMarker,
//         UploadIdMarker: r.NextUploadIdMarker,
//     }, printResponse)
// })

// listMultipartUploads
// listParts
// uploadPart

function listing(a, b) {
    const params = {
        Bucket: bucket,
        MaxUploads: MAX_KEYS,
    };

    if (a && b) {
        console.log('setting:', a, b)
        params.KeyMarker = a;
        params.UploadIdMarker = b;
    }

    s3.listMultipartUploads(params, (e, r) => {
        console.log('=====')
        if (e) {
            console.log('E:', e);
        } else {
            // console.log('D:', r)
            const up = r.Uploads.map(u => u.UploadId);
            console.log(up);
        }

        if (r.IsTruncated) {
            listing(r.NextKeyMarker, r.NextUploadIdMarker);
        }
    });
}

listing()

// s3.listMultipartUploads({
//     Bucket: bucket,
//     // MaxUploads: MAX_KEYS,
// }, (err, res) => {
//     if (!err) {
//         res.Uploads.forEach(u => {
//             s3.abortMultipartUpload({
//                 Bucket: bucket,
//                 Key: 'helloworld',
//                 UploadId: u.UploadId,
//             }, () => {})
//         })
//     }
// })


// s3.listParts({
//     Bucket: bucket,
//     Key: key,
//     UploadId: '05c325552c344c879dd8a91c324c6eda',
// }, printResponse)

// s3.uploadPart({
//     Bucket: bucket,
//     Key: key,
//     UploadId: '05c325552c344c879dd8a91c324c6eda',
//     PartNumber: XYZ
// })




/*
05c325552c344c879dd8a91c324c6eda

R: { Bucket: 'my-bucket',
  Key: 'my-key-2',
  UploadId: 'af617c206a744b8faa1a1d1fa8abd125' }
R: { Bucket: 'my-bucket',
  Key: 'my-key-3',
  UploadId: 'e8939a041a0e4b93acd1bff2348034ca' }
R: { Bucket: 'my-bucket',
  Key: 'my-key-4',
  UploadId: '2abad9b31029485a8d1b458f0d1dcf0d' }
R: { Bucket: 'my-bucket',
  Key: 'my-key-5',
  UploadId: '4c9e4c191cc54667b12b64066ceea01b' }
R: { Bucket: 'my-bucket',
  Key: 'my-key-6',
  UploadId: '90fec7ffb61143cab93674c2a6855bf6' }
R: { Bucket: 'my-bucket',
  Key: 'my-key-7',
  UploadId: '6a0b8083431948a8a2166cc4600b2671' }
R: { Bucket: 'my-bucket',
  Key: 'my-key-8',
  UploadId: '70f9a7390c544f1ca994f5aa849d73eb' }
R: { Bucket: 'my-bucket',
  Key: 'my-key-9',
  UploadId: '258ad65067374c7e9dfb77f93a6b6ce6' }
*/



//
