const url = require('url');

class S3Mock {
    getMetadata(objectKey) {
        return JSON.stringify({
            /* eslint-disable quote-props */
            Body: JSON.stringify({
                'owner-display-name': 'retryuser',
                'owner-id': '1d79449d2b781e585051d057ecf99cad1898120cda80b7b2' +
                    'a94e3fe99d8cad82',
                'cache-control': '',
                'content-disposition': '',
                'content-encoding': '',
                expires: '',
                'content-length': 1,
                'content-type': 'application/octet-stream',
                'content-md5': '93b885adfe0da089cdf634904fd59f71',
                'x-amz-version-id': 'null',
                'x-amz-server-version-id': '',
                'x-amz-storage-class': 'STANDARD',
                'x-amz-server-side-encryption': '',
                'x-amz-server-side-encryption-aws-kms-key-id': '',
                'x-amz-server-side-encryption-customer-algorithm': '',
                'x-amz-website-redirect-location': '',
                acl: {
                    Canned: 'private',
                    FULL_CONTROL: [],
                    WRITE_ACP: [],
                    READ: [],
                    READ_ACP: [],
                },
                key: objectKey,
                location: [
                    {
                        key: '5e96140c5b7d92fadf28dd4d6ec905445df7e8d6',
                        size: 1,
                        start: 0,
                        dataStoreName: 'us-east-1',
                        dataStoreETag: '1:93b885adfe0da089cdf634904fd59f71',
                    },
                ],
                isNull: '',
                nullVersionId: '',
                isDeleteMarker: false,
                versionId: '98500086134471999999RG001  0',
                tags: {},
                replicationInfo: {
                    status: 'FAILED',
                    backends: [{
                        site: 'zenko',
                        status: 'FAILED',
                        dataStoreVersionId: '',
                    }, {
                        site: 'us-east-2',
                        status: 'COMPLETED',
                        dataStoreVersionId: 'H80T9KLWneT8Ab0lbkMJQuN.LVG09a1n',
                    }],
                    content: ['DATA', 'METADATA'],
                    destination: 'arn:aws:s3:::destination-bucket',
                    storageClass: 'zenko,us-east-2',
                    role: 'arn:aws:iam::604563867484:role',
                    storageType: 'aws_s3',
                    dataStoreVersionId: '',
                },
                dataStoreName: 'us-east-1',
                'last-modified': '2018-03-30T22:22:34.384Z',
                'md-model-version': 3,
            }),
            /* eslint-enable quote-props */
        });
    }

    onRequest(req, res) {
        const { pathname } = url.parse(req.url);
        if (req.method === 'GET' &&
            pathname.startsWith('/_/backbeat/metadata')) {
            res.writeHead(200);
            const resources = pathname.split('/');
            const objectKey = resources[5];
            return res.end(this.getMetadata(objectKey));
        }
        res.writeHead(501);
        return res.end(JSON.stringify({
            error: 'mock not implemented',
            method: req.method,
            url: req.url,
            host: req.headers.host,
        }));
    }
}

module.exports = S3Mock;
