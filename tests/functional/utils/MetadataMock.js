const errors = require('arsenal').errors;
const { decode } = require('arsenal').versioning.VersionID;
const mockRes = require('./mockRes');

const mockLogs = mockRes.raftLogs['1'];
const objectList = mockRes.objectList.objectList1;
const dummyBucketMD = mockRes.bucketMD;
const objectMD = mockRes.objectMD;


class MetadataMock {
    onRequest(req, res) {
        // TODO: for PUT/POST, edit the mockRes object
        if (req.method === 'GET') {
            const [url, queryparams] = req.url.split('?');
            const queryParamsObj = new URLSearchParams(queryparams);
            const resObj = mockRes.GET.responses[url];
            if (!resObj && req.url.startsWith('/default/attributes')) {
                const err = errors.NoSuchBucket;
                res.writeHead(err.code, 'NoSuchBucket');
                res.end();
            } else if (!resObj) {
                res.end(JSON.stringify({}));
            } else if (resObj.resType === 'error') {
                const err = errors[resObj.name];
                res.writeHead(err.code, resObj.name);
                res.end();
            } else {
                const resType =
                    JSON.parse(JSON.stringify(mockRes[resObj.resType]));
                if (resObj['content-type']) {
                    res.writeHead(200, {
                        'Content-Type': resObj['content-type'],
                    });
                    return;
                }
                const resContent = resType[resObj.name];
                if (resObj.resType === 'objectList') {
                    resContent.Contents.forEach((obj, i) => {
                        resContent.Contents[i].value =
                            JSON.stringify(obj.value);
                    });
                }
                if (resObj.resType === 'raftLogs') {
                    resContent.log.forEach((log, i) => {
                        log.entries.forEach((entry, j) => {
                            resContent.log[i].entries[j].value.attributes =
                                JSON.stringify(entry.value.attributes);
                            resContent.log[i].entries[j].value =
                                JSON.stringify(entry.value);
                        });
                    });
                    const beginParam = queryParamsObj.get('begin');
                    const begin = beginParam ? Number.parseInt(beginParam, 10) : undefined;
                    if (begin !== undefined) {
                        resContent.log = resContent.log.slice(begin - resContent.info.start);
                        resContent.info.start = begin;
                    }
                    const limitParam = queryParamsObj.get('limit');
                    const limit = limitParam ? Number.parseInt(limitParam, 10) : undefined;
                    if (limit !== undefined) {
                        resContent.log = resContent.log.slice(0, limit);
                    }
                }
                if (resObj.resType === 'objectMD') {
                    const versionParam = queryParamsObj.get('versionId');
                    let decodedVersionId;
                    if (versionParam) {
                        decodedVersionId = decode(versionParam);
                    }
                    const objMd = Object.assign({}, resContent, {
                        versionId: decodedVersionId,
                    });
                    res.end(JSON.stringify({
                        Body: JSON.stringify(objMd)
                    }));
                    return;
                }
                res.end(JSON.stringify(resContent));
            }
        } else if (req.method === 'POST') {
            const resObj = mockRes.POST.responses[req.url];
            if (resObj && resObj.resType === 'error') {
                const err = errors[resObj.name];
                res.writeHead(err.code, resObj.name);
                res.end();
            } else {
                let body = '';
                req.on('data', part => {
                    body += part.toString();
                });
                req.on('end', () => {
                    const data = JSON.parse(body);
                    const usersBucketInfo = {};
                    usersBucketInfo.key = data.
                    res.end('ok');
                });
            }
        }
    }
}

module.exports = {
    MetadataMock,
    mockLogs,
    objectList,
    dummyBucketMD,
    objectMD,
};
