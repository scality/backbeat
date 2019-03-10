const errors = require('arsenal').errors;
const mockRes = require('./mockRes');

const mockLogs = mockRes.raftLogs['1'];
const objectList = mockRes.objectList.objectList1;
const dummyBucketMD = mockRes.bucketMD;
const objectMD = mockRes.objectMD;

/**
 * Strip version id from request url to fetch metadata mock entry by specified
 * bucket and object names
 * @param {String} path - request path
 * @return {String} path with no version id
 */
function _versionIdRemovedURLPath(path) {
    const urlEncodedNullCharacter = '%00';
    return path.split(urlEncodedNullCharacter)[0];
}

class MetadataMock {
    onRequest(req, res) {
        // TODO: for PUT/POST, edit the mockRes object
        if (req.method === 'GET') {
            const url = _versionIdRemovedURLPath(req.url.split('?')[0]);
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
        res.end(JSON.stringify({}));
    }
}

module.exports = {
    MetadataMock,
    mockLogs,
    objectList,
    dummyBucketMD,
    objectMD,
};
