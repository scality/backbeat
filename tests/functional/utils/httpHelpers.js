const http = require('http');

const config = require('../../config.json');

function makePOSTRequest(options, body, cb) {
    const req = http.request(options, res => cb(null, res));
    req.on('error', err => cb(err));
    req.end(body);
}

function makeRetryPOSTRequest(body, cb) {
    const options = {
        host: config.server.host,
        port: config.server.port,
        method: 'POST',
        path: '/_/crr/failed',
    };
    makePOSTRequest(options, body, cb);
}

function getRequest(path, done) {
    const params = {
        host: '127.0.0.1',
        port: 8900,
        method: 'GET',
        path,
    };
    // eslint-disable-next-line
    const req = http.request(params, res => {
        if (res.statusCode !== 200) {
            return done(res);
        }

        const chunks = [];
        res.on('data', chunk => {
            chunks.push(chunk);
        });
        res.on('end', () => {
            let body;
            try {
                body = JSON.parse(Buffer.concat(chunks).toString());
            } catch (e) {
                return done(e);
            }
            return done(null, body);
        });
    });
    req.on('error', err => done(err));
    req.end();
}

function getResponseBody(res, cb) {
    res.setEncoding('utf8');
    const resBody = [];
    res.on('data', chunk => resBody.push(chunk));
    res.on('end', () => cb(null, resBody.join('')));
    res.on('error', err => cb(err));
}

module.exports = {
    makePOSTRequest,
    makeRetryPOSTRequest,
    getRequest,
    getResponseBody,
};
