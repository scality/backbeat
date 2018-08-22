const http = require('http');

function makeRequest(options, body, cb) {
    const req = http.request(options, res => cb(null, res));
    req.on('error', err => cb(err));
    req.end(body);
}

function getResponseBody(res, cb) {
    res.setEncoding('utf8');
    const resBody = [];
    res.on('data', chunk => resBody.push(chunk));
    res.on('end', () => cb(null, resBody.join('')));
    res.on('error', err => cb(err));
}

module.exports = { makeRequest, getResponseBody };
