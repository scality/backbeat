const async = require('async');
const assert = require('assert');
const http = require('http');
const querystring = require('querystring');
const URL = require('url');
const werelogs = require('werelogs');
const Logger = werelogs.Logger;

const IngestionProducer =
    require('../../../lib/queuePopulator/IngestionProducer');

class MetadataMock {
    constructor() {
        this.log = new Logger('IngestionProducer:test:metadataMock');
        const bucketList = ['bucket1', 'bucket2', 'bucket3'];
        const routes = {
            getBuckets: () => ({
                method: 'GET',
                path: '/_/raft_sessions/1/bucket',
                handler: () => this._getBuckets,
            }),
            getBucketMd: () => ({
                method: 'GET',
                path: '/default/attributes/bucket1',
                handler: () => this._getBucketMd,
            }),
            listObject: () => ({
                method: 'GET',
                path: '/default/bucket/bucket1?listingType=Delimiter',
                handler: () => this._getBucketMd,
            }),
            getObjectMD: () => ({
                method: 'GET',
                
            })
        };
        // const path = {
        //     '/_/raft_sessions/'
        // }
        const params = {};
    }

    onRequest(req, res) {
        console.log('RECEIVED REQUEST');
        console.log('req url', req.url);
        console.log('req query', req.query);
        console.log('req headers', req.headers);
        const url = URL.parse(req.url);
        const query = querystring.parse(req.query);
        const host = req.headers.host.split(':')[0];
        // this.requestsPerHost[host] += 1;
        if(req.method !== 'GET') {
            return res.end(JSON.stringify({
                error: 'mock server only supports GET requests',
            }));
        }
        if(req.url === 'default/bucket/bucket1?listingType=Delimiter') {
            console.log('listing objects in bucket');
            console.log(req.url);
            return res.end(JSON.stringify({
                error: 'failed',
            }));
        } else if (req.url === '/_/raft_sessions/1/bucket') {
            console.log('getting buckets for raft session');
            return res.end(JSON.stringify({
                error: 'failed',
            }));
        } else if (req.url === '/default/attributes/bucket1') {
            console.log('getting bucket metadata');
            return res.end(JSON.stringify({
                error: 'failed',
            }));
        } else if (req.url === '/default/bucket/bucket1/testobject1?') {
            console.log('getting object metadata');
            return res.end(JSON.stringify({
                error: 'failed',
            }));
        }
        return res.end(JSON.stringify({
            error: 'invalid path',
        }));
    }
}

describe.only('ingestion producer functional tests with mock', () => {
    let httpServer;
    let metadataMock;

    before(done => {
        console.log('before the ingesion producer test');
        metadataMock = new MetadataMock();
        httpServer = http.createServer(
            (req, res) => metadataMock.onRequest(req, res));
        httpServer.listen(7778);
        done();
    });

    after(done => {
        httpServer.close();
        done();
    });

    it('Sending a request', done => {
        console.log('we made it');
        const iProducer = new IngestionProducer({
            bucketdBootstrap: ['localhost:7778'],
            bucketdLog: undefined,
        });

        return iProducer.snapshot(1, (err, res) => {
            console.log(err, res);
            console.log('we are getting snapshot');
            return done();
        });
    });
});