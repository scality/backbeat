const url = require('url');
const http = require('http');
const assert = require('assert');

const werelogs = require('werelogs');
const logger = new werelogs.Logger('Backbeat:syntheticbucketd');

const bucketdPort = 9001;

const bucketNumber = Number.parseInt(process.argv[2], 10);
const accountsNumber = Number.parseInt(process.argv[3], 10);

if (!Number.isSafeInteger(bucketNumber) ||
    !Number.isSafeInteger(accountsNumber)) {
    logger.error(`usage: ${process.argv[1]} <bucketNumber> <accountsNumber>`);
    process.exit(1);
}

function getMarkerIndex(marker) {
    if (marker) {
        return Number.parseInt(marker.split('..|..')[1], 10) + 1;
    }

    return 0;
}

function generateAccountIds(accountsNumber) {
    const accountPadding16 = '0000000000000000';
    const accountPadding = accountPadding16 + accountPadding16 +
        accountPadding16 + accountPadding16;
    const ret = [];

    for (let i = 0; i < accountsNumber; i++) {
        ret.push((`${accountPadding}${i}`.slice(-20)));
    }

    ret.sort(() => Math.random() - 0.5);

    return ret;
}

function makeListing(marker, maxKeys) {
    const accountIds = generateAccountIds(accountsNumber);
    const index = getMarkerIndex(marker);
    const ret = [];

    const upperBound = Math.min(index + maxKeys, bucketNumber);

    for (let i = index; i < upperBound; i += 1) {
        const account = accountIds[i % accountsNumber];
        const name = (`000000000000000000000000${i}`.slice(-24));
        ret.push(`${account}..|..${name}`);
    }

    return ret;
}

const bucketdHandler = (req, res) => {
    const { pathname, query } = url.parse(req.url, true);
    const { maxKeys: maxKeysS, marker } = query;
    const maxKeys = Number.parseInt(maxKeysS, 10);

    assert.strictEqual(pathname, '/default/bucket/users..bucket');

    const reqUid = req.headers['x-scal-request-uids'];
    const log = reqUid ?
        logger.newRequestLoggerFromSerializedUids(reqUid) :
        logger.newRequestLogger();

    const index = getMarkerIndex(marker);
    const isTruncated = (index + maxKeys) < bucketNumber;

    log.info('listing request', { marker, maxKeys, isTruncated, index });

    res.end(JSON.stringify({
        Contents: makeListing(marker, maxKeys).map(key => ({
            key,
            value: {},
        })),
        IsTruncated: isTruncated,
    }));
};

http.createServer(bucketdHandler)
    .listen(bucketdPort, () => {
        logger.info('listening', {
            bucketdPort,
            bucketNumber,
            accountsNumber,
        });
    });
