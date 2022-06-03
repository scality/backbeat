const url = require('url');
const http = require('http');
const assert = require('assert');

const werelogs = require('werelogs');
const logger = new werelogs.Logger('Backbeat:syntheticbucketd');

const bucketdPort = 9001;

const bucketNumber = Number.parseInt(process.argv[2], 10);
const accountsNumber = Number.parseInt(process.argv[3], 10);
const extraBucketsFilename = process.argv[4];

let extraBuckets;
let predefinedAccounts;

if (!Number.isSafeInteger(bucketNumber) ||
    !Number.isSafeInteger(accountsNumber)) {
    logger.error(`usage: ${process.argv[1]} <bucketNumber> <accountsNumber> [file-with-extra-accounts]`);
    process.exit(1);
}

function getMarkerIndex(marker) {
    if (marker) {
        return Number.parseInt(marker.split('..|..')[1], 10) + 1;
    }

    return 0;
}

function generateAccountIds(accountsNumber) {
    const accountPadding = '0'.repeat(64);
    const ret = [];

    for (let i = 0; i < accountsNumber; i++) {
        const canonicalId = predefinedAccounts ?
            predefinedAccounts[i % predefinedAccounts.length] :
            `${accountPadding}${i}`.slice(-20);
        ret.push(canonicalId);
    }

    ret.sort(() => Math.random() - 0.5);

    return ret;
}

function makeListing(marker, maxKeys) {
    const accountIds = generateAccountIds(accountsNumber);
    const index = getMarkerIndex(marker);
    const ret = [];

    const upperBound = Math.min(index + maxKeys, bucketNumber);
    const namePadding = '0'.repeat(24);

    for (let i = index; i < upperBound; i += 1) {
        const account = accountIds[i % accountsNumber];
        const name = (`${namePadding}${i}`.slice(-24));
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

    let contents = makeListing(marker, maxKeys).map(key => ({
        key,
        value: {},
    }));

    // add any non-generated predefined buckets/accounts at the end
    // of the listing
    if (!isTruncated && extraBuckets) {
        contents = contents.concat(...extraBuckets);
    }

    res.end(JSON.stringify({
        Contents: contents,
        IsTruncated: isTruncated,
    }));
};

if (extraBucketsFilename) {
    extraBuckets = require(`./${extraBucketsFilename}`);

    const canonicalIds = extraBuckets
        .map(b => b.key.split('..|..')[0]);

    predefinedAccounts = [... new Set(canonicalIds)];
}

http.createServer(bucketdHandler)
    .listen(bucketdPort, () => {
        logger.info('listening', {
            bucketdPort,
            bucketNumber,
            accountsNumber,
        });
    });
