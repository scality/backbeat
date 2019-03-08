const arsenal = require('arsenal');
const config = require('../../conf/Config');

const USERSBUCKET = arsenal.constants.usersBucket;
const PENSIEVE = 'PENSIEVE';
const METASTORE = '__metastore';

function getIngestionBuckets(metadata, logger, cb) {
    const mongoClient = metadata.getClient();
    const m = mongoClient.getCollection(METASTORE);
    m.find({
        '_id': {
            $nin: [PENSIEVE, USERSBUCKET],
        },
        'value.ingestion': {
            $type: 'object',
        },
    }).project({
        'value.name': 1,
        'value.ingestion': 1,
        'value.locationConstraint': 1,
    }).toArray((err, doc) => {
        if (err) {
            logger.error('error getting ingestion buckets',
                { error: err.message });
            return cb(err);
        }
        return cb(null, doc.map(i => i.value));
    });
}

function updateLocations(conf, ingestionBuckets, logger, cb) {
    try {
        const locations = conf.locations;
        const locationsWithReplicationBackend = Object.keys(locations)
        // NOTE: In Orbit, we don't need to have Scality location in our
        // destination bootstrapList config, since we do not replicate to
        // any Scality Instance yet.
        .filter(key => locations[key].locationType !== 'location-file-v1')
        .reduce((obj, key) => {
            /* eslint no-param-reassign:0 */
            obj[key] = locations[key];
            return obj;
        }, {});
        config.setBootstrapList(locationsWithReplicationBackend);
        config.setIngestionBuckets(locationsWithReplicationBackend,
            ingestionBuckets);

        Object.keys(locations).forEach(locName => {
            config.setIsTransientLocation(
                locName, locations[locName].isTransient);
        });
    } catch (error) {
        logger.error('error updating locations', { error: error.message });
        return cb(error);
    }
    return cb(null, conf.version);
}


function patchConfiguration(overlayVersion, conf, metadata, logger, cb) {
    if (conf.version === undefined) {
        return process.nextTick(cb, null);
    }
    if (overlayVersion !== undefined &&
        overlayVersion >= conf.version) {
        return process.nextTick(cb, null);
    }
    if (conf && conf.locations) {
        return getIngestionBuckets(metadata, logger, (err, buckets) => {
            if (err) {
                return cb(err);
            }
            return updateLocations(conf, buckets, logger, cb);
        });
    }
    return process.nextTick(cb, null);
}

module.exports = patchConfiguration;
