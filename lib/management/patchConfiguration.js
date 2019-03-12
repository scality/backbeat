'use strict'; // eslint-disable-line strict

const async = require('async');

const config = require('../../conf/Config');

function updateIngestionBuckets(locations, metadata, logger, cb) {
    metadata.getIngestionBuckets(logger, (err, buckets) => {
        if (err) {
            logger.error('error get ingestion buckets from mongo', {
                method: 'patchConfiguration::updateIngestionBuckets',
                error: err.message,
            });
            return cb(err);
        }
        config.setIngestionBuckets(locations, buckets, logger);
        return cb();
    });
}

function updateLocations(conf, logger, cb) {
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

        Object.keys(locations).forEach(locName => {
            config.setIsTransientLocation(
                locName, locations[locName].isTransient);
        });
    } catch (error) {
        logger.error('error updating locations', {
            error: error.message,
            method: 'patchConfiguration::updateLocations',
        });
        return cb(error);
    }
    return cb(null, conf.version);
}

function patchConfiguration(overlayVersion, conf, metadata, ingestionEnabled,
logger, cb) {
    if (conf.version === undefined) {
        return process.nextTick(cb, null);
    }
    if (conf && conf.locations) {
        return async.waterfall([
            next => {
                // only update ingestion buckets list when ingestionEnabled
                if (!ingestionEnabled) {
                    return process.nextTick(next);
                }
                return updateIngestionBuckets(conf.locations, metadata, logger,
                    next);
            },
            next => {
                // only update bootstraplist on new overlay version
                if (overlayVersion !== undefined &&
                    overlayVersion >= conf.version) {
                    return process.nextTick(next);
                }
                return updateLocations(conf, logger, cb);
            }
        ], cb);
    }
    return process.nextTick(cb, null);
}

module.exports = patchConfiguration;
