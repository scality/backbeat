const assert = require('assert');
const QueueProcessor =
    require('../../../extensions/replication/queueProcessor/QueueProcessor');
const QueueEntry =
          require('../../../extensions/replication/utils/QueueEntry');
const kafkaEntry = require('../../utils/kafkaEntry');
const config = require('../../../conf/Config');

describe('QueueProcessor._getLocations helper method', () => {
    const zkConfig = config.zookeeper;
    const repConfig = config.extensions.replication;
    const sourceConfig = repConfig.source;
    const destConfig = repConfig.destination;
    const queueProcessor = new QueueProcessor(zkConfig, sourceConfig,
        destConfig, repConfig);

    it('should not alter an array when each part has only one element', () => {
        const entry = QueueEntry.createFromKafkaEntry(kafkaEntry);
        const locations = [
            {
                key: 'd1d1e055b19eb5a61adb8a665e626ff589cff233',
                size: 1,
                start: 0,
                dataStoreName: 'file',
                dataStoreETag: '1:0e5a6f42662652d44fcf978399ef5709',
            },
            {
                key: '4e67844b674b093a9e109d42172922ea1f32ec12',
                size: 3,
                start: 2,
                dataStoreName: 'file',
                dataStoreETag: '2:9ca655158ca025aa00a818b6b81f9e48',
            },
        ];
        entry.objMd.location = locations;
        assert.deepStrictEqual(queueProcessor._getLocations(entry), locations);
    });

    it('should reduce an array when first part is > 1 element', () => {
        const entry = QueueEntry.createFromKafkaEntry(kafkaEntry);
        entry.objMd.location = [
            {
                key: 'd1d1e055b19eb5a61adb8a665e626ff589cff233',
                size: 1,
                start: 0,
                dataStoreName: 'file',
                dataStoreETag: '1:0e5a6f42662652d44fcf978399ef5709',
            },
            {
                key: 'deebfb287cfcee1d137b0136562d2d776ba491e1',
                size: 1,
                start: 1,
                dataStoreName: 'file',
                dataStoreETag: '1:0e5a6f42662652d44fcf978399ef5709',
            },
            {
                key: '4e67844b674b093a9e109d42172922ea1f32ec12',
                size: 3,
                start: 2,
                dataStoreName: 'file',
                dataStoreETag: '2:9ca655158ca025aa00a818b6b81f9e48',
            },
        ];
        assert.deepStrictEqual(queueProcessor._getLocations(entry),
            [
                {
                    key: 'deebfb287cfcee1d137b0136562d2d776ba491e1',
                    size: 2,
                    start: 1,
                    dataStoreName: 'file',
                    dataStoreETag: '1:0e5a6f42662652d44fcf978399ef5709',
                },
                {
                    key: '4e67844b674b093a9e109d42172922ea1f32ec12',
                    size: 3,
                    start: 2,
                    dataStoreName: 'file',
                    dataStoreETag: '2:9ca655158ca025aa00a818b6b81f9e48',
                },
            ]);
    });

    it('should reduce an array when second part is > 1 element', () => {
        const entry = QueueEntry.createFromKafkaEntry(kafkaEntry);
        entry.objMd.location = [
            {
                key: 'd1d1e055b19eb5a61adb8a665e626ff589cff233',
                size: 1,
                start: 0,
                dataStoreName: 'file',
                dataStoreETag: '1:0e5a6f42662652d44fcf978399ef5709',
            },
            {
                key: 'deebfb287cfcee1d137b0136562d2d776ba491e1',
                size: 1,
                start: 1,
                dataStoreName: 'file',
                dataStoreETag: '2:9ca655158ca025aa00a818b6b81f9e48',
            },
            {
                key: '4e67844b674b093a9e109d42172922ea1f32ec12',
                size: 3,
                start: 2,
                dataStoreName: 'file',
                dataStoreETag: '2:9ca655158ca025aa00a818b6b81f9e48',
            },
        ];
        assert.deepStrictEqual(queueProcessor._getLocations(entry),
            [
                {
                    key: 'd1d1e055b19eb5a61adb8a665e626ff589cff233',
                    size: 1,
                    start: 0,
                    dataStoreName: 'file',
                    dataStoreETag: '1:0e5a6f42662652d44fcf978399ef5709',
                },
                {
                    key: '4e67844b674b093a9e109d42172922ea1f32ec12',
                    size: 4,
                    start: 2,
                    dataStoreName: 'file',
                    dataStoreETag: '2:9ca655158ca025aa00a818b6b81f9e48',
                },
            ]);
    });

    it('should reduce an array when multiple parts are > 1 element', () => {
        const entry = QueueEntry.createFromKafkaEntry(kafkaEntry);
        entry.objMd.location = [
            {
                key: 'd1d1e055b19eb5a61adb8a665e626ff589cff233',
                size: 1,
                start: 0,
                dataStoreName: 'file',
                dataStoreETag: '1:0e5a6f42662652d44fcf978399ef5709',
            },
            {
                key: 'd1d1e055b19eb5a61adb8a665e626ff589cff233',
                size: 2,
                start: 1,
                dataStoreName: 'file',
                dataStoreETag: '1:0e5a6f42662652d44fcf978399ef5709',
            },
            {
                key: 'deebfb287cfcee1d137b0136562d2d776ba491e1',
                size: 1,
                start: 3,
                dataStoreName: 'file',
                dataStoreETag: '2:9ca655158ca025aa00a818b6b81f9e48',
            },
            {
                key: '4e67844b674b093a9e109d42172922ea1f32ec12',
                size: 3,
                start: 4,
                dataStoreName: 'file',
                dataStoreETag: '2:9ca655158ca025aa00a818b6b81f9e48',
            },
        ];
        assert.deepStrictEqual(queueProcessor._getLocations(entry),
            [
                {
                    key: 'd1d1e055b19eb5a61adb8a665e626ff589cff233',
                    size: 3,
                    start: 1,
                    dataStoreName: 'file',
                    dataStoreETag: '1:0e5a6f42662652d44fcf978399ef5709',
                },
                {
                    key: '4e67844b674b093a9e109d42172922ea1f32ec12',
                    size: 4,
                    start: 4,
                    dataStoreName: 'file',
                    dataStoreETag: '2:9ca655158ca025aa00a818b6b81f9e48',
                },
            ]);
    });
});
