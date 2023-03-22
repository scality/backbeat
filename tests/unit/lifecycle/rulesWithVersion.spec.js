const assert = require('assert');

const { rulesToParams } = require(
    '../../../extensions/lifecycle/util/rules');

const ONE_DAY_IN_SEC = 60 * 60 * 24 * 1000;

const bucketName = 'bucket1';
const ownerId = 'f2a3ae88659516fbcad23cae38acc9fbdfcbcaf2e38c05d2e5e1bd1b9f930ff3';
const accountId = '345320934593';
const locationName = 'aws-loc';
const locationName2 = 'aws-loc2';

const bucketData = {
    action: 'processObjects',
    target: {
      bucket: bucketName,
      owner: ownerId,
      accountId,
    },
    details: {}
};

const expectedEmptyResult = {
    listingDetails: undefined,
    params: undefined,
    remainings: []
};

describe('rulesReducer with versioning Enabled', () => {
    const versioningStatus = 'Enabled';

    it('with no rule', () => {
        const currentDate = Date.now();
        const bucketLCRules = [];

        const result = rulesToParams(versioningStatus, currentDate, bucketLCRules, bucketData);
        assert.deepStrictEqual(result, expectedEmptyResult);
    });

    it('with irrelevant rule with date that has not passed', () => {
        const currentDate = Date.now();
        const bucketLCRules = [
            {
                Expiration: { Date: currentDate + 1000 },
                ID: '123',
                Prefix: '',
                Status: 'Enabled',
            }
        ];

        const result = rulesToParams(versioningStatus, currentDate, bucketLCRules, bucketData);
        assert.deepStrictEqual(result, expectedEmptyResult);
    });

    it('with rule targeting orphan delete markers', () => {
        const currentDate = Date.now();
        const bucketLCRules = [
            {
                Expiration: {
                    ExpiredObjectDeleteMarker: true,
                },
                ID: '123',
                Prefix: '',
                Status: 'Enabled',
            },
        ];

        const expected = {
            params: {
               Bucket: bucketName,
               Prefix: '',
               MaxKeys: 1000,
            },
            listingDetails: {
               listType: 'orphan',
               prefix: '',
            },
            remainings: []
        };

        const result = rulesToParams(versioningStatus, currentDate, bucketLCRules, bucketData);
        assert.deepStrictEqual(result, expected);
    });

    it('with disabled rule', () => {
        const currentDate = Date.now();
        const bucketLCRules = [
            {
                Expiration: { Date: currentDate },
                ID: '123',
                Prefix: '',
                Status: 'Disabled',
            }
        ];

        const result = rulesToParams(versioningStatus, currentDate, bucketLCRules, bucketData);
        assert.deepStrictEqual(result, expectedEmptyResult);
    });

    it('with details', () => {
        const currentDate = Date.now();
        const details = {
            listType: 'current',
            prefix: '',
            marker: 'key1'
        };
        const bd = { ...bucketData, details };
        const bucketLCRules = [
            {
                Expiration: { Date: currentDate },
                ID: '123',
                Prefix: '',
                Status: 'Enabled',
            }
        ];

        const result = rulesToParams(versioningStatus, currentDate, bucketLCRules, bd);
        const expected = {
            params: {
               Bucket: bucketName,
               Prefix: '',
               MaxKeys: 1000,
               Marker: 'key1',
            },
            listingDetails: {
               listType: 'current',
               prefix: '',
            },
            remainings: []
        };
        assert.deepStrictEqual(result, expected);
    });

    it('with details and prefix', () => {
        const currentDate = Date.now();
        const prefix = 'pre';
        const details = {
            listType: 'current',
            prefix,
            marker: 'key1'
        };
        const bd = { ...bucketData, details };
        const bucketLCRules = [
            {
                Expiration: { Date: currentDate },
                ID: '123',
                Prefix: '',
                Status: 'Enabled',
            }
        ];

        const result = rulesToParams(versioningStatus, currentDate, bucketLCRules, bd);
        const expected = {
            params: {
               Bucket: bucketName,
               Prefix: prefix,
               MaxKeys: 1000,
               Marker: 'key1',
            },
            listingDetails: {
               listType: 'current',
               prefix,
            },
            remainings: []
        };
        assert.deepStrictEqual(result, expected);
    });

    it('with details and beforeDate', () => {
        const currentDate = Date.now();
        const prefix = 'pre';
        const beforeDate = (new Date(currentDate)).toISOString();
        const details = {
            listType: 'current',
            prefix,
            beforeDate,
            marker: 'key1'
        };
        const bd = { ...bucketData, details };
        const bucketLCRules = [
            {
                Expiration: { Date: currentDate },
                ID: '123',
                Prefix: '',
                Status: 'Enabled',
            }
        ];

        const result = rulesToParams(versioningStatus, currentDate, bucketLCRules, bd);
        const expected = {
            params: {
               Bucket: bucketName,
               Prefix: prefix,
               MaxKeys: 1000,
               Marker: 'key1',
               BeforeDate: beforeDate,
            },
            listingDetails: {
               listType: 'current',
               prefix,
               beforeDate,
            },
            remainings: []
        };
        assert.deepStrictEqual(result, expected);
    });

    it('with Expiration rule using Date', () => {
        const currentDate = Date.now();
        const bucketLCRules = [
            {
                Expiration: { Date: currentDate },
                ID: '123',
                Prefix: '',
                Status: 'Enabled',
            }
        ];
        const expected = {
            params: {
               Bucket: bucketName,
               Prefix: '',
               MaxKeys: 1000,
            },
            listingDetails: {
               listType: 'current',
               prefix: '',
            },
            remainings: [{
                listType: 'orphan',
                prefix: '',
            }]
        };

        const result = rulesToParams(versioningStatus, currentDate, bucketLCRules, bucketData);
        assert.deepStrictEqual(result, expected);
    });

    it('with Expiration rule using Days', () => {
        const currentDate = Date.now();
        const prefix = '';
        const expectedBeforeDate = (new Date(currentDate - ONE_DAY_IN_SEC)).toISOString();
        const bucketLCRules = [
            {
                Expiration: { Days: 1 },
                ID: '123',
                Prefix: prefix,
                Status: 'Enabled',
            }
        ];
        const expected = {
            params: {
               Bucket: bucketName,
               Prefix: prefix,
               MaxKeys: 1000,
               BeforeDate: expectedBeforeDate,
            },
            listingDetails: {
               listType: 'current',
               prefix,
               beforeDate: expectedBeforeDate,
            },
            remainings: [{
                listType: 'orphan',
                prefix: '',
                beforeDate: expectedBeforeDate,
            }]
         };

        const result = rulesToParams(versioningStatus, currentDate, bucketLCRules, bucketData);
        assert.deepStrictEqual(result, expected);
    });

    it('with Expiration rule using Days and prefix', () => {
        const currentDate = Date.now();
        const prefix = 'pre';
        const expectedBeforeDate = (new Date(currentDate - ONE_DAY_IN_SEC)).toISOString();
        const bucketLCRules = [
            {
                Expiration: { Days: 1 },
                ID: '123',
                Prefix: prefix,
                Status: 'Enabled',
            }
        ];
        const expected = {
            params: {
               Bucket: bucketName,
               Prefix: prefix,
               MaxKeys: 1000,
               BeforeDate: expectedBeforeDate,
            },
            listingDetails: {
               listType: 'current',
               prefix,
               beforeDate: expectedBeforeDate,
            },
            remainings: [{
                listType: 'orphan',
                prefix,
                beforeDate: expectedBeforeDate,
            }]
         };

        const result = rulesToParams(versioningStatus, currentDate, bucketLCRules, bucketData);
        assert.deepStrictEqual(result, expected);
    });

    it('with Transitions rule using Date', () => {
        const currentDate = Date.now();
        const prefix = '';
        const bucketLCRules = [
            {
                Transitions: [{ Date: currentDate, StorageClass: locationName }],
                ID: '123',
                Prefix: prefix,
                Status: 'Enabled',
            }
        ];
        const expected = {
            params: {
               Bucket: bucketName,
               Prefix: prefix,
               MaxKeys: 1000,
            },
            listingDetails: {
               listType: 'current',
               prefix,
            },
            remainings: []
         };

        const result = rulesToParams(versioningStatus, currentDate, bucketLCRules, bucketData);
        assert.deepStrictEqual(result, expected);
    });

    it('with Expiration and Transitions rule using Days', () => {
        const currentDate = Date.now();
        const prefix = '';
        const expectedBeforeDate = (new Date(currentDate - ONE_DAY_IN_SEC)).toISOString();
        const bucketLCRules = [
            {
                Expiration: { Days: 1 },
                Transitions: [{ Days: 2, StorageClass: locationName }],
                ID: '123',
                Prefix: prefix,
                Status: 'Enabled',
            }
        ];
        const expected = {
            params: {
               Bucket: bucketName,
               Prefix: prefix,
               MaxKeys: 1000,
               BeforeDate: expectedBeforeDate,
            },
            listingDetails: {
               listType: 'current',
               prefix,
               beforeDate: expectedBeforeDate,
            },
            remainings: [{
                listType: 'orphan',
                prefix,
                beforeDate: expectedBeforeDate,
            }]
         };

        const result = rulesToParams(versioningStatus, currentDate, bucketLCRules, bucketData);
        assert.deepStrictEqual(result, expected);
    });

    it('with Expiration and Transitions rule using Days and Date', () => {
        const currentDate = Date.now();
        const expectedBeforeDate = (new Date(currentDate - ONE_DAY_IN_SEC)).toISOString();
        const prefix = '';
        const bucketLCRules = [
            {
                Expiration: { Days: 1 },
                Transitions: [{ Date: currentDate, StorageClass: locationName }],
                ID: '123',
                Prefix: prefix,
                Status: 'Enabled',
            }
        ];
        const expected = {
            params: {
               Bucket: bucketName,
               Prefix: prefix,
               MaxKeys: 1000,
            },
            listingDetails: {
               listType: 'current',
               prefix,
            },
            remainings: [{
                listType: 'orphan',
                beforeDate: expectedBeforeDate,
                prefix,
            }]
         };

        const result = rulesToParams(versioningStatus, currentDate, bucketLCRules, bucketData);
        assert.deepStrictEqual(result, expected);
    });

    it('with Transition rules with multiple transitions', () => {
        const currentDate = Date.now();
        const expectedBeforeDate = (new Date(currentDate - ONE_DAY_IN_SEC)).toISOString();
        const bucketLCRules = [
            {
                Transitions: [
                    { Days: 1, StorageClass: locationName },
                    { Days: 2, StorageClass: locationName2 }
                ],
                ID: '123',
                Prefix: 'toto/titi',
                Status: 'Enabled',
            },
        ];
        const expected = {
            params: {
               Bucket: bucketName,
               Prefix: 'toto/titi',
               MaxKeys: 1000,
               BeforeDate: expectedBeforeDate,
            },
            listingDetails: {
               listType: 'current',
               prefix: 'toto/titi',
               beforeDate: expectedBeforeDate,
            },
            remainings: []
         };

        const result = rulesToParams(versioningStatus, currentDate, bucketLCRules, bucketData);
        assert.deepStrictEqual(result, expected);
    });

    it('with multiple Expiration rules that share prefix', () => {
        const currentDate = Date.now();
        const expectedBeforeDate = (new Date(currentDate - ONE_DAY_IN_SEC)).toISOString();
        const bucketLCRules = [
            {
                Expiration: { Days: 1 },
                ID: '123',
                Prefix: 'toto/titi',
                Status: 'Enabled',
            },
            {
                Expiration: { Days: 2 },
                ID: '456',
                Prefix: 'toto',
                Status: 'Enabled',
            }
        ];
        const expected = {
            params: {
               Bucket: bucketName,
               Prefix: 'toto',
               MaxKeys: 1000,
               BeforeDate: expectedBeforeDate,
            },
            listingDetails: {
               listType: 'current',
               prefix: 'toto',
               beforeDate: expectedBeforeDate,
            },
            remainings: [{
                listType: 'orphan',
                prefix: 'toto',
                beforeDate: expectedBeforeDate,
            }]
         };

        const result = rulesToParams(versioningStatus, currentDate, bucketLCRules, bucketData);
        assert.deepStrictEqual(result, expected);
    });

    it('with multiple Transition rules that share prefix', () => {
        const currentDate = Date.now();
        const expectedBeforeDate = (new Date(currentDate - ONE_DAY_IN_SEC)).toISOString();
        const bucketLCRules = [
            {
                Transitions: [{ Days: 1, StorageClass: locationName }],
                ID: '123',
                Prefix: 'toto/titi',
                Status: 'Enabled',
            },
            {
                Transitions: [{ Days: 2, StorageClass: locationName }],
                ID: '456',
                Prefix: 'toto',
                Status: 'Enabled',
            }
        ];
        const expected = {
            params: {
               Bucket: bucketName,
               Prefix: 'toto',
               MaxKeys: 1000,
               BeforeDate: expectedBeforeDate,
            },
            listingDetails: {
               listType: 'current',
               prefix: 'toto',
               beforeDate: expectedBeforeDate,
            },
            remainings: []
         };

        const result = rulesToParams(versioningStatus, currentDate, bucketLCRules, bucketData);
        assert.deepStrictEqual(result, expected);
    });

    it('with multiple Expiration rules that do not share prefix', () => {
        const currentDate = Date.now();
        const expectedBeforeDate = (new Date(currentDate - ONE_DAY_IN_SEC)).toISOString();
        const expectedBeforeDate2 = (new Date(currentDate - 2 * ONE_DAY_IN_SEC)).toISOString();
        const bucketLCRules = [
            {
                Expiration: { Days: 1 },
                ID: '123',
                Prefix: 'titi',
                Status: 'Enabled',
            },
            {
                Expiration: { Days: 2 },
                ID: '456',
                Prefix: 'toto',
                Status: 'Enabled',
            }
        ];
        const expected = {
            params: {
               Bucket: bucketName,
               Prefix: 'toto',
               MaxKeys: 1000,
               BeforeDate: expectedBeforeDate2,
            },
            listingDetails: {
               listType: 'current',
               prefix: 'toto',
               beforeDate: expectedBeforeDate2,
            },
            remainings: [{
                listType: 'current',
                prefix: 'titi',
                beforeDate: expectedBeforeDate,
            }, {
                listType: 'orphan',
                prefix: 'toto',
                beforeDate: expectedBeforeDate2,
            }, {
                listType: 'orphan',
                prefix: 'titi',
                beforeDate: expectedBeforeDate,
            }]
         };

        const result = rulesToParams(versioningStatus, currentDate, bucketLCRules, bucketData);
        assert.deepStrictEqual(result, expected);
    });

    it('with multiple Transitions rules that do not share prefix', () => {
        const currentDate = Date.now();
        const expectedBeforeDate = (new Date(currentDate - ONE_DAY_IN_SEC)).toISOString();
        const bucketLCRules = [
            {
                Transitions: [{ Days: 1, StorageClass: locationName }],
                ID: '123',
                Prefix: 'titi',
                Status: 'Enabled',
            },
            {
                Transitions: [{ Date: currentDate, StorageClass: locationName2 }],
                ID: '456',
                Prefix: 'toto',
                Status: 'Enabled',
            }
        ];
        const expected = {
            params: {
               Bucket: bucketName,
               Prefix: 'toto',
               MaxKeys: 1000,
            },
            listingDetails: {
               listType: 'current',
               prefix: 'toto',
            },
            remainings: [{
                listType: 'current',
                prefix: 'titi',
                beforeDate: expectedBeforeDate,
             }]
         };

        const result = rulesToParams(versioningStatus, currentDate, bucketLCRules, bucketData);
        assert.deepStrictEqual(result, expected);
    });

    it('with multiple Expiration and Transitions rules that do share prefix', () => {
        const currentDate = Date.now();
        const expectedBeforeDate = (new Date(currentDate - ONE_DAY_IN_SEC)).toISOString();
        const bucketLCRules = [
            {
                Expiration: { Days: 1 },
                ID: '123',
                Prefix: 'toto/titi',
                Status: 'Enabled',
            },
            {
                Transitions: [{ Date: currentDate, StorageClass: locationName2 }],
                ID: '456',
                Prefix: 'toto',
                Status: 'Enabled',
            }
        ];
        const expected = {
            params: {
               Bucket: bucketName,
               Prefix: 'toto',
               MaxKeys: 1000,
            },
            listingDetails: {
               listType: 'current',
               prefix: 'toto',
            },
            remainings: [{
                listType: 'orphan',
                prefix: 'toto/titi',
                beforeDate: expectedBeforeDate,
            }]
         };

        const result = rulesToParams(versioningStatus, currentDate, bucketLCRules, bucketData);
        assert.deepStrictEqual(result, expected);
    });

    it('with multiple Expiration and Transitions rules that do not share prefix', () => {
        const currentDate = Date.now();
        const expectedBeforeDate = (new Date(currentDate - ONE_DAY_IN_SEC)).toISOString();
        const bucketLCRules = [
            {
                Expiration: { Days: 1 },
                ID: '123',
                Prefix: 'titi',
                Status: 'Enabled',
            },
            {
                Transitions: [{ Date: currentDate, StorageClass: locationName2 }],
                ID: '456',
                Prefix: 'toto',
                Status: 'Enabled',
            }
        ];
        const expected = {
            params: {
               Bucket: bucketName,
               Prefix: 'toto',
               MaxKeys: 1000,
            },
            listingDetails: {
               listType: 'current',
               prefix: 'toto',
            },
            remainings: [{
                listType: 'current',
                prefix: 'titi',
                beforeDate: expectedBeforeDate,
            }, {
                listType: 'orphan',
                prefix: 'titi',
                beforeDate: expectedBeforeDate,
            }]
        };

        const result = rulesToParams(versioningStatus, currentDate, bucketLCRules, bucketData);
        assert.deepStrictEqual(result, expected);
    });

    it('with NoncurrentVersionExpiration and NoncurrentVersionTransitions rules', () => {
        const currentDate = Date.now();
        const bucketLCRules = [
            {
                NoncurrentVersionExpiration: { NoncurrentDays: 1 },
                ID: '123',
                Prefix: '',
                Status: 'Enabled',
            },
            {
                NoncurrentVersionTransitions: [{ NoncurrentDays: 0, StorageClass: locationName }],
                ID: '456',
                Prefix: '',
                Status: 'Enabled',
            }
        ];

        const expected = {
            params: {
                Bucket: bucketName,
                Prefix: '',
                MaxKeys: 1000,
            },
            listingDetails: {
                listType: 'noncurrent',
                prefix: '',
            },
            remainings: []
        };

        const result = rulesToParams(versioningStatus, currentDate, bucketLCRules, bucketData);
        assert.deepStrictEqual(result, expected);
    });

    it('with NoncurrentVersionExpiration and NoncurrentVersionTransitions rules with different prefix', () => {
        const currentDate = Date.now();
        const expectedBeforeDate = (new Date(currentDate - ONE_DAY_IN_SEC)).toISOString();
        const bucketLCRules = [
            {
                NoncurrentVersionExpiration: { NoncurrentDays: 1 },
                ID: '123',
                Prefix: 'p1',
                Status: 'Enabled',
            },
            {
                NoncurrentVersionTransitions: [{ NoncurrentDays: 0, StorageClass: locationName }],
                ID: '456',
                Prefix: 'p2',
                Status: 'Enabled',
            },
        ];

        const expected = {
            params: {
                Bucket: bucketName,
                Prefix: 'p2',
                MaxKeys: 1000,
            },
            listingDetails: {
                listType: 'noncurrent',
                prefix: 'p2',
            },
            remainings: [{
                listType: 'noncurrent',
                prefix: 'p1',
                beforeDate: expectedBeforeDate,
            }]
        };

        const result = rulesToParams(versioningStatus, currentDate, bucketLCRules, bucketData);
        assert.deepStrictEqual(result, expected);
    });

    it('with NoncurrentVersionExpiration and NoncurrentVersionTransitions rules with shared prefix', () => {
        const currentDate = Date.now();
        const bucketLCRules = [
            {
                NoncurrentVersionExpiration: { NoncurrentDays: 1 },
                ID: '123',
                Prefix: 'toto',
                Status: 'Enabled',
            },
            {
                NoncurrentVersionTransitions: [{ NoncurrentDays: 0, StorageClass: locationName }],
                ID: '456',
                Prefix: 'toto/titi',
                Status: 'Enabled',
            },
        ];

        const expected = {
            params: {
                Bucket: bucketName,
                Prefix: 'toto',
                MaxKeys: 1000,
            },
            listingDetails: {
                listType: 'noncurrent',
                prefix: 'toto',
            },
            remainings: []
        };

        const result = rulesToParams(versioningStatus, currentDate, bucketLCRules, bucketData);
        assert.deepStrictEqual(result, expected);
    });

    it('with Expiration, Transtions, NoncurrentVersionExpiration and NoncurrentVersionTransitions rules', () => {
        const currentDate = Date.now();
        const expectedBeforeDate = (new Date(currentDate - ONE_DAY_IN_SEC)).toISOString();
        const expectedBeforeDate2 = (new Date(currentDate - 2 * ONE_DAY_IN_SEC)).toISOString();
        const bucketLCRules = [
            {
                Expiration: { Days: 2 },
                ID: '123',
                Prefix: '',
                Status: 'Enabled',
            },
            {
                Transitions: [{ Days: 1, StorageClass: locationName }],
                ID: '456',
                Prefix: '',
                Status: 'Enabled',
            },
            {
                NoncurrentVersionExpiration: { NoncurrentDays: 2 },
                ID: '789',
                Prefix: '',
                Status: 'Enabled',
            },
            {
                NoncurrentVersionTransitions: [{ NoncurrentDays: 1, StorageClass: locationName }],
                ID: '101',
                Prefix: '',
                Status: 'Enabled',
            },
        ];

        const expected = {
            params: {
                Bucket: bucketName,
                Prefix: '',
                MaxKeys: 1000,
                BeforeDate: expectedBeforeDate,
            },
            listingDetails: {
                listType: 'current',
                prefix: '',
                beforeDate: expectedBeforeDate,
            },
            remainings: [{
                listType: 'noncurrent',
                prefix: '',
                beforeDate: expectedBeforeDate,
            }, {
                listType: 'orphan',
                prefix: '',
                beforeDate: expectedBeforeDate2,
            }]
        };

        const result = rulesToParams(versioningStatus, currentDate, bucketLCRules, bucketData);
        assert.deepStrictEqual(result, expected);
    });

    it('with Expiration and ExpiredObjectDeleteMarker rules', () => {
        const currentDate = Date.now();
        const expectedBeforeDate = (new Date(currentDate - ONE_DAY_IN_SEC)).toISOString();
        const bucketLCRules = [
            {
                Expiration: { Days: 1 },
                ID: '123',
                Prefix: '',
                Status: 'Enabled',
            },
            {
                Expiration: { ExpiredObjectDeleteMarker: true },
                ID: '456',
                Prefix: '',
                Status: 'Enabled',
            },
        ];

        const expected = {
            params: {
                Bucket: bucketName,
                Prefix: '',
                MaxKeys: 1000,
                BeforeDate: expectedBeforeDate,
            },
            listingDetails: {
                listType: 'current',
                prefix: '',
                beforeDate: expectedBeforeDate,
            },
            remainings: [{
                listType: 'orphan',
                prefix: '',
            }]
        };

        const result = rulesToParams(versioningStatus, currentDate, bucketLCRules, bucketData);
        assert.deepStrictEqual(result, expected);
    });
});
