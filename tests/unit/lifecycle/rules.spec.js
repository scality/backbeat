const assert = require('assert');

const { rulesToParams, rulesSupportTransition } = require('../../../extensions/lifecycle/util/rules');

const ONE_MINUTE_IN_SEC = 60 * 1000;
const ONE_DAY_IN_SEC = 24 * 60 * ONE_MINUTE_IN_SEC;
const MAX_KEYS = process.env.CI === 'true' ? 3 : 1000;

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
    listType: undefined,
    params: undefined,
    remainings: []
};

const options = {
    expireOneDayEarlier: false,
    transitionOneDayEarlier: false,
};

describe('rulesToParams with versioning Disabled', () => {
    const versioningStatus = 'Disabled';

    it('with no rule', () => {
        const currentDate = Date.now();
        const bucketLCRules = [];

        const result = rulesToParams(versioningStatus, currentDate, bucketLCRules, bucketData, options);
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

        const result = rulesToParams(versioningStatus, currentDate, bucketLCRules, bucketData, options);
        assert.deepStrictEqual(result, expectedEmptyResult);
    });

    it('with rule targeting non current versions', () => {
        const currentDate = Date.now();
        const bucketLCRules = [
            {
                NoncurrentVersionExpiration: { NoncurrentDays: 1 },
                ID: '123',
                Prefix: '',
                Status: 'Enabled',
            },
            {
                NoncurrentVersionTransitions: [{ NoncurrentDays: 1, StorageClass: locationName }],
                ID: '456',
                Prefix: '',
                Status: 'Enabled',
            }
        ];

        const result = rulesToParams(versioningStatus, currentDate, bucketLCRules, bucketData, options);
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

        const result = rulesToParams(versioningStatus, currentDate, bucketLCRules, bucketData, options);
        assert.deepStrictEqual(result, expectedEmptyResult);
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

        const result = rulesToParams(versioningStatus, currentDate, bucketLCRules, bucketData, options);
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

        const result = rulesToParams(versioningStatus, currentDate, bucketLCRules, bd, options);
        const expected = {
            params: {
               Bucket: bucketName,
               Prefix: '',
               MaxKeys: MAX_KEYS,
               Marker: 'key1',
            },
            listType: 'current',
            remainings: []
        };
        assert.deepStrictEqual(result, expected);
    });

    it('with invalid details listType', () => {
        const currentDate = Date.now();
        const prefix = 'pre';
        const details = {
            listType: 'invalid',
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

        const result = rulesToParams(versioningStatus, currentDate, bucketLCRules, bd, options);
        const expected = {};
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

        const result = rulesToParams(versioningStatus, currentDate, bucketLCRules, bd, options);
        const expected = {
            params: {
               Bucket: bucketName,
               Prefix: prefix,
               MaxKeys: MAX_KEYS,
               Marker: 'key1',
            },
            listType: 'current',
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

        const result = rulesToParams(versioningStatus, currentDate, bucketLCRules, bd, options);
        const expected = {
            params: {
               Bucket: bucketName,
               Prefix: prefix,
               MaxKeys: MAX_KEYS,
               Marker: 'key1',
               BeforeDate: beforeDate,
            },
            listType: 'current',
            remainings: []
        };
        assert.deepStrictEqual(result, expected);
    });

    it('with details and storageClass', () => {
        const currentDate = Date.now();
        const prefix = 'pre';
        const details = {
            listType: 'current',
            prefix,
            storageClass: locationName,
            marker: 'key1'
        };
        const bd = { ...bucketData, details };
        const bucketLCRules = [
            {
                Transition: [{ Days: 10, StorageClass: locationName }],
                ID: '123',
                Prefix: '',
                Status: 'Enabled',
            }
        ];

        const result = rulesToParams(versioningStatus, currentDate, bucketLCRules, bd, options);
        const expected = {
            params: {
               Bucket: bucketName,
               Prefix: prefix,
               MaxKeys: MAX_KEYS,
               Marker: 'key1',
               ExcludedDataStoreName: locationName,
            },
            listType: 'current',
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
               MaxKeys: MAX_KEYS,
            },
            listType: 'current',
            remainings: []
        };

        const result = rulesToParams(versioningStatus, currentDate, bucketLCRules, bucketData, options);
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
               MaxKeys: MAX_KEYS,
               BeforeDate: expectedBeforeDate,
            },
            listType: 'current',
            remainings: []
         };

        const result = rulesToParams(versioningStatus, currentDate, bucketLCRules, bucketData, options);
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
               MaxKeys: MAX_KEYS,
               BeforeDate: expectedBeforeDate,
            },
            listType: 'current',
            remainings: []
         };

        const result = rulesToParams(versioningStatus, currentDate, bucketLCRules, bucketData, options);
        assert.deepStrictEqual(result, expected);
    });

    it('with Expiration rule using Filter.Prefix', () => {
        const currentDate = Date.now();
        const prefix = 'pre';
        const expectedBeforeDate = (new Date(currentDate - ONE_DAY_IN_SEC)).toISOString();
        const bucketLCRules = [
            {
                Expiration: { Days: 1 },
                ID: '123',
                Filter: {
                    Prefix: prefix,
                },
                Status: 'Enabled',
            }
        ];
        const expected = {
            params: {
               Bucket: bucketName,
               Prefix: prefix,
               MaxKeys: MAX_KEYS,
               BeforeDate: expectedBeforeDate,
            },
            listType: 'current',
            remainings: []
         };

        const result = rulesToParams(versioningStatus, currentDate, bucketLCRules, bucketData, options);
        assert.deepStrictEqual(result, expected);
    });

    it('with Expiration rule using Filter.And.Prefix', () => {
        const currentDate = Date.now();
        const prefix = 'pre';
        const expectedBeforeDate = (new Date(currentDate - ONE_DAY_IN_SEC)).toISOString();
        const bucketLCRules = [
            {
                Expiration: { Days: 1 },
                ID: '123',
                Filter: {
                    And: {
                        Prefix: prefix,
                    }
                },
                Status: 'Enabled',
            }
        ];
        const expected = {
            params: {
               Bucket: bucketName,
               Prefix: prefix,
               MaxKeys: MAX_KEYS,
               BeforeDate: expectedBeforeDate,
            },
            listType: 'current',
            remainings: []
         };

        const result = rulesToParams(versioningStatus, currentDate, bucketLCRules, bucketData, options);
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
               MaxKeys: MAX_KEYS,
               ExcludedDataStoreName: locationName,
            },
            listType: 'current',
            remainings: []
         };

        const result = rulesToParams(versioningStatus, currentDate, bucketLCRules, bucketData, options);
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
               MaxKeys: MAX_KEYS,
               BeforeDate: expectedBeforeDate,
            },
            listType: 'current',
            remainings: []
         };

        const result = rulesToParams(versioningStatus, currentDate, bucketLCRules, bucketData, options);
        assert.deepStrictEqual(result, expected);
    });

    it('with Expiration and Transitions rule using Days and Date', () => {
        const currentDate = Date.now();
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
               MaxKeys: MAX_KEYS,
            },
            listType: 'current',
            remainings: []
         };

        const result = rulesToParams(versioningStatus, currentDate, bucketLCRules, bucketData, options);
        assert.deepStrictEqual(result, expected);
    });

    it('with one Transition rule but multiple transitions', () => {
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
               MaxKeys: MAX_KEYS,
               BeforeDate: expectedBeforeDate,
            },
            listType: 'current',
            remainings: []
         };

        const result = rulesToParams(versioningStatus, currentDate, bucketLCRules, bucketData, options);
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
               MaxKeys: MAX_KEYS,
               BeforeDate: expectedBeforeDate,
            },
            listType: 'current',
            remainings: []
         };

        const result = rulesToParams(versioningStatus, currentDate, bucketLCRules, bucketData, options);
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
               MaxKeys: MAX_KEYS,
               BeforeDate: expectedBeforeDate,
               ExcludedDataStoreName: locationName,
            },
            listType: 'current',
            remainings: []
         };

        const result = rulesToParams(versioningStatus, currentDate, bucketLCRules, bucketData, options);
        assert.deepStrictEqual(result, expected);
    });

    it('with multiple Transition rules that share prefix with one disabled', () => {
        const currentDate = Date.now();
        const expectedBeforeDate = (new Date(currentDate - 2 * ONE_DAY_IN_SEC)).toISOString();
        const bucketLCRules = [
            {
                Transitions: [{ Days: 1, StorageClass: locationName }],
                ID: '123',
                Prefix: 'toto/titi',
                Status: 'Disabled',
            },
            {
                Transitions: [{ Days: 2, StorageClass: locationName2 }],
                ID: '456',
                Prefix: 'toto',
                Status: 'Enabled',
            }
        ];
        const expected = {
            params: {
               Bucket: bucketName,
               Prefix: 'toto',
               MaxKeys: MAX_KEYS,
               BeforeDate: expectedBeforeDate,
               ExcludedDataStoreName: locationName2,
            },
            listType: 'current',
            remainings: []
         };

        const result = rulesToParams(versioningStatus, currentDate, bucketLCRules, bucketData, options);
        assert.deepStrictEqual(result, expected);
    });

    it('with multiple Transition rules that share prefix but not location', () => {
        const currentDate = Date.now();
        const expectedBeforeDate = (new Date(currentDate - ONE_DAY_IN_SEC)).toISOString();
        const bucketLCRules = [
            {
                Transitions: [{ Days: 1, StorageClass: locationName }],
                ID: '123',
                Prefix: 'p1',
                Status: 'Enabled',
            },
            {
                Transitions: [{ Days: 2, StorageClass: locationName2 }],
                ID: '456',
                Prefix: 'p1',
                Status: 'Enabled',
            }
        ];
        const expected = {
            params: {
               Bucket: bucketName,
               Prefix: 'p1',
               MaxKeys: MAX_KEYS,
               BeforeDate: expectedBeforeDate,
            },
            listType: 'current',
            remainings: []
         };

        const result = rulesToParams(versioningStatus, currentDate, bucketLCRules, bucketData, options);
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
               Prefix: 'titi',
               MaxKeys: MAX_KEYS,
               BeforeDate: expectedBeforeDate,
            },
            listType: 'current',
            remainings: [{
                listType: 'current',
                prefix: 'toto',
                beforeDate: expectedBeforeDate2,
             }]
         };

        const result = rulesToParams(versioningStatus, currentDate, bucketLCRules, bucketData, options);
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
               Prefix: 'titi',
               BeforeDate: expectedBeforeDate,
               MaxKeys: MAX_KEYS,
               ExcludedDataStoreName: locationName,
            },
            listType: 'current',
            remainings: [{
                listType: 'current',
                prefix: 'toto',
                storageClass: locationName2,
             }]
         };

        const result = rulesToParams(versioningStatus, currentDate, bucketLCRules, bucketData, options);
        assert.deepStrictEqual(result, expected);
    });

    it('with Expiration and Transitions rules that share prefix', () => {
        const currentDate = Date.now();
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
               MaxKeys: MAX_KEYS,
            },
            listType: 'current',
            remainings: []
         };

        const result = rulesToParams(versioningStatus, currentDate, bucketLCRules, bucketData, options);
        assert.deepStrictEqual(result, expected);
    });

    it('with Expiration and Transitions rules that do not share prefix', () => {
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
               Prefix: 'titi',
               MaxKeys: MAX_KEYS,
               BeforeDate: expectedBeforeDate,
            },
            listType: 'current',
            remainings: [{
                listType: 'current',
                prefix: 'toto',
                storageClass: locationName2,
             }]
         };

        const result = rulesToParams(versioningStatus, currentDate, bucketLCRules, bucketData, options);
        assert.deepStrictEqual(result, expected);
    });

    it('with expireOneDayEarlier', () => {
        const currentDate = Date.now();
        const customOptions = {
            expireOneDayEarlier: true,
            transitionOneDayEarlier: false,
            timeProgressionFactor: 1,
        };
        const expectedBeforeDate = (new Date(currentDate - ONE_DAY_IN_SEC)).toISOString();
        const bucketLCRules = [
            {
                Expiration: { Days: 1 },
                ID: '123',
                Prefix: 'titi',
                Status: 'Enabled',
            },
            {
                Transitions: [{ Days: 1, StorageClass: locationName2 }],
                ID: '456',
                Prefix: 'toto',
                Status: 'Enabled',
            }
        ];
        const expected = {
            params: {
               Bucket: bucketName,
               Prefix: 'titi',
               MaxKeys: MAX_KEYS,
            },
            listType: 'current',
            remainings: [{
                listType: 'current',
                prefix: 'toto',
                beforeDate: expectedBeforeDate,
                storageClass: locationName2,
             }]
         };

        const result = rulesToParams(versioningStatus, currentDate, bucketLCRules, bucketData, customOptions);
        assert.deepStrictEqual(result, expected);
    });

    it('with transitionOneDayEarlier', () => {
        const currentDate = Date.now();
        const customOptions = {
            expireOneDayEarlier: false,
            transitionOneDayEarlier: true,
            timeProgressionFactor: 1,
        };
        const expectedBeforeDate = (new Date(currentDate - ONE_DAY_IN_SEC)).toISOString();
        const bucketLCRules = [
            {
                Expiration: { Days: 1 },
                ID: '123',
                Prefix: 'titi',
                Status: 'Enabled',
            },
            {
                Transitions: [{ Days: 1, StorageClass: locationName2 }],
                ID: '456',
                Prefix: 'toto',
                Status: 'Enabled',
            }
        ];

        const expected = {
            params: {
               Bucket: bucketName,
               Prefix: 'titi',
               MaxKeys: MAX_KEYS,
               BeforeDate: expectedBeforeDate,
            },
            listType: 'current',
            remainings: [{
                listType: 'current',
                prefix: 'toto',
                storageClass: locationName2,
            }]
        };

        const result = rulesToParams(versioningStatus, currentDate, bucketLCRules, bucketData, customOptions);
        assert.deepStrictEqual(result, expected);
    });

    it('with expireOneDayEarlier and transitionOneDayEarlier', () => {
        const currentDate = Date.now();
        const customOptions = {
            expireOneDayEarlier: true,
            transitionOneDayEarlier: true,
            timeProgressionFactor: 1,
        };
        const bucketLCRules = [
            {
                Expiration: { Days: 1 },
                ID: '123',
                Prefix: 'titi',
                Status: 'Enabled',
            },
            {
                Transitions: [{ Days: 1, StorageClass: locationName2 }],
                ID: '456',
                Prefix: 'toto',
                Status: 'Enabled',
            }
        ];

        const expected = {
            params: {
               Bucket: bucketName,
               Prefix: 'titi',
               MaxKeys: MAX_KEYS,
            },
            listType: 'current',
            remainings: [{
                listType: 'current',
                prefix: 'toto',
                storageClass: locationName2,
            }]
        };
        const result = rulesToParams(versioningStatus, currentDate, bucketLCRules, bucketData, customOptions);
        assert.deepStrictEqual(result, expected);
    });

    it('with timeProgressionFactor set to 2', () => {
        const currentDate = Date.now();
        const customOptions = {
            expireOneDayEarlier: false,
            transitionOneDayEarlier: false,
            timeProgressionFactor: 2,
        };
        const expectedBeforeDate = (new Date(currentDate -  (ONE_DAY_IN_SEC / 2))).toISOString();
        const bucketLCRules = [
            {
                Expiration: { Days: 1 },
                ID: '123',
                Prefix: 'titi',
                Status: 'Enabled',
            },
            {
                Transitions: [{ Days: 1, StorageClass: locationName2 }],
                ID: '456',
                Prefix: 'toto',
                Status: 'Enabled',
            }
        ];
        const expected = {
            params: {
               Bucket: bucketName,
               Prefix: 'titi',
               MaxKeys: MAX_KEYS,
               BeforeDate: expectedBeforeDate,
            },
            listType: 'current',
            remainings: [{
                listType: 'current',
                prefix: 'toto',
                beforeDate: expectedBeforeDate,
                storageClass: locationName2,
             }]
         };

        const result = rulesToParams(versioningStatus, currentDate, bucketLCRules, bucketData, customOptions);
        assert.deepStrictEqual(result, expected);
    });

    it('with timeProgressionFactor set to one day in minutes (24 * 60)', () => {
        const currentDate = Date.now();
        const customOptions = {
            expireOneDayEarlier: false,
            transitionOneDayEarlier: false,
            timeProgressionFactor: 24 * 60,
        };
        const expectedBeforeDate = (new Date(currentDate -  ONE_MINUTE_IN_SEC)).toISOString();
        const bucketLCRules = [
            {
                Expiration: { Days: 1 },
                ID: '123',
                Prefix: 'titi',
                Status: 'Enabled',
            },
            {
                Transitions: [{ Days: 1, StorageClass: locationName2 }],
                ID: '456',
                Prefix: 'toto',
                Status: 'Enabled',
            }
        ];
        const expected = {
            params: {
               Bucket: bucketName,
               Prefix: 'titi',
               MaxKeys: MAX_KEYS,
               BeforeDate: expectedBeforeDate,
            },
            listType: 'current',
            remainings: [{
                listType: 'current',
                prefix: 'toto',
                beforeDate: expectedBeforeDate,
                storageClass: locationName2,
             }]
         };

        const result = rulesToParams(versioningStatus, currentDate, bucketLCRules, bucketData, customOptions);
        assert.deepStrictEqual(result, expected);
    });
});

describe('rulesSupportTransition', () => {
    it('sould return false if no transition rule supported', () => {
        const rules = [
            'expiration',
            'noncurrentVersionExpiration',
            'abortIncompleteMultipartUpload',
        ];
        assert.strictEqual(rulesSupportTransition(rules), false);
    });

    it('sould return true if "transitions" rule supported', () => {
        const rules = [
            'expiration',
            'noncurrentVersionExpiration',
            'abortIncompleteMultipartUpload',
            'transitions',
        ];
        assert.strictEqual(rulesSupportTransition(rules), true);
    });

    it('sould return true if "noncurrentVersionTransition" rule supported', () => {
        const rules = [
            'expiration',
            'noncurrentVersionExpiration',
            'abortIncompleteMultipartUpload',
            'noncurrentVersionTransition',
        ];
        assert.strictEqual(rulesSupportTransition(rules), true);
    });
});
