const assert = require('assert');

const { RulesReducer } = require('../../../extensions/lifecycle/util/RulesReducer');

const locationName = 'aws-loc';
const locationName2 = 'aws-loc2';

const expectedEmptyResult = {
    currents: []
};

const options = {
    expireOneDayEarlier: false,
    transitionOneDayEarlier: false,
};

describe('RulesReducer with versioning Disabled', () => {
    const versioningStatus = 'Disabled';

    it('with no rule', () => {
        const currentDate = Date.now();
        const bucketLCRules = [];
        const rulesReducer = new RulesReducer(versioningStatus, currentDate, bucketLCRules, options);
        const result = rulesReducer.toListings();

        assert.deepStrictEqual(result, expectedEmptyResult);
    });

    it('with date that has not passed', () => {
        const currentDate = Date.now();
        const bucketLCRules = [
            {
                Expiration: { Date: currentDate + 1000 },
                ID: '123',
                Prefix: '',
                Status: 'Enabled',
            }
        ];
        const rulesReducer = new RulesReducer(versioningStatus, currentDate, bucketLCRules, options);
        const result = rulesReducer.toListings();

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
        const rulesReducer = new RulesReducer(versioningStatus, currentDate, bucketLCRules, options);
        const result = rulesReducer.toListings();

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
        const rulesReducer = new RulesReducer(versioningStatus, currentDate, bucketLCRules, options);
        const result = rulesReducer.toListings();

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
        const rulesReducer = new RulesReducer(versioningStatus, currentDate, bucketLCRules, options);
        const result = rulesReducer.toListings();

        assert.deepStrictEqual(result, expectedEmptyResult);
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
            currents: [{
                prefix: '',
                days: 0,
            }]
        };
        const rulesReducer = new RulesReducer(versioningStatus, currentDate, bucketLCRules, options);
        const result = rulesReducer.toListings();

        assert.deepStrictEqual(result, expected);
    });

    it('with Expiration rule using Days', () => {
        const currentDate = Date.now();
        const prefix = '';
        const bucketLCRules = [
            {
                Expiration: { Days: 1 },
                ID: '123',
                Prefix: prefix,
                Status: 'Enabled',
            }
        ];
        const expected = {
            currents: [{
                prefix: '',
                days: 1,
            }]
        };
        const rulesReducer = new RulesReducer(versioningStatus, currentDate, bucketLCRules, options);
        const result = rulesReducer.toListings();

        assert.deepStrictEqual(result, expected);
    });

    it('with Expiration rule using Days and prefix', () => {
        const currentDate = Date.now();
        const prefix = 'pre';
        const bucketLCRules = [
            {
                Expiration: { Days: 1 },
                ID: '123',
                Prefix: prefix,
                Status: 'Enabled',
            }
        ];
        const expected = {
            currents: [{
                prefix,
                days: 1,
            }]
        };
        const rulesReducer = new RulesReducer(versioningStatus, currentDate, bucketLCRules, options);
        const result = rulesReducer.toListings();

        assert.deepStrictEqual(result, expected);
    });

    it('with Expiration rule using Filter.Prefix', () => {
        const currentDate = Date.now();
        const prefix = 'pre';
        const bucketLCRules = [
            {
                Expiration: { Days: 1 },
                ID: '123',
                Status: 'Enabled',
                Filter: {
                    Prefix: prefix,
                }
            }
        ];
        const expected = {
            currents: [{
                prefix,
                days: 1,
            }]
        };
        const rulesReducer = new RulesReducer(versioningStatus, currentDate, bucketLCRules, options);
        const result = rulesReducer.toListings();

        assert.deepStrictEqual(result, expected);
    });

    it('with Expiration rule using Filter.And.Prefix', () => {
        const currentDate = Date.now();
        const prefix = 'pre';
        const bucketLCRules = [
            {
                Expiration: { Days: 1 },
                ID: '123',
                Status: 'Enabled',
                Filter: {
                    And: {
                        Prefix: prefix,
                    }
                }
            }
        ];
        const expected = {
            currents: [{
                prefix,
                days: 1,
            }]
        };
        const rulesReducer = new RulesReducer(versioningStatus, currentDate, bucketLCRules, options);
        const result = rulesReducer.toListings();

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
            currents: [{
                prefix: '',
                days: 0,
            }]
        };
        const rulesReducer = new RulesReducer(versioningStatus, currentDate, bucketLCRules, options);
        const result = rulesReducer.toListings();

        assert.deepStrictEqual(result, expected);
    });

    it('with Expiration and Transitions rule using Days', () => {
        const currentDate = Date.now();
        const prefix = '';
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
            currents: [{
                prefix: '',
                days: 1,
            }]
        };
        const rulesReducer = new RulesReducer(versioningStatus, currentDate, bucketLCRules, options);
        const result = rulesReducer.toListings();

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
            currents: [{
                prefix: '',
                days: 0,
            }]
        };
        const rulesReducer = new RulesReducer(versioningStatus, currentDate, bucketLCRules, options);
        const result = rulesReducer.toListings();

        assert.deepStrictEqual(result, expected);
    });

    it('with one Transition rule but multiple transitions', () => {
        const currentDate = Date.now();
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
            currents: [{
                prefix: 'toto/titi',
                days: 1,
            }]
        };
        const rulesReducer = new RulesReducer(versioningStatus, currentDate, bucketLCRules, options);
        const result = rulesReducer.toListings();

        assert.deepStrictEqual(result, expected);
    });

    it('with multiple Expiration rules that share prefix', () => {
        const currentDate = Date.now();
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
            currents: [{
                prefix: 'toto',
                days: 1,
            }]
        };
        const rulesReducer = new RulesReducer(versioningStatus, currentDate, bucketLCRules, options);
        const result = rulesReducer.toListings();

        assert.deepStrictEqual(result, expected);
    });

    it('with multiple Transition rules that share prefix', () => {
        const currentDate = Date.now();
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
            currents: [{
                prefix: 'toto',
                days: 1,
            }]
        };
        const rulesReducer = new RulesReducer(versioningStatus, currentDate, bucketLCRules, options);
        const result = rulesReducer.toListings();

        assert.deepStrictEqual(result, expected);
    });

    it('with multiple Expiration rules that do not share prefix', () => {
        const currentDate = Date.now();
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
            currents: [{
                prefix: 'titi',
                days: 1,
            }, {
                prefix: 'toto',
                days: 2,
            }]
        };
        const rulesReducer = new RulesReducer(versioningStatus, currentDate, bucketLCRules, options);
        const result = rulesReducer.toListings();

        assert.deepStrictEqual(result, expected);
    });

    it('with multiple Transitions rules that do not share prefix', () => {
        const currentDate = Date.now();
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
            currents: [{
                prefix: 'titi',
                days: 1,
            }, {
                prefix: 'toto',
                days: 0,
            }]
        };
        const rulesReducer = new RulesReducer(versioningStatus, currentDate, bucketLCRules, options);
        const result = rulesReducer.toListings();

        assert.deepStrictEqual(result, expected);
    });

    it('with multiple Expiration and Transitions rules that do share prefix', () => {
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
            currents: [{
                prefix: 'toto',
                days: 0,
            }]
        };
        const rulesReducer = new RulesReducer(versioningStatus, currentDate, bucketLCRules, options);
        const result = rulesReducer.toListings();

        assert.deepStrictEqual(result, expected);
    });

    it('with multiple Expiration and Transitions rules that do not share prefix', () => {
        const currentDate = Date.now();
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
            currents: [{
                prefix: 'titi',
                days: 1,
            }, {
                prefix: 'toto',
                days: 0,
            }]
        };
        const rulesReducer = new RulesReducer(versioningStatus, currentDate, bucketLCRules, options);
        const result = rulesReducer.toListings();

        assert.deepStrictEqual(result, expected);
    });

    it('with multiple Expiration and Transitions rules with different prefixes', () => {
        const currentDate = Date.now();
        const bucketLCRules = [
            {
                Expiration: { Days: 10 },
                ID: '0',
                Prefix: 'a',
                Status: 'Enabled',
            },
            {
                Transitions: [{ Date: currentDate, StorageClass: locationName2 }],
                ID: '1',
                Prefix: 'b',
                Status: 'Enabled',
            },
            {
                Transitions: [{ Date: currentDate, StorageClass: locationName2 }],
                ID: '2',
                Prefix: 'caa',
                Status: 'Enabled',
            },
            {
                Expiration: { Days: 4 },
                ID: '3',
                Prefix: 'ca',
                Status: 'Enabled',
            },
            {
                Transitions: [{ Days: 8, StorageClass: locationName2 }],
                ID: '4',
                Prefix: 'cb',
                Status: 'Enabled',
            },
            {
                Expiration: { Days: 10 },
                ID: '5',
                Prefix: 'c',
                Status: 'Enabled',
            },
            {
                Transitions: [{ Days: 2, StorageClass: locationName2 }],
                ID: '6',
                Prefix: 'daa',
                Status: 'Enabled',
            },
            {
                Expiration: { Days: 4 },
                ID: '7',
                Prefix: 'da',
                Status: 'Enabled',
            },
            {
                Expiration: { Days: 6 },
                ID: '8',
                Prefix: 'db',
                Status: 'Enabled',
            },
            {
                Expiration: { Days: 8 },
                ID: '9',
                Prefix: 'd',
                Status: 'Enabled',
            }
        ];
        const expected = {
            currents: [
                { prefix: 'a', days: 10 },
                { prefix: 'b', days: 0 },
                { prefix: 'c', days: 0 },
                { prefix: 'd', days: 2 },
            ]
        };
        const rulesReducer = new RulesReducer(versioningStatus, currentDate, bucketLCRules, options);
        const result = rulesReducer.toListings();

        assert.deepStrictEqual(result, expected);
    });

    it('with expireOneDayEarlier', () => {
        const currentDate = Date.now();
        const customOptions = {
            expireOneDayEarlier: true,
            transitionOneDayEarlier: false,
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
            currents: [{
                prefix: 'titi',
                days: 0,
            }, {
                prefix: 'toto',
                days: 1,
            }]
        };
        const rulesReducer = new RulesReducer(versioningStatus, currentDate, bucketLCRules, customOptions);
        const result = rulesReducer.toListings();

        assert.deepStrictEqual(result, expected);
    });

    it('with transitionOneDayEarlier', () => {
        const currentDate = Date.now();
        const customOptions = {
            expireOneDayEarlier: false,
            transitionOneDayEarlier: true,
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
            currents: [{
                prefix: 'titi',
                days: 1,
            }, {
                prefix: 'toto',
                days: 0,
            }]
        };
        const rulesReducer = new RulesReducer(versioningStatus, currentDate, bucketLCRules, customOptions);
        const result = rulesReducer.toListings();

        assert.deepStrictEqual(result, expected);
    });

    it('with expireOneDayEarlier and transitionOneDayEarlier', () => {
        const currentDate = Date.now();
        const customOptions = {
            expireOneDayEarlier: true,
            transitionOneDayEarlier: true,
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
            currents: [{
                prefix: 'titi',
                days: 0,
            }, {
                prefix: 'toto',
                days: 0,
            }]
        };
        const rulesReducer = new RulesReducer(versioningStatus, currentDate, bucketLCRules, customOptions);
        const result = rulesReducer.toListings();

        assert.deepStrictEqual(result, expected);
    });
});
