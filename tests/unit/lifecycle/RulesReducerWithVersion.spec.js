const assert = require('assert');

const { RulesReducer } = require('../../../extensions/lifecycle/util/RulesReducer');

const locationName = 'aws-loc';
const locationName2 = 'aws-loc2';

const expectedEmptyResult = {
    currents: [],
    nonCurrents: [],
    orphans: [],
};

const options = {
    expireOneDayEarlier: false,
    transitionOneDayEarlier: false,
};

describe('RulesReducer with versioning Enabled', () => {
    const versioningStatus = 'Enabled';

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

    it('with disabled rule', () => {
        const currentDate = Date.now();
        const bucketLCRules = [
            {
                Expiration: { Days: 1 },
                ID: '0',
                Prefix: '',
                Status: 'Disabled',
            },
            {
                Transitions: [{ Days: 1, StorageClass: locationName }],
                ID: '1',
                Prefix: '',
                Status: 'Disabled',
            },
            {
                NoncurrentVersionExpiration: { NoncurrentDays: 1 },
                ID: '2',
                Prefix: '',
                Status: 'Disabled',
            },
            {
                NoncurrentVersionTransitions: [{ NonCurrentDays: 1, StorageClass: locationName }],
                ID: '3',
                Prefix: '',
                Status: 'Disabled',
            },
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
            currents: [{ prefix: '', days: 0 }],
            nonCurrents: [],
            orphans: [{ prefix: '', days: 0 }],
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
            currents: [{ prefix: '', days: 1 }],
            nonCurrents: [],
            orphans: [{ prefix: '', days: 1 }],
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
            currents: [{ prefix, days: 1 }],
            nonCurrents: [],
            orphans: [{ prefix: 'pre', days: 1 }],
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
            currents: [{ prefix: '', days: 0 }],
            nonCurrents: [],
            orphans: [],
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
            currents: [{ prefix: '', days: 1 }],
            nonCurrents: [],
            orphans: [{ prefix: '', days: 1 }],
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
            currents: [{ prefix: '', days: 0 }],
            nonCurrents: [],
            orphans: [{ prefix: '', days: 1 }],
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
            currents: [{ prefix: 'toto/titi', days: 1 }],
            nonCurrents: [],
            orphans: [],
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
            currents: [{ prefix: 'toto', days: 1 }],
            nonCurrents: [],
            orphans: [{ prefix: 'toto', days: 1 }],
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
            currents: [{ prefix: 'toto', days: 1 }],
            nonCurrents: [],
            orphans: [],
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
            currents: [
                { prefix: 'titi', days: 1 },
                { prefix: 'toto', days: 2 }
            ],
            nonCurrents: [],
            orphans: [
                { prefix: 'titi', days: 1 },
                { prefix: 'toto', days: 2 }
            ],
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
            currents: [
                { prefix: 'titi', days: 1 },
                { prefix: 'toto', days: 0 }
            ],
            nonCurrents: [],
            orphans: [],
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
            currents: [{ prefix: 'toto', days: 0 }],
            nonCurrents: [],
            orphans: [{ prefix: 'toto/titi', days: 1 }],
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
            currents: [
                { prefix: 'titi', days: 1 },
                { prefix: 'toto', days: 0, },
            ],
            nonCurrents: [],
            orphans: [{ prefix: 'titi', days: 1 }],
        };
        const rulesReducer = new RulesReducer(versioningStatus, currentDate, bucketLCRules, options);
        const result = rulesReducer.toListings();

        assert.deepStrictEqual(result, expected);
    });

    it('with NoncurrentVersionExpiration rule', () => {
        const currentDate = Date.now();
        const bucketLCRules = [
            {
                NoncurrentVersionExpiration: { NoncurrentDays: 2 },
                ID: '0',
                Prefix: '',
                Status: 'Enabled',
            },
        ];
        const rulesReducer = new RulesReducer(versioningStatus, currentDate, bucketLCRules, options);
        const result = rulesReducer.toListings();

        const expected = {
            currents: [],
            nonCurrents: [{
                prefix: '',
                days: 2,
            }],
            orphans: [],
        };
        assert.deepStrictEqual(result, expected);
    });

    it('with NoncurrentVersionTransitions rule', () => {
        const currentDate = Date.now();
        const bucketLCRules = [
            {
                NoncurrentVersionTransitions: [{ NoncurrentDays: 2, StorageClass: locationName }],
                ID: '0',
                Prefix: '',
                Status: 'Enabled',
            },
        ];
        const rulesReducer = new RulesReducer(versioningStatus, currentDate, bucketLCRules, options);
        const result = rulesReducer.toListings();

        const expected = {
            currents: [],
            nonCurrents: [{
                prefix: '',
                days: 2,
            }],
            orphans: [],
        };
        assert.deepStrictEqual(result, expected);
    });

    it('with NoncurrentVersionExpiration and NoncurrentVersionTransitions rule that do not share prefix', () => {
        const currentDate = Date.now();
        const bucketLCRules = [
            {
                NoncurrentVersionExpiration: { NoncurrentDays: 1 },
                ID: '0',
                Prefix: 'toto/titi',
                Status: 'Enabled',
            },
            {
                NoncurrentVersionTransitions: [{ NoncurrentDays: 2, StorageClass: locationName }],
                ID: '1',
                Prefix: 'toto',
                Status: 'Enabled',
            },
        ];
        const rulesReducer = new RulesReducer(versioningStatus, currentDate, bucketLCRules, options);
        const result = rulesReducer.toListings();

        const expected = {
            currents: [],
            nonCurrents: [{
                prefix: 'toto',
                days: 1,
            }],
            orphans: [],
        };
        assert.deepStrictEqual(result, expected);
    });

    it('with NoncurrentVersionExpiration and NoncurrentVersionTransitions rule that do not share prefix', () => {
        const currentDate = Date.now();
        const bucketLCRules = [
            {
                NoncurrentVersionExpiration: { NoncurrentDays: 1 },
                ID: '0',
                Prefix: 'p1',
                Status: 'Enabled',
            },
            {
                NoncurrentVersionTransitions: [{ NoncurrentDays: 2, StorageClass: locationName }],
                ID: '1',
                Prefix: 'p2',
                Status: 'Enabled',
            },
        ];
        const rulesReducer = new RulesReducer(versioningStatus, currentDate, bucketLCRules, options);
        const result = rulesReducer.toListings();

        const expected = {
            currents: [],
            nonCurrents: [
                { prefix: 'p1', days: 1 },
                { prefix: 'p2', days: 2 },
            ],
            orphans: [],
        };
        assert.deepStrictEqual(result, expected);
    });

    it('with Expiration.ExpiredObjectDeleteMarker', () => {
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

        const expected = {
            currents: [],
            nonCurrents: [],
            orphans: [{ prefix: '', days: 0 }],
        };
        assert.deepStrictEqual(result, expected);
    });

    it('with Expiration and Expiration.ExpiredObjectDeleteMarker', () => {
        const currentDate = Date.now();
        const bucketLCRules = [
            {
                Expiration: {
                    ExpiredObjectDeleteMarker: true,
                },
                ID: '0',
                Prefix: '',
                Status: 'Enabled',
            },
            {
                Expiration: {  Days: 1 },
                ID: '1',
                Prefix: '',
                Status: 'Enabled',
            },
        ];
        const rulesReducer = new RulesReducer(versioningStatus, currentDate, bucketLCRules, options);
        const result = rulesReducer.toListings();

        const expected = {
            currents: [{ prefix: '', days: 1 }],
            nonCurrents: [],
            orphans: [{ prefix: '', days: 0 }],
        };
        assert.deepStrictEqual(result, expected);
    });

    it('with Expiration, Transitions, NoncurrentVersionExpiration and NoncurrentVersionTransitions rules', () => {
        const currentDate = Date.now();
        const bucketLCRules = [
            {
                Expiration: { Days: 10 },
                ID: '0',
                Prefix: 'a',
                Status: 'Enabled',
            },
            {
                Expiration: { Days: 4 },
                ID: '3',
                Filter: {
                    Prefix: 'ca',
                },
                Status: 'Enabled',
            },
            {
                Expiration: { Days: 10 },
                ID: '5',
                Filter: {
                    And: {
                        Prefix: 'c',
                    }
                },
                Status: 'Enabled',
            },
            {
                Expiration: { Days: 6 },
                ID: '8',
                Filter: {
                    Prefix: 'db',
                },
                Status: 'Enabled',
            },
            {
                Transitions: [{ Date: currentDate, StorageClass: locationName2 }],
                ID: '2',
                Prefix: 'caa',
                Status: 'Enabled',
            },
            {
                Transitions: [{ Days: 2, StorageClass: locationName2 }],
                ID: '6',
                Prefix: 'daa',
                Status: 'Enabled',
            },
            {
                NoncurrentVersionTransitions: [{ NoncurrentDays: 1, StorageClass: locationName2 }],
                ID: '1',
                Filter: {
                    Prefix: 'b',
                },
                Status: 'Enabled',
            },
            {
                NoncurrentVersionTransitions: [{ NoncurrentDays: 8, StorageClass: locationName2 }],
                ID: '4',
                Prefix: 'cb',
                Filter: {
                    And: {
                        Prefix: 'cb',
                    }
                },
                Status: 'Enabled',
            },
            {
                NoncurrentVersionExpiration: { NoncurrentDays: 4 },
                ID: '7',
                Prefix: 'da',
                Status: 'Enabled',
            },
            {
                NoncurrentVersionExpiration: { NoncurrentDays: 8 },
                ID: '9',
                Filter: {
                    And: {
                        Prefix: 'd',
                    }
                },
                Status: 'Enabled',
            }
        ];
        const expected = {
            currents: [
                { prefix: 'a', days: 10 },
                { prefix: 'c', days: 0 },
                { prefix: 'daa', days: 2 },
                { prefix: 'db', days: 6 },
            ],
            nonCurrents: [
                { prefix: 'b', days: 1 },
                { prefix: 'cb', days: 8 },
                { prefix: 'd', days: 4 },
            ],
            orphans: [
                { prefix: 'a', days: 10 },
                { prefix: 'c', days: 4 },
                { prefix: 'db', days: 6 },
            ],
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
                ID: '1',
                Prefix: 'p1',
                Status: 'Enabled',
            },
            {
                Transitions: [{ Days: 1, StorageClass: locationName2 }],
                ID: '2',
                Prefix: 'p2',
                Status: 'Enabled',
            },
            {
                NoncurrentVersionExpiration: { NoncurrentDays: 1 },
                ID: '3',
                Prefix: 'p3',
                Status: 'Enabled',
            },
            {
                NoncurrentVersionTransitions: [{ NoncurrentDays: 1, StorageClass: locationName }],
                ID: '4',
                Prefix: 'p4',
                Status: 'Enabled',
            }
        ];
        const expected = {
            currents: [
                { prefix: 'p1', days: 0 },
                { prefix: 'p2', days: 1 },
            ],
            nonCurrents: [
                { prefix: 'p3', days: 0 },
                { prefix: 'p4', days: 1 }],
            orphans: [
                { prefix: 'p1', days: 0, },
            ],
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
                ID: '1',
                Prefix: 'p1',
                Status: 'Enabled',
            },
            {
                Transitions: [{ Days: 1, StorageClass: locationName2 }],
                ID: '2',
                Prefix: 'p2',
                Status: 'Enabled',
            },
            {
                NoncurrentVersionExpiration: { NoncurrentDays: 1 },
                ID: '3',
                Prefix: 'p3',
                Status: 'Enabled',
            },
            {
                NoncurrentVersionTransitions: [{ NoncurrentDays: 1, StorageClass: locationName }],
                ID: '4',
                Prefix: 'p4',
                Status: 'Enabled',
            }
        ];
        const expected = {
            currents: [
                { prefix: 'p1', days: 1 },
                { prefix: 'p2', days: 0 },
            ],
            nonCurrents: [
                { prefix: 'p3', days: 1 },
                { prefix: 'p4', days: 0 }],
            orphans: [
                { prefix: 'p1', days: 1, },
            ],
        };
        const rulesReducer = new RulesReducer(versioningStatus, currentDate, bucketLCRules, customOptions);
        const result = rulesReducer.toListings();

        assert.deepStrictEqual(result, expected);
    });


    it('with transitionOneDayEarlier', () => {
        const currentDate = Date.now();
        const customOptions = {
            expireOneDayEarlier: true,
            transitionOneDayEarlier: true,
        };
        const bucketLCRules = [
            {
                Expiration: { Days: 1 },
                ID: '1',
                Prefix: 'p1',
                Status: 'Enabled',
            },
            {
                Transitions: [{ Days: 1, StorageClass: locationName2 }],
                ID: '2',
                Prefix: 'p2',
                Status: 'Enabled',
            },
            {
                NoncurrentVersionExpiration: { NoncurrentDays: 1 },
                ID: '3',
                Prefix: 'p3',
                Status: 'Enabled',
            },
            {
                NoncurrentVersionTransitions: [{ NoncurrentDays: 1, StorageClass: locationName }],
                ID: '4',
                Prefix: 'p4',
                Status: 'Enabled',
            }
        ];
        const expected = {
            currents: [
                { prefix: 'p1', days: 0 },
                { prefix: 'p2', days: 0 },
            ],
            nonCurrents: [
                { prefix: 'p3', days: 0 },
                { prefix: 'p4', days: 0 }],
            orphans: [
                { prefix: 'p1', days: 0, },
            ],
        };
        const rulesReducer = new RulesReducer(versioningStatus, currentDate, bucketLCRules, customOptions);
        const result = rulesReducer.toListings();

        assert.deepStrictEqual(result, expected);
    });
});
