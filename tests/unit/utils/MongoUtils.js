const assert = require('assert');
const MongoUtils = require('../../../extensions/utils/MongoUtils');

const mongoConfRepl = {
    replicaSetHosts:
    'localhost:27017,localhost:27018,localhost:27019',
    writeConcern: 'majority',
    replicaSet: 'rs0',
    readPreference: 'primary',
    database: 'metadata',
    authCredentials: {
        username: 'user',
        password: 'pass',
    }
};

const mongoConfShard = {
    replicaSetHosts:
    'localhost:27017,localhost:27018,localhost:27019',
    writeConcern: 'majority',
    readPreference: 'primary',
    database: 'metadata',
    authCredentials: {
        username: 'user',
        password: 'pass',
    }
};

describe('constructConnectionString', () => {
    it('Should construct correct mongo connection string', done => {
        const url = MongoUtils.constructConnectionString(mongoConfRepl);
        assert.strictEqual(url, 'mongodb://user:pass@localhost:27017,localhost:27018,localhost:27019' +
            '/?w=majority&readPreference=primary&replicaSet=rs0');
        return done();
    });

    it('Should construct correct mongo connection string (replica)', done => {
        const url = MongoUtils.constructConnectionString(mongoConfShard);
        assert.strictEqual(url, 'mongodb://user:pass@localhost:27017,localhost:27018,localhost:27019' +
            '/?w=majority&readPreference=primary');
        return done();
    });
});
