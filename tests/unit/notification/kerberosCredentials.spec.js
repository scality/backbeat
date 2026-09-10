const assert = require('assert');
const fs = require('fs');
const os = require('os');
const path = require('path');

const FakeLogger = require('../../utils/fakeLogger');

const {
    KerberosCredentials,
    mergeKeytabs,
    KEYTAB_VERSION,
} = require('../../../extensions/notification/destination/kerberosCredentials');

/**
 * Build a keytab holding the given entry payloads.
 *
 * The entries here are opaque: the merge only needs the header and each
 * entry's size field, which is all a keytab tells a reader about its shape.
 *
 * @param {string[]} payloads - entry payloads, prefixed with '-' for a
 *   deleted entry, which a keytab records in place with a negative size
 * @param {number} [version] - file format version to write
 * @return {Buffer} keytab contents
 */
function makeKeytab(payloads, version) {
    const header = Buffer.alloc(2);
    header.writeUInt16BE(version === undefined ? KEYTAB_VERSION : version, 0);
    const entries = payloads.map(payload => {
        const deleted = payload.startsWith('-');
        const body = Buffer.from(deleted ? payload.slice(1) : payload);
        const size = Buffer.alloc(4);
        size.writeInt32BE(deleted ? -body.length : body.length, 0);
        return Buffer.concat([size, body]);
    });
    return Buffer.concat([header, ...entries]);
}

/**
 * Read back the entry payloads of a keytab
 * @param {Buffer} keytab - keytab contents
 * @return {string[]} entry payloads
 */
function keytabEntries(keytab) {
    const payloads = [];
    let offset = 2;
    while (offset + 4 <= keytab.length) {
        const size = keytab.readInt32BE(offset);
        payloads.push(keytab.subarray(offset + 4, offset + 4 + Math.abs(size)).toString());
        offset += 4 + Math.abs(size);
    }
    return payloads;
}

describe('notification kerberosCredentials', () => {
    describe('mergeKeytabs', () => {
        it('should concatenate the entries of every keytab under one header', () => {
            const merged = mergeKeytabs([
                makeKeytab(['alpha-key']),
                makeKeytab(['beta-key', 'beta-older-key']),
            ]);
            assert.strictEqual(merged.readUInt16BE(0), KEYTAB_VERSION);
            assert.deepStrictEqual(keytabEntries(merged),
                ['alpha-key', 'beta-key', 'beta-older-key']);
        });

        it('should drop entries a keytab records as deleted', () => {
            const merged = mergeKeytabs([makeKeytab(['live', '-dead', 'alsolive'])]);
            assert.deepStrictEqual(keytabEntries(merged), ['live', 'alsolive']);
        });

        it('should produce a header only keytab from empty keytabs', () => {
            const merged = mergeKeytabs([makeKeytab([]), makeKeytab([])]);
            assert.strictEqual(merged.length, 2);
        });

        it('should refuse a keytab in the host byte order format', () => {
            // 0x0501 stores its integers in the writing host's byte order,
            // so the entry sizes cannot be read portably
            assert.throws(() => mergeKeytabs([makeKeytab(['k'], 0x0501)]),
                /unsupported keytab version 0x501/);
        });

        it('should name the keytab that is too short to hold a header', () => {
            assert.throws(
                () => mergeKeytabs([makeKeytab(['k']), Buffer.from([5])]),
                /keytab 1 is too short/);
        });

        it('should refuse a keytab whose last entry runs past the end', () => {
            const truncated = makeKeytab(['abcdefgh']).subarray(0, 8);
            assert.throws(() => mergeKeytabs([truncated]), /keytab 0 is truncated/);
        });

        it('should refuse a zero sized entry rather than loop on it', () => {
            const bogus = Buffer.concat([
                makeKeytab([]), Buffer.from([0, 0, 0, 0]),
            ]);
            assert.throws(() => mergeKeytabs([bogus]), /keytab 0 is truncated/);
        });
    });

    describe('process setup', () => {
        let stateDir;
        let env;
        let credentials;

        beforeEach(() => {
            stateDir = fs.mkdtempSync(path.join(os.tmpdir(), 'krbcreds-'));
            env = {};
            credentials = new KerberosCredentials({ stateDir, env });
        });

        afterEach(() => fs.rmSync(stateDir, { recursive: true, force: true }));

        const writeKeytab = (name, payloads) => {
            const keytabPath = path.join(stateDir, name);
            fs.writeFileSync(keytabPath, makeKeytab(payloads));
            return keytabPath;
        };

        it('should point the cache at a collection, which a single cache is not', () => {
            const value = credentials.useCollectionCache(FakeLogger);
            assert.strictEqual(value, `DIR:${credentials.ccacheDir}`);
            assert.strictEqual(env.KRB5CCNAME, value);
            assert.strictEqual(fs.statSync(credentials.ccacheDir).isDirectory(), true);
        });

        it('should keep a collection the deployment already configured', () => {
            env.KRB5CCNAME = 'DIR:/var/run/krb5cc';
            assert.strictEqual(credentials.useCollectionCache(FakeLogger),
                'DIR:/var/run/krb5cc');
            assert.strictEqual(env.KRB5CCNAME, 'DIR:/var/run/krb5cc');
        });

        it('should replace a single file cache, which holds one principal only', () => {
            env.KRB5CCNAME = 'FILE:/tmp/krb5cc_0';
            const value = credentials.useCollectionCache(FakeLogger);
            assert.strictEqual(value, `DIR:${credentials.ccacheDir}`);
            assert.strictEqual(env.KRB5CCNAME, value);
        });

        it('should use the destination keytab as it is when it is the only one', () => {
            const keytabPath = writeKeytab('alpha.keytab', ['alpha-key']);
            credentials.registerKeytab(keytabPath, FakeLogger);
            // no copy of the keys is written while one keytab covers everything
            assert.strictEqual(env.KRB5_CLIENT_KTNAME, keytabPath);
            assert.strictEqual(credentials.clientKeytabPath, keytabPath);
        });

        it('should configure the cache collection when the first keytab arrives', () => {
            credentials.registerKeytab(writeKeytab('alpha.keytab', ['a']), FakeLogger);
            assert.strictEqual(env.KRB5CCNAME, `DIR:${credentials.ccacheDir}`);
        });

        it('should merge a second principal in, so both can be asked for', () => {
            credentials.registerKeytab(writeKeytab('alpha.keytab', ['alpha-key']), FakeLogger);
            credentials.registerKeytab(writeKeytab('beta.keytab', ['beta-key']), FakeLogger);
            const merged = env.KRB5_CLIENT_KTNAME;
            assert.notStrictEqual(merged, path.join(stateDir, 'beta.keytab'));
            assert.deepStrictEqual(keytabEntries(fs.readFileSync(merged)),
                ['alpha-key', 'beta-key']);
        });

        it('should keep the merged keytab readable by its owner only', () => {
            credentials.registerKeytab(writeKeytab('alpha.keytab', ['a']), FakeLogger);
            credentials.registerKeytab(writeKeytab('beta.keytab', ['b']), FakeLogger);
            const mode = fs.statSync(env.KRB5_CLIENT_KTNAME).mode & 0o777;
            assert.strictEqual(mode, 0o600);
        });

        it('should ignore a keytab it already holds', () => {
            const keytabPath = writeKeytab('alpha.keytab', ['alpha-key']);
            credentials.registerKeytab(keytabPath, FakeLogger);
            credentials.registerKeytab(keytabPath, FakeLogger);
            // still the operator's file, so nothing was merged with itself
            assert.strictEqual(env.KRB5_CLIENT_KTNAME, keytabPath);
        });

        it('should grow the merged keytab as destinations are added', () => {
            ['a', 'b', 'c'].forEach(name =>
                credentials.registerKeytab(
                    writeKeytab(`${name}.keytab`, [`${name}-key`]), FakeLogger));
            assert.deepStrictEqual(
                keytabEntries(fs.readFileSync(env.KRB5_CLIENT_KTNAME)),
                ['a-key', 'b-key', 'c-key']);
        });

        it('should fail loudly on a keytab that cannot be read', () => {
            assert.throws(
                () => credentials.registerKeytab(path.join(stateDir, 'missing.keytab'),
                    FakeLogger),
                /ENOENT/);
        });
    });
});
