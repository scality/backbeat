const fs = require('fs');
const os = require('os');
const path = require('path');

/**
 * Process wide credential setup for kerberos destinations.
 *
 * MIT krb5 reads two settings from the environment, and the environment has
 * one value each: KRB5CCNAME for the credential cache and KRB5_CLIENT_KTNAME
 * for the keytab it may initiate tickets from. A process serving destinations
 * that authenticate as different principals therefore cannot give each
 * producer its own value.
 *
 * What makes several principals work anyway is that GSSAPI is asked for a
 * credential *by name*. So the process needs, once:
 *
 *  - a credential cache COLLECTION, not a single cache. A DIR: collection
 *    holds one subsidiary cache per principal, and MIT matches the requested
 *    name against them. The FILE: cache backbeat uses today holds exactly one
 *    principal, which is why two kerberised destinations overwrite each other.
 *  - one client keytab holding every principal's keys, so that MIT can obtain
 *    a ticket for whichever name is asked for. Keytabs are a header plus a
 *    flat list of self describing entries, so the merge is a concatenation.
 *
 * With those in place, no kinit runs and no relogin timer is needed: MIT
 * obtains and renews each principal's ticket inside gss_acquire_cred.
 */

// MIT keytab format version this code understands. 0x0502 stores its integers
// big endian; 0x0501 stores them in the writing host's byte order and is not
// worth guessing at.
const KEYTAB_VERSION = 0x0502;
const KEYTAB_HEADER_SIZE = 2;
const KEYTAB_ENTRY_SIZE_FIELD = 4;

/**
 * Concatenate keytabs into one keytab.
 *
 * Deleted entries are recorded in place with a negative size, so they are
 * skipped rather than copied over.
 *
 * @param {Buffer[]} keytabs - contents of the keytab files to merge
 * @return {Buffer} merged keytab contents
 */
function mergeKeytabs(keytabs) {
    const parts = [Buffer.from([KEYTAB_VERSION >> 8, KEYTAB_VERSION & 0xff])];
    keytabs.forEach((keytab, index) => {
        if (keytab.length < KEYTAB_HEADER_SIZE) {
            throw new Error(`keytab ${index} is too short to hold a header`);
        }
        const version = keytab.readUInt16BE(0);
        if (version !== KEYTAB_VERSION) {
            throw new Error(`unsupported keytab version 0x${version.toString(16)} ` +
                `in keytab ${index}, expected 0x${KEYTAB_VERSION.toString(16)}`);
        }
        let offset = KEYTAB_HEADER_SIZE;
        while (offset + KEYTAB_ENTRY_SIZE_FIELD <= keytab.length) {
            const size = keytab.readInt32BE(offset);
            const entryEnd = offset + KEYTAB_ENTRY_SIZE_FIELD + Math.abs(size);
            if (size === 0 || entryEnd > keytab.length) {
                throw new Error(`keytab ${index} is truncated at offset ${offset}`);
            }
            if (size > 0) {
                parts.push(Buffer.from(keytab.subarray(offset, entryEnd)));
            }
            offset = entryEnd;
        }
    });
    return Buffer.concat(parts);
}

class KerberosCredentials {
    /**
     * @constructor
     * @param {Object} [params] - constructor params
     * @param {string} [params.stateDir] - directory holding the merged keytab
     *   and the credential cache collection, defaults to the temp directory
     * @param {Object} [params.env] - environment object to configure,
     *   defaults to process.env
     */
    constructor(params) {
        const options = params || {};
        this._stateDir = options.stateDir || os.tmpdir();
        this._env = options.env || process.env;
        // keytab path -> file contents, in registration order
        this._keytabs = new Map();
        this._mergedKeytabPath = null;
    }

    /**
     * Path of the DIR: collection this process keeps its tickets in
     *
     * @return {string} directory path
     */
    get ccacheDir() {
        return path.join(this._stateDir, `backbeat-notification-krb5-${process.pid}.ccache`);
    }

    /**
     * Path of the merged client keytab, once more than one has been registered
     *
     * @return {string|null} keytab path, or null when nothing is registered
     */
    get clientKeytabPath() {
        return this._mergedKeytabPath;
    }

    /**
     * Point KRB5CCNAME at a credential cache collection.
     *
     * A cache that is not a collection can only hold one principal, so an
     * existing non collection value is replaced rather than reused: keeping it
     * would silently give every destination the same identity.
     *
     * @param {Logger} log - logger object
     * @return {string} the KRB5CCNAME value now in effect
     */
    useCollectionCache(log) {
        const current = this._env.KRB5CCNAME;
        if (current && current.startsWith('DIR:')) {
            return current;
        }
        const value = `DIR:${this.ccacheDir}`;
        fs.mkdirSync(this.ccacheDir, { recursive: true, mode: 0o700 });
        if (current) {
            log.warn('replacing the credential cache with a collection, so that ' +
                'destinations authenticating as different principals do not ' +
                'overwrite each other', {
                method: 'KerberosCredentials.useCollectionCache',
                previous: current,
                ccache: value,
            });
        }
        this._env.KRB5CCNAME = value;
        return value;
    }

    /**
     * Register a destination's keytab so that MIT can obtain tickets for its
     * principal, and make sure the cache is a collection.
     *
     * Registering the same keytab twice adds nothing, but the cache is
     * checked every time rather than only on the first call: the settings
     * live in the environment, which anything in the process can change, and
     * a cache that stopped being a collection would silently hand every
     * destination the same identity. Registering a second, different keytab
     * rebuilds the merged client keytab, so a destination added later is
     * served without restarting the process.
     *
     * @param {string} keytabPath - path of the destination's keytab
     * @param {Logger} log - logger object
     * @return {undefined}
     */
    registerKeytab(keytabPath, log) {
        this.useCollectionCache(log);
        if (this._keytabs.has(keytabPath)) {
            return;
        }
        this._keytabs.set(keytabPath, fs.readFileSync(keytabPath));
        if (this._keytabs.size === 1) {
            // one keytab needs no merge, and not writing a copy of it keeps
            // the operator's file the only place the keys live
            this._mergedKeytabPath = keytabPath;
        } else {
            const merged = mergeKeytabs([...this._keytabs.values()]);
            const mergedPath = path.join(this._stateDir,
                `backbeat-notification-krb5-${process.pid}.keytab`);
            fs.writeFileSync(mergedPath, merged, { mode: 0o600 });
            this._mergedKeytabPath = mergedPath;
        }
        this._env.KRB5_CLIENT_KTNAME = this._mergedKeytabPath;
        log.info('registered a kerberos keytab for the process', {
            method: 'KerberosCredentials.registerKeytab',
            keytab: keytabPath,
            keytabs: this._keytabs.size,
            clientKeytab: this._mergedKeytabPath,
            ccache: this._env.KRB5CCNAME,
        });
    }
}

module.exports = {
    KerberosCredentials,
    mergeKeytabs,
    // one per process: the settings it writes are process wide
    processCredentials: new KerberosCredentials(),
    KEYTAB_VERSION,
};
