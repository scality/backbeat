/**
 * Log entries for the storage layouts of the "Masters and null versions"
 * analysis, keyed by the row number of its table. Each row lists the entries
 * one write produces, with the raw metadata fields that distinguish them;
 * `shadowed` tells whether a version key entry duplicates the master entry.
 *
 * https://scality.atlassian.net/wiki/spaces/~88394258/pages/4305551381
 */

const VID = '98500086134471999999RG001  0';
const NULL_VID = '98500086134472999999RG001  0';

const rows = {
    1: {
        desc: 'non-versioned bucket',
        entries: [{ key: 'obj', value: {}, shadowed: false }],
    },
    2: {
        desc: 'pre-versioning object',
        entries: [{ key: 'obj', value: {}, shadowed: false }],
    },
    3: {
        desc: 'current regular version',
        entries: [
            { key: 'obj', value: { versionId: VID }, shadowed: true },
            { key: `obj\x00${VID}`, value: { versionId: VID }, shadowed: false },
        ],
    },
    7: {
        desc: 'null master (PUT while suspended)',
        entries: [{ key: 'obj', value: { versionId: VID, isNull: true }, shadowed: false }],
    },
    8: {
        desc: 'null delete marker (DELETE while suspended)',
        entries: [{
            key: 'obj',
            value: { versionId: VID, isNull: true, isDeleteMarker: true },
            shadowed: false,
        }],
    },
    10: {
        desc: 'non-current null, null key (S3C)',
        entries: [{
            key: 'obj\x00',
            value: { versionId: VID, isNull: true, isNull2: true },
            shadowed: false,
        }],
    },
    11: {
        desc: 'non-current null, null versioned key',
        entries: [{
            key: `obj\x00${NULL_VID}`,
            value: { versionId: NULL_VID, isNull: true },
            shadowed: false,
        }],
    },
    12: {
        desc: 'null master after a metadata update (Artesca twin)',
        entries: [
            { key: `obj\x00${VID}`, value: { versionId: VID, isNull: true }, shadowed: false },
            { key: 'obj', value: { versionId: VID, isNull: true }, shadowed: false },
        ],
    },
    13: {
        desc: 'PHD master',
        entries: [{ key: 'obj', value: { versionId: VID, isPHD: true }, shadowed: true }],
    },
};

/**
 * Flat list of `{ row, desc, key, value, shadowed }` for table-driven tests.
 *
 * @param {Object} [base] - metadata fields merged under each entry value
 * @return {Object[]} one item per entry
 */
function listEntries(base = {}) {
    return Object.entries(rows).flatMap(([row, { desc, entries }]) =>
        entries.map(({ key, value, shadowed }) => ({
            row: Number(row),
            desc: `row ${row}, ${desc}, ${key.includes('\x00') ? 'version' : 'master'} key`,
            key,
            value: { ...base, ...value },
            shadowed,
        })));
}

module.exports = { rows, listEntries, VID, NULL_VID };
