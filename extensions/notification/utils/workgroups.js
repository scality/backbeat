const crypto = require('crypto');
const joi = require('joi');

// reserved key prefix of a cutover barrier record. encodeURIComponent emits a
// literal '%' only as the first character of a percent escape and '%00' is the
// escape for NUL, so no destination key can start with it
const BARRIER_KEY_PREFIX = '%00';
const BARRIER_KEY = `${BARRIER_KEY_PREFIX}wg-barrier`;
const BARRIER_RECORD_TYPE = 'delivery-workgroup-barrier';

// encodeURIComponent form of the '|' buildDeliveryKey puts between a
// destination resource and its sub key
const SUB_KEY_SEPARATOR = '%7C';
const PLAIN_SUB_KEY_SEPARATOR = '|';

const SKIP_NOT_IN_SLICE = 'not_in_slice';
const SKIP_BARRIER = 'barrier';

const CONFIG_VERSION = 1;

// a workgroup id becomes part of a kafka consumer group id and of a prometheus
// label value, so it is constrained at the edge rather than sanitised later
const WORKGROUP_ID_PATTERN = /^[a-zA-Z0-9][a-zA-Z0-9_-]{0,63}$/;

// a stream of unknown keys must not grow the memoisation map without bound
const OWNER_CACHE_MAX = 10000;

const hashmodRuleSchema = joi.object({
    type: joi.string().valid('hashmod').required(),
    modulo: joi.number().integer().min(1).required(),
    remainders: joi.array().items(joi.number().integer().min(0))
        .min(1).required(),
});

const staticRuleSchema = joi.object({
    type: joi.string().valid('static').required(),
    destinationIds: joi.array().items(joi.string()).min(1).required(),
});

const workgroupsDocSchema = joi.object({
    configVersion: joi.number().integer().valid(CONFIG_VERSION).required(),
    generation: joi.number().integer().min(1).required(),
    topic: joi.string().required(),
    updatedAt: joi.string(),
    workgroups: joi.array().min(1).required().items(joi.object({
        id: joi.string().pattern(WORKGROUP_ID_PATTERN).required(),
        rule: joi.alternatives()
            .try(hashmodRuleSchema, staticRuleSchema).required(),
    })),
    // partition number as a JSON object key, barrier offset as its value
    barriers: joi.object().pattern(/^\d+$/, joi.number().integer().min(0)),
});

/**
 * Encodes a destination id the way the delivery key carries it on the wire
 *
 * @param {String} destinationId - destination resource name
 * @return {String} encoded destination token
 */
function encodeDestinationToken(destinationId) {
    return encodeURIComponent(destinationId);
}

/**
 * @param {Buffer|String|null} key - record key as consumed
 * @return {String} the encoded destination token, '' for a missing key
 */
function destinationTokenFromKey(key) {
    if (key === null || key === undefined) {
        return '';
    }
    const encoded = Buffer.isBuffer(key) ? key.toString() : String(key);
    const separator = encoded.indexOf(SUB_KEY_SEPARATOR);
    return separator === -1 ? encoded : encoded.slice(0, separator);
}

/**
 * @param {Buffer|String|null} key - record key as consumed
 * @return {boolean} true when the record is a cutover barrier
 */
function isBarrierKey(key) {
    if (!key) {
        return false;
    }
    if (Buffer.isBuffer(key)) {
        // '%', '0', '0' compared as bytes, so a worker with no slice filter
        // does not allocate a string for every record it consumes
        return key.length >= 3 && key[0] === 0x25 && key[1] === 0x30
            && key[2] === 0x30;
    }
    return String(key).startsWith(BARRIER_KEY_PREFIX);
}

/**
 * Applies the ownership rules that make "exactly one workgroup owns every
 * record" structurally true rather than a convention
 *
 * @param {Object} doc - joi validated document
 * @return {Error|null} the first rule broken, or null when all of them hold
 */
function checkOwnershipRules(doc) {
    const seenIds = new Set();
    const staticOwners = new Map();
    const hashmodWorkgroups = [];
    let modulo;
    for (const wg of doc.workgroups) {
        if (seenIds.has(wg.id)) {
            return new Error(`workgroup id "${wg.id}" is declared twice`);
        }
        seenIds.add(wg.id);
        if (wg.rule.type === 'hashmod') {
            if (modulo !== undefined && wg.rule.modulo !== modulo) {
                return new Error('every hashmod workgroup must share one ' +
                    `modulo, found ${modulo} and ${wg.rule.modulo}`);
            }
            modulo = wg.rule.modulo;
            hashmodWorkgroups.push(wg);
        } else {
            for (const destinationId of wg.rule.destinationIds) {
                if (destinationId.includes(PLAIN_SUB_KEY_SEPARATOR)) {
                    return new Error(`static destination "${destinationId}" ` +
                        `of workgroup "${wg.id}" contains ` +
                        `"${PLAIN_SUB_KEY_SEPARATOR}": it would be routed by ` +
                        'the part before it and never match this rule');
                }
                const claimedBy = staticOwners.get(destinationId);
                if (claimedBy !== undefined) {
                    return new Error(`static destination "${destinationId}" ` +
                        `is claimed by both "${claimedBy}" and "${wg.id}"`);
                }
                staticOwners.set(destinationId, wg.id);
            }
        }
    }
    if (modulo === undefined) {
        return new Error('at least one hashmod workgroup is required, so ' +
            'that a record for an unknown destination still has an owner');
    }
    const remainderOwners = new Array(modulo).fill(null);
    for (const wg of hashmodWorkgroups) {
        for (const remainder of wg.rule.remainders) {
            if (remainder >= modulo) {
                return new Error(`workgroup "${wg.id}" claims remainder ` +
                    `${remainder}, which is outside [0, ${modulo})`);
            }
            if (remainderOwners[remainder] !== null) {
                return new Error(`remainder ${remainder} is claimed by both ` +
                    `"${remainderOwners[remainder]}" and "${wg.id}"`);
            }
            remainderOwners[remainder] = wg.id;
        }
    }
    const uncovered = [];
    remainderOwners.forEach((owner, remainder) => {
        if (owner === null) {
            uncovered.push(remainder);
        }
    });
    if (uncovered.length > 0) {
        return new Error(`remainders ${uncovered.join(', ')} of modulo ` +
            `${modulo} are claimed by no workgroup`);
    }
    return null;
}

/**
 * Validates a workgroups document read from zookeeper or from the cache
 *
 * @param {Object} doc - parsed document
 * @return {Object} { error, value }: error is an Error with a human readable
 *   message when the document is unusable, value is the validated document
 *   with joi defaults applied otherwise
 */
function validateWorkgroupsDoc(doc) {
    const { error, value } = workgroupsDocSchema.validate(doc);
    if (error) {
        return {
            error: new Error(`invalid workgroups document: ${error.message}`),
            value,
        };
    }
    const ruleError = checkOwnershipRules(value);
    if (ruleError) {
        return { error: ruleError, value };
    }
    return { error: null, value };
}

/**
 * Precomputes the lookup tables the ownership function needs
 *
 * @param {Object} doc - validated document
 * @return {Object} index with { modulo, remainderOwners, staticOwners }
 */
function buildOwnershipIndex(doc) {
    const staticOwners = new Map();
    let modulo = 1;
    doc.workgroups.forEach(wg => {
        if (wg.rule.type === 'static') {
            wg.rule.destinationIds.forEach(id =>
                staticOwners.set(encodeDestinationToken(id), wg.id));
        } else {
            modulo = wg.rule.modulo;
        }
    });
    const remainderOwners = new Array(modulo);
    doc.workgroups.forEach(wg => {
        if (wg.rule.type === 'hashmod') {
            wg.rule.remainders.forEach(r => { remainderOwners[r] = wg.id; });
        }
    });
    return { modulo, remainderOwners, staticOwners };
}

/**
 * Resolves which workgroup owns a destination token
 *
 * A static rule beats a hashmod one for every workgroup evaluating this, so a
 * statically claimed destination is carved out of the hashmod space.
 *
 * @param {Object} index - ownership index
 * @param {String} token - encoded destination token, possibly empty
 * @return {String} workgroup id, always defined for a validated document
 */
function ownerOfToken(index, token) {
    const claimed = index.staticOwners.get(token);
    if (claimed !== undefined) {
        return claimed;
    }
    const hash = crypto.createHash('md5').update(token).digest()
        .readUInt32BE(0);
    return index.remainderOwners[hash % index.modulo];
}

/**
 * Operator facing form of ownerOfToken: takes a plain destination id and
 * does the encoding itself, so tooling and workers cannot disagree
 *
 * @param {Object} doc - validated document
 * @param {String} destinationId - destination resource name
 * @return {String} workgroup id
 */
function workgroupIdForDestination(doc, destinationId) {
    const token = destinationTokenFromKey(
        encodeDestinationToken(destinationId));
    return ownerOfToken(buildOwnershipIndex(doc), token);
}

/**
 * @param {Object} params - { generation, partition }
 * @return {String} serialised barrier record payload
 */
function buildBarrierRecord(params) {
    return JSON.stringify({
        type: BARRIER_RECORD_TYPE,
        generation: params.generation,
        partition: params.partition,
        createdAt: new Date().toISOString(),
    });
}

/**
 * @param {Buffer|String} value - record payload
 * @return {Object|null} the parsed barrier record, or null when the payload is
 *   not one. Never throws
 */
function parseBarrierRecord(value) {
    let parsed;
    try {
        parsed = JSON.parse(value);
    } catch {
        return null;
    }
    if (!parsed || parsed.type !== BARRIER_RECORD_TYPE) {
        return null;
    }
    return parsed;
}

/**
 * @param {Object} params - { doc, workgroupId }
 * @return {Object} filter with classify(key) -> null when the record belongs
 *   to this workgroup, or a skip reason string otherwise
 */
function createSliceFilter(params) {
    const { doc, workgroupId } = params;
    const index = buildOwnershipIndex(doc);
    const cache = new Map();
    return {
        workgroupId,
        generation: doc.generation,
        classify(key) {
            if (isBarrierKey(key)) {
                return SKIP_BARRIER;
            }
            const token = destinationTokenFromKey(key);
            let owner = cache.get(token);
            if (owner === undefined) {
                owner = ownerOfToken(index, token);
                if (cache.size >= OWNER_CACHE_MAX) {
                    cache.clear();
                }
                cache.set(token, owner);
            }
            return owner === workgroupId ? null : SKIP_NOT_IN_SLICE;
        },
    };
}

/**
 * @param {String} baseGroupId - deliveryPool.groupId
 * @param {String} workgroupId - this workgroup's id
 * @param {Number} generation - config generation
 * @return {String} `${baseGroupId}-${workgroupId}-gen${generation}`
 */
function buildGroupId(baseGroupId, workgroupId, generation) {
    return `${baseGroupId}-${workgroupId}-gen${generation}`;
}

module.exports = {
    // constants
    BARRIER_KEY,
    BARRIER_KEY_PREFIX,
    BARRIER_RECORD_TYPE,
    SUB_KEY_SEPARATOR,
    SKIP_NOT_IN_SLICE,
    SKIP_BARRIER,
    CONFIG_VERSION,
    WORKGROUP_ID_PATTERN,
    // document
    validateWorkgroupsDoc,
    buildOwnershipIndex,
    ownerOfToken,
    workgroupIdForDestination,
    // keys
    encodeDestinationToken,
    destinationTokenFromKey,
    isBarrierKey,
    buildBarrierRecord,
    parseBarrierRecord,
    // runtime
    createSliceFilter,
    buildGroupId,
};
