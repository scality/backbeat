const { internalCollectionsRegex, hashedOperationTypes } = require('../constants');
const PipelineFactory = require('./PipelineFactory');

/**
 * @class HashedPipelineFactory
 *
 * @classdesc Generates the pipeline of one hashed connector: every bucket
 * collection whose name hashes to the connector index.
 */
class HashedPipelineFactory extends PipelineFactory {
    /**
     * Hashed connectors are never adopted as bucket list connectors.
     * @returns {boolean} false
     */
    isValid() {
        return false;
    }

    /**
     * Makes the stage selecting the collections of one connector. The
     * `ns.coll` condition stays first so that the per-bucket mode recognizes
     * (and deletes) hashed connectors.
     * @param {Object} params params
     * @param {number} params.nbConnectors number of hashed connectors
     * @param {number} params.index connector index, in [0, nbConnectors)
     * @returns {object} connector pipeline stage
     */
    getPipelineStage({ nbConnectors, index }) {
        return {
            $match: {
                'ns.coll': { $not: { $regex: internalCollectionsRegex } },
                'operationType': { $in: hashedOperationTypes },
                // $mod keeps the sign of the (signed 64-bit) hash: connector k
                // gets the remainders k and k - nbConnectors
                '$expr': {
                    $in: [
                        { $mod: [{ $toHashedIndexKey: '$ns.coll' }, nbConnectors] },
                        [index, index - nbConnectors],
                    ],
                },
            },
        };
    }
}

module.exports = HashedPipelineFactory;
