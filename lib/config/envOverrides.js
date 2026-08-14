'use strict';

function toEnvSegment(segment) {
    return segment.replace(/([a-z0-9])([A-Z])/g, '$1_$2').toUpperCase();
}

function deriveEnvName(pathSegments) {
    return pathSegments.map(toEnvSegment).join('_');
}

function getNodeDescription(node) {
    if (node && node.type === 'alternatives' && Array.isArray(node.matches)) {
        const objectMatch = node.matches
            .map(match => match.schema)
            .find(schema => schema && (schema.keys || schema.type === 'array'));
        if (objectMatch) {
            return objectMatch;
        }
        return node.matches[0] && node.matches[0].schema;
    }
    return node;
}

function collectMetaAlias(node) {
    if (!node || !Array.isArray(node.metas)) {
        return undefined;
    }
    const envMeta = node.metas.find(meta => meta && typeof meta.env === 'string');
    return envMeta && envMeta.env;
}

function hasProbeServerMeta(node) {
    if (!node || !Array.isArray(node.metas)) {
        return false;
    }
    return node.metas.some(meta => meta && meta.probeServer === true);
}

function setPath(config, pathSegments, value) {
    let cursor = config;
    for (let i = 0; i < pathSegments.length - 1; i++) {
        const segment = pathSegments[i];
        if (typeof cursor[segment] !== 'object' || cursor[segment] === null) {
            cursor[segment] = {};
        }
        cursor = cursor[segment];
    }
    cursor[pathSegments[pathSegments.length - 1]] = value;
}

function walkSchema(description, pathPrefix, pathSegments, visitLeaf, visitNode) {
    const resolved = getNodeDescription(description);
    if (!resolved) {
        return;
    }

    if (visitNode) {
        visitNode(resolved, pathSegments);
    }

    if (resolved.type === 'object') {
        if (!resolved.keys) {
            // BB-809 covers unconstrained joi.object() leaves (e.g. kafka.producerParams,
            // queuePopulator.circuitBreaker, per-extension circuitBreaker) via the merge-patch stage.
            return;
        }
        Object.keys(resolved.keys).forEach(key => {
            const childDescription = resolved.keys[key];
            const childPath = pathSegments.concat([key]);
            walkSchema(childDescription, pathPrefix, childPath, visitLeaf, visitNode);
        });
        return;
    }

    if (resolved.type === 'array') {
        if (visitLeaf) {
            visitLeaf(resolved, pathSegments, { isArray: true });
        }
        return;
    }

    if (visitLeaf) {
        visitLeaf(resolved, pathSegments, { isArray: false });
    }
}

function applyEnvOverrides(joiSchema, config, pathPrefix = []) {
    const description = joiSchema.describe();

    walkSchema(description, pathPrefix, [], (node, pathSegments, { isArray }) => {
        const derivedName = deriveEnvName(pathPrefix.concat(pathSegments));
        const alias = collectMetaAlias(node);

        const candidateNames = [derivedName];
        if (alias) {
            candidateNames.push(alias);
        }

        const envName = candidateNames.find(name => process.env[name] !== undefined);
        if (!envName) {
            return;
        }

        const rawValue = process.env[envName];
        const value = isArray ? rawValue.split(',') : rawValue;
        setPath(config, pathSegments, value);
    });

    return config;
}

function applyProbeServerPortOverride(joiSchema, config, port = process.env.LIVENESS_PROBE_PORT) {
    if (!port) {
        return config;
    }

    const description = joiSchema.describe();

    walkSchema(description, [], [], null, (node, pathSegments) => {
        if (pathSegments.length === 0) {
            return;
        }
        if (hasProbeServerMeta(node)) {
            setPath(config, pathSegments.concat(['bindAddress']), '0.0.0.0');
            setPath(config, pathSegments.concat(['port']), port);
        }
    });

    return config;
}

module.exports = {
    applyEnvOverrides,
    applyProbeServerPortOverride,
};
