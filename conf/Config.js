'use strict'; // eslint-disable-line

const { EventEmitter } = require('events');

const fs = require('fs');
const path = require('path');
const joi = require('joi');

const extensions = require('../extensions');
const backbeatConfigJoi = require('./config.joi.js');

const locationTypeMatch = {
    'location-mem-v1': 'mem',
    'location-file-v1': 'file',
    'location-azure-v1': 'azure',
    'location-do-spaces-v1': 'aws_s3',
    'location-aws-s3-v1': 'aws_s3',
    'location-wasabi-v1': 'aws_s3',
    'location-gcp-v1': 'gcp',
};

class Config extends EventEmitter {
    constructor() {
        super();
        /*
         * By default, the config file is "config.json" at the root.
         * It can be overridden using the BACKBEAT_CONFIG_FILE environment var.
         */
        this._basePath = __dirname;
        if (process.env.BACKBEAT_CONFIG_FILE !== undefined) {
            this._configPath = process.env.BACKBEAT_CONFIG_FILE;
        } else {
            this._configPath = path.join(this._basePath, 'config.json');
        }

        let config;
        try {
            const data = fs.readFileSync(this._configPath,
                                         { encoding: 'utf-8' });
            config = JSON.parse(data);
        } catch (err) {
            throw new Error(`could not parse config file: ${err.message}`);
        }

        const parsedConfig = joi.attempt(config, backbeatConfigJoi,
                                         'invalid backbeat config');

        if (parsedConfig.extensions) {
            Object.keys(parsedConfig.extensions).forEach(extName => {
                const index = extensions[extName];
                if (!index) {
                    throw new Error(`configured extension ${extName}: ` +
                                    'not found in extensions directory');
                }
                if (index.configValidator) {
                    const extConfig = parsedConfig.extensions[extName];
                    const validatedConfig =
                              index.configValidator(this, extConfig);
                    parsedConfig.extensions[extName] = validatedConfig;
                }
            });
        }
        if (parsedConfig.extensions && parsedConfig.extensions.replication
            && parsedConfig.extensions.replication.destination
            && parsedConfig.extensions.replication.destination.bootstrapList) {
            this.replicationEndpoints = parsedConfig.extensions.replication
            .destination.bootstrapList;
        } else {
            this.replicationEndpoints = [];
        }
        // config is validated, safe to assign directly to the config object
        Object.assign(this, parsedConfig);
    }

    getBasePath() {
        return this._basePath;
    }

    getConfigPath() {
        return this._configPath;
    }

    setReplicationEndpoints(locationConstraints) {
        this.replicationEndpoints =
        Object.keys(locationConstraints)
        .map(key => ({ site: key, type:
              locationTypeMatch[locationConstraints[key].locationType] }));
        this.emit('replication-endpoints-update');
    }

    getReplicationEndpoints() {
        return this.replicationEndpoints;
    }
}

module.exports = new Config();
