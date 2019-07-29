'use strict'; // eslint-disable-line

const fs = require('fs');
const path = require('path');
const joi = require('@hapi/joi');

const extensions = require('../extensions');
const backbeatConfigJoi = require('./config.joi.js');

class Config {
    constructor() {
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
        // whitelist IP, CIDR for health checks
        const defaultHealthChecks = ['127.0.0.1/8', '::1'];
        const healthChecks = parsedConfig.server.healthChecks;
        healthChecks.allowFrom =
            healthChecks.allowFrom.concat(defaultHealthChecks);

        // default to standalone configuration if sentinel not setup
        if (!parsedConfig.redis || !parsedConfig.redis.sentinels) {
            this.redis = Object.assign({}, parsedConfig.redis,
                { host: '127.0.0.1', port: 6379 });
        }

        // additional certs checks
        if (parsedConfig.certFilePaths) {
            parsedConfig.https = this._parseCertFilePaths(
                parsedConfig.certFilePaths);
        }
        if (parsedConfig.internalCertFilePaths) {
            parsedConfig.internalHttps = this._parseCertFilePaths(
                parsedConfig.internalCertFilePaths);
        }
        // config is validated, safe to assign directly to the config object
        Object.assign(this, parsedConfig);
    }

    _parseCertFilePaths(certFilePaths) {
        const { key, cert, ca } = certFilePaths;

        const makePath = value =>
              (value.startsWith('/') ?
               value : `${this._basePath}/${value}`);
        const https = {};
        if (key && cert) {
            const keypath = makePath(key);
            const certpath = makePath(cert);
            fs.accessSync(keypath, fs.F_OK | fs.R_OK);
            fs.accessSync(certpath, fs.F_OK | fs.R_OK);
            https.cert = fs.readFileSync(certpath, 'ascii');
            https.key = fs.readFileSync(keypath, 'ascii');
        }
        if (ca) {
            const capath = makePath(ca);
            fs.accessSync(capath, fs.F_OK | fs.R_OK);
            https.ca = fs.readFileSync(capath, 'ascii');
        }
        return https;
    }

    getBasePath() {
        return this._basePath;
    }

    getConfigPath() {
        return this._configPath;
    }
}

module.exports = new Config();
