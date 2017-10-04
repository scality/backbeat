'use strict'; // eslint-disable-line

const fs = require('fs');
const path = require('path');
const joi = require('joi');

const backbeatConfigJoi = require('./config.joi.js');

class Config {
    constructor() {
        /*
         * By default, the config file is "config.json" at the root.
         * It can be overridden using the BACKBEAT_CONFIG_FILE environment var.
         */
        this._basePath = __dirname;
        if (process.env.BACKBEAT_CONFIG_FILE !== undefined) {
            this.configPath = process.env.BACKBEAT_CONFIG_FILE;
        } else {
            this.configPath = path.join(this._basePath, 'config.json');
        }

        let config;
        try {
            const data = fs.readFileSync(this.configPath,
              { encoding: 'utf-8' });
            config = JSON.parse(data);
        } catch (err) {
            throw new Error(`could not parse config file: ${err.message}`);
        }

        const parsedConfig = joi.attempt(config, backbeatConfigJoi,
                                         'invalid backbeat config');

        // config is validated, safe to assign directly to the config object
        Object.assign(this, parsedConfig);

        if (this.extensions !== undefined &&
            this.extensions.replication !== undefined) {
            // additional target certs checks
            const { certFilePaths } = this.extensions.replication.destination;
            const { key, cert, ca } = certFilePaths;

            const makePath = value => (value.startsWith('/') ?
                                       value : `${this._basePath}/${value}`);
            const keypath = makePath(key);
            const certpath = makePath(cert);
            let capath = undefined;
            fs.accessSync(keypath, fs.F_OK | fs.R_OK);
            fs.accessSync(certpath, fs.F_OK | fs.R_OK);
            if (ca) {
                capath = makePath(ca);
                fs.accessSync(capath, fs.F_OK | fs.R_OK);
            }

            this.extensions.replication.destination.https = {
                cert: fs.readFileSync(certpath, 'ascii'),
                key: fs.readFileSync(keypath, 'ascii'),
                ca: ca ? fs.readFileSync(capath, 'ascii') : undefined,
            };
        }

        const healthChecks = config.server.healthChecks.allowFrom;
        if (healthChecks && healthChecks.length === 0) {
            this.healthChecks = { allowFrom: ['127.0.0.1/8', '::1'] };
        }
    }
}

module.exports = new Config();
