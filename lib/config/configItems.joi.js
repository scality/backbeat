'use strict'; // eslint-disable-line

const joi = require('joi');

const hostPortJoi = joi.object({
    host: joi.string().required(),
    port: joi.number().greater(0).required(),
});

const logJoi =
          joi.object({
              logLevel: joi.alternatives()
                  .try('error', 'warn', 'info', 'debug', 'trace'),
              dumpLevel: joi.alternatives()
                  .try('error', 'warn', 'info', 'debug', 'trace'),
          }).default({
              logLevel: 'info',
              dumpLevel: 'error',
          });

const zookeeperNamespaceJoi =
          joi.string().regex(/^(\/[a-zA-Z0-9-]+)*$/).allow('');

const zookeeperJoi = joi.object({
    endpoint: joi.string().regex(/^[a-z0-9-.]+:[0-9]+(\/[a-zA-Z0-9-]+)*$/)
        .error(new Error('bad zookeeper endpoint, expect a string ' +
                         'of form "host:port[/chroot]"')),
});

hostPortJoi
          .keys({
              namespace: zookeeperNamespaceJoi.required(),
          }).default({
              host: '127.0.0.1',
              port: 2181,
              namespace: '/backbeat',
          });

module.exports = {
    hostPortJoi,
    logJoi,
    zookeeperNamespaceJoi,
    zookeeperJoi,
};
