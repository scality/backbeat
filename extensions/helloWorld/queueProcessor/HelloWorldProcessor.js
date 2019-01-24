const AWS = require('aws-sdk');
const http = require('http');

const Logger = require('werelogs').Logger;
const errors = require('arsenal').errors;

const BackbeatConsumer = require('../../../lib/BackbeatConsumer');
const QueueEntry = require('../../../lib/models/QueueEntry');
const ObjectQueueEntry = require('../../../lib/models/ObjectQueueEntry');
const { getAccountCredentials } =
    require('../../../lib/credentials/AccountCredentials');

class HelloWorldProcessor {
    constructor(kafkaConfig, hwConfig, serviceAuth) {
        this.kafkaConfig = kafkaConfig;
        this.hwConfig = hwConfig;
        this._serviceAuth = serviceAuth;

        this._consumer = null;
        this._s3Client = null;

        this.httpAgent = new http.Agent({ keepAlive: true });
        this.logger = new Logger('HelloWorldProccessor');
    }

    _getS3Client(accountCreds) {
        const { host, port } = this.hwConfig.s3;
        const endpoint = `http://${host}:${port}`;

        return new AWS.S3({
            endpoint,
            credentials: accountCreds,
            sslEnabled: false,
            s3ForcePathStyle: true,
            signatureVersion: 'v4',
            httpOptions: {
                agent: this.httpAgent,
                timeout: 0,
            },
            maxRetries: 0,
        });
    }

    start() {
        // on setup, save an s3 client instance
        const credentials = getAccountCredentials(this._serviceAuth,
                                                  this.logger);
        this._s3Client = this._getS3Client(credentials);

        // start a BackbeatConsumer (a wrapper around kafka Consumer)
        let consumerReady = false;
        this._consumer = new BackbeatConsumer({
            kafka: { hosts: this.kafkaConfig.hosts },
            topic: this.hwConfig.topic,
            groupId: this.hwConfig.groupId,
            concurrency: 10,
            queueProcessor: this.processKafkaEntry.bind(this),
        });
        this._consumer.on('error', () => {
            if (!consumerReady) {
                this.logger.fatal('error starting helloworld processor');
                process.exit(1);
            }
        });
        this._consumer.on('ready', () => {
            consumerReady = true;
            this._consumer.subscribe();
            this.logger.info('helloworld processor is ready');
        });
    }

    _putTagging(entry, done) {
        const Bucket = entry.getBucket();
        const Key = entry.getObjectKey();

        // our default tag to apply
        const Tagging = {
            TagSet: [{
                Key: 'hello',
                Value: 'world',
            }],
        };

        // https://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/S3.html#putObjectTagging-property
        return this._s3Client.putObjectTagging({
            Bucket,
            Key,
            Tagging,
        }, err => {
            if (err) {
                this.logger.error('error putting object tagging', {
                    method: 'HelloWorldProcessor._putTagging',
                    error: err,
                });
            }
            return done(err);
        });
    }

    processKafkaEntry(kafkaEntry, done) {
        const entry = QueueEntry.createFromKafkaEntry(kafkaEntry);
        if (entry.error) {
            this.logger.error('error processing kafka entry', {
                error: entry.error,
            });
            return process.nextTick(() => done(errors.InternalError));
        }

        // NOTE: Additional filtering can be done on processor side, as seen
        // below. This is more in place as a fail safe and validation. However,
        // I do also check for versioning here, but this can be done on the
        // populator side filtering.

        // do not process if entry is either:
        // - not an object entry
        // - object key name is for specific version
        if (!entry instanceof ObjectQueueEntry) {
            this.logger.info('skipping invalid helloworld entry', {
                method: 'HelloWorldProcessor.processKafkaEntry',
                type: entry.constructor.name,
                isVersion: entry.isVersion(),
            });
            return process.nextTick(done);
        }

        return this._putTagging(entry, done);
    }

    // This is used as a healthcheck in Zenko to check internals. Can extend
    // this to check other internals if you use/require them for your extension
    isReady() {
        return this._consumer && this._consumer.isReady();
    }
}

module.exports = HelloWorldProcessor;
