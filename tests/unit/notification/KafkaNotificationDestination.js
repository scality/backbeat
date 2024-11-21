const assert = require('assert');
const sinon = require('sinon');

const FakeLogger = require('../../utils/fakeLogger');

const KafkaNotificationDestination =
    require('../../../extensions/notification/destination/KafkaNotificationDestination');
const KafkaProducer =
    require('../../../extensions/notification/destination/KafkaProducer');

describe('KafkaNotificationDestination ::', () => {
    afterEach(() => {
        sinon.restore();
    });
    it('should properly configure producer', done => {
        const destConfig = {
            host: 'localhost',
            port: 9092,
            topic: 'test',
            pollIntervalMs: 1000,
            requiredAcks: 1,
            compressionType: 'none',
        };

        sinon.stub(KafkaProducer.prototype, 'connect').callsFake(function connect() {
            setTimeout(() => this.emit('ready'), 100);
        });

        const kafkaNotificationDestination = new KafkaNotificationDestination({ destConfig, logger: FakeLogger });
        kafkaNotificationDestination._setupProducer(err => {
            assert.ifError(err);
            assert.strictEqual(kafkaNotificationDestination._notificationProducer._kafkaHosts, 'localhost:9092');
            assert.strictEqual(kafkaNotificationDestination._notificationProducer._pollIntervalMs, 1000);
            assert.strictEqual(kafkaNotificationDestination._notificationProducer._topic, 'test');
            assert.strictEqual(kafkaNotificationDestination._notificationProducer._compressionType, 'none');
            assert.strictEqual(kafkaNotificationDestination._notificationProducer._requiredAcks, 1);
            done();
        });
    });
});
