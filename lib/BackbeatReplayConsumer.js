const BackbeatConsumer = require('./BackbeatConsumer');
const { maxPollInterval } = require('./constants');

class BackbeatReplayConsumer extends BackbeatConsumer {
    constructor(config) {
        const { zookeeper, kafka, topic, groupId,
                fromOffset, concurrency, fetchMaxBytes,
                bootstrap, logConsumerMetricsIntervalS, replayDelayInSec } = config;

        // const usePauseLogic = replayDelayInSec * 1000 >= maxPollInterval;
        const usePauseLogic = true;

        super({ zookeeper, kafka, topic, groupId,
                fromOffset, concurrency: usePauseLogic ? 1 : concurrency, fetchMaxBytes,
                bootstrap, logConsumerMetricsIntervalS });

        // Note: if we don't call consume() at least as frequently as the configured max interval,
        // then the client will proactively leave the group so that another consumer can take over
        // its partitions.
        // this._usePauseLogic = replayDelayInSec * 1000 >= maxPollInterval;
        this._usePauseLogic = usePauseLogic;

        this._replayDelayInSec = replayDelayInSec;
    }

    // processEntry(entry, done) {
    //     let msBeforeResume = (entry.timestamp + this._replayDelayInSec * 1000) - Date.now();
    //     if (msBeforeResume < 0) {
    //         msBeforeResume = 0;
    //     }

    //     console.log('1 -> key!!!', entry.key.toString());
    //     console.log('1 -> WILL BE PROCESSED IN S:', msBeforeResume / 1000);
    //     return setTimeout(() => {
    //         console.log('2 -> PROCESSED key!!!', entry.key.toString());
    //         return this._queueProcessor(entry, done);
    //     }, msBeforeResume);
    // }

    pause(entry) {
        if (this._consumer) {
            const { topic, partition, offset } = entry;
            this._consumer.pause([{ topic, partition }]);
            // consumer position advances every time the consumer receives messages (consume());
            // We want to make sure the paused entry gets replayed once partition is resumed.
            this._consumer.seek({ topic, partition, offset }, 0, err => {
                console.log('SEEK ERROR', err);
            });
        }
        return;
    }

    resume(entry) {
        if (this._consumer) {
            const { topic, partition, offset } = entry;
            this._consumer.resume([{ topic, partition }]);
            this._tryConsume();
        }
        return;
    }

    delayConsumption(entry) {
        const msBeforeResume = (entry.timestamp + this._replayDelayInSec * 1000) - Date.now();
        console.log('key!!!', entry.key.toString());
        console.log('value!!!', entry.value.toString());
        console.log('offset!!!', entry.offset);
        console.log('READY IN S:', msBeforeResume / 1000);
        if (msBeforeResume > 0) {
            console.log('PAUSE!!!!');
            this.pause(entry);
            setTimeout(() => this.resume(entry), msBeforeResume);
            return true;
        }
        return false;
    }


}

module.exports = BackbeatReplayConsumer;
