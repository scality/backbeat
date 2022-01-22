const BackbeatConsumer = require('./BackbeatConsumer');

class BackbeatReplayConsumer extends BackbeatConsumer {
    constructor(config) {
        const { zookeeper, kafka, topic, groupId,
                fromOffset, fetchMaxBytes,
                bootstrap, logConsumerMetricsIntervalS, replayDelayInSec } = config;

        super({ zookeeper, kafka, topic, groupId,
                fromOffset, concurrency: 1, fetchMaxBytes,
                bootstrap, logConsumerMetricsIntervalS });

        this._replayDelayInSec = replayDelayInSec;
    }

    delayConsumption(entry) {
        // const msBeforeResume = (entry.timestamp + this._replayDelayInSec * 1000) - Date.now();
        // console.log('key!!!', entry.key.toString());
        // console.log('value!!!', entry.value.toString());
        // console.log('READY IN S:', msBeforeResume / 1000);
        // if (msBeforeResume > 0) {
        //     if (!this.isPaused()) {
        //         console.log('PAUSE!!!!');
        //         this.unsubscribe();
        //         setTimeout(this.resume.bind(this), msBeforeResume);
        //     }
        //     return true;
        // }
        return false;
    }


}

module.exports = BackbeatReplayConsumer;
