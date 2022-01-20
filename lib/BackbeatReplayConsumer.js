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
        const msBeforeResume = (entry.timestamp + this._replayDelayInSec * 1000) - Date.now();
        console.log('msBeforeResume!!!', msBeforeResume);
        console.log('this.isPaused()!!!', this.isPaused());
        if (msBeforeResume > 0 && !this.isPaused()) {
            this.unsubscribe();
            console.log('PAUSED!!!');
            // TODO: FIX SET TIMEOUT MEMEORY ISSUE: (node:28994) MaxListenersExceededWarning: Possible EventEmitter memory leak detected. 11 event.error listeners added. Use emitter.setMaxListeners() to increase limit
            setTimeout(this.resume.bind(this), msBeforeResume);
            return true;
        }
        console.log('PUSHED!!!');
        return false;
    }


}

module.exports = BackbeatReplayConsumer;
