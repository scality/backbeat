const lolex = require('lolex');

class TimeMachine {
    moveTimeForward(hours) {
        if (this._clock) {
            // in milliseconds
            const ms = hours * 60 * 60 * 1000;
            this._clock.tick(ms);
        }
    }

    install(now) {
        const currentTime = now || new Date();
        this._clock = lolex.install({
            now: currentTime,
            shouldAdvanceTime: true,
        });
        return this._clock;
    }

    uninstall() {
        if (this._clock) {
            this._clock.uninstall();
            this._clock = null;
        }
    }
}

module.exports = TimeMachine;
