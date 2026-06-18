'use strict';

const tracing = require('arsenal/build/lib/tracing');
const { version } = require('../../package.json');

function init() {
    tracing.init({
        serviceName: 'backbeat',
        serviceVersion: version,
        instrumentations: () => {
            const { HttpInstrumentation } = require('@opentelemetry/instrumentation-http');
            const { IORedisInstrumentation } = require('@opentelemetry/instrumentation-ioredis');
            const { MongoDBInstrumentation } = require('@opentelemetry/instrumentation-mongodb');
            const { AwsInstrumentation } = require('@opentelemetry/instrumentation-aws-sdk');
            return [
                // Outbound only: backbeat pods serve no application HTTP, so
                // disable inbound spans; the spread brings arsenal's trust-boundary
                // requestHook (strips traceparent on calls to untrusted hosts).
                new HttpInstrumentation({
                    ...tracing.makeHttpInstrumentationConfig(),
                    disableIncomingRequestInstrumentation: true,
                }),
                new IORedisInstrumentation({ requireParentSpan: true }),
                new MongoDBInstrumentation({ enhancedDatabaseReporting: false }),
                new AwsInstrumentation(),
            ];
        },
    });
}

module.exports = {
    init,
    close: tracing.close,
    isEnabled: tracing.isEnabled,
    endSpan: tracing.endSpan,
};
