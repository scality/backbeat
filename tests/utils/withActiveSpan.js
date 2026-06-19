'use strict';

const api = require('@opentelemetry/api');
const { AsyncLocalStorageContextManager } = require('@opentelemetry/context-async-hooks');
const { W3CTraceContextPropagator } = require('@opentelemetry/core');

let registered = false;

// The global OTEL API is a no-op until a context manager + propagator are
// registered: without them context.active() never carries a span and
// propagation.inject() emits nothing, so trace-context helpers return undefined.
// Register real ones once so tests can exercise the header-stamping path.
function ensureOtelTestGlobals() {
    if (registered) {
        return;
    }
    api.context.setGlobalContextManager(new AsyncLocalStorageContextManager().enable());
    api.propagation.setGlobalPropagator(new W3CTraceContextPropagator());
    registered = true;
}

// Run `fn` with a valid remote span active, so traceHeadersFromCurrentContext()
// emits a real traceparent header.
function withActiveSpan(fn) {
    ensureOtelTestGlobals();
    const spanContext = {
        traceId: '0af7651916cd43dd8448eb211c80319c',
        spanId: 'b7ad6b7169203331',
        traceFlags: 1,
    };
    const ctx = api.trace.setSpanContext(api.context.active(), spanContext);
    return api.context.with(ctx, fn);
}

module.exports = { withActiveSpan };
