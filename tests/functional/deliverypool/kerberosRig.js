const http = require('http');

/**
 * Control of the kerberos rig from inside the test container.
 *
 * The decisive evidence for "which principal did this connection authenticate
 * as" is the broker's own log line, not anything the client reports, so the
 * suite has to read the broker container's log. Restarting the broker and
 * shortening a principal's ticket lifetime need the same access. All three go
 * through the docker API over the mounted socket, so the image needs no
 * docker CLI.
 */

const SOCKET_PATH = process.env.DOCKER_SOCKET || '/var/run/docker.sock';

/**
 * Whether the rig can be controlled, so a suite can skip instead of failing
 * when the socket is not mounted
 * @return {boolean} true when the docker socket is reachable
 */
function rigControlAvailable() {
    try {
        return require('fs').statSync(SOCKET_PATH).isSocket();
    } catch {
        return false;
    }
}

/**
 * One request against the docker API
 * @param {Object} params - { method, path, body, raw }
 * @param {function} cb - cb(err, payload)
 * @return {undefined}
 */
function dockerRequest(params, cb) {
    const { method, path, body, raw } = params;
    const payload = body === undefined ? null : Buffer.from(JSON.stringify(body));
    const req = http.request({
        socketPath: SOCKET_PATH,
        method,
        path,
        headers: payload
            ? { 'Content-Type': 'application/json', 'Content-Length': payload.length }
            : {},
    }, res => {
        const chunks = [];
        res.on('data', chunk => chunks.push(chunk));
        res.on('end', () => {
            const data = Buffer.concat(chunks);
            if (res.statusCode >= 400) {
                return cb(new Error(`docker API ${method} ${path} -> ` +
                    `${res.statusCode}: ${data.toString().slice(0, 200)}`));
            }
            return cb(null, raw ? data : data.toString());
        });
    });
    req.on('error', cb);
    if (payload) {
        req.write(payload);
    }
    req.end();
}

/**
 * Demultiplex a docker log stream.
 *
 * A container without a TTY has its output framed, 8 bytes of header per
 * frame, so the payloads have to be stitched back together before the log
 * can be grepped.
 *
 * @param {Buffer} stream - raw log stream
 * @return {string} the log text
 */
function demuxDockerStream(stream) {
    const parts = [];
    let offset = 0;
    while (offset + 8 <= stream.length) {
        const size = stream.readUInt32BE(offset + 4);
        parts.push(stream.subarray(offset + 8, offset + 8 + size).toString());
        offset += 8 + size;
    }
    return parts.join('');
}

/**
 * Read a container's log from a point in time
 * @param {string} container - container name
 * @param {number} sinceSeconds - unix timestamp to read from
 * @param {function} cb - cb(err, logText)
 * @return {undefined}
 */
function containerLogSince(container, sinceSeconds, cb) {
    dockerRequest({
        method: 'GET',
        path: `/containers/${container}/logs?stdout=1&stderr=1&since=${sinceSeconds}`,
        raw: true,
    }, (err, data) => {
        if (err) {
            return cb(err);
        }
        return cb(null, demuxDockerStream(data));
    });
}

/**
 * Restart a container and wait for it to report itself running again
 * @param {string} container - container name
 * @param {function} cb - cb(err)
 * @return {undefined}
 */
function restartContainer(container, cb) {
    dockerRequest({
        method: 'POST',
        path: `/containers/${container}/restart?t=5`,
    }, err => {
        if (err) {
            return cb(err);
        }
        return waitForRunning(container, Date.now() + 60000, cb);
    });
}

function waitForRunning(container, deadline, cb) {
    dockerRequest({ method: 'GET', path: `/containers/${container}/json` }, (err, body) => {
        if (!err) {
            try {
                if (JSON.parse(body).State.Running) {
                    return cb();
                }
            } catch {
                // fall through to the retry
            }
        }
        if (Date.now() > deadline) {
            return cb(new Error(`${container} did not come back up`));
        }
        return setTimeout(() => waitForRunning(container, deadline, cb), 500);
    });
}

/**
 * Run a command in a container and return its output
 * @param {string} container - container name
 * @param {string[]} command - argv
 * @param {function} cb - cb(err, output)
 * @return {undefined}
 */
function execInContainer(container, command, cb) {
    dockerRequest({
        method: 'POST',
        path: `/containers/${container}/exec`,
        body: { AttachStdout: true, AttachStderr: true, Cmd: command },
    }, (err, body) => {
        if (err) {
            return cb(err);
        }
        let id;
        try {
            id = JSON.parse(body).Id;
        } catch {
            return cb(new Error(`unexpected exec create response: ${body.slice(0, 200)}`));
        }
        return dockerRequest({
            method: 'POST',
            path: `/exec/${id}/start`,
            body: { Detach: false, Tty: false },
            raw: true,
        }, (startErr, data) => {
            if (startErr) {
                return cb(startErr);
            }
            return cb(null, demuxDockerStream(data));
        });
    });
}

/**
 * Principals the broker says it authenticated, in log order.
 *
 * Only the broker's own line counts as evidence: a client stack can report
 * the identity it was configured with rather than the one it authenticated
 * as, which is exactly how a credential collision hides.
 *
 * @param {string} logText - broker log text
 * @return {string[]} authentication ids, one per authenticated connection
 */
function authenticatedPrincipals(logText) {
    const matches = logText.match(/authenticationID=[^;\s]+/g) || [];
    return matches.map(match => match.replace('authenticationID=', ''));
}

module.exports = {
    authenticatedPrincipals,
    containerLogSince,
    demuxDockerStream,
    execInContainer,
    restartContainer,
    rigControlAvailable,
};
