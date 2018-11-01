const config = require('../../config.json');

function getUrl(path, options) {
    const defaultOptions = {
        host: config.server.host,
        port: config.server.port,
        method: 'GET',
    };
    const { host, port } = options || defaultOptions;
    return `http://${host}:${port}${path}`;
}

module.exports = getUrl;
