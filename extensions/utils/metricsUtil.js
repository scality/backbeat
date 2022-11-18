function getStatusLabel(err) {
    if (!err) {
        return '2xx';
    }

    const statusCode = err.statusCode;

    if (!statusCode) {
        return '2xx';
    }

    if (statusCode >= 500) {
        return '5xx';
    }
    if (statusCode >= 400) {
        return '4xx';
    }

    if (statusCode >= 300) {
        return '3xx';
    }

    return '2xx';
}

module.exports = {
    getStatusLabel,
};
