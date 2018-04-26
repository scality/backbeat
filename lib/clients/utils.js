function attachReqUids(s3req, log) {
    s3req.on('build', () => {
        // eslint-disable-next-line no-param-reassign
        s3req.httpRequest.headers['X-Scal-Request-Uids'] =
            log.getSerializedUids();
    });
}

module.exports = {
    attachReqUids,
};
