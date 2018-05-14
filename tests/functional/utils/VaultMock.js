class VaultMock {
    onRequest(req, res) {
        res.writeHead(200);
        res.end(JSON.stringify({
            Credentials: {
                AccessKeyId: 'accessKeyId',
                SecretAccessKey: 'secretAccessKey',
                SessionToken: 'sessionToken',
                Expiration: 'expiration',
            },
        }));
    }
}

module.exports = VaultMock;
