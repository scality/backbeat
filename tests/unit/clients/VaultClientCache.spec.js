const assert = require('assert');

const VaultClientCache = require('../../../lib/clients/VaultClientCache');

const TEST_SSL_KEY = `-----BEGIN PRIVATE KEY-----
MIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQCjoMnwibTjPlft
qQwu2BlTzd6cxgzqN4Q2zQzgGm5LcDeSZZSjlduP5QSkAFD0+76v6G9jw3pZx+Iz
knXDGiZMFjX43NT1aIpeuzzrTkogkel4t31UT5wI7FNIqUfSSWqTCrE0R9GDTJQk
JoIeNjKtpv5VOj9IGrDrYghbhQ0kf8fRomYlw6aXkmFZ3YpP2n8DRb22gTLfPzwf
v5qff9MINIt+TSNy6EDx+64r1Wj665sfhm+Mgmes/6sV2LLqfYTdfvX3Ia8dGVxa
Up79heo7WZl3luIV7bReKLTMzF0MGoPQ0/uQEoOOelkByadUmyT/E72wooEp8dEW
mbRHJbv/AgMBAAECggEARsedstQOkCH+rQpr/7NwuUSbYBt3qLUdFwt531LvlOhU
0ZnpQx3m3QbHDB4q5t4i7TrRPElpmn6RRZe8IwojuNP+wsjbwdBX0oSR5IN4I0Ca
yqIsr5TEPUPk/tBjBf7GABcm8iOC6JXumvihXmo5X86Vw84vY4RQNXGxhc03EysV
rOicaYyP0jnNkDiVlNQV3699CHuj+TOQBtgm/DHwWDSwCy3IFup28fkFf9CusdXo
PFgbijVVyA4qzT6k2p4xkpT0bKftCFNZX0EqTdDf9B47ylfntA3zKWlPJnrYq0Ol
4W4ux/BigRWVWb0BvVpAuUhsx60eBG+93128WZRwyQKBgQDYqdufExa0jb9cC6Ds
lTnEXFrx4gWdNCBoDqYLciBf4+2fu3u5mBK1D5AKHaL3zWQz4shqLznbSP+l6kZw
qtk8e54ByQ7gxEZgEYi/4JCrNsi/9e/YF0wwTGrh5CHNY+gCD8MmDX1ztM1XvZ2x
nDnlxTjNyv7lJ7Cf1BDb5mrHowKBgQDBVe/RREd8l5p5kg2R7oTa8kox4bcQKCh9
Ho/5pC9pqJkFrd6Kh3hvrcrMQoRXoq6/ZdhYZ3h/2BEm51fnclHAy12uJ0wtqNaV
EAKc5zLxeYZLRiVDoFuTAt1Fdcq1KvrzfXbf4DKp68ov3BgiTwFfIA3nTuyfRdSg
mj/Yyldv9QKBgEozAY+cze3PjXVMVjQvdrUUm+CycxG/REnemmbZEtVEDaDiaCDL
P7zaM44DUEhlAqfyRoh22+2JNmPvs2fqWrMn8pjR7lJzZVaJKrfrhB/ehymWZCkw
8VqpEQGDS0A3ssDh/QcPH6N8i8Y8ncCxq/JQdH+lwV1hFk/mJE/qvS7ZAoGACJWm
RmZ/vhqFM2y2yYoLwCUOAlUBaeg+k/+taOpPaKOh18y2mvQU9vCClrtFYRbKJ5mA
F7zQbuzLJi0TjCVZV/QvvrHkAgsDLC8/znO9oVdCDUmaEfym1EpGRPVMAOtdpT4m
7x3nYgAkRCfDspJLf0vPEjxA6XmSTWdL+nZRl5kCgYBjNo1DWy705UnoI0Gy/8/l
gcqQDrZu+hb+q5wGR7YuXeZf6O0GiMYvsth2//jLVxFNtlQbTho4HSdPzZlP+ZUe
bxtOU+kSWci1f0sbEVFlZWL9DcZovSSGxE0TdXKe4HlMRh0pSXbelhzqSxJjaTiR
epTXxEtz5JoK768GVi9Png==
-----END PRIVATE KEY-----
`;

const TEST_SSL_CERT = `-----BEGIN CERTIFICATE-----
MIIDaTCCAlGgAwIBAgIJALVuIUks3BFZMA0GCSqGSIb3DQEBCwUAMD4xCzAJBgNV
BAYTAlVTMQswCQYDVQQIDAJDQTEOMAwGA1UECgwFWmVua28xEjAQBgNVBAMMCWxv
Y2FsaG9zdDAeFw0xODEwMDExMDAwMDhaFw0xODEwMzExMDAwMDhaMD4xCzAJBgNV
BAYTAlVTMQswCQYDVQQIDAJDQTEOMAwGA1UECgwFWmVua28xEjAQBgNVBAMMCWxv
Y2FsaG9zdDCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBAKOgyfCJtOM+
V+2pDC7YGVPN3pzGDOo3hDbNDOAabktwN5JllKOV24/lBKQAUPT7vq/ob2PDelnH
4jOSdcMaJkwWNfjc1PVoil67POtOSiCR6Xi3fVRPnAjsU0ipR9JJapMKsTRH0YNM
lCQmgh42Mq2m/lU6P0gasOtiCFuFDSR/x9GiZiXDppeSYVndik/afwNFvbaBMt8/
PB+/mp9/0wg0i35NI3LoQPH7rivVaPrrmx+Gb4yCZ6z/qxXYsup9hN1+9fchrx0Z
XFpSnv2F6jtZmXeW4hXttF4otMzMXQwag9DT+5ASg456WQHJp1SbJP8TvbCigSnx
0RaZtEclu/8CAwEAAaNqMGgwHQYDVR0OBBYEFIe3z8k5FovvDM4CuYTEOZRk8KOE
MB8GA1UdIwQYMBaAFIe3z8k5FovvDM4CuYTEOZRk8KOEMA8GA1UdEwEB/wQFMAMB
Af8wFQYDVR0RBA4wDIcEfwAAAYcErBEABDANBgkqhkiG9w0BAQsFAAOCAQEAALfM
6GoToDHTPZo0dQBMeiKybLMg3f5CYfc9JLXo2PYEhZkKScEzIL+eftKM5xrIxpSs
LOIzF5nXzTwbqR1GTIyNIBfVs87xGIFLnZGfjYEsFNSLEHSLUbD6vIbYAtCtEooc
kRftMOD1tazZv8KwLPlukgCf5zA1dMpJnRo9YHL6h/j8KUsqV8eHNL56wDWdE/w+
32RKZssIoYRsYzkPsaBh7qPo/GIJGh4gPfeaZaTW6Ez0cGJn867BD2V/ClQzHMzl
JPhqBrmBy48x1tsHynuNMT4FMGOs7U0vYPM36DuWxsfIidy+bnBa1AONuODqx4sF
hsIqbYEmH5/4WFXOgQ==
-----END CERTIFICATE-----
`;

describe('Vault client cache', () => {
    let client;
    let client2;
    let vcc;

    beforeEach(() => {
        vcc = new VaultClientCache();
        vcc.setHost('source:s3', '1.2.3.4');
        vcc.setPort('source:s3', 8500);
        vcc.setHost('dest:s3', '5.6.7.8');
        vcc.setPort('dest:s3', 8500);
        vcc.setHttps('dest:s3', TEST_SSL_KEY, TEST_SSL_CERT);
    });

    it('should cache client instances', () => {
        client = vcc.getClient('source:s3');
        client2 = vcc.getClient('dest:s3');
        assert.notStrictEqual(client, client2);
        assert.strictEqual(vcc.getClient('source:s3'), client);
        assert.strictEqual(vcc.getClient('dest:s3'), client2);

        client = vcc.getClient('dest:multi', '10.0.0.1', 8443);
        client2 = vcc.getClient('dest:multi', '10.0.0.2', 8443);
        assert.notStrictEqual(client2, client);
        client2 = vcc.getClient('dest:multi', '10.0.0.1', 8443);
        assert.strictEqual(client2, client);
    });

    it('should honor setHost()/setPort()/setHttps()', () => {
        client = vcc.getClient('source:s3');
        assert.strictEqual(client.getServerHost(), '1.2.3.4');
        assert.strictEqual(client.getServerPort(), 8500);
        assert.strictEqual(client.useHttps, false);

        client2 = vcc.getClient('dest:s3');
        assert.strictEqual(client2.getServerHost(), '5.6.7.8');
        assert.strictEqual(client2.getServerPort(), 8500);
        assert.strictEqual(client2.useHttps, true);

        client = vcc.getClient('source:s3', '42.42.42.42', 1234);
        // setHost() has precedence
        assert.strictEqual(client.getServerHost(), '1.2.3.4');
        assert.strictEqual(client.getServerPort(), 8500);
    });

    it('should use host/port provided to getClient()', () => {
        client = vcc.getClient('dest:multi', '10.0.0.1', 8443);
        assert.strictEqual(client.getServerHost(), '10.0.0.1');
        assert.strictEqual(client.getServerPort(), 8443);

        client2 = vcc.getClient('dest:multi', '10.0.0.2', 8443);
        assert.strictEqual(client2.getServerHost(), '10.0.0.2');
        assert.strictEqual(client2.getServerPort(), 8443);
    });
});
