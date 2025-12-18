const assert = require('assert');
const fs = require('fs');
const os = require('os');
const path = require('path');

const { generateKafkaAuthObject } = require('../../../../extensions/notification/utils/auth');


describe('generateKafkaAuthObject', () => {
    const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'backbeat-test-'));
    const currentConfDir = process.env.CONF_DIR;

    before(() => {
        fs.mkdirSync(path.join(tempDir, 'ssl'), { recursive: true });
        [
            'keytab.keytab',
            'ca-cert.pem',
            'client-cert.pem',
            'client-key.pem'
        ].forEach(file => fs.writeFileSync(path.join(tempDir, 'ssl', file), `fake-${file}`));
        fs.writeFileSync(
            path.join(tempDir, 'ssl', 'credentials.json'),
            JSON.stringify({
                username: 'testuser',
                password: 'testpassword'
            }
        ));
        process.env.CONF_DIR = tempDir;
    });

    after(() => {
        fs.rmSync(tempDir, { recursive: true, force: true });
        process.env.CONF_DIR = currentConfDir;
    });

    const testCases = [
        {
            description: 'empty auth object',
            valid: true,
            input: {},
            expected: {},
        },
        // SSL configurations
        {
            description: 'SSL-only with ssl flag enabled',
            valid: true,
            input: {
                ssl: true
            },
            expected: {
                'security.protocol': 'ssl',
            },
        },
        {
            description: 'SSL-only with ssl flag disabled',
            valid: true,
            input: {
                ssl: false
            },
            expected: {},
        },
        {
            description: 'SSL with custom certificates',
            valid: true,
            input: {
                ssl: true,
                ca: 'ca-cert.pem',
                client: 'client-cert.pem',
                key: 'client-key.pem',
                keyPassword: 'test-password',
            },
            expected: {
                'security.protocol': 'ssl',
                'ssl.ca.location': path.join(tempDir, 'ssl', 'ca-cert.pem'),
                'ssl.certificate.location': path.join(tempDir, 'ssl', 'client-cert.pem'),
                'ssl.key.location': path.join(tempDir, 'ssl', 'client-key.pem'),
                'ssl.key.password': 'test-password',
            },
        },
        // Kerberos authentication
        {
            description: 'Kerberos authentication with valid parameters',
            valid: true,
            input: {
                type: 'kerberos',
                protocol: 'SASL_PLAINTEXT',
                keytab: 'keytab.keytab',
                principal: 'test-principal',
                serviceName: 'test-service',
            },
            expected: {
                'sasl.mechanisms': 'GSSAPI',
                'security.protocol': 'SASL_PLAINTEXT',
                'sasl.kerberos.service.name': 'test-service',
                'sasl.kerberos.principal': 'test-principal',
                'sasl.kerberos.keytab': path.join(tempDir, 'ssl', 'keytab.keytab'),
                'sasl.kerberos.kinit.cmd': `kinit -k test-principal -t ${path.join(tempDir, 'ssl', 'keytab.keytab')}`,
            },
        },
        {
            description: 'Kerberos authentication with missing keytab file',
            valid: false,
            input: {
                type: 'kerberos',
                protocol: 'SASL_PLAINTEXT',
                keytab: 'nonexistent.keytab',
                principal: 'test-principal',
                serviceName: 'test-service',
            },
        },
        {
            description: 'Kerberos authentication with unsupported protocol',
            valid: false,
            input: {
                type: 'kerberos',
                protocol: 'UNSUPPORTED_PROTOCOL',
                keytab: 'keytab.keytab',
                principal: 'test-principal',
                serviceName: 'test-service',
            },
        },
        // Basic authentication
        {
            description: 'Basic authentication with valid parameters',
            valid: true,
            input: {
                type: 'basic',
                protocol: 'SASL_PLAINTEXT',
                credentialsFile: 'credentials.json',
            },
            expected: {
                'security.protocol': 'SASL_PLAINTEXT',
                'sasl.mechanisms': 'PLAIN',
                'sasl.username': 'testuser',
                'sasl.password': 'testpassword',
            },
        },
        {
            description: 'Basic authentication with inline credentials',
            valid: true,
            input: {
                type: 'basic',
                protocol: 'SASL_PLAINTEXT',
                username: 'testuser',
                password: 'testpassword',
            },
            expected: {
                'security.protocol': 'SASL_PLAINTEXT',
                'sasl.mechanisms': 'PLAIN',
                'sasl.username': 'testuser',
                'sasl.password': 'testpassword',
            },
        },
        {
            description: 'Basic authentication with missing credentials file',
            valid: false,
            input: {
                type: 'basic',
                protocol: 'SASL_PLAINTEXT',
                credentialsFile: 'nonexistent.json',
            },
        },
        {
            description: 'Basic authentication with missing credentials',
            valid: false,
            input: {
                type: 'basic',
                protocol: 'SASL_PLAINTEXT',
            },
        },
        {
            description: 'Basic authentication with unsupported protocol',
            valid: false,
            input: {
                type: 'basic',
                protocol: 'UNSUPPORTED_PROTOCOL',
                credentialsFile: 'credentials.json',
            },
        }
    ];

    testCases.forEach(({ description, input, expected, valid }) => {
        it(description, () => {
            const tester = valid ? assert.doesNotThrow : assert.throws;
            tester(() => {
                const result = generateKafkaAuthObject(input);
                if (valid) {
                    assert.deepStrictEqual(result, expected);
                }
            });
        });
    });
});
