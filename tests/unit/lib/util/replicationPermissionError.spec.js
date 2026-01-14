const assert = require('assert');

const {
    isAccessDeniedError,
    getAccessDeniedLogFields,
} = require('../../../../lib/util/replicationPermissionError');

describe('isAccessDeniedError', () => {
    it('should return true for AccessDenied error code', () => {
        const err = { code: 'AccessDenied' };
        assert(isAccessDeniedError(err));
    });

    it('should return false for other error codes', () => {
        const err = { code: 'NoSuchKey' };
        assert(!isAccessDeniedError(err));
    });

    it('should return false for null error', () => {
        assert(!isAccessDeniedError(null));
    });

    it('should return false for undefined error', () => {
        assert(!isAccessDeniedError(undefined));
    });

    it('should return false for error without code', () => {
        const err = { message: 'some error' };
        assert(!isAccessDeniedError(err));
    });
});

describe('getAccessDeniedLogFields', () => {
    it('should return log fields with bucket and sourceRole', () => {
        const bucket = 'test-bucket';
        const sourceRole = 'arn:aws:iam::123456789012:role/replication-role';
        const result = getAccessDeniedLogFields(bucket, sourceRole);
        assert.deepStrictEqual(result, {
            accessDeniedHint: 'Verify that the source role has the required ' +
                'permissions on the source bucket.',
            sourceRole: 'arn:aws:iam::123456789012:role/replication-role',
            bucket: 'test-bucket',
        });
    });

    it('should handle undefined values', () => {
        const result = getAccessDeniedLogFields(undefined, undefined);
        assert.deepStrictEqual(result, {
            accessDeniedHint: 'Verify that the source role has the required ' +
                'permissions on the source bucket.',
            sourceRole: undefined,
            bucket: undefined,
        });
    });
});
