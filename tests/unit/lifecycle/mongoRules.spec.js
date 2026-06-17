'use strict';

const assert = require('assert');

const {
    isMassExpirationRule,
    hasMassExpirationRule,
} = require('../../../extensions/lifecycle/util/mongoRules');

function rule(overrides = {}) {
    return Object.assign({
        ruleID: 'r1',
        ruleStatus: 'Enabled',
        actions: [{ actionName: 'Expiration', days: 0 }],
    }, overrides);
}

describe('mongoRules.isMassExpirationRule', () => {
    it('returns true for Expiration days=0, no filter, enabled', () => {
        assert.strictEqual(isMassExpirationRule(rule()), true);
    });

    it('returns false for Expiration days=1', () => {
        assert.strictEqual(isMassExpirationRule(
            rule({ actions: [{ actionName: 'Expiration', days: 1 }] })), false);
    });

    it('returns false for Expiration days=30', () => {
        assert.strictEqual(isMassExpirationRule(
            rule({ actions: [{ actionName: 'Expiration', days: 30 }] })), false);
    });

    it('returns false for date-based Expiration (not the days=0 signal)', () => {
        assert.strictEqual(isMassExpirationRule(
            rule({ actions: [{ actionName: 'Expiration', date: '2099-01-01T00:00:00Z' }] })), false);
    });

    it('returns false when ruleStatus is Disabled', () => {
        assert.strictEqual(isMassExpirationRule(
            rule({ ruleStatus: 'Disabled' })), false);
    });

    it('returns false when filter.rulePrefix is set', () => {
        assert.strictEqual(isMassExpirationRule(
            rule({ filter: { rulePrefix: 'foo/' } })), false);
    });

    it('returns false when filter.tags is non-empty', () => {
        assert.strictEqual(isMassExpirationRule(
            rule({ filter: { tags: [{ key: 'k', val: 'v' }] } })), false);
    });

    it('returns true when filter is present but empty', () => {
        assert.strictEqual(isMassExpirationRule(
            rule({ filter: {} })), true);
    });

    it('returns false for non-Expiration action types with days=0', () => {
        assert.strictEqual(isMassExpirationRule(
            rule({ actions: [{ actionName: 'Transition', days: 0 }] })), false);
        assert.strictEqual(isMassExpirationRule(
            rule({ actions: [{ actionName: 'NoncurrentVersionExpiration', days: 0 }] })), false);
    });

    it('returns false when actions is missing', () => {
        const r = rule();
        delete r.actions;
        assert.strictEqual(isMassExpirationRule(r), false);
    });

    it('returns true when any action is an Expiration days=0', () => {
        assert.strictEqual(isMassExpirationRule(rule({
            actions: [
                { actionName: 'NoncurrentVersionExpiration', days: 30 },
                { actionName: 'Expiration', days: 0 },
            ],
        })), true);
    });

    it('returns false for null/undefined rule', () => {
        assert.strictEqual(isMassExpirationRule(null), false);
        assert.strictEqual(isMassExpirationRule(undefined), false);
    });
});

describe('mongoRules.hasMassExpirationRule', () => {
    it('returns false for empty/missing rules', () => {
        assert.strictEqual(hasMassExpirationRule([]), false);
        assert.strictEqual(hasMassExpirationRule(null), false);
        assert.strictEqual(hasMassExpirationRule(undefined), false);
    });

    it('returns false when all rules are Disabled', () => {
        assert.strictEqual(hasMassExpirationRule([
            rule({ ruleStatus: 'Disabled' }),
            rule({ ruleStatus: 'Disabled' }),
        ]), false);
    });

    it('returns true when any enabled rule is mass-expiration', () => {
        assert.strictEqual(hasMassExpirationRule([
            rule({ ruleID: 'r1', filter: { rulePrefix: 'foo/' } }),
            rule({ ruleID: 'r2' }),
        ]), true);
    });

    it('returns false when no enabled rule is mass-expiration', () => {
        assert.strictEqual(hasMassExpirationRule([
            rule({ filter: { rulePrefix: 'foo/' } }),
            rule({ ruleID: 'r2', actions: [{ actionName: 'Expiration', days: 1 }] }),
        ]), false);
    });

    it('ignores disabled rules when scanning for a mass-expiration rule', () => {
        assert.strictEqual(hasMassExpirationRule([
            rule({ ruleStatus: 'Disabled' }),
            rule({ ruleID: 'r2', actions: [{ actionName: 'Expiration', days: 30 }] }),
        ]), false);
    });
});
