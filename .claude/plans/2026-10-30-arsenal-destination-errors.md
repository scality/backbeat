# Arsenal: add the five destination API errors (task 26)

Code PR: https://github.com/scality/Arsenal/pull/2743

## Context

The destination API of D1 needs five named errors that Arsenal does not have. The tech spec contract table names four with their status. The fifth, `CredentialsUnavailable`, is the record error: CloudServer raises it when a destination record cannot be decrypted, because the master key is lost or the envelope was tampered with. The spec leaves its status open, and the developer chose 500. No Arsenal code raises these errors, so the change is additive and independent of the IAM actions PR and the routing PR. It is cut from `development/8.7` and ships in Arsenal 8.7.0 (task 28).

## Changes

1. `lib/errors/arsenalErrors.ts`, section "Special non-AWS S3 errors", after `LocationNotFound`: five `ErrorFormat` entries in the file's style. `TooManyBucketNotificationDestinations` takes the wording of `TooManyBuckets`. The `CredentialsUnavailable` description does not say why, so that no detail of the key or the envelope reaches a caller. `lib/errors/index.ts` derives `Name`, `errors` and `errorInstances` from the keys of that file, so nothing else changes. No existing name clashes.
2. `tests/unit/errors.spec.ts`: one `it.each` over the five names, in the style of the `KMS.NotFoundException` test: an `ArsenalError` instance, its `type`, `is[name]`, `code` and `description`. Fixing the old non-enumerable loop is out of scope.

## Contract value

`CredentialsUnavailable` 500 is in the "Record error" row of the contract table, marked provisional until this PR merges.

## Commits

A Prettier-only commit of the errors spec, then the test (red on the base), then the five entries (green).

## Verification

Red and green on `tests/unit/errors.spec.ts`; then `yarn build`, `yarn lint`, `yarn prettier:diff --check` and the full `yarn test`; in CI, the unit, lint, prettier and codecov/patch checks, and the Claude Code review.
