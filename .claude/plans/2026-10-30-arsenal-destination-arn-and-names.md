# Arsenal: export the destination ARN parser and name grammar (task 27)

## Context

CloudServer (destination API handlers, the attach gate of `PutBucketNotificationConfiguration`) and backbeat (the worker) must read a destination ARN and check a destination name the same way. The tech spec fixes both: the ARN has six fields, `arn:scality:bucketnotif:<region>:<account>:<name>`, with the region always empty; a global destination has an empty account, a tenant one a 12-digit account; the name follows IAM's group and policy grammar `[\w+=,.@ -]`, 1 to 128 characters, which excludes `|` (the record key separator) and `*`. Arsenal has neither today:

- `ARN.createFromString` (`lib/models/ARN.ts:40`) rejects `bucketnotif`, because `validServices.scality` lists only `utapi` and `sso`.
- `NotificationConfiguration.parseArn` (`lib/models/NotificationConfiguration.ts:300`) is private, and checks only the `arn:scality:bucketnotif` prefix (`notificationArnPrefix`, `lib/constants.ts:160`) and the six fields; its comment leaves "the remaining 3 parts" to CloudServer.
- The name check the spec reuses lives in Vault (`lib/Check.js:68`, `regexpName = /^[\w+=,.@ -]+$/`), which CloudServer and backbeat do not import.

Worktree `arsenal/.claude/worktrees/ARSN-672`, branch `improvement/ARSN-672-arn-parser-name-grammar`, from `origin/development/8.7` (3e799f5c). It ships in Arsenal 8.7.0 (task 28). Fix Version 8.7.0 is set.

**Constraint.** `development/8.7` is production library code that other teams bump. This change is purely additive: a new module and one new export. No existing function, export or answer changes. So `ARN.createFromString` and `NotificationConfiguration.parseArn` stay untouched, even though either could have hosted the parser: widening `validServices` would change an answer of a public function.

## Changes

1. New `lib/models/NotificationDestination.ts`:
   - `nameMaxLength = 128` and `namePattern = /^[\w+=,.@ -]+$/`, the pattern of Vault's `regexpName`.
   - `isValidName(name: unknown): boolean`: a string of 1 to 128 characters that matches the pattern. A plain boolean, not a type predicate, because a string that fails the grammar is still a string.
   - `parseArn(arn: unknown): { accountId: string | null; name: string } | null`: `null` unless the value is a string with exactly six `:` fields starting `arn`, `scality`, `bucketnotif`. A tenant ARN (12-digit account) also needs an empty region and a name that passes `isValidName`. A global ARN (empty account, `accountId: null`) is read as today, with any region and any non-empty name, because the spec keeps stored global ARNs valid with no rewrite and today's path accepts them (CloudServer and backbeat take any string as a global name). It returns `null` instead of an error, so that each caller picks its own answer: the CloudServer attach gate an error to the client, the backbeat worker a dropped message.
   - `isGlobal(destination)`: `accountId === null`, for readability at call sites.
   - A short doc comment states the six-field format, the two forms, and why it is separate from `ARN.createFromString`.
2. `lib/models/index.ts`: `export * as NotificationDestination from './NotificationDestination';`, as `WebsiteConfiguration` is exported, so callers use `models.NotificationDestination.parseArn(...)`.

## Tests (red, then green)

`tests/unit/models/NotificationDestination.spec.js` (new), modeled on `tests/unit/models/ARN.spec.js`:
- `parseArn`: a global ARN (`arn:scality:bucketnotif:::target1`) and a tenant ARN (`arn:scality:bucketnotif::123456789012:prod-events`) parse to the right `accountId` and `name`; a global ARN with a region or a name outside the grammar (`kafka/orders`) parses as before; `null` for a non-string, a wrong partition or service, a tenant ARN with a region, an 11- or 13-digit account, `*` as account, seven fields, five fields, an empty name, a tenant name with `|`, `*` or `:`, and a 129-character tenant name.
- `isValidName`: 1 and 128 characters pass, 0 and 129 fail, a space, surrounding spaces (as in Vault) and each of `+=,.@-_` pass, `|`, `*`, `/`, `:`, a trailing newline and non-ASCII letters fail, a non-string fails. Whether the create handler trims or refuses surrounding spaces is for CloudServer to decide.
- The public export: `require('../../../index').models.NotificationDestination.parseArn` is a function, and `models.ARN` and `models.NotificationConfiguration` are unchanged.

## Commits (Rule 7)

1. `ARSN-672: test the destination ARN parser and names`: the tests, red on the base.
2. `ARSN-672: export the destination ARN parser and names`: the module and the export, green.

`bnaas-reviewer` before each commit, the local review gate before the push. Then a draft PR into `development/8.7` with the five sections, "additive only: one new module and one new export" in System impact, the row 27 link, the link to its plan PR, and no reviewers.

## Where this plan lives (Rule 5)

Right after approval, this plan goes to the backbeat epic as `.claude/plans/2026-10-30-arsenal-destination-arn-and-names.md`, through a draft plan PR from `improvement/ARSN-672-plan-destination-arn-and-names`, titled `ARSN-672: plan destination arn and names`, before the code PR. The code PR links that plan PR.

## Contract values

The spec already fixes the ARN format and the name grammar. The export names (`models.NotificationDestination.parseArn`, `isValidName`, `isGlobal`) are new glue between Arsenal, CloudServer and backbeat, so they go into the "ARN" and "Destination name" rows of the contract table, marked provisional until this PR merges.

## Verification

- `source ~/.nvm/nvm.sh && yarn install --frozen-lockfile` in the worktree (started).
- Red: after commit 1, the new spec fails (module not found), and `tests/unit/models` otherwise passes.
- Green: after commit 2, `npx jest tests/unit/models` passes; `yarn build`, `yarn lint`, `yarn prettier:diff --check origin/development/8.7..HEAD` and the full `yarn test` pass (the known open-handle exit of `requestUtils.spec.js` aside); `git diff --stat` shows only the new module, the one export line and the new spec.
- In CI: unit, lint, prettier and codecov/patch green. The Claude Code review waits on the review app's token outage.
