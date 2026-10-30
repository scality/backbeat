# Arsenal: route the service-root notification-destination requests (task 24)

## Context

CloudServer serves the five tenant destination calls at the service root, addressed by a `name` query parameter: `PUT|POST|GET|DELETE /?notification-destination&name=<name>`, and `GET /?notification-destination` for the list. Arsenal's routing does not know them. Today a service-root PUT, POST or DELETE gets 405 from `checkBucketAndKey` (`lib/s3routes/routes.ts:48`), and `GET /?notification-destination` lists buckets, because `routeGET.ts:30-32` ignores the query. The routes must reach the API method keys of task 25 (`notificationDestinationCreate|Update|Get|Delete|List`, in `lib/policyEvaluator/utils/actionMaps.ts` on the Arsenal#2742 branch). So this branch is cut from that branch and retargets `development/8.7` after it merges. It ships in Arsenal 8.7.0 (task 28).

Worktree `arsenal/.claude/worktrees/ARSN-671`, branch `improvement/ARSN-671-route-notification-destination`, from `improvement/ARSN-668-bucketnotif-iam-actions` (ef187aff).

**The constraint that shapes the design.** Arsenal `development/8.7` is production library code, and other teams can bump it into their components while D1 lives only on epic branches (CloudServer, backbeat, Vault). So this PR must not change any existing API or behavior, and must give nobody a reason to wait or to object. The IAM actions (#2742) and the errors (#2743) are purely additive. Routing is not, because it changes the answer to requests that exist today. So the new routing is opt-in, isolated in its own route file, and off by default.

## Design

1. **Off by default.** `Params` of `routes()` gets an optional `notificationDestinationRoutes?: boolean` (name provisional), documented in the `routes()` comment and type-checked in `checkTypes` when it is truthy, so that `null`, `0` or `''` count as off instead of throwing in dev mode. It is a field of the object that the caller passes, not an environment variable. When it is not `true`, nothing changes: the only new line that runs is the check of that field. CloudServer sets it from its own D1 flag, in the same change that defines the five handlers.
   - Why: a component that bumps Arsenal 8.7 without D1 sees no change. Without the switch, CloudServer's `callApiMethod` would authorize such a request and then call `this[apiMethod]` with no existence check (`lib/api/api.js:497`, `446-457`), so a service-root PUT that gets 405 today would throw after authentication.

2. **Isolated.** One new file, `lib/s3routes/routes/routeNotificationDestination.ts`, in the style of `routeWebsite.ts`, which `routes.ts` already selects before `method(...)`. `routes.ts` calls it after `checkUnsupportedRoutes` and before `checkBucketAndKey` (line 275), only when the field is `true`, the host is not a website endpoint (those keep their own route, computed once and shared with the website branch), the method is not OPTIONS, and `req.query['notification-destination'] !== undefined` (`querystring.parse` gives `''`, so presence is `!== undefined`). The existing route files `routeGET|PUT|POST|DELETE.ts` and their tests stay untouched, so no other API can change.

3. **What the new route does** (only with the field on). One comment above it states these rules and why, with no ticket key, and the PR's Intended change repeats them:
   - **A bucket or a key with the query gets `MethodNotAllowed`**, in both host styles. Today such a request falls through to `bucketDelete`, `bucketPut` or `bucketGet`, because bucket routes ignore unknown queries, and a mis-addressed destination call must never delete or create a bucket. It is the same shape check as `POST ?delete` with no bucket (`routePOST.ts:18`), in the other direction. Decision 9 (bucket-style host names) stays open, and this is its reversible default: accepting bucket hosts later turns an error into a success and breaks no client.
   - **A missing or empty `name` gets `InvalidArgument`** for PUT, POST and DELETE, and for a GET with `name=`. Only a GET with no `name` at all is the list. The name is the destination's address, as the bucket is a path's, and routing already checks addresses (`isValidBucketName`, `routes.ts:54`; a key-less `?uploadId`, `routeDELETE.ts:23`). It keeps one invariant with the IAM PR: the wildcard ARN `arn:scality:bucketnotif::<account>:*`, which `_buildArn` uses with no `generalResource`, only ever authorizes a list. Presence only: the name grammar stays with the CloudServer handler and task 27. `InvalidArgument` is in the contract's API error list, `InvalidRequest` is not; the description is customized: 'A bucket notification destination name must be specified.'
   - **Any other method** (HEAD and others) with the query at the service root gets `MethodNotAllowed`, as today.
   - **Dispatch and answer**, through `api.callApiMethod` with `routesUtils.statsReport500` as the other routes do: PUT `notificationDestinationCreate` and POST `notificationDestinationUpdate` answer 200 with no body (`responseNoBody`); DELETE `notificationDestinationDelete` answers 204 with no body, as `bucketDelete`; GET `notificationDestinationGet` (with a name) or `notificationDestinationList` answers 200 with the XML body (`responseXMLBody`). The spec names no success status, so these are provisional.

## Tests (red, then green)

- `tests/unit/s3routes/routes.spec.js`, modeled on its line-133 case, with `params.websiteEndpoints = []` (the suite sets `['localhost']`, which would send every request to `routeWebsite`):
  - Field unset, then `false`: a service-root PUT, POST, DELETE with the query still gets 405; `GET /?notification-destination` still calls `serviceGet`; a bucket request with the query still reaches its bucket call. This is the proof that nothing changes for other components.
  - Field `true`: each of the five calls reaches its method with its status; a bucket in the path, then a bucket host, with the query gets `MethodNotAllowed` and no `callApiMethod`; no name and `name=` get `InvalidArgument`; a bare `DELETE /` still gets 405 and a bare `GET /` still lists the buckets; OPTIONS keeps its CORS answer; a website host keeps its website route.
  - Field `null`, `0` or `''`: off, as unset. A production-mode load of `routes()` (`NODE_ENV=production`, where `checkTypes` does not run) treats a non-boolean value as off.
- `tests/unit/s3routes/routeNotificationDestination.spec.js` (new), modeled on `routeGET.spec.js:45` (`http.IncomingMessage`, `api.callApiMethod` as `jest.fn()`): the method mapping, the callbacks' status codes and bodies, the rule errors.
- The existing route specs are not modified, and they must pass unchanged.

## Commits (Rule 7)

1. `ARSN-671: test the notification-destination routes`: the tests, red on the base.
2. `ARSN-671: route the notification-destination requests`: the code, green.

A Prettier-only commit comes first if `prettier:diff --check` fails on a touched file as a whole. `bnaas-reviewer` before each commit, the local review gate before the push. Then a draft PR into the #2742 branch, retargeted to `development/8.7` after #2742 merges: the five sections, with "no behavior change unless the caller sets `notificationDestinationRoutes`; nothing sets it outside the D1 epic branches" in the first lines of System impact, the rules and their reasons in Intended change, the row 24 link, a link to this plan in the backbeat epic, and no reviewers.

## Where this plan lives

Never in an Arsenal or cloudserverclient branch or PR body: only in the backbeat epic. A draft plan PR into backbeat's `epic/S3C-10971-bnaas-d1` adds `.claude/plans/2026-10-30-arsenal-destination-routes.md`, from `improvement/ARSN-671-plan-destination-routes`, titled `ARSN-671: plan destination routes`, with the five sections. The Arsenal PR links that file. The same for the errors PR: its plan moves to `.claude/plans/2026-10-30-arsenal-destination-errors.md` through a plan PR from `improvement/ARSN-670-plan-destination-errors`, and the description of Arsenal#2743 swaps its collapsed plan for the link. Backbeat's `.claude/` already has its Remove before merge row.

## Contract values (Rule 5)

Before the PR leaves draft, write into the contract table, marked provisional: in the "Routes" row, the opt-in field `notificationDestinationRoutes` of `routes()`, off by default; with it on, `MethodNotAllowed` for a bucket or key with the query, `InvalidArgument` for a missing or empty name, and the success statuses (200, 200, 204, 200 with XML). Add the holding position to the decision 9 row.

## Follow-ups outside this PR

- Backlog Notes of task 31, where CloudServer takes Arsenal 8.7.0: CloudServer sets `notificationDestinationRoutes` only with its D1 flag on, in the change that defines the five handlers.
- A self-improvement draft PR into the backbeat epic on the standing ticket (Rule 8): Rules 5 and 7 and the skill text that put Arsenal and cloudserverclient plans in the PR description now send them to the backbeat epic through a plan PR per ticket, linked from the PR.
- Memories: a change on a shared Arsenal or cloudserverclient development branch that can affect another component stays behind an opt-in switch, off by default; and the plan of such work lives only in the backbeat epic.
- The waiting journal batch 07565393e4ff is flushed once plan mode ends.

## Verification

- `source ~/.nvm/nvm.sh && yarn install --frozen-lockfile` in the worktree.
- Red: after commit 1, the new cases fail and every existing route case passes (`npx jest tests/unit/s3routes`).
- Green: after commit 2, `tests/unit/s3routes` passes, and `git diff <base>..HEAD --stat` shows no change in the existing route files or their specs. Then `yarn build`, `yarn lint`, `yarn prettier:diff --check <base>..HEAD` and the full `yarn test` (the known open-handle exit of `requestUtils.spec.js` aside).
- In CI: unit, lint, prettier and codecov/patch green. The Claude Code review waits on the review app's token outage.
