# Task 3: epic branches, first epic tags and pins, one Jira ticket per repository

## Context

Every later D1 step targets an epic branch. Task 3 in the breakdown is "Set up epic branches, epic tags and Integration CI against the Federation epic", and Anurag set the ticket scope to match it: a repository's ticket is done only when its epic branch exists, its first epic tag is released, and the next repository in the chain has pinned that tag. The chain: Arsenal tag, then Vault, CloudServer and backbeat pin it and release their tags, then Federation pins those images, then Integration points at the Federation epic branch and runs once. This proves the release and pin chain before any D1 code lands.

Branches are cut from the freshly pulled development branch and pushed once with plain git, using Anurag's local credentials. Every later change, including the tag bumps and pins, reaches an epic branch by PR with two approvals. A bot protects epic branches on GitHub. No GitHub API calls.

Facts (read only, 2026-10-14):
- All eight repositories are cloned in `bnaas-d1-delivery/`. Seven sit clean on their base branch. backbeat sits on `improvement/BB-916-shared-claude-setup`, at the same commit as `development/9.6`, with the uncommitted shared setup.
- `epic/S3C-10971-bnaas-d1` does not exist yet anywhere. S3C-10971 is an Epic (assignee Taylor) with no child tickets.
- Arsenal published 8.6.0 GA today, and `development/8.6` is at that release commit.

## The eight tickets (scope = done when)

All are Tasks with parent S3C-10971, assignee Anurag, priority Medium, Severity Medium (required, no default), no label. Created through the Atlassian MCP.

| Row | Project | Summary | Done when |
|---|---|---|---|
| 3.1 | ARSN | BNaaS D1: set up the arsenal epic branch and first epic tag | Branch cut from `development/8.6`. A PR bumps `package.json` to `8.6.1-bnaas.1`, and that release is published from the epic branch |
| 3.2 | VAULT | BNaaS D1: set up the vault epic branch and first epic tag | Branch cut from `development/7`. A PR pins Arsenal `8.6.1-bnaas.1` (an alias keeps `require('arsenal')`). A PR bumps to `7.95.0-bnaas.1`, and that release is published |
| 3.3 | CLDSRV | BNaaS D1: set up the cloudserver epic branch and first epic tag | Branch cut from `development/9.6`. A PR pins Arsenal `8.6.1-bnaas.1`. A PR bumps to `9.6.0-bnaas.1`, and that release is published, which also starts the scality-kms release with the same version |
| 3.4 | CLDSRVCLT | BNaaS D1: set up the cloudserverclient epic branch and first epic tag | Branch cut from `development/1.0`. One PR into the epic lets the release workflow publish epic prereleases under the dist-tag `prerelease`, and bumps to `1.0.14-bnaas.1`. The workflow change reaches `development/1.0` with the final merge. Then that release is published |
| 3.5 | BB | BNaaS D1: set up the backbeat epic branch and first epic tag | Branch cut from `development/9.6`. A PR pins Arsenal `8.6.1-bnaas.1`. A PR bumps to `9.6.0-bnaas.1`, and that release is published |
| 3.6 | S3C | BNaaS D1: set up the federation epic branch and its epic image pins | Branch cut from `development/10`. A PR pins Vault `7.95.0-bnaas.1`, CloudServer and scality-kms `9.6.0-bnaas.1-federation` and backbeat `9.6.0-bnaas.1-federation` in `group_vars/all`, with `build-tests` green |
| 3.7 | RING | BNaaS D1: set up the ring epic branch | Branch cut from `development/10`. Ring releases no tag, because Federation pins the RING build artifacts |
| 3.8 | INTGR | BNaaS D1: point the integration epic branch at the Federation epic | Branch cut from `development/10`. A PR sets `FEDERATION_BRANCH` to the Federation epic branch. One manual `end-to-end.yaml` run with the artifacts link of the 3.6 build passes, and the ticket links it |

s3utils (3.9) waits for task 84.

## What this session does now

1. Update Rule 3 in `backbeat/.claude/rules/bnaas-d1.md`: the epic branches are created once, in task 3, by one push from the pulled development branch.
2. Create the eight tickets. The first one alone, then the other seven only if it lands under S3C-10971 as expected.
3. Create the eight branches, each after its ticket:

   ```
   git -C <repo> switch <base>
   git -C <repo> pull --ff-only
   git -C <repo> switch -c epic/S3C-10971-bnaas-d1
   git -C <repo> push -u origin epic/S3C-10971-bnaas-d1
   ```

   After each push, a Jira comment on the repository's ticket records the cut sha, the last release tag and the compare link (see Provenance below). The pull picks up any PR merged since today's check. In backbeat the uncommitted setup files ride along through the switches and stay local, and backbeat switches back to `improvement/BB-916-shared-claude-setup` after its push. I report the workflow runs the pushes start, and leave them alone.
4. Update the tech spec:
   - Tickets tab: rows 3.1 to 3.8 get the scope above, Owner Anurag and the ticket link. Rows 3.10 to 3.14 fold into them. The paragraph "The tags ... wait until D1 code exists" goes. Rows 28 to 44 stay for the later tags that carry D1 code.
   - Merge strategy tab: M1 and M3 set to Approved, and "through the GitHub API" becomes "by one push from the pulled development branch". The Epic tags section says the first tags come at setup.

## Provenance in commit messages

Every version bump and every pin commit says where the epic branch came from, so that the history can be traced later. The messages follow the existing style (`CLDSRV-1012: Bump version`). The values come from git after the pull: the cut sha from `git rev-parse origin/<base>`, and the last release from `git describe --tags --abbrev=0 origin/<base>`. Example for Arsenal:

```
ARSN-<n>: Bump version to 8.6.1-bnaas.1

First BNaaS D1 epic tag for arsenal.

Epic branch: epic/S3C-10971-bnaas-d1
Cut from: development/8.6 at 765bda812467 on 2026-10-14
Last release on that line: 8.6.0
Changes since that release: https://github.com/scality/arsenal/compare/8.6.0...765bda812467
```

A pin commit adds the tag it pins and the compare link of the pinned repository, for example `Pins @scality/arsenal 8.6.1-bnaas.1: https://github.com/scality/arsenal/compare/8.6.0...8.6.1-bnaas.1`. The first push of a branch makes no commit, so right after each push a Jira comment on its ticket records the same four lines.

Today's last release on each line (before the pull): vault 7.94.0, cloudserver 9.5.0-preview.5 (the 9.6 line has no tag yet), cloudserverclient 1.0.13, backbeat 9.6.0-preview.5, federation, ring and integration 10.1.0.0_pw6. Arsenal will read 8.6.0 after the pull.

## Next, one PR at a time, in chain order

Arsenal bump and release (3.1). Then the Arsenal pins and the bumps and releases in Vault, CloudServer, cloudserverclient and backbeat (3.2 to 3.5). Then the Federation pins (3.6), then Integration (3.8). Each PR goes from a ticket branch, `improvement/<ticket>-<message>` (for example `improvement/ARSN-660-bump-version`), cut from the epic branch, and I ask before each push and each release. The shared setup PR (22a) comes first in backbeat, with this plan file.

## Verification for this session

- Jira: `parent = S3C-10971` lists the eight tickets with their scope.
- `git -C <repo> ls-remote --heads origin epic/S3C-10971-bnaas-d1` shows the branch in all eight repositories, each at the sha of its pulled base.
