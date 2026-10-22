---
name: bnaas-pins
description: Keep the Federation epic pinned to the CI images of the cloudserver, backbeat and vault epic heads. Use it to pin the epic images in Federation, to bump Federation, to open the Federation pin PR, or to answer "are the Federation pins current".
---

# Federation image pins

The D1 team does not release from the epic branches. Federation pins the images that CI builds for every epic commit: `ghcr.io/scality/cloudserver:<sha>-federation`, `ghcr.io/scality/backbeat:<sha>-federation` and `ghcr.io/scality/vault:<sha>`, where `<sha>` is the head of the epic branch in each repository. The pins are three keys of `group_vars/all`: the top-level `cloudserver_image_version`, `defaults.vault.og_docker_image_version` and `defaults.backbeat.og_docker_image_version`. No scality-kms image exists per sha, so `defaults.kms.og_docker_image_version`, which follows the cloudserver key by default, takes a fixed released tag such as `9.4.4-federation` through `--kms`. The script `scripts/pins.py` next to this file does the survey and the edit. It never commits.

The first pin PR replaces Federation PR 7261: the tags it pins, `9.6.0-bnaas.1-federation` and `7.95.0-bnaas.1`, do not exist on ghcr.io.

## Survey

1. Find the Federation epic checkout: the worktree of `scality/Federation` on the epic branch, or the main clone with the epic fetched. The script needs `docker login ghcr.io` done once.
2. Run the dry run, the default, from this skill's folder:
   `python3 scripts/pins.py --federation <federation checkout> --kms 9.4.4-federation`
3. Show the developer the table as printed: repo, current pin, epic head, image state, action. `pin` means the image exists and differs from the pin, `same` means it is current, `wait` means CI has not published the image of that head yet, `error` means the lookup failed. The `scality-kms` row shows the fixed tag; without `--kms` the script warns instead when scality-kms still follows `cloudserver_image_version`.
4. If every row says `same`, answer that the pins are current and stop.
5. Look for the CI pin PR: `gh pr list --repo scality/Federation --base <epic> --head improvement/<pin ticket>-epic-image-pins-bot --state open`, where `<pin ticket>` is `gh variable get BNAAS_PINS_TICKET --repo scality/Federation`. When it is open, the bot owns that branch and force-pushes it on each run: give its link, and offer to review and merge it instead of opening a second PR. Never push to that branch.
6. Otherwise, ask the developer whether to open the pin PR. Ask which open Federation D1 ticket to use; do not create one here.

## Pin PR, after a yes

1. Branch `improvement/<ticket>-epic-image-pins` from the Federation epic, never the CI branch `improvement/<ticket>-epic-image-pins-bot`, in a worktree at `<federation>/.claude/worktrees/<ticket>-pins`. Reuse the branch when it exists on origin, and rebase it on the epic.
2. Run `python3 scripts/pins.py --federation <worktree> --apply --kms 9.4.4-federation`. It rewrites only the pins whose image exists and prints the diff.
3. One commit per changed service, in the order cloudserver, backbeat, vault, then scality-kms (`<ticket>: Pin scality-kms at <tag>`). Subject: `<ticket>: Pin <service> at <short sha>`. Body: one line on why (the epic head moved), then the compare link `https://github.com/scality/<repo>/compare/<old sha or version>...<new sha>`. Use `git add -p` so each commit holds one service. Repositories: `cloudserver`, `backbeat`, `Vault`.
4. Add the row to the Remove before merge tab of the tech spec in the same PR when it is missing, as Rule 4 asks.
5. Ask the developer before the push.
6. Push, then open or update the draft PR into the epic. Reuse the open pin PR of that branch when one exists (`gh pr list --base <epic> --head <branch>`). Title `<ticket>: Pin the D1 epic images`. Five sections, one to three sentences each:
   - `## Intent: why does this change exist?`
   - `## System impact: what's affected, including downstream?`
   - `## Preserved behavior: what explicitly stays the same?`
   - `## Intended change: what's different after this PR?`
   - `## Verification: how do we know this worked, or how would we know if it didn't?`
   Name the three images and their compare links in Intended change. Verification names the Federation build of the PR and the Integration run.
7. Request the reviews of Rule 4 and tell the developer the branch, the PR and the ticket.

## CI

The Federation workflow `.github/workflows/bnaas-pins.yaml` runs the same script on `workflow_dispatch` from the epic branch, on its own branch `improvement/<ticket>-epic-image-pins-bot`, and keeps one pin PR open. Its ticket is the Federation repository variable `BNAAS_PINS_TICKET`, and the run stops with one line when that variable is unset or not a ticket key. It downloads `pins.py` at the backbeat commit given by its required input `setup_ref`, so the script must be on the backbeat epic first. Each service epic's CI starts it after a push. To start it by hand, pass the backbeat epic head:

```
sha=$(git ls-remote https://github.com/scality/backbeat refs/heads/<epic> | cut -f1)
gh workflow run bnaas-pins.yaml --repo scality/Federation --ref <epic> -f setup_ref=$sha
```

## Self-improvement

At the end of every run, ask: did any step, path or assumption here turn out wrong or incomplete, and did the run need a step this file does not mention? If so, fix the section where it belongs, in a worktree of your own on the standing self-improvement ticket, and open a draft PR into the backbeat epic (Rule 8). Find that ticket in Jira with the search `parent = <epic key> AND summary ~ "self-improvements of the D1 skills"`; if there is none, stop and say that the ticket is missing. The key stays out of this file (Rule 7). Keep it lean: no changelog, no list of workarounds. Never edit the shared setup checkout in place.
