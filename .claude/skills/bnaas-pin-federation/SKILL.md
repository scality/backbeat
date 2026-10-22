---
name: bnaas-pin-federation
description: Keep the Federation epic pinned to the CI images of the cloudserver, backbeat and vault epic heads. Use it to pin the epic images in Federation, to bump Federation, to open the Federation pin PR, or to answer "are the Federation pins current".
---

# Federation image pins

The D1 team does not release from the epic branches. Federation pins the images that CI builds for every epic commit: `ghcr.io/scality/cloudserver:<sha>-federation`, `ghcr.io/scality/backbeat:<sha>-federation` and `ghcr.io/scality/vault:<sha>`, where `<sha>` is the head of the epic branch in each repository. The pins are three keys of `group_vars/all`: the top-level `cloudserver_image_version`, `defaults.vault.og_docker_image_version` and `defaults.backbeat.og_docker_image_version`. No scality-kms image exists per sha, so `defaults.kms.og_docker_image_version`, which follows the cloudserver key by default, takes a fixed released tag such as `9.4.4-federation` through `--kms`. The script `scripts/pins.py` next to this file does the survey and the edit. It never commits.

The first pin PR replaces Federation PR 7261: the tags it pins, `9.6.0-bnaas.1-federation` and `7.95.0-bnaas.1`, do not exist on ghcr.io. Its branch does not end in `-epic-image-pins`, so Survey step 5 does not list it: when a pin PR is open and `gh pr view 7261 --repo scality/Federation --json state --jq .state` prints `OPEN`, ask the developer, then close PR 7261 with a link to the new PR (`gh pr close 7261 --repo scality/Federation --comment "Replaced by <new PR link>"`).

## Survey

1. Find a Federation checkout, any branch, and fetch the epic: `git -C <federation> fetch origin <epic>`. The script reads `group_vars/all` from `origin/<epic>` with `git show`, never from the working tree, so a stale fetch gives stale pins. It needs `docker login ghcr.io` done once, and `gh` logged in, which reads the epic heads (scality/Vault is private).
2. Run the dry run, the default, from this skill's folder:
   `python3 scripts/pins.py --federation <federation checkout> --kms <kms tag>`
   `<kms tag>` is the epic's current `defaults.kms.og_docker_image_version` when it is already a released tag, so the run keeps it. Use `9.4.4-federation` only while that key still follows `cloudserver_image_version`.
3. Show the developer the table as printed: repo, current pin, epic head, image state, action. `pin` means the image exists and differs from the pin, `same` means it is current, `wait` means CI has not published the image of that head yet, `error` means the lookup failed. The `scality-kms` row shows the fixed tag, and a tag that does not exist is an `error`, never a `wait`; without `--kms` the script warns instead when scality-kms still follows `cloudserver_image_version`.
4. If no row says `pin`, stop: say that the pins are current, name each `wait` row as waiting for its CI image, and give the reason of each `error` row. `--apply` skips `wait` rows, so going on would give a branch with nothing to commit.
5. List every open pin PR into the epic, the developers' and the bot's: `gh pr list --repo scality/Federation --base <epic> --state open --json number,headRefName,url --jq '.[] | select(.headRefName | test("-epic-image-pins(-bot)?$"))'`. Rule 6 keeps one pin PR open.
   - The bot's (`-epic-image-pins-bot`, its ticket in `gh variable get BNAAS_PINS_TICKET --repo scality/Federation`): the bot owns that branch and force-pushes it on each run. Give its link, and offer to take it instead of opening a second PR: a pin PR has no shepherd, so the first developer to see it marks it ready, reviews it and merges it once its `build-tests` pass, with one approval from either developer (Rule 4). Never push to that branch.
   - A developer's (`improvement/<their ticket>-epic-image-pins`): offer to update that PR, with its ticket, through the steps below, instead of opening a second one.
6. With no open pin PR, ask the developer whether to open one. Ask which open Federation D1 ticket to use; do not create one here.

## Pin PR, after a yes

Run every `gh` command of this section with `--repo scality/Federation`: the session runs from the backbeat checkout, where `gh` resolves to backbeat.

1. Branch `improvement/<ticket>-epic-image-pins` from the Federation epic, never the CI branch `improvement/<ticket>-epic-image-pins-bot`, in a worktree at `<federation>/.claude/worktrees/<ticket>-pins`. Reuse the branch when it exists on origin, and rebase it on the epic.
2. Fetch the epic again (`git -C <worktree> fetch -q origin`). Then, for each service in the order kms, cloudserver, backbeat, vault, run `python3 scripts/pins.py --federation <worktree> --apply --only <service> --kms <kms tag>`, with the `<kms tag>` of the survey. It compares with the epic, rewrites that one pin in the worktree's `group_vars/all` when its image exists, and prints the diff. It refuses unless the worktree is on a pin branch (`improvement/<ticket>-epic-image-pins`, or the CI branch with `-bot`). kms goes first: on the epic it can still follow `cloudserver_image_version`, so pinning it first keeps every commit of the PR deployable.
3. After each run that changed the file, `git -C <worktree> add group_vars/all` and commit it, so that each commit holds one service; never use interactive git (`add -p`). Subject: `<ticket>: Pin <service> at <short sha>`, or `<ticket>: Pin scality-kms at <tag>`. Body: one line on why (the epic head moved), then the compare link `https://github.com/scality/<repo>/compare/<old>...<new sha>`, where `<old>` is the old pin without its `-federation` suffix (an image tag is not a git ref). Repositories: `cloudserver`, `backbeat`, `Vault`.
4. Add the row to the Remove before merge tab of the tech spec in the same PR when it is missing, as Rule 4 asks.
5. Ask the developer before the push.
6. Push, with `--force-with-lease` when step 1 rebased a branch that was already on origin, then open or update the draft PR into the epic. Reuse the open pin PR of that branch when one exists (`gh pr list --base <epic> --head <branch>`). Title `<ticket>: Pin the D1 epic images`. Five sections, one to three sentences each:
   - `## Intent: why does this change exist?`
   - `## System impact: what's affected, including downstream?`
   - `## Preserved behavior: what explicitly stays the same?`
   - `## Intended change: what's different after this PR?`
   - `## Verification: how do we know this worked, or how would we know if it didn't?`
   Link the task row of the ticket in the Tickets tab (Rule 7). Name the three images and their compare links in Intended change. Verification names the Federation build of the PR and the Integration run.
7. Request no reviewers on the draft: the three reviewers of Rule 4 (the other developer, `nicolas2bert` and the team `scality/hopocalypse`) are requested when the developer marks the PR ready. Tell the developer the branch, the PR and the ticket.
8. After every push of a PR branch, run the review loop of Rule 5: wait for the Claude Code review, address or answer each comment, keep the stack rebased, never mark the PR ready yourself, and watch the CI checks until the developer marks it ready.

## CI

The Federation workflow `.github/workflows/bnaas-pins.yaml` runs the same script on `workflow_dispatch` from the epic branch, on its own branch `improvement/<ticket>-epic-image-pins-bot`, and keeps one pin PR open. Its ticket is the Federation repository variable `BNAAS_PINS_TICKET`, and the run stops with one line when that variable is unset or not a ticket key. It downloads `.claude/skills/bnaas-pin-federation/scripts/pins.py` at the backbeat commit given by its required input `setup_ref`, so the script must be on the backbeat epic first. The workflow checks out its bot branch before it runs `--apply`, because the script writes on a pin branch only. A schedule does not fire from an epic branch, so a person or what's next starts it after an epic moves; once the `notify-federation` job lands, each service epic's CI starts it after a push. To start it by hand, pass the backbeat epic head:

```
sha=$(git ls-remote https://github.com/scality/backbeat refs/heads/<epic> | cut -f1)
gh workflow run bnaas-pins.yaml --repo scality/Federation --ref <epic> -f setup_ref=$sha
```

## Self-improvement

At the end of every run, ask: did any step, path or assumption here turn out wrong or incomplete, and did the run need a step this file does not mention? If so, fix the section where it belongs, in a worktree of your own on the standing self-improvement ticket, and open a draft PR into the backbeat epic (Rule 8). Find that ticket in Jira with the search `parent = <epic key> AND summary ~ "self-improvements of the D1 skills"`; if there is none, stop and say that the ticket is missing. The key stays out of this file (Rule 7). Keep it lean: no changelog, no list of workarounds. Never edit the shared setup checkout in place.
