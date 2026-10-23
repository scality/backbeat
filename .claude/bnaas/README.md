# BNaaS D1 backlog

The backlog lives in the Backlog tab of the tech spec doc: https://claude.ai/code/artifact/bfe390f9-d819-4a93-9050-ffd5eee1b80a. It is one table, a row per task, with the columns ID, Task, Area, Repository, Estimate (days), Depends on, Origin and Notes.

The tab holds only the task graph. Owner, ticket, PR and status are not in it. They live in Jira and GitHub, and what's next renders them into the Tickets tab and the charts of the Progress tab.

The doc wins day to day. People fix a dependency, add a task or correct a title in the tab itself, with no PR:

- Depends on lists task ids, comma-separated. A step of a task, such as 3.1, counts for its task.
- A task added after the breakdown gets a letter id, such as 22b, and Origin "new scope". The scope chart counts it.
- Notes is free text, except a cell that starts with "Dependencies unknown:". The scripts read the texts after it, separated by semicolons, as dependencies that name no task, and such a task is never ready. Replace them with task ids in Depends on, then clear the note.

## Seed the tab

The tab was seeded on 2026-10-22 from the breakdown artifact: https://claude.ai/artifact/QC6Cbrfm7a5hAfRw752c7L. Save the artifact as HTML and run the importer on it:

```sh
python3 -I .claude/bnaas/scripts/import_breakdown.py <breakdown.html> --markdown <backlog.md> --json <backlog.json>
```

The Markdown is the tab: an intro line and the table. The JSON is the same table for scripts, with the breakdown comment of each task. The importer drops the "Not in D1" group. Expect to adjust it if the table layout of the breakdown changes.

Re-seeding is a deliberate step, because it overwrites every edit made in the tab. Before you re-seed, carry those edits into the override tables at the top of `import_breakdown.py`, through a PR into the backbeat epic:

- `DEP_ALIASES`: a dependency text that names a group of tasks, mapped to their ids.
- `TITLE_OVERRIDES`: a task whose title the team changed, mapped to its new title.
- `SUMMARY_NOTES`: one sentence appended to a task's comment in the JSON.

New-scope rows are not in the breakdown, so add them back to the tab after the re-seed.

## Query it

Export the Backlog tab to Markdown (the docs connector `export`, format `markdown`) and pass the file. The JSON of the importer works too. The scripts need only the Python standard library.

```sh
python3 -I .claude/bnaas/scripts/backlog.py --backlog <backlog.md> list
python3 -I .claude/bnaas/scripts/backlog.py --backlog <backlog.md> deps <id>
python3 -I .claude/bnaas/scripts/backlog.py --backlog <backlog.md> ready --done 1,2,3
python3 -I .claude/bnaas/scripts/backlog.py --backlog <backlog.md> blocked --done 1,2,3
python3 -I .claude/bnaas/scripts/backlog.py --backlog <backlog.md> unknown --done 1,2,3
python3 -I .claude/bnaas/scripts/backlog.py --backlog <backlog.md> check
```

`list` prints every task with its area, repositories, estimate and direct dependencies.

`deps` prints all the tasks a task depends on, directly or through other tasks.

`ready`, `blocked` and `unknown` split the open tasks into three lists, and each needs `--done` (`--done ''` when nothing is done).

`ready` prints the tasks whose dependencies are all in the done set.

`blocked` prints the tasks that still wait for a dependency, with those dependencies.

`unknown` prints the tasks whose Notes start with "Dependencies unknown:". They are never ready.

`check` verifies that every dependency id exists and that there is no cycle, then prints counts.
