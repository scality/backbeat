# The rendered views

What's next renders three views on every run, with no question, because they hold no truth of their own: the "Tickets / Tasks" tab and the two charts of the Progress tab in the tech spec doc, and the journal doc. Load the docs skill or the Claude Docs connector guide first, as the connector asks. `C` is the doc container, `<base>` the folder of the skill and `<tmp>` the session scratchpad.

## The Tickets tab

Render the Markdown with `render_tickets.py` without `--json` (read 5 of `SKILL.md`). Overwrite the "Tickets / Tasks" tab with that Markdown, through the Claude Docs connector. Read the tab body with a `view` read, and note its `rev` and the ids of its root blocks. Then send one prose `update` that `replace`s all those root block ids with the rendered Markdown (`"as":"markdown"`, several ids, so `"ifRev"` alone, the `rev` of that read). If the connector refuses with `guard_mismatch`, someone wrote in between: never force. Re-read the tab, re-run reads 1 and 5, and try once more. If it refuses with `anchors_affected`, a comment hangs on the old text: stop, and tell the developer which comment, because only they can decide to detach it. If the tab still cannot be written, say so: Jira holds the claim, and the next run renders it.

## The charts of the Progress tab

The Progress tab holds two charts, each a widget whose rows sit in its code. The Pivots table that the scope chart counts lives in the journal doc, so render the journal doc (below) before the charts: a pivot decided in this run then shows on the chart of this run. Read the tab with an `outline` read: its two `embed` blocks name the widgets (`ref='node/<id>'`), "Progress by area" first, then "Scope and pivots over time". For each widget, in turn:

1. Read it: `read( ref = {"object":"node","id":"<widget id>"}, engine = "widget", container = C )`. Write `value.code` to `<tmp>/<chart>-code.txt` and note `value.pub`.
2. Build the payload. For the scope chart, also `export` the journal doc to Markdown into `<tmp>/pivots.md`, for the dates of its Pivots table.

   ```
   python3 -I <base>/scripts/charts.py progress --render <tmp>/render.json --code <tmp>/progress-code.txt --pub <pub>
   python3 -I <base>/scripts/charts.py scope --render <tmp>/render.json --code <tmp>/scope-code.txt --pub <pub> --pivots <tmp>/pivots.md
   ```

   The script swaps the rows and prints the payload `{"kind":"draft","basePub":<pub>,"code":"<the new code>"}`, or `unchanged`, and then you skip that chart. The scope chart keeps one row per week and series: the script sets the counts of the current week and recounts the pivots of every week from the table.
3. Send the payload as one call, which validates and publishes: `update( ref = {"object":"node","id":"<widget id>"}, engine = "widget", container = C, payload = <the printed JSON> )`. If it is refused with `widget-draft-stale`, someone changed the widget since the read: never force. Re-read it and build the payload again, once. If it still fails, or the outcome is `staged-with-errors`, say so: the next run updates it.

## The journal doc

The journal doc (`doc` in `<setup>/.claude/bnaas/journal.json`) is where the journal lives. Nobody edits it by hand: the Stop hook has each session append its facts to the "Log" section, and this render keeps the two tables above it. It has three sections, in this order: "Current state", "Pivots" and "Log". No question: it is a render.:

1. Read the doc with an `outline` read through the Claude Docs connector, and note its `rev` and the block ids of each section: the table under "Current state" and the table under "Pivots". Never touch the "Log" section: the sessions write it.
2. Send one prose `update` with two ops, guarded with `"ifRev"` (the `rev` of that read):
   - `replace` the "Current state" table with a table rendered from `<tmp>/journal-state.json`: Repository, Branch, Last commit, Tests, PR, Open notes, one row per active branch. A branch is active while it has an open PR, unpushed commits on this machine (read 6), or a fact in the Log of the last 7 days: the other developer's local work only shows there, so a branch of theirs never drops out because this machine cannot see it. Its Open notes cell also holds the corrections of read 0, marked "corrected:".
   - `replace` the "Pivots" table with the columns Date, What changed, Why and Tasks affected: the rows already there, plus one row per decision note of `<tmp>/decisions.json` (read 0) (its date, its text, the `why:` and `tasks:` parts when the note has them) whose date and text are not already a row: last-read is kept per machine, so the other developer's run sees the same note as new, and a first run on a new machine sees every past one. Never drop a row: a pivot stays history.
3. If the connector refuses with `guard_mismatch`, re-read the doc and try once more; never force. If it refuses with `anchors_affected`, stop and tell the developer which comment hangs on the old text. If the doc still cannot be written, say so: the next run renders it.
4. Run `journal mark-read`, so that the next run starts after these bullets (never in a test run, which writes its renders to `<tmp>` only). Run it only after the doc write succeeded, so that a failed render reads the same bullets again.
