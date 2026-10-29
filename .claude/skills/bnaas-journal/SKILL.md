---
name: bnaas-journal
description: Write the D1 journal. Use it every time a Stop message says "journal facts wait in batch <id>" (start the journal agent on that batch in the background, then go on), and whenever the developer says "journal this", "log the decision", "note that we decided ...", "record a blocker", "note that ...", or names a decision, a blocker or a surprise that the other developer should read. It records the note and appends it to the Log of the Journal tab through a background agent, so the session never waits.
---

# Write the D1 journal

The hooks log the facts as work happens (Rule 2); this skill writes them, and the notes that only a session can see, into the Log of the Journal tab. The write runs in the background `bnaas-journal` agent, so that the session goes on at once. The target (doc, tab file, body node, tab name and link) comes from `<setup>/.claude/bnaas/journal.json`, or from the file that `BNAAS_JOURNAL_CONFIG` names. `<setup>` is the folder that holds `.claude/` of the shared setup, and `journal` is `python3 -I <setup>/.claude/hooks/journal`.

## A Stop message names a batch

"<n> journal facts wait in batch <id>": start the `bnaas-journal` agent in the background (`run_in_background`), with the batch id and the `journal` path of the message, and go on with the work. One agent per batch: two batches waiting means two agents. The agent skips a fact shown before only when the Log already holds a bullet with the same id and the same text; on an id that matches with another text it appends anyway and reports `id clash <id>`. When the agent's completion notice arrives, repeat its one line in your reply ("Journal batch <id>: <n> appended, <m> skipped"), with any failure or clash it reports. If it says that it has no Claude Docs connector, run its steps yourself ([agents/bnaas-journal.md](../../agents/bnaas-journal.md)).

## The developer asks to record something

1. Write it as one sentence of fact, at most 300 characters once the journal adds its ` | why: ` and ` | tasks: ` parts (it refuses a longer note), with no names, and run one of these, as is (no `--help` first), from the folder of the repository it is about, so that the note takes its repository and branch:

   ```
   python3 -I <setup>/.claude/hooks/journal note "<sentence>"
   python3 -I <setup>/.claude/hooks/journal note --decision "<sentence>" --why "<reason>" --tasks <ids>
   ```

   The first is for a blocker, a surprise or a note, the second for a decision or a change of plan; leave out `--why` or `--tasks` when it is not known.
2. `python3 -I <setup>/.claude/hooks/journal take` puts the facts that wait in a new batch and prints its id (nothing when none waits).
3. Start the `bnaas-journal` agent in the background on that batch, as above, with the absolute path of `<setup>/.claude/hooks/journal` written out (no variable such as `$CLAUDE_PROJECT_DIR`, which the pre-approval does not match), and tell the developer in one line what was recorded.

Facts only, no names, no ticket keys outside what the facts already hold (Rule 7).

## Self-improvement

At the end of every run, ask: did any step, path or assumption here turn out wrong or incomplete, and did the run need a step this file does not mention? If so, fix the section where it belongs, in a worktree of your own on the standing self-improvement ticket, and open a draft PR into the backbeat epic (Rule 8). Find that ticket in Jira with the search `parent = <epic key> AND summary ~ "self-improvements of the D1 skills"`; if there is none, stop and say that the ticket is missing. The key stays out of this file (Rule 7). Keep it lean: no changelog, no list of workarounds. Never edit the shared setup checkout in place.
