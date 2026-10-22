#!/usr/bin/env bash
# Push gate for Rule 7. Runs as a PreToolUse hook on Bash.
# When the command holds a git push, it finds the repository (git -C <dir>,
# a cd <dir> before it, or the session's cwd) and the commit being pushed
# (the refspec source, or HEAD). The push goes through only if
# review-branch.sh saved a review for that exact commit. Every other command
# passes, and so does a push with --delete or --dry-run.
# The gate refuses what it cannot check: a path with a shell variable,
# GIT_DIR or --git-dir, --all, --mirror or --tags, a push hidden behind an
# unknown command, and any step before the push that is not read-only.
# The gate catches a forgotten review. It is not a security boundary: a
# command written to evade it can still get through.

HOOK_DIR=$(cd "$(dirname "$0")" && pwd)

GATE='
import json, os, re, shlex, subprocess, sys

review = os.path.join(sys.argv[1], "review-branch.sh")


def deny(reason):
    print(json.dumps({"hookSpecificOutput": {
        "hookEventName": "PreToolUse",
        "permissionDecision": "deny",
        "permissionDecisionReason": reason}}))
    sys.exit(0)


def git(repo, *args):
    r = subprocess.run(["git", "-C", repo, *args], capture_output=True, text=True)
    return r.stdout.strip() if r.returncode == 0 else None


CANNOT = ("Rule 7: the review gate cannot check the git push in this "
          "command. Push one branch of one repository per command, for "
          "example: git -C <literal path> push -u origin <branch>")
MIXED = ("Rule 7: this command runs other steps before the git push, so the "
         "review gate cannot check the pushed commit. Run the push as a "
         "command of its own.")
KEYWORDS = {"do", "then", "else", "elif", "if", "while", "until", "!", "{", "}"}
WRAPPERS = {"env", "nice", "nohup", "command", "exec", "time", "sudo",
            "timeout", "stdbuf", "caffeinate"}
SHELLS = {"bash", "sh", "zsh", "dash"}
SAFE = {"cd", "pushd", "popd", "echo", "printf", "true", "pwd", "ls", "test",
        "[", "sleep", "export", ":"}
GIT_READ = {"status", "log", "show", "diff", "rev-parse", "rev-list", "fetch",
            "remote", "ls-remote", "describe", "config"}
PUSH_AT_CMD = re.compile(
    r"(^|[;&|(`]|\$\()\s*((\w+=\S*|env|nice|nohup|command|exec|time|sudo|"
    r"xargs|timeout\s+\S+)\s+)*(\S*/)?git\b[^;&|\n]*\bpush\b", re.M)


def strip_heredocs(text):
    lines, out, i = text.split("\n"), [], 0
    while i < len(lines):
        out.append(lines[i])
        line, i = lines[i], i + 1
        for m in re.finditer(r"(?<!<)<<(?!<)-?\s*([\x27\"]?)(\w+)\1", line):
            while i < len(lines) and lines[i].strip() != m.group(2):
                i += 1
            i += 1
    return "\n".join(out)


def tokenize(text):
    text = text.replace("\\\n", "")
    lexer = shlex.shlex(text.replace("\n", " ; "), posix=True,
                        punctuation_chars=True)
    lexer.whitespace_split = True
    lexer.commenters = ""
    clean, it = [], iter(list(lexer))
    for tok in it:
        if tok and set(tok) <= set("<>&") and set(tok) & set("<>"):
            if clean and clean[-1].isdigit():
                clean.pop()
            next(it, None)
            continue
        clean.append(tok)
    return clean


def resolve(base, path):
    if "$" in path or "`" in path:
        return None
    path = os.path.expanduser(path)
    if os.path.isabs(path):
        return os.path.normpath(path)
    return os.path.normpath(os.path.join(base, path)) if base else None


def handle(seg, here, events):
    for k, tok in enumerate(seg):
        if tok.startswith("#"):
            seg = seg[:k]
            break
    gitdir = False
    while seg:
        if seg[0] in KEYWORDS:
            seg = seg[1:]
        elif re.match(r"^\w+=", seg[0]):
            gitdir = gitdir or bool(re.match(r"^GIT_(DIR|WORK_TREE)=", seg[0]))
            seg = seg[1:]
        elif os.path.basename(seg[0]) in WRAPPERS:
            seg = seg[1:]
            while seg and (seg[0].startswith("-")
                           or re.match(r"^\d+(\.\d+)?[smhd]?$", seg[0])):
                seg = seg[1:]
        else:
            break
    if not seg:
        return here
    head = os.path.basename(seg[0])
    if head in ("cd", "pushd"):
        if len(seg) < 2 or seg[1].startswith("-"):
            return None
        return resolve(here, seg[1])
    if head == "popd":
        return None
    if head in SHELLS:
        opts = [t for t in seg[1:] if t.startswith("-")]
        script = next((t for t in seg[1:] if not t.startswith("-")), None)
        if script is not None and any("c" in o for o in opts):
            scan(script, here, events)
        else:
            events.append(("other",))
        return here
    if head == "eval":
        scan(" ".join(seg[1:]), here, events)
        return here
    if head == "git":
        repo, i = here, 1
        while i < len(seg) and seg[i].startswith("-"):
            if seg[i] == "-C" and i + 1 < len(seg):
                repo = resolve(repo, seg[i + 1])
                i += 2
            elif seg[i] == "-c" and i + 1 < len(seg):
                i += 2
            else:
                if seg[i].split("=")[0] in ("--git-dir", "--work-tree"):
                    gitdir = True
                    if "=" not in seg[i]:
                        i += 1
                i += 1
        sub = seg[i] if i < len(seg) else ""
        if sub == "push":
            events.append(("cannot",) if gitdir else ("push", repo, seg[i + 1:]))
        else:
            events.append(("safe",) if sub in GIT_READ else ("other",))
        return here
    if "push" in seg and any(os.path.basename(t) == "git" for t in seg):
        events.append(("cannot",))
    else:
        events.append(("safe",) if head in SAFE else ("other",))
    return here


def scan(text, here, events):
    stack, seg = [], []
    for tok in tokenize(text) + [";"]:
        if tok and all(c in ";&|()" for c in tok):
            if seg:
                here = handle(seg, here, events)
            seg = []
            for c in tok:
                if c == "(":
                    stack.append(here)
                elif c == ")" and stack:
                    here = stack.pop()
        else:
            seg.append(tok)


def main(raw):
    data = json.loads(raw)
    cmd = (data.get("tool_input") or {}).get("command") or ""
    cwd = data.get("cwd") or os.getcwd()
    text = strip_heredocs(cmd)
    if not re.search(r"\bgit\b", text) or not re.search(r"\bpush\b", text):
        return
    events = []
    try:
        scan(text, cwd, events)
    except ValueError:
        if PUSH_AT_CMD.search(text):
            deny(CANNOT)
        return
    check(events)


def check(events):
    if any(e[0] == "cannot" for e in events):
        deny(CANNOT)
    last = max((k for k, e in enumerate(events) if e[0] == "push"), default=-1)
    if last < 0:
        return
    if any(e[0] == "other" for e in events[:last]):
        deny(MIXED)

    pushes = [(e[1], e[2]) for e in events if e[0] == "push"]
    for repo, args in pushes:
        if any(a in ("--delete", "-d", "--dry-run", "-n") for a in args):
            continue
        if any(a in ("--all", "--mirror", "--tags") for a in args):
            deny("Rule 7: --all, --mirror and --tags push more than one branch, "
                 "so the review gate cannot check them. Push one branch per "
                 "command.")
        if repo is None:
            deny("Rule 7: the review gate cannot tell which repository this git "
                 "push targets, because its path holds a shell variable. Push one "
                 "repository per command, with a literal path.")
        if git(repo, "rev-parse", "--is-inside-work-tree") != "true":
            deny(CANNOT)
        positional, j = [], 0
        while j < len(args):
            a = args[j]
            if a in ("-o", "--push-option", "--repo", "--receive-pack", "--exec"):
                j += 2
                continue
            if not a.startswith("-"):
                positional.append(a)
            j += 1
        sources = []
        for spec in positional[1:] or ["HEAD"]:
            spec = spec.lstrip("+")
            if not spec.startswith(":"):
                sources.append(spec.split(":", 1)[0])
        common = git(repo, "rev-parse", "--git-common-dir")
        for src in sources:
            sha = git(repo, "rev-parse", "--verify", "--quiet", src + "^{commit}")
            if not sha or not common:
                deny(f"Rule 7: the review gate cannot resolve {src} in {repo}.")
            record = os.path.join(repo, common, "bnaas-review", sha + ".md")
            if not (os.path.isfile(record) and os.path.getsize(record) > 0):
                deny(f"Rule 7: no local review of {sha[:8]} ({src}) in {repo}. "
                     f"Run {review} {repo}, fix or report each finding, then "
                     "ask the developer for the push again.")


raw = sys.stdin.read()
try:
    main(raw)
except Exception:
    if re.search(r"\bgit\b", raw) and re.search(r"\bpush\b", raw):
        deny(CANNOT)
'

exec python3 -I -c "$GATE" "$HOOK_DIR"
