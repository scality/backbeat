#!/usr/bin/env python3
"""Read the D1 tickets from Jira through its REST API, for read 1.

Usage:
  jira.py --epic <epic key> --out <jira.json> [--jql '<search>']
  jira.py --self-test

The search defaults to `parent = <epic key> ORDER BY key`, with the
fields summary, status, assignee, customfield_10004 (the sprint) and
description. It pages through the answer and writes the list of issues to
--out, the input of render_tickets.py. The connector is not needed, so
nothing is retyped.

It needs ATLASSIAN_BASE_URL (https://scality.atlassian.net),
ATLASSIAN_EMAIL and ATLASSIAN_API_TOKEN in the environment. When one is
unset, or Jira refuses them, it prints one line that says what to export
and exits 2, and the caller falls back to the connector. The token goes to curl on stdin, never in the
arguments, and nothing prints it.
"""
import json
import os
import subprocess
import sys

FIELDS = ["summary", "status", "assignee", "customfield_10004", "description"]
NEEDED = ("ATLASSIAN_BASE_URL", "ATLASSIAN_EMAIL", "ATLASSIAN_API_TOKEN")
PAGE = 100


def quote(value):
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'


class AuthError(Exception):
    pass


def call(base, email, token, path, body=None):
    """One request to the Jira API. curl reads its whole config from
    stdin, so the credentials never show in the process list."""
    lines = ["url = " + quote(base.rstrip("/") + path),
             "user = " + quote(email + ":" + token),
             'header = "Accept: application/json"',
             'write-out = "\\n%{http_code}"', "silent", "show-error"]
    if body is not None:
        lines += ['header = "Content-Type: application/json"', "data = " + quote(json.dumps(body))]
    out = subprocess.run(["curl", "--config", "-"], input="\n".join(lines) + "\n",
                         capture_output=True, text=True, timeout=60)
    text, _, code = out.stdout.rpartition("\n")
    if out.returncode != 0:
        raise RuntimeError("curl exit %d: %s" % (out.returncode, out.stderr.strip()[:300]))
    if code in ("401", "403"):
        raise AuthError(code)
    if not code.startswith("2"):
        raise RuntimeError("HTTP %s: %s" % (code, text.strip()[:300]))
    return json.loads(text)


def search(jql, send):
    """All the issues of a search, page after page."""
    issues, token = [], None
    while True:
        body = {"jql": jql, "fields": FIELDS, "maxResults": PAGE}
        if token:
            body["nextPageToken"] = token
        page = send(body)
        issues += page.get("issues", [])
        token = page.get("nextPageToken")
        if page.get("isLast", True) or not token:
            return issues


def self_test():
    pages = [{"issues": [{"key": "ZZ-1"}], "nextPageToken": "t1", "isLast": False},
             {"issues": [{"key": "ZZ-2"}], "isLast": True}]
    sent = []

    def fake(body):
        sent.append(body)
        return pages[len(sent) - 1]
    got = search("parent = ZZ-0", fake)
    assert [i["key"] for i in got] == ["ZZ-1", "ZZ-2"], got
    assert "nextPageToken" not in sent[0] and sent[1]["nextPageToken"] == "t1", sent
    assert quote('a "b"') == '"a \\"b\\""'
    print("self-test ok")


def main(argv):
    if argv[:1] == ["--self-test"]:
        self_test()
        return 0
    args = dict(zip(argv[::2], argv[1::2]))
    if "--out" not in args or not ("--epic" in args or "--jql" in args):
        print("usage: jira.py --epic <key> --out <file> [--jql '<search>'] | --self-test",
              file=sys.stderr)
        return 2
    missing = [v for v in NEEDED if not os.environ.get(v)]
    if missing:
        print("Export %s to read Jira through its API; use the connector meanwhile."
              % ", ".join(missing))
        return 2
    jql = args.get("--jql") or "parent = %s ORDER BY key" % args["--epic"]
    creds = [os.environ[v] for v in NEEDED]
    try:
        # A search with bad credentials answers 200 with no issue, as for
        # an anonymous user, so check the login first.
        call(*creds, "/rest/api/3/myself")
        issues = search(jql, lambda body: call(*creds, "/rest/api/3/search/jql", body))
    except AuthError as e:
        print("Jira refused ATLASSIAN_EMAIL and ATLASSIAN_API_TOKEN (HTTP %s): renew the "
              "token and export it again; use the connector meanwhile." % e)
        return 2
    except (RuntimeError, ValueError, subprocess.TimeoutExpired) as e:
        print("Error: %s" % e, file=sys.stderr)
        return 1
    with open(args["--out"], "w", encoding="utf-8") as f:
        json.dump(issues, f, indent=1)
    print("%d issues written to %s" % (len(issues), args["--out"]))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
