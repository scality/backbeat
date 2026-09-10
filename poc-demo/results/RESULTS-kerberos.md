<!-- markdownlint-disable -->
<!-- A verbatim copy of the POC write-up. Reflowing it would make
     this copy differ from the original, so it is not linted. -->
<!-- The one edit: absolute home paths are written as ~. -->

# GATE 2: can one Node process hold Kafka producers for two Kerberos principals?

Spike run 2026-09-09. Everything under `~/capsule-corp/bnaas-poc/krb-spike/`.
tmux session `bnaas-krb`, docker compose project `bnaaskrb`. No git repository was
modified.

## Part 1 (KDC only, no Kafka): the credential-cache mechanics

Rig: KDC container `bnaaskrb-kdc` (realm `SCALITY.TEST`, aes256-cts-hmac-sha1-96 only),
principals `kafka/localhost`, `notifa`, `notifb`, three separate keytabs.
Client: `backbeat:bb828-gssapi` (MIT krb5 1.20.1, node-rdkafka 2.18.0 / librdkafka 2.3.0).

Raw evidence: `evidence/step1-ccache.txt`, `evidence/step1b-dir.txt`.
Scripts: `harness/step1-ccache.sh`, `harness/step1b-dir.sh`.

| Sub-arm | Setup | Result |
|---|---|---|
| 1a | default FILE cache, `kinit -k -t <kt> <princ>` for notifa then notifb (exactly backbeat's command today) | **Overwrite confirmed.** `klist -l` lists ONE cache, `notifb@SCALITY.TEST FILE:/tmp/krb5cc_0`. notifa's ticket is gone, not merely deprioritised. |
| 1b | `KRB5CCNAME=DIR:/tmp/cc`, `kinit -c DIR::/tmp/cc/a` and `-c DIR::/tmp/cc/b` (the design doc's literal proposal) | **Both kinits FAIL.** `kinit: Subsidiary cache path /tmp/cc/a filename does not begin with "tkt" resolving ccache DIR::/tmp/cc/a`, rc=1. MIT 1.20 requires every subsidiary filename in a DIR: collection to start with `tkt`. |
| 1b-fixed | same but `-c DIR::/tmp/cc3/tkta` / `tktb` | Both tickets **coexist** (`klist -l` shows notifa->tkta and notifb->tktb) but **neither becomes the default**. The `primary` pointer file still reads the literal `tkt`, and a bare `klist` returns `No credentials cache found (filename: /tmp/cc3/tkt)`. Re-running kinit with `-c` does not move `primary`. |
| 1c | `KRB5CCNAME=DIR:/tmp/cc2`, bare `kinit` (no `-c`) for a then b | Both coexist, MIT auto-allocates a subsidiary per principal (`tkt`, `tktQR5yQN`), and **`primary` flips to whichever kinit ran last**. A bare `klist` resolves to `notifb`. |
| 1d | as 1c, three rounds of a-then-b | No cache growth: still two subsidiaries after six kinits (MIT reuses the existing cache for a repeated principal). `primary` = notifb's cache, i.e. it flips on every refresh. |
| 1e | `KRB5CCNAME=FILE:/tmp/fa kinit ...` / `FILE:/tmp/fb kinit ...` | Per-kinit env prefix works and isolates the two tickets. But `KRB5CCNAME` for the *handshake* is read from the Node process environment, which is single-valued, so this only relocates the collision. |

### Verdicts on the design doc's two claims, Kerberos layer only

**(a) "distinct principals in one pooled process overwrite each other": CONFIRMED,
literally, with backbeat's exact kinit command.** Arm 1a: the second `kinit -k -t` destroys
the first principal's ticket in `FILE:/tmp/krb5cc_0`. Since
`extensions/notification/utils/auth.js:83` emits `kinit -k <principal> -t <keytab>` with no
`-c`, two kerberised destinations in one process land on this exact behaviour, and each
producer's independent 60 s relogin timer keeps flipping it.

**(b) "the MIT DIR: collection cache is a workaround": NOT as written, and the failure is
worse than predicted.** Two separate findings:
1. The proposed command syntax is invalid. `-c DIR::/tmp/cc/a` is rejected by MIT 1.20
   because subsidiary names must begin with `tkt`. Anyone implementing the doc verbatim
   gets rc=1 from kinit and no ticket at all.
2. With the syntax corrected, DIR: gives storage for both tickets but **no default**. The
   collection's `primary` pointer is only moved by `kswitch` or by a bare (no `-c`) kinit,
   never by `kinit -c`. librdkafka passes no desired name to GSSAPI, so the handshake
   resolves "default", which under explicit-`-c` DIR: does not exist. DIR: with bare kinit
   does produce a default, but it is the last-refreshed principal, so it collides exactly
   like arm 1a, only with a flip instead of a delete.

So at the Kerberos layer the doc's fix is not merely insufficient, it is a config that
cannot authenticate at all unless something also calls `kswitch` between handshakes.

---

## Part 2 (full rig): what librdkafka actually authenticates as

Rig, all in docker compose project `bnaaskrb` on its own bridge network, with a
netns-holder container so KDC, ZooKeeper, broker and harness all share `localhost`
(which keeps the broker principal `kafka/localhost@SCALITY.TEST` valid):

- `bnaaskrb-kdc` MIT KDC, realm `SCALITY.TEST`, aes256 only
- `bnaaskrb-zk` ZooKeeper 3.9.4
- `bnaaskrb-kafka` Kafka 3.4.0, `KERB://:19095` SASL_PLAINTEXT/GSSAPI and
  `VERIFY://:19096` PLAINTEXT, `AclAuthorizer`,
  `allow.everyone.if.no.acl.found=false`, `super.users=User:ANONYMOUS`
- ACLs: `User:notifa` Write+Describe on `topic-a` only, `User:notifb` on `topic-b` only
- Harness `harness/two-principal-producers.js` in `backbeat:bb828-gssapi`
  (node-rdkafka 2.18.0 / librdkafka 2.3.0), two `Kafka.Producer` handles in ONE
  process, `sasl.kerberos.min.time.before.relogin=10000`, `debug: 'security'`,
  9 rounds x 5 s of interleaved sends, so every arm crosses 4+ refresh cycles.

**Controls proving the rig discriminates:**
one producer as notifa to topic-a delivered 2/2 and the broker logged
`Successfully authenticated client: authenticationID=notifa@SCALITY.TEST`
(`evidence/smoke-single-a.txt`). The same producer forced onto `topic-b` got
`Broker: Topic authorization failed`, 0 delivered (`evidence/arm-aclcontrol.*`).

### Results

`notifa`/`topic-a` is producer a, `notifb`/`topic-b` is producer b. "Broker saw"
counts `authenticationID=` lines in the broker log slice for that arm.

| Arm | Config | Broker saw | Delivered a | Delivered b | Verdict |
|---|---|---|---|---|---|
| 1 baseline | shared default FILE ccache, `kinit -k <p> -t <kt>` (backbeat today) | notifb x2 | 0/9, all `Topic authorization failed` | 9/9 | **FAIL, collision** |
| 2 DIR + `-c` | `KRB5CCNAME=DIR:/tmp/cc`, `kinit ... -c DIR::/tmp/cc/tkt<n>` (the doc's proposal, syntax corrected) | notifa x2 | 9/9 | 0/9 denied | **FAIL, collision** |
| 2b DIR bare | `KRB5CCNAME=DIR:/tmp/cc`, bare `kinit` | notifb x2 | 0/9 denied | 9/9 | **FAIL, collision** |
| 3 two processes | one principal per OS process | notifa x1 + notifb x1 | 9/9 | 9/9 | **PASS** (rig is sound) |
| 4 `sasl.username` | arm 1 + `sasl.username=<principal>` per producer | notifb x2 | 0/9 denied | 9/9 | **FAIL, no effect** |
| 5 baseline, b created first | arm 1 with producer order reversed | notifa x2 | 9/9 | 0/9 denied | **FAIL**, and the winner is whichever kinit ran LAST |
| 6 DIR + `-c`, b first | arm 2 with producer order reversed | notifa x2 | 9/9 | 0/9 denied | **FAIL**, winner unchanged by order |
| 7 DIR + `-c`, only b | arm 2 with producer a removed | notifb x1 | n/a | 3/3 | passes, so the collection IS scanned |
| 8 DIR + `-c`, names swapped (a->`tktz`, b->`tkta`) | arm 2 with subsidiary filenames reversed | notifb x2 | 0/3 denied | 3/3 | **FAIL**, and the winner follows the CACHE FILENAME |
| 9 DIR + `kswitch` | `kinit ... -c DIR::/tmp/cc/tkt<n> && kswitch -p <principal>` | notifa x1 + notifb x1 | 9/9 | 9/9 | **PASSES for 45 s** |
| 9r DIR + `kswitch`, broker restarted 4x | arm 9 over 120 s with a broker restart every 25 s | epoch 1: notifa x11 + notifb x12; epochs 2-5: notifb x2 each, notifa NEVER again | 4/24 | 24/24 | **FAIL on reconnect** |

Read-back over the PLAINTEXT listener (`evidence/readback.txt`) matches the
delivery reports arm by arm, so no message was acked without landing in the log.

### Three findings that change how this should be diagnosed

**1. The client-side librdkafka log lies.** In every failing arm both producers
printed `Authenticated as notifa@SCALITY.TEST` and `Authenticated as
notifb@SCALITY.TEST` while the broker recorded only ONE identity for both
connections. librdkafka's `SASL_CB_CANON_USER` callback rewrites the display name
to the configured `sasl.kerberos.principal`, so that line reports what you
configured, not what you authenticated as. Only the broker's
`authenticationID=` line is evidence. Arm 1: client emitted both names, broker
logged `notifb` twice.

**2. Under `DIR:` with per-producer `-c`, the winning identity is chosen by cache
filename, not by producer.** Arms 2, 6, 7 and 8 together: with only `tktb`
present notifb authenticates (so MIT's GSSAPI scans the collection rather than
failing on the missing `primary`); with both present the winner is the same
regardless of which producer connects first; renaming a's cache to `tktz` and
b's to `tkta` flips the winner to notifb. So the process gets exactly ONE usable
Kerberos identity and the other producer can never win.

**3. `DIR:` + `kswitch` is a startup race, not a fix.** It passed cleanly for 45 s
and four refresh cycles because librdkafka serializes the FIRST kinit per client
(`SASLREFRESH ... First kinit command finished: waking up broker threads`) under a
process-global mutex, so each client handshakes in the window where its own
principal is still the collection primary. Restart the broker and that window is
gone: after the first restart at 10:39:36Z, producer a hit `topic authorization
failed` at 10:39:38Z and every subsequent epoch shows only
`authenticationID=notifb`. Producer a delivered 4 of 24 and never recovered.

## Verdict on the design doc's two claims

**(a) "distinct principals in one pooled process overwrite each other": CONFIRMED,
end to end.** With backbeat's exact kinit command, one of the two producers is
permanently authenticated as the wrong principal and every send it makes returns
`TOPIC_AUTHORIZATION_FAILED`. Which one loses is decided by kinit ordering, so in
production it is a coin flip resolved at startup. Two separate OS processes, the
shipping architecture, pass the identical test 9/9 and 9/9.

**(b) "the MIT `DIR:` collection cache is a workaround": REFUTED.** Three ways:
- As written it does not run: `-c DIR::/tmp/cc/a` is rejected by MIT 1.20 because
  subsidiary cache names must begin with `tkt`.
- With legal names it still collides, and worse than the baseline, because the
  winner is fixed by cache filename and cannot be steered per producer.
- Adding `kswitch` to make each ticket primary appears to fix it and does not.
  It survives startup and refreshes but fails permanently the first time a
  producer reconnects, which in production means any broker restart, rolling
  upgrade or network blip silently turns one destination into a 100%-failing
  destination.

`sasl.username` has no effect. No configuration-only fix was found. Consistent
with the survey's source reading: librdkafka calls `sasl_client_new` with no
client identity and passes no desired name to `gss_acquire_cred`, so the GSSAPI
plugin always takes the process-default credential, and `KRB5CCNAME` is
process-global. Making one process serve two Kerberos principals needs either a
librdkafka change (per-client `KRB5CCNAME` set under the existing kinit mutex, or
`gss_acquire_cred` with a desired name) or a process boundary per principal.

## What was NOT proven

- Only two principals in one realm were tested, on Kafka 3.4.0, librdkafka 2.3.0,
  linux/arm64. Not tested against Active Directory, cross-realm, or a newer librdkafka.
- Arm 9 was falsified by broker restart. It was not falsified by an idle-timeout
  or client-side reconnect, though there is no reason to expect a different result.
- Non-Kerberos destinations sharing a process were not exercised; nothing here says
  anything about SCRAM, PLAIN or TLS destinations, which carry per-client credentials.
- No SSL/SASL_SSL variant was tested; the rig is SASL_PLAINTEXT only.

## How to attach or tear down

```
tmux attach -t bnaas-krb                 # windows: build, kdc, kafka, harness
docker compose -p bnaaskrb ps            # from .../krb-spike/rig
docker compose -p bnaaskrb down -v       # tear down (leaves the ft/* project alone)
tmux kill-session -t bnaas-krb
```

Re-run one arm: `./harness/run-arm.sh <label> -e ARM=<baseline|dir|dir-bare|username|dir-kswitch> [-e KRB5CCNAME=DIR:/tmp/cc] -e ROUNDS=9 -e ROUND_MS=5000`
