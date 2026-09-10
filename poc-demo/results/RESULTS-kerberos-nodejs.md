<!-- markdownlint-disable -->
<!-- A verbatim copy of the POC write-up. Reflowing it would make
     this copy differ from the original, so it is not linted. -->

# GATE 2 follow-up: can ONE Node process authenticate as TWO Kerberos principals?

Pure-JS producer stack, run 2026-09-10. Everything under
`/Users/anurag/capsule-corp/bnaas-poc/krb-spike/pure-js/`.
tmux session `bnaas-krb`, window `pure-js`. docker compose project `bnaaskrb`
(the librdkafka spike's rig, reused unchanged). No git repository was modified.

The question: the design claims a "pure-JS producer for Kerberos destinations"
with per-connection credentials lets one process serve destinations with
distinct principals. librdkafka was already proven to allow exactly one
identity per process (see `../RESULTS.md`). This tests the alternative.

Stack under test: **KafkaJS 2.2.4** (`sasl.authenticationProvider`, its custom
SASL mechanism API) + **npm `kerberos` 2.2.2** (MongoDB's N-API binding over MIT
libgssapi 1.20.1), on `node:22-bookworm` / linux-arm64.

Decisive-evidence rule inherited from the librdkafka spike: only the broker's
`Successfully authenticated client: authenticationID=...` line counts.

## Verdict

**PROVEN.** One Node process held producers authenticating as two different
principals concurrently, and separately as **50** different principals, over the
same broker, with per-connection credentials. It survives the two failure modes
that killed librdkafka's workaround: broker restart / reconnect, and the
`DIR:` collection layout itself. The mechanism is not a KafkaJS feature, it is
`gss_acquire_cred` being called with a **desired name** per client object.

## Why this works and librdkafka cannot

`node_modules/kerberos/src/unix/kerberos_gss.cc`, `authenticate_gss_client_init`:

```c
238:    else if (principal && *principal) {
239:        gss_name_t name;
240:        principal_token.length = strlen(principal);
241:        principal_token.value = (char*)principal;
242:
243:        maj_stat = GSS_CALL(gss_import_name)(&min_stat, &principal_token, GSS_VALUE(GSS_C_NT_USER_NAME), &name);
...
249:        maj_stat = GSS_CALL(gss_acquire_cred)(&min_stat,
250:                                    name,                       /* <-- DESIRED NAME */
251:                                    GSS_C_INDEFINITE,
252:                                    GSS_C_NO_OID_SET,
253:                                    GSS_C_INITIATE,
254:                                    &state->client_creds,       /* <-- PER-CLIENT HANDLE */
255:                                    NULL,
256:                                    NULL);
```

and the handle is then used per client on every step:

```c
316:    maj_stat = GSS_CALL(gss_init_sec_context)(&min_stat,
317:                                    state->client_creds,
318:                                    &state->context,
```

`src/unix/kerberos_unix.cc:132` reads it from JS: `std::string principal =
ToStringWithNonStringAsEmpty(options["principal"]);`.

That single argument is the whole difference. With a desired name, MIT resolves
the credential per call: `krb5_cc_cache_match()` picks the matching subsidiary
out of a `DIR:` collection (arm A), and failing that MIT initiates a fresh TGT
for exactly that principal from the **client keytab** (arm B). librdkafka passes
`GSS_C_NO_CREDENTIAL`, so it always gets the process-default ccache, which is
what `KRB5CCNAME` names and is process-global.

`kerberos` **7.0.0** (current registry `latest`) has the identical call at
`kerberos_gss.cc:256`, so this is not a 2.x accident.

## Rig and controls

Unchanged from the librdkafka spike: KDC realm `SCALITY.TEST`, Kafka 3.4.0 with
`KERB://:19095` SASL_PLAINTEXT/GSSAPI and `VERIFY://:19096` PLAINTEXT,
`AclAuthorizer`, `allow.everyone.if.no.acl.found=false`, `User:notifa` Write +
Describe on `topic-a` only and `User:notifb` on `topic-b` only. Added for this
run: 50 principals `notif0..notif49`, topic `topic-many`, and ACLs for all 50 on
it.

Smoke: one producer as notifa to topic-a, 2/2 delivered, broker logged
`authenticationID=notifa@SCALITY.TEST; authorizationID=notifa@SCALITY.TEST`
(`evidence/arm-smoke-a.*`). ACL negative control: notifa forced onto topic-b
authenticates and delivers **0/2** with `Not authorized to access topics:
[Topic authorization failed]` (`evidence/arm-aclcontrol.*`). The assertion is
binary.

## Results

`notifa`/`topic-a` is producer a, `notifb`/`topic-b` is producer b. Both live in
ONE Node process except in arm D. 9 rounds x 7 s = 63 s per arm unless noted.

| Arm | Config | Broker saw | Delivered a | Delivered b | Verdict |
|---|---|---|---|---|---|
| A | `KRB5CCNAME=DIR:/tmp/cc`, `kinit -c DIR::/tmp/cc/tkta`/`tktb`, `initializeClient(..., {principal})` per producer | notifa x1 + notifb x1 | 9/9 | 9/9 | **PASS** |
| B | NO kinit. merged client keytab via `KRB5_CLIENT_KTNAME`, empty `DIR:/tmp/cc2` | notifa x1 + notifb x1 | 9/9 | 9/9 | **PASS** |
| C | control: default `FILE:` cache, kinit a then b, NO `principal` passed | notifb x2 | 0/9, all `Topic authorization failed` | 9/9 | **collides, as designed** |
| D | control: two OS processes, one principal each | notifa x1 + notifb x1 | 9/9 | 9/9 | **PASS** |
| E | arm B, 24 rounds over 168 s, broker restarted 3x mid-run | 4 epochs, **both principals in every epoch** (notifa x4 + notifb x4) | 24/24 | 24/24 | **PASS** |
| F | arm B with 2-minute KDC ticket lifetime, 5-minute run, forced re-handshake every 36 s | notifa x9 + notifb x9 | 25/25 | 25/25 | **PASS** |
| G | footprint, 1/2/20/50 producers and 50 distinct principals | up to 50 distinct identities | 250/250 | see table | **PASS** |

Arm A is the exact credential-cache layout that **failed** as librdkafka spike
arm 2, where the winning identity was fixed by cache filename and one producer
could never win. Arm E is the exact scenario that **permanently broke**
librdkafka spike arm 9r, where after the first broker restart producer a was
never able to re-authenticate again and delivered 4 of 24.

### Arm C is the control that proves the desired name is doing the work

Same process, same two producers, same KafkaJS mechanism, only difference is
that `principal` is not passed to `initializeClient`. The broker then sees
`notifb` on both connections and producer a is denied on every send. So the pass
in arms A, B and E is attributable to `gss_acquire_cred`'s desired name, not to
KafkaJS opening two sockets.

One incidental difference from librdkafka worth recording: the client side here
does **not** lie. In arm C the `kerberos` binding reported
`client.username = notifb@SCALITY.TEST` for producer a, matching the broker.
librdkafka printed the configured name and hid the collision.

### Arm G: footprint in one process

Credential shape of arm B. Threads counted as `/proc/self/task`, sockets as
socket entries in `/proc/self/fd`.

| Producers | Peak RSS (MB) | Threads | Sockets | Delivered | Distinct broker identities |
|---|---|---|---|---|---|
| 1 | 67.5 | 11 | 1 | 5/5 | 1 |
| 2 | 66.9 | 11 | 2 | 10/10 | 2 |
| 20 | 77.0 | 11 | 20 | 100/100 | 2 (round-robin over 2 principals) |
| 50 | 76.8 | 11 | 50 | 250/250 | 2 (round-robin over 2 principals) |
| 50 distinct principals | 82.2 | 11 | 50 | 250/250 | **50 of 50 connections** |

Thread count is constant at 11 across all of them, which is Node's own baseline
(main thread + libuv pool + V8 helpers). KafkaJS is pure JS, so a producer costs
a socket and some JS objects, not a thread. `kerberos` is a native addon but its
calls run on the libuv pool, and it is only called during the handshake, not on
the send path.

Marginal cost measured here: about **0.3 MB RSS and one socket per producer**
between 2 and 50 producers, plus one `DIR:` subsidiary ccache per distinct
principal.

### Arm F: ticket expiry and reacquisition, per principal, in one process

`kadmin.local modprinc -maxlife "2 minutes"` on both principals, arm B
credential shape, 25 rounds over 300 s, with a forced disconnect/reconnect
every 3rd round (~36 s) so a fresh SASL handshake happens repeatedly after the
tickets have expired. An in-process ticker dumped `klist -A` every 30 s.

Four ticket generations were observed, and **both** principals reacquired
independently, each into its own subsidiary cache in the collection
(`evidence/arm-F.ticket-windows.txt`):

```
11:44:27  notifa  valid 11:43:57 -> 11:45:57      11:44:27  notifb  valid 11:43:57 -> 11:45:57
11:45:27  notifa  valid 11:44:58 -> 11:46:58      11:45:27  notifb  valid 11:44:58 -> 11:46:58
11:46:27  notifa  valid 11:46:10 -> 11:48:10      11:46:27  notifb  valid 11:46:10 -> 11:48:10
11:47:27  notifa  valid 11:47:22 -> 11:49:22      11:47:27  notifb  valid 11:47:22 -> 11:49:22
```

9 handshakes per producer (1 initial + 8 reconnects), broker logged 9 x notifa
and 9 x notifb, 25/25 and 25/25 delivered, zero authorization failures. So MIT's
client-keytab initiation refreshes each principal on demand with no kinit, no
refresh timer in the application, and no collision.

The `errorCount` in arm F's summary (12 and 16) is entirely
`Connection error: write after end` from the harness calling KafkaJS
`disconnect()` and `connect()` back to back; KafkaJS retries and succeeds. It is
a harness artifact, not a Kerberos failure. Every send delivered.

## Read-back cross-check

Read back over the PLAINTEXT listener (`evidence/readback.txt`), per-arm counts
match the delivery reports exactly, so nothing was acked without landing in the
log. `topic-many` holds 250 messages from 50 distinct producers.

| Topic | Arm A | Arm B (incl. E and G round-robin) | Arm D | Arm F |
|---|---|---|---|---|
| topic-a | 9 | 33 (9 + 24 from E) + footprint producers | 11 (9 + 2 smoke) | 25 |
| topic-b | 9 | 33 (9 + 24 from E) + footprint producers | 9 | 25 |
| topic-many | | 250 from 50 distinct principals (arm G50) | | |

Arm C's producer a and the ACL control contributed 0 messages, as they must.

## Verdict on the design claim

> "a producer with per-connection Kerberos credentials lets one Node process
> serve destinations with distinct principals"

**Supported by evidence, with the mechanism identified.** Concretely:

- KafkaJS 2.2.4's custom SASL mechanism API is sufficient to speak SASL GSSAPI
  to Kafka. It took about 90 lines: `initializeClient` / `step` / `unwrap` /
  `wrap({user})` driven over Kafka's `SaslAuthenticate` framing, three client
  round trips. There is no GSSAPI mechanism in KafkaJS itself, so this is code
  the team would own.
- The property that makes it work is `gss_acquire_cred` with a desired name, in
  the `kerberos` addon. It holds under the `DIR:` collection layout that broke
  librdkafka, under a client keytab with no kinit at all, across broker
  restarts, and across ticket expiry.
- The operationally cleanest shape is **arm B**: one merged client keytab in
  `KRB5_CLIENT_KTNAME`, an empty `DIR:` collection in `KRB5CCNAME`, and no kinit
  and no refresh timer in the application. MIT does the acquisition and the
  refresh. This removes the `kinit` subprocess and the 60 s relogin timer that
  backbeat runs today.
- Footprint is not the constraint at these numbers: 50 producers with 50
  distinct principals cost 82 MB RSS, 11 threads (Node's baseline, unchanged)
  and 50 sockets in one process.

### One implementation gotcha worth writing down

A KafkaJS custom mechanism must hand `saslAuthenticate` an **already
length-prefixed** BYTES buffer. Its SaslAuthenticate request encoder does
`new Encoder().writeBuffer(authBytes)`, which writes the buffer verbatim
(`protocol/requests/saslAuthenticate/v0/request.js:17`), while its response
decoder re-wraps `sasl_auth_bytes` with the length prefix
(`.../v0/response.js:31`). Getting this wrong produces a broker-side
`InvalidRequestException: Error reading byte array of 1619133117 byte(s)` and a
closed connection, with nothing useful on the client. See
`harness/gssapi.js:writeKafkaBytes`.

## What was NOT proven

- **No Active Directory.** One MIT KDC, realm `SCALITY.TEST`, aes256-cts-hmac-sha1-96
  only. AD issues larger tickets with PACs and different enctypes and canonicalizes
  names differently. Untested.
- **No cross-realm.** All principals in one realm, no referrals, no trust.
- **No SASL_SSL / TLS.** The rig is SASL_PLAINTEXT only. KafkaJS's TLS is
  independent of the SASL mechanism, but the combination was not run.
- **No channel bindings.** `gss_init_sec_context` is called with
  `GSS_C_NO_CHANNEL_BINDINGS`; a broker requiring them would need work.
- **Scale is 50, not 10,000.** The design's 10,000-distinct-endpoint case was
  not run. What the 50-principal arm establishes is that cost is linear in
  sockets and flat in threads, and that MIT keeps 50 subsidiary caches in one
  collection without collision. 10,000 principals in one `DIR:` collection,
  10,000 keytab entries in one file, and 10,000 concurrent TCP connections from
  one Node process are each separately unvalidated. A `DIR:` collection is a
  directory scanned by `krb5_cc_cache_match`, so lookup is not obviously O(1).
- **No throughput or latency measurement.** Rounds were 1 message per producer.
  The `kerberos` calls are native and run on the libuv threadpool, and they only
  happen at handshake time, but nothing here measures produce throughput.
- **No reauthentication testing.** `connections.max.reauth.ms` is 0 (default) on
  this broker, so Kafka's in-place SASL reauthentication path was never
  exercised; arm F forced full reconnects instead.
- **No failure-injection on credentials.** A missing keytab entry, a wrong
  password, a KDC outage mid-run, and a revoked principal were not tested.

## Maintenance facts on the two packages

| | `kafkajs` | `kerberos` |
|---|---|---|
| Version tested | 2.2.4 | 2.2.2 |
| Registry `latest` | **2.2.4** | 7.0.0 |
| Last publish | **2023-02-27** | 2.2.2 on 2025-03-19; 7.0.0 on 2025-11-05 |
| License | MIT | Apache-2.0 |
| Native or pure JS | **pure JS**, zero runtime dependencies | **native** N-API addon (`node-addon-api`, `prebuild-install`) |
| Repo | tulios/kafkajs | mongodb-js/kerberos |
| Notes | no release in ~3.5 years; no GSSAPI mechanism of its own | prebuilt arm64 binary available, no compile needed; needs `libkrb5`/`libgssapi-krb5-2` at runtime and `libkrb5-dev` to build from source |

Two things a decision should weigh. **KafkaJS's last release was February
2023.** It works against Kafka 3.4.0 here, but adopting it for Kerberos
destinations means depending on an unreleased-for-years client and owning a
GSSAPI mechanism that upstream does not ship. **`kerberos` is maintained** (it
is MongoDB's), but it is a native addon, its major version has moved 2.x -> 7.0.0,
and the arm here ran 2.2.2 while `latest` is 7.0.0. The desired-name call is
identical in 7.0.0 (`kerberos_gss.cc:256`), so the mechanism is stable across
that jump, but the API surface change was not exercised.

## Where this now lives

The standalone harness under `harness/` is what proved the mechanism. The
implementation moved into backbeat, branch `poc/S3C-11127-kerberos-producer`
off `poc/S3C-11127-workgroups`, worktree
`/Users/anurag/capsule-corp/scality/backbeat-krb`:

- `extensions/notification/destination/saslGssapi.js`, the RFC 4752 exchange as
  a kafkajs custom SASL mechanism, with the GSSAPI binding injected so the
  state machine is unit testable without the native module.
- `extensions/notification/destination/kerberosCredentials.js`, the process
  wide setup that makes several principals usable at once: a `DIR:` collection
  cache and one merged client keytab, the keytab merge written in JS rather
  than shelling out to `ktutil`.
- `extensions/notification/destination/KerberosKafkaProducer.js`, the producer,
  exposing the BackbeatProducer surface the destinations and the delivery pool
  already use.
- `extensions/notification/destination/deliveryProducerFactory.js` and the
  `deliveryPool.kerberosProducer` config switch, defaulting to `rdkafka` so
  flag-off behaviour is unchanged.
- `tests/functional/deliverypool/kerberos.js`, the arms below driven through
  `DeliveryProducerPool`, plus 76 unit tests.

The keytab merge was checked against the rig's real keytabs: merging
`notifa.keytab` and `notifb.keytab` in JS produces a file MIT's `klist -kte`
reads as both principals, aes256, kvno 2.

## The arms, re-run through backbeat

`yarn ft_test:notification:kerberos`, two destinations served by one
`DeliveryProducerPool` in one process, 8 passing in 7 minutes, exit 0.
Raw output `evidence/backbeat-suite.txt`.

| Arm | Driven through backbeat as | Result |
|---|---|---|
| A | `credentialSource: 'ccache'`, `DIR:` collection populated by kinit | PASS, both identities, 9/9 and 9/9 |
| B | `credentialSource: 'keytab'`, no kinit | PASS, both identities, 9/9 and 9/9 |
| C | `kerberosProducer: 'rdkafka'`, the shipping producer | one identity for both connections, one destination denied |
| D | one principal per forked process | PASS, both identities |
| E | arm B, broker restarted 3x over 161 s | PASS, both re-authenticated per epoch, 24/24 and 24/24, zero authorization failures |
| F | arm B, `modprinc -maxlife "2 minutes"`, 5 fresh pools | PASS, 15/15 and 15/15, zero authorization failures |
| G | 50 destinations over the two principals in one pool | PASS, RSS 140 -> 146 MB, threads 11 -> 11 |

Read back over the plaintext listener, counted per arm and destination:

| Arm | topic-a | topic-b |
|---|---|---|
| A | 9 | 9 |
| B | 9 | 9 |
| C | 3 | **0** |
| E | 24 | 24 |
| F | 15 | 15 |
| G | 25 producers x 1 | 25 producers x 1 |

Arm C is the line that matters: under the shipping producer the destination
that lost its identity landed **nothing at all**, while its partner delivered
normally. That is the failure mode in production terms, one destination at
100% failure decided by which kinit ran last.

## How to reproduce

```
tmux attach -t bnaas-krb                    # window `pure-js`
docker compose -p bnaaskrb ps               # from .../krb-spike/rig
cd .../krb-spike/pure-js
docker build -t krbjs:spike -f harness/Dockerfile harness
./harness/run-arm.sh A -e ARM=A -e ROUNDS=9 -e ROUND_MS=7000    # arms A B C D
./harness/run-abcd.sh
./harness/run-arm-restart.sh                # arm E, broker restarted 3x
./harness/run-f.sh                          # arm F, 2-minute tickets
./harness/run-g.sh                          # arm G footprint
docker exec -i bnaaskrb-kafka bash -s < harness/readback.sh
```

`ARM` values are handled in `harness/entrypoint.sh`: `A` (DIR + kinit -c),
`B`/`F` (merged client keytab, no kinit), `C` (default FILE cache, no desired
name), `D` (one principal per process, needs `ONLY=a|b`), `G50` (50-principal
client keytab).
