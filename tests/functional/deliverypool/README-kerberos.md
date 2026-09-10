# Kerberos delivery pool functional suite

`yarn ft_test:notification:kerberos` proves that ONE delivery worker process
can serve external kafka destinations that authenticate as different Kerberos
principals. It is out of CI, like the other delivery pool suites, because it
needs a KDC and a kerberised broker.

## What it asserts

Two destinations, `notifa` writing `topic-a` and `notifb` writing `topic-b`,
served by one `DeliveryProducerPool`. Broker ACLs allow each principal its own
topic only, so a producer that authenticated as the wrong principal is denied
rather than delivering as the wrong user. The identity is taken from the
broker's own `authenticationID=` log line: a client stack can report the
identity it was configured with rather than the one it authenticated as.

Arms: a credential cache collection (A), a client keytab with no kinit (B),
the shipping node-rdkafka producer as a collision control (C), one principal
per process (D), three broker restarts mid run (E), a two minute ticket
lifetime (F), and 50 producers in one process (G), then a read back over the
plaintext listener.

## Rig

The suite needs a KDC and a broker with GSSAPI, a plaintext listener to read
back through, and ACLs. It also needs the docker socket, because the broker
log is the evidence and arms E and F restart the broker and reconfigure the
KDC. Without the socket the whole suite skips.

Expected, all overridable by environment variable:

| Variable | Default | What |
|---|---|---|
| `KRB_BROKERS` | `localhost:19095` | SASL_PLAINTEXT/GSSAPI listener |
| `KRB_VERIFY_BROKERS` | `localhost:19096` | PLAINTEXT listener used for the read back |
| `KRB_KAFKA_CONTAINER` | `bnaaskrb-kafka` | broker container, for its log and for restarts |
| `KRB_KDC_CONTAINER` | `bnaaskrb-kdc` | KDC container, for `kadmin.local` |
| `KRB_REALM` | `SCALITY.TEST` | realm of both principals |
| `KRB_SERVICE` | `kafka` | broker service name |
| `CONF_DIR` | none, required | its `ssl/` holds `notifa.keytab` and `notifb.keytab` |

The broker needs `AclAuthorizer` with `allow.everyone.if.no.acl.found=false`,
`User:notifa` Write and Describe on `topic-a`, `User:notifb` the same on
`topic-b`, a broker principal `kafka/<broker host>`, and `topic-a`, `topic-b`
created. Every container has to resolve the broker host the broker principal
names, which is simplest with one shared network namespace.

## Running it

The tests need MIT krb5 and a Linux `node_modules`, so run them in a container
sharing the rig's network namespace, with the docker socket and the keytabs
mounted:

```
docker run --rm \
  --network container:<rig netns container> \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v <repo>:/usr/src/app \
  -v <keytabs dir>:/conf/ssl:ro \
  -v <krb5.conf>:/etc/krb5.conf:ro \
  -e CONF_DIR=/conf \
  <image with krb5-user and a linux node_modules> \
  yarn ft_test:notification:kerberos
```

Keep the Linux `node_modules` in a volume rather than a bind mount from a
macOS checkout: the native modules are platform specific.
