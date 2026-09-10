#!/bin/bash
# Realm SCALITY.TEST, broker principal kafka/localhost, and the client
# principals the notification producers authenticate as, each with its own
# keytab plus one merged client keytab. Unchanged in substance from
# bnaas-poc/krb-spike/rig/kdc-entrypoint.sh.
#
# ktutil's `add_entry -password` prompts on a tty and cannot be driven from
# a pipe, so keys are extracted with `kadmin.local ktadd`.
set -ex

REALM=SCALITY.TEST
KT=/keytabs
CLIENTS="${KRB_CLIENT_PRINCIPALS:-notifa notifb}"

if [ ! -f /var/lib/krb5kdc/principal ]; then
    kdb5_util create -s -r "$REALM" -P kdc_master_password
    kadmin.local -q "addprinc -pw kafka_broker_test_password kafka/localhost@$REALM"
    for c in $CLIENTS; do
        kadmin.local -q "addprinc -pw ${c}_test_password ${c}@$REALM"
    done
fi

if [ ! -f "$KT/merged.keytab" ]; then
    rm -f "$KT"/*.keytab
    # -norandkey matters. Plain `ktadd` randomises the principal's key and
    # bumps its kvno, so writing the same principal into a second keytab
    # invalidates the first one and the client then fails the AS exchange
    # with an AES decryption error. With -norandkey every keytab carries the
    # key the KDC derived from the fixed test password, so a per-principal
    # keytab and the merged one are both valid at the same time.
    #
    # No -e keysaltlist here: kadmin refuses "cannot specify keysaltlist
    # when not changing key" alongside -norandkey, and it is unnecessary
    # because kdc.conf restricts supported_enctypes to aes256 only, so
    # aes256 is the only key these principals have.
    kadmin.local -q "ktadd -norandkey -k $KT/kafka.keytab kafka/localhost@$REALM"
    for c in $CLIENTS; do
        kadmin.local -q "ktadd -norandkey -k $KT/${c}.keytab ${c}@$REALM"
    done
    # One merged client keytab: the credential shape the Node producer uses,
    # KRB5_CLIENT_KTNAME plus an empty DIR: collection, so MIT acquires and
    # refreshes each principal on demand with no kinit and no relogin timer.
    for c in $CLIENTS; do
        kadmin.local -q "ktadd -norandkey -k $KT/merged.keytab ${c}@$REALM"
    done
    chmod 0644 "$KT"/*.keytab
fi

# kadmin.local exits 0 even when ktadd fails, so assert rather than trust
for want in kafka merged $CLIENTS; do
    [ -s "$KT/${want}.keytab" ] || { echo "FATAL: $KT/${want}.keytab was not written" >&2; exit 1; }
done

for f in "$KT"/*.keytab; do klist -kte "$f"; done

exec krb5kdc -n
