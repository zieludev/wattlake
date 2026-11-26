#!/bin/sh
# Spark's Hadoop code asks the system for the name of the user it runs as. Compose starts the
# containers with the host user's UID, so the files they write in ./data are that user's, and
# that UID has no name in the image. nss_wrapper gives it one without a writable /etc/passwd.
if ! whoami >/dev/null 2>&1; then
    export NSS_WRAPPER_PASSWD=/tmp/passwd NSS_WRAPPER_GROUP=/etc/group
    cp /etc/passwd "$NSS_WRAPPER_PASSWD"
    echo "wattlake:x:$(id -u):$(id -g):wattlake:/tmp:/usr/sbin/nologin" >>"$NSS_WRAPPER_PASSWD"
    export LD_PRELOAD=libnss_wrapper.so
fi
exec "$@"
