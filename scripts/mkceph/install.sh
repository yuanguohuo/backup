#!/bin/bash

DESTDIR=/usr/local/ceph

rm -f /etc/init.d/ceph
rm -fr $DESTDIR
mkdir -p $DESTDIR

cp -fr                        \
    ssh-conf.sh               \
    ceph                      \
    ceph-common.sh            \
    cleanup.sh                \
    deploy                    \
    deploy.sh                 \
    execute-cmd.sh            \
    getallhosts.sh            \
    log.sh                    \
    mkrados.sh                \
    mnt.sh                    \
    myhostname.sh             \
    remote-tool.sh            \
$DESTDIR


SYSTEMDDIR=/lib/systemd/system

cp -f ceph-mon.service      $SYSTEMDDIR/ceph-mon@.service
cp -f ceph-osd.service      $SYSTEMDDIR/ceph-osd@.service
cp -f ceph-radosgw.service  $SYSTEMDDIR/ceph-radosgw@.service
systemctl daemon-reload
