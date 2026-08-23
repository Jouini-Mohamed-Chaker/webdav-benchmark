#!/bin/bash

ssh root@192.168.95.2 'fio --name=disktest --filename=/data/webdav/fio_test --size=200M \
  --rw=write --bs=1M --iodepth=8 --numjobs=8 --direct=0 --group_reporting'

# ssh root@192.168.95.2 rm /data/webdav/fio_test