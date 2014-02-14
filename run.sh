#!/bin/bash

SMSHOME=/home/david/projects/agora-sms
PYTHON=/home/david/virtual_env/sms/bin/python
(
  # Wait for lock (fd 200) for 10 seconds
  flock -x -w 10 200 || exit 1
  
  cd $SMSHOME && $PYTHON $SMSHOME/sms.py process --count 10 > /tmp/smscronlog.txt 2>&1

) 200>/var/lock/.smsservice.lock
