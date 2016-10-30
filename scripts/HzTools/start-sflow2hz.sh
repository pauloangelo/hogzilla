#!/bin/bash

BINPATH="/home/hogzilla/bin"

( 
while : ; do 
  $BINPATH/sflowtool -p 6343 -l | $BINPATH/sflow2hz -h 127.0.0.1 -p 9090 &> /dev/null 
  sleep 300
done )>&/dev/null  &
