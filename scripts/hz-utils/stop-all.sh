#!/bin/bash

HADOOP_HOME=/home/hogzilla/hadoop
HBASE_HOME=/home/hogzilla/hbase

/home/hogzilla/bin/stop-pigtail.sh   
/home/hogzilla/bin/stop-hogzilla.sh  
/home/hogzilla/bin/stop-sflow2hz.sh 
/home/hogzilla/bin/stop-dbupdates.sh 

$HBASE_HOME/bin/hbase-daemon.sh stop thrift
$HBASE_HOME/bin/stop-hbase.sh
$HADOOP_HOME/sbin/stop-dfs.sh
$HADOOP_HOME/sbin/stop-yarn.sh
