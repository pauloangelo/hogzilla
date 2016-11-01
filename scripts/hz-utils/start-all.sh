#!/bin/bash

HADOOP_HOME=/home/hogzilla/hadoop
HBASE_HOME=/home/hogzilla/hbase

whoami | grep root > /dev/null
if [ $? -eq 0 ] ; then
   su - hogzilla -c "$HADOOP_HOME/sbin/start-dfs.sh"
   su - hogzilla -c "$HADOOP_HOME/sbin/start-yarn.sh"
   su - hogzilla -c "$HBASE_HOME/bin/start-hbase.sh"
   su - hogzilla -c "$HBASE_HOME/bin/hbase-daemon.sh start thrift"
   su - hogzilla -c "/home/hogzilla/bin/start-pigtail.sh"
   su - hogzilla -c "/home/hogzilla/bin/start-hogzilla.sh"
   su - hogzilla -c "/home/hogzilla/bin/start-sflow2hz.sh"
   su - hogzilla -c "/home/hogzilla/bin/start-dbupdates.sh"
else
   $HADOOP_HOME/sbin/start-dfs.sh
   $HADOOP_HOME/sbin/start-yarn.sh
   $HBASE_HOME/bin/start-hbase.sh
   $HBASE_HOME/bin/hbase-daemon.sh start thrift
   /home/hogzilla/bin/start-pigtail.sh   
   /home/hogzilla/bin/start-hogzilla.sh  
   /home/hogzilla/bin/start-sflow2hz.sh 
   /home/hogzilla/bin/start-dbupdates.sh 
fi
