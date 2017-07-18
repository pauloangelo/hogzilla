#!/bin/bash

# Confira as variaveis abaixo
HBASE_PATH=/home/hogzilla/hbase
HBASE_VERSION="1.2.6"

# Needed by the AuthModule
HOGDIR="/home/hogzilla/hogzilla"
EXTRAJAR=",$HOGDIR/jars/uap-scala_2.10-0.2.1-SNAPSHOT.jar,$HOGDIR/jars/snakeyaml-1.18.jar"
FILES="--files $HOGDIR/conf/sflow.conf"



(while : ; do 
     #cd /home/hogzilla
     /home/hogzilla/spark/bin/spark-submit \
     --master yarn-cluster \
         --num-executors 2 \
         --driver-memory 1g \
         --executor-memory 3g \
         --executor-cores 4 \
     --jars $HBASE_PATH/lib/hbase-annotations-$HBASE_VERSION.jar,$HBASE_PATH/lib/hbase-annotations-$HBASE_VERSION-tests.jar,$HBASE_PATH/lib/hbase-client-$HBASE_VERSION.jar,$HBASE_PATH/lib/hbase-common-$HBASE_VERSION.jar,$HBASE_PATH/lib/hbase-common-$HBASE_VERSION-tests.jar,$HBASE_PATH/lib/hbase-examples-$HBASE_VERSION.jar,$HBASE_PATH/lib/hbase-hadoop2-compat-$HBASE_VERSION.jar,$HBASE_PATH/lib/hbase-hadoop-compat-$HBASE_VERSION.jar,$HBASE_PATH/lib/hbase-it-$HBASE_VERSION.jar,$HBASE_PATH/lib/hbase-it-$HBASE_VERSION-tests.jar,$HBASE_PATH/lib/hbase-prefix-tree-$HBASE_VERSION.jar,$HBASE_PATH/lib/hbase-procedure-$HBASE_VERSION.jar,$HBASE_PATH/lib/hbase-protocol-$HBASE_VERSION.jar,$HBASE_PATH/lib/hbase-rest-$HBASE_VERSION.jar,$HBASE_PATH/lib/hbase-server-$HBASE_VERSION.jar,$HBASE_PATH/lib/hbase-server-$HBASE_VERSION-tests.jar,$HBASE_PATH/lib/hbase-shell-$HBASE_VERSION.jar,$HBASE_PATH/lib/hbase-thrift-$HBASE_VERSION.jar,$HBASE_PATH/lib/htrace-core-3.1.0-incubating.jar,$HBASE_PATH/lib/guava-12.0.1.jar,$HBASE_PATH/lib/metrics-core-2.2.0.jar$EXTRAJAR --driver-class-path ./$HBASE_PATH/conf/ $FILES --class Hogzilla  /home/hogzilla/Hogzilla.jar  &> /tmp/hogzilla.log  &

sleep 21600 # 6h

#rm -rf /tmp/hadoop-hogzilla*

done) &

