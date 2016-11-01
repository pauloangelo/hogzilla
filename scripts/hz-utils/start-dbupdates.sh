#!/bin/bash

BIN="/home/hogzilla/bin"

cd $BIN

(
while : ; do
    $BIN/genCnCList.sh > /tmp/cclist.temp
    php $BIN/updateReputationList.php -t blacklist -n CCBotNet -f /tmp/cclist.temp
    rm /tmp/cclist.temp
    
    for os in windows linux freebsd android apple ; do
      $BIN/getReposList.sh $os > /tmp/$os.txt
      php $BIN/updateReputationList.php -t $os -n OSRepo -f /tmp/$os.txt
      rm -f /tmp/$os.txt
    done

    sleep 86400 # daily
done
)&
