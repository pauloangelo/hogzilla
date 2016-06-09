#!/bin/bash

FILE=$1


#TTalker|whitelist|Big Talker|10.1.2.226

cat $FILE | while read line ; do
   list=`echo $line | cut -d'|' -f1`
   listType=`echo $line | cut -d'|' -f2`
   description=`echo $line | cut -d'|' -f3`
   ip=`echo $line | cut -d'|' -f4`

cat << EOF
put 'hogzilla_reputation', '$ip', 'rep:description', '$description'
put 'hogzilla_reputation', '$ip', 'rep:ip', '$ip'
put 'hogzilla_reputation', '$ip', 'rep:list', '$list'
put 'hogzilla_reputation', '$ip', 'rep:list_type', '$listType'

EOF
done
