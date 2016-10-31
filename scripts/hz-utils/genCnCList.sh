#!/bin/bash

# 
# C&C  list

URL="https://rules.emergingthreats.net/blockrules/emerging-botcc.rules"


TMPRULES=`mktemp -t rules.XXXXXXX` || exit 1

wget -q -O $TMPRULES https://rules.emergingthreats.net/blockrules/emerging-botcc.rules

cat $TMPRULES | grep -v "^#" | cut -d "[" -f2 | cut -d "]" -f1 | sed 's/,/\n/g'

rm -f $TMPRULES
