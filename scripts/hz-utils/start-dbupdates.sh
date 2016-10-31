#!/bin/bash

SOURCE="${BASH_SOURCE[0]}"

echo $SOURCE

exit

./genCnCList.sh > /tmp/cclist.temp
php updateReputationList.php -t blacklist -n CCBotNet -f /tmp/cclist.temp

for os in windows linux freebsd android apple ; do
  ./getReposList.sh $os > /tmp/$os.txt
  php updateReputationList.php -t $os -n OSRepo -f /tmp/$os.txt
done
