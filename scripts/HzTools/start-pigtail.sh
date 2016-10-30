#!/bin/bash

(
cd /home/hogzilla/pigtail/
while : ; do 
   php /home/hogzilla/pigtail/pigtail.php
   sleep 10
done
)&
