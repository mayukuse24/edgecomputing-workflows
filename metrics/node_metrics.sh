#!/bin/bash
for itr in {1..10000000}
do

y=$(mpstat 5 1 | tail -n 1 | gawk '{print 100-$12}')
x=$(docker container ls -q | wc -l)
#y=$(top -bn2 -d .01| grep '^%Cpu'| tail -n 1 | gawk '{print i$2+$4+6}')
z=$(free | head -n 2 | tail -n 1 | gawk '{print $3}')
echo $itr","$x","$y","$z>>metrics.txt
sleep 5

done
