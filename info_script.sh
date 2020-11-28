#!/bin/bash
x=$(sudo docker container ls -q | wc -l)
y=$(top -bn2 -d .01| grep '^%Cpu'| tail -n 1 | gawk '{print i$2+$4+6}')
z=$(free | head -n 2 | tail -n 1 | gawk '{print $3}')
echo $x","$y","$z>>metrics.txt
