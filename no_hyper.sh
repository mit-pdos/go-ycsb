#!/usr/bin/bash

echo WARNING: this only makes sense on one particular machine with 6 cores

for x in {1..5}; do
  # echo /sys/devices/system/cpu/cpu$x/online
  echo 1 > /sys/devices/system/cpu/cpu$x/online
done


for x in {6..11}; do
  # echo /sys/devices/system/cpu/cpu$x/online
  echo 0 > /sys/devices/system/cpu/cpu$x/online
done
