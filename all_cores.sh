#!/bin/bash

for x in /sys/devices/system/cpu/cpu*/online; do
  echo 1 >"$x"
done
