#!/bin/bash

old=$1
new=$2
digit=$3
if [ "$digit" == "" ]
then
  echo "Usage: copy-instance.sh <config-dir> <new-config-dir> <new-config-port-digit>"
  exit 1
fi

# Copy the directory
cp -r $old $new

# Change the port numbers in the new directory
sed -i -e "s/2.03/2${digit}03/; s/2.04/2${digit}04/;" $new/listeners.conf
sed -i -e "s/7.02/7${digit}02/;" $new/writer.conf
sed -i -e "s/7.22/7${digit}22/;" $new/management.conf
