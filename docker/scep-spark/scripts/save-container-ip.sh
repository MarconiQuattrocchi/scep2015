#!/bin/bash
a="$(ifconfig | grep 'inet addr:' | cut -d':' -f 2 | cut -d ' ' -f 1 | head -n 1)"
sed -i "s/####/$a/g" /opt/launch-client.sh
echo $a 
