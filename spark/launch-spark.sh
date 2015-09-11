#!/bin/bash
#SPARK_HOME=/usr/local/spark
echo "starting spark..."
#cd $SPARK_HOME
spark-submit ${SCEP_HOME}/spark/spark_taxi.py localhost 9999 /opt/checkpoint_dir /opt/my_files/ranking1.txt /opt/my_files/ranking2.txt &> /opt/spark_streaming.log &
#child_pid=$!
#echo "[$0] waiting for (PID: $child_pid)"
#wait
nc -lk 9998 | nc -lk 9999 
exit
