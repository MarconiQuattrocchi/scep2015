#!/bin/bash
#SPARK_HOME=/usr/local/spark
echo "TURNING SAFE MODE OFF..."
./usr/local/hadoop/bin/hadoop dfsadmin -safemode leave
echo 'saving ip address on client script'
./opt/save-container-ip.sh
cd $SPARK_HOME && \
echo "START STANDALONE CLUSTER" && \
./sbin/start-all.sh && \
echo "launching Spark Taxi Application..." &&\
./bin/spark-submit \
--master spark://sandbox:7077 \
${SCEP_HOME}/spark/spark_taxi.py localhost 9999 /opt/checkpoint_dir /opt/my_files/ranking1.txt /opt/my_files/ranking2.txt 5 1 &> /opt/spark_streaming.log &
#child_pid=$!
#echo "[$0] waiting for (PID: $child_pid)"
#wait
nc -lk 9998 | nc -lk 9999 
