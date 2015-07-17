#!/bin/bash
cd /home/francesco/Desktop/spark-1.4.0 && ./bin/spark-submit my_files/spark_taxi.py localhost 9999 ~/Desktop/spark-Checkpoint/ my_files/pinella_out.txt my_files/ranking.txt &> delete.log
