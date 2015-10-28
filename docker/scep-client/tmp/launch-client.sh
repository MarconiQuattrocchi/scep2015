#!/bin/bash
docker run -e HOST_IP='172.17.0.164' -e HOST_PORT='9998' -e DATASET_PATH='../dataset/little-2000.csv' scep-client
