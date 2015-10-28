#!/bin/bash
docker run -e HOST_IP='####' -e HOST_PORT='9998' -e DATASET_PATH='../dataset/little-2000.csv' scep-client
