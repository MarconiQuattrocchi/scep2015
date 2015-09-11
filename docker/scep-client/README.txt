# build the client 
docker build --no-cache=true -t scep-client .
# run the client
docker run -e HOST_IP='131.175.135.184' -e HOST_PORT='4343' -e DATASET_PATH='../dataset/little-2000.csv' scep-client