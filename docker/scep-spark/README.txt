#DOCKER BUILD IMAGE
docker build --no-cache=true -t spark-scep .

#RUN CONTAINER
docker run -it --name="roncioschifo" -p 8088:8088 -p 8042:8042 -p 9998:9998 -p 9999:9999 -h sandbox spark-scep

#OPEN BASH ON CONTAINER
docker exec -ti rincioschifo bash
