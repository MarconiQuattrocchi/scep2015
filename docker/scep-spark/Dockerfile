FROM sequenceiq/spark:1.4.0
MAINTAINER Franco Maroni
COPY ./scripts/launch-spark.sh /opt/launch-spark.sh
COPY ./scripts/launch-client.sh /opt/launch-client.sh
COPY ./scripts/save-container-ip.sh /opt/save-container-ip.sh
COPY ./conf/slaves ${SPARK_HOME}/conf/slaves
COPY ./conf/spark-env.sh ${SPARK_HOME}/conf/spark-env.sh
ENV SCEP_HOME /opt/scep2015
RUN yum install -y \
	git \
	nc.x86_64 \
	vim
RUN  git clone https://github.com/MarconiQuattrocchi/scep2015.git /opt/scep2015
RUN mkdir /opt/checkpoint_dir
RUN mkdir /opt/my_files
EXPOSE 9999 9998
CMD ["./opt/launch-spark.sh"]
