FROM sequenceiq/spark:1.4.0
MAINTAINER Franco Maroni
ENV SCEP_HOME /opt/scep2015
RUN yum install -y \
	git \
	nc.x86_64 \
	vim
RUN  git clone https://github.com/MarconiQuattrocchi/scep2015.git /opt/scep2015
RUN mkdir /opt/checkpoint_dir
RUN mkdir /opt/my_files
EXPOSE 9999 9998
CMD [".${SCEP_HOME}/spark/launch-spark.sh"]
