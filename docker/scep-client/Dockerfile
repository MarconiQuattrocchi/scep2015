FROM python:2.7.10
MAINTAINER Giovanni Quattrocchi
COPY ../dataset/little-2000.csv /usr/dataset/little-2000.csv
COPY ../dataset/little-200000.csv /usr/dataset/little-200000.csv
COPY ../client/client.py /usr/src/client.py
COPY ../client/client.conf /usr/src/client.conf
WORKDIR /usr/src/
CMD ["python", "/usr/src/client.py"]
