FROM 	python:2-slim

RUN	apt-get update -y && apt-get upgrade -y && apt-get install -y \
		build-essential \
		libgdal-dev 

ENV	CPLUS_INCLUDE_PATH=/usr/include/gdal
ENV	C_INCLUDE_PATH=/usr/include/gdal

#RUN	pip install  GDAL
