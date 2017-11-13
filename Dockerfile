FROM 	python:2-slim

RUN	apt-get update -y && apt-get upgrade -y && apt-get install -y \
		build-essential \
		libgrib-api-dev
		
RUN	pip install \
		numpy \
		pyproj

RUN	pip install \
		pygrib
