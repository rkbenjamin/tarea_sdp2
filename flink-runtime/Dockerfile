FROM flink:1.18

USER root
RUN apt-get update && apt-get install -y --no-install-recommends python3 python3-pip \
 && pip3 install --no-cache-dir "apache-flink==1.18.*" requests \
 && rm -rf /var/lib/apt/lists/*

COPY flink_lib/*.jar /opt/flink/lib/

ENV PYFLINK_PYTHON=/usr/bin/python3
ENV PYFLINK_CLIENT_EXECUTABLE=/usr/bin/python3

USER flink