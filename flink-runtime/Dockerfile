FROM flink:1.18

USER root
RUN apt-get update && apt-get install -y --no-install-recommends python3 python3-pip \
 && pip3 install --no-cache-dir "apache-flink==1.18.*" requests \
 && rm -rf /var/lib/apt/lists/*

# Conectores (ya los tienes en tu repo)
COPY flink_lib/*.jar /opt/flink/lib/

# Sugerencia: fija el intÃ©rprete de PyFlink
ENV PYFLINK_PYTHON=/usr/bin/python3
ENV PYFLINK_CLIENT_EXECUTABLE=/usr/bin/python3

USER flink