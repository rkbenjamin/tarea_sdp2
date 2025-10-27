FROM flink:1.18

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 python3-pip && \
    ln -sf /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*

RUN python3 -m pip install --no-cache-dir apache-flink==1.18.1 requests \
 || python3 -m pip install --no-cache-dir apache-flink==1.18.0 requests

# Con contexto raíz, los jars están en ./flink-runtime/flink_lib/
COPY ./flink-runtime/flink_lib/*.jar /opt/flink/lib/

WORKDIR /opt/job
# Con contexto raíz, tu main está en ./flink_job/main.py
COPY ./flink_job/main.py /opt/job/main.py

USER flink