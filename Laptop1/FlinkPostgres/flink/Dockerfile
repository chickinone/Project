FROM apache/flink:1.17

USER root

RUN apt-get update && apt-get install -y python3 python3-pip && \
    ln -s /usr/bin/python3 /usr/bin/python

COPY flink_pkgs/requirements.txt /opt/flink/flink_pkgs/requirements.txt
RUN pip install --no-cache-dir -r /opt/flink/flink_pkgs/requirements.txt

COPY flink_job.py /opt/flink/
COPY pyflink-conf.yaml /opt/flink/conf/
COPY usrlib/flink-connector-kafka-1.17.0.jar /opt/flink/usrlib/

WORKDIR /opt/flink
CMD ["python", "flink_job.py"]