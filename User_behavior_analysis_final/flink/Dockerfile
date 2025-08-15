FROM flink:1.17

# Cài Python + pip
RUN apt-get update && apt-get install -y python3 python3-pip netcat-openbsd && \
    ln -s /usr/bin/python3 /usr/bin/python


# Cài PyFlink
RUN pip3 install apache-flink==1.17

# Tạo thư mục chứa JARs
RUN mkdir -p /opt/flink/jars

# Tải các JAR cần thiết cho Flink + Kafka
RUN curl -o /opt/flink/jars/flink-connector-kafka-1.17.0.jar https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/1.17.0/flink-connector-kafka-1.17.0.jar && \
    curl -o /opt/flink/jars/kafka-clients-3.3.1.jar https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.3.1/kafka-clients-3.3.1.jar && \
    curl -o /opt/flink/jars/slf4j-api-1.7.30.jar https://repo1.maven.org/maven2/org/slf4j/slf4j-api/1.7.30/slf4j-api-1.7.30.jar && \
    curl -o /opt/flink/jars/flink-streaming-java-1.17.0.jar https://repo1.maven.org/maven2/org/apache/flink/flink-streaming-java/1.17.0/flink-streaming-java-1.17.0.jar && \
    curl -o /opt/flink/jars/flink-connector-base-1.17.0.jar https://repo1.maven.org/maven2/org/apache/flink/flink-connector-base/1.17.0/flink-connector-base-1.17.0.jar && \
    curl -o /opt/flink/jars/commons-logging-1.2.jar https://repo1.maven.org/maven2/commons-logging/commons-logging/1.2/commons-logging-1.2.jar && \
    curl -o /opt/flink/jars/log4j-api-2.17.1.jar https://repo1.maven.org/maven2/org/apache/logging/log4j/log4j-api/2.17.1/log4j-api-2.17.1.jar

# Sao chép mã job Flink + script đợi Kafka
COPY flink_job.py /opt/flink/flink_job.py
COPY wait-for-kafka.sh /opt/flink/wait-for-kafka.sh

# Cấp quyền chạy cho shell script
RUN chmod +x /opt/flink/wait-for-kafka.sh

# Khi container chạy → chạy script chờ Kafka rồi mới chạy job
CMD ["/opt/flink/wait-for-kafka.sh"]
