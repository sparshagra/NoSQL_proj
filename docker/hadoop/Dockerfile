FROM eclipse-temurin:8-jdk

ENV JAVA_HOME=/opt/java/openjdk
ENV HADOOP_VERSION=2.10.2
ENV PIG_VERSION=0.17.0
ENV HADOOP_HOME=/opt/hadoop
ENV PIG_HOME=/opt/pig
ENV HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
ENV PATH=/opt/hadoop/bin:/opt/hadoop/sbin:/opt/pig/bin:$PATH

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    procps \
    netcat-openbsd \
    python3 \
    && rm -rf /var/lib/apt/lists/*

# Install Hadoop 2.10.2
RUN curl -fL https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz \
    -o /tmp/hadoop.tar.gz \
    && tar -xzf /tmp/hadoop.tar.gz -C /opt \
    && mv /opt/hadoop-${HADOOP_VERSION} ${HADOOP_HOME} \
    && rm /tmp/hadoop.tar.gz

# Install Pig 0.17.0
RUN curl -fL https://archive.apache.org/dist/pig/pig-${PIG_VERSION}/pig-${PIG_VERSION}.tar.gz \
    -o /tmp/pig.tar.gz \
    && tar -xzf /tmp/pig.tar.gz -C /opt \
    && mv /opt/pig-${PIG_VERSION} ${PIG_HOME} \
    && rm /tmp/pig.tar.gz

# Copy Hadoop configurations
COPY core-site.xml ${HADOOP_CONF_DIR}/core-site.xml
COPY hdfs-site.xml ${HADOOP_CONF_DIR}/hdfs-site.xml
COPY mapred-site.xml ${HADOOP_CONF_DIR}/mapred-site.xml
COPY yarn-site.xml ${HADOOP_CONF_DIR}/yarn-site.xml

RUN mkdir -p /hadoop/dfs/name /hadoop/dfs/data /hadoop/tmp \
    && mkdir -p /workspace

WORKDIR /workspace

EXPOSE 9000 9870 8088 8042 19888

CMD ["bash"]

# Add Hadoop startup script
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
