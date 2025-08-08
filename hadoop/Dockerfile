# Base image
FROM ubuntu:20.04

# Environments
ENV HADOOP_VERSION=3.4.1
ENV HADOOP_HOME=/opt/hadoop
ENV PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop

# Install dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    wget \
    openjdk-11-jdk-headless \
    ssh \
    rsync && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Download and extract Hadoop
RUN wget https://dlcdn.apache.org/hadoop/common/stable/hadoop-${HADOOP_VERSION}.tar.gz && \
    tar -xzf hadoop-${HADOOP_VERSION}.tar.gz -C /opt/ && \
    mv /opt/hadoop-${HADOOP_VERSION} ${HADOOP_HOME} && \
    rm hadoop-${HADOOP_VERSION}.tar.gz

RUN mkdir -p ${HADOOP_HOME}/data/dfs/namenode && \
    mkdir -p ${HADOOP_HOME}/data/dfs/datanode && \
    mkdir -p ${HADOOP_HOME}/data/tmp && \
    mkdir -p ${HADOOP_HOME}/logs

# Create Hadoop users and groups
RUN groupadd --system hadoop && \
    useradd --system --create-home --gid hadoop hadoopuser

# Set Hadoop user environment variables
ENV HDFS_NAMENODE_USER=hadoopuser
ENV HDFS_DATANODE_USER=hadoopuser
ENV HDFS_SECONDARYNAMENODE_USER=hadoopuser
ENV YARN_RESOURCEMANAGER_USER=hadoopuser
ENV YARN_NODEMANAGER_USER=hadoopuser

# Append JAVA_HOME to hadoop-env.sh
RUN echo "export JAVA_HOME=${JAVA_HOME}" >> /opt/hadoop/etc/hadoop/hadoop-env.sh

# Configure SSH for passwordless access
RUN mkdir -p /home/hadoopuser/.ssh && \
    chown -R hadoopuser:hadoop /home/hadoopuser/.ssh && \
    chmod 700 /home/hadoopuser/.ssh && \
    ssh-keygen -t rsa -P '' -f /home/hadoopuser/.ssh/id_rsa && \
    cat /home/hadoopuser/.ssh/id_rsa.pub >> /home/hadoopuser/.ssh/authorized_keys && \
    chown -R hadoopuser:hadoop /home/hadoopuser/.ssh && \
    chmod 600 /home/hadoopuser/.ssh/authorized_keys

# Copy settings
COPY core-site.xml ${HADOOP_CONF_DIR}/core-site.xml
COPY hdfs-site.xml ${HADOOP_CONF_DIR}/hdfs-site.xml

# Create data directories and set permissions
RUN chown -R hadoopuser:hadoop ${HADOOP_HOME}/data ${HADOOP_HOME}/logs

# Entrypoint script
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

RUN mkdir -p /run/sshd && \
    chown -R hadoopuser:hadoop /run/sshd && \
    chmod 0755 /run/sshd

RUN chown -R hadoopuser:hadoop /etc/ssh 

# Switch to custom user
USER hadoopuser

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]

# Expose necessary ports.
EXPOSE 9000 8088 50070 8042