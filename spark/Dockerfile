FROM apache/spark:3.5.5-scala2.12-java17-python3-ubuntu
ENV SPARK_VERSION=3.5.5
WORKDIR /opt/spark/work-dir/

COPY entrypoint.sh /opt/spark/work-dir/entrypoint.sh
COPY requirements.txt /opt/spark/work-dir/requirements.txt

USER root
RUN chmod +x /opt/spark/work-dir/entrypoint.sh

RUN pip3 install --no-cache-dir -r /opt/spark/work-dir/requirements.txt && rm -rf /root/.cache/pip

USER spark

# Log directory for history server
RUN mkdir -p /opt/spark/spark-events

# Path to store local data in Spark
RUN mkdir -p /opt/spark/tmp

ENTRYPOINT ["/opt/spark/work-dir/entrypoint.sh"]