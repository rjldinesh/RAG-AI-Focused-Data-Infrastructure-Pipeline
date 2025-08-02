#FROM apache/airflow:2.9.1
FROM apache/airflow:3.0.2

# Switch to root
USER root

# Install Java 17 and wget
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk wget && \
    ln -s /usr/lib/jvm/java-17-openjdk-amd64 /usr/lib/jvm/java-1.17.0-openjdk-amd64 || true && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install Spark manually (adjust version if needed)
ENV SPARK_VERSION=3.5.3
RUN wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    tar xvf spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop3 /opt/spark && \
    rm spark-${SPARK_VERSION}-bin-hadoop3.tgz

# Set environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV SPARK_HOME=/opt/spark
ENV PATH="${JAVA_HOME}/bin:${SPARK_HOME}/bin:${PATH}"


# Switch back to airflow user
USER airflow

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

