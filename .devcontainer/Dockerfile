# Use a specific Ubuntu version
FROM ubuntu:22.04

# Set environment variables for non-interactive apt
ENV DEBIAN_FRONTEND=noninteractive

# Install necessary tools and dependencies
RUN apt-get update && apt-get install -y \
    curl \
    gnupg \
    software-properties-common \
    zip \
    unzip \
    git \
    sudo \
    openjdk-11-jdk \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME to the installed JDK location
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# Install Azure CLI
RUN curl -sL https://aka.ms/InstallAzureCLIDeb | bash

# Install Scala
RUN apt-get update && \
    apt-get install -y wget gnupg && \
    wget https://downloads.lightbend.com/scala/2.13.12/scala-2.13.12.deb && \
    dpkg -i scala-2.13.12.deb && \
    apt-get install -f && \
    rm scala-2.13.12.deb

# Define user variables
ARG USERNAME=vscode
ARG USER_UID=2000
ARG USER_GID=2000

# Create the vscode group and user if they don't already exist
RUN if ! getent group $USER_GID > /dev/null 2>&1; then \
    groupadd --gid $USER_GID $USERNAME; \
    fi && \
    if ! id -u $USER_UID > /dev/null 2>&1; then \
    useradd --uid $USER_UID --gid $USER_GID -m -s /bin/bash $USERNAME; \
    fi && \
    echo "$USERNAME ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers.d/$USERNAME && \
    chmod 0440 /etc/sudoers.d/$USERNAME

# Ensure the home directory is owned by the vscode user
RUN chown -R $USERNAME:$USERNAME /home/$USERNAME

# Set the HOME environment variable for the vscode user
ENV HOME=/home/$USERNAME

# Install Apache Spark
ENV SPARK_VERSION=3.4.1
ENV HADOOP_VERSION=3
RUN wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar -xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

# Set environment variables
ENV PATH="/opt/spark/bin:${PATH}"
ENV SCALA_HOME="/usr/share/scala"
ENV PATH="${SCALA_HOME}/bin:${PATH}"

# Install sbt
RUN echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | tee /etc/apt/sources.list.d/sbt.list && \
    curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x99e82a75642ac823" | apt-key add && \
    apt-get update && \
    apt-get install -y sbt

# Switch to the vscode user
USER $USERNAME

COPY ./scripts/setup.sh /home/$USERNAME/setup.sh