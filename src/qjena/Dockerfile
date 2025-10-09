# Use an official OpenJDK runtime as a parent image
FROM docker.io/library/openjdk:21-jdk-slim

RUN apt-get update && apt-get install -y wget unzip jq

WORKDIR /opt

# Download and extract Apache Jena Fuseki and Apache Jena using the latest version dynamically
RUN LATEST_VERSION=$(wget -qO- https://dlcdn.apache.org/jena/binaries/ \
    | grep -oP 'apache-jena-\K[0-9]+\.[0-9]+\.[0-9]+' \
    | sort -V \
    | tail -n 1) \
    && wget https://dlcdn.apache.org/jena/binaries/apache-jena-fuseki-${LATEST_VERSION}.zip \
    && unzip apache-jena-fuseki-${LATEST_VERSION}.zip \
    && rm -f apache-jena-fuseki-${LATEST_VERSION}.zip \
    && wget https://dlcdn.apache.org/jena/binaries/apache-jena-${LATEST_VERSION}.zip \
    && unzip apache-jena-${LATEST_VERSION}.zip \
    && rm -f apache-jena-${LATEST_VERSION}.zip \
    && mv apache-jena-${LATEST_VERSION} /opt/apache-jena \
    && mv apache-jena-fuseki-${LATEST_VERSION} /opt/apache-jena-fuseki

# Set ownership to the user passed by UID and GID
ARG UID
ARG GID
RUN if [ "${UID:-}" != "" ] && [ "${GID:-}" != "" ]; then \
        chown -R ${UID}:${GID} /opt; \
    fi

# Ensure the bin and fuseki folders are in PATH
ENV JENA_HOME="/opt/apache-jena"
ENV FUSEKI_HOME="/opt/apache-jena-fuseki"
ENV PATH="${JENA_HOME}/bin:${FUSEKI_HOME}:${PATH}"

# Make sure scripts are executable
RUN chmod +x /opt/apache-jena/bin/*

RUN chmod +x /opt/apache-jena-fuseki/fuseki-server.jar

# Set entrypoint
CMD ["bash"]
