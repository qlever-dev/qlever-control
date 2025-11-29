# Use an official OpenJDK runtime as a parent image
FROM docker.io/library/openjdk:21-jdk-slim

RUN apt-get update && apt-get install -y wget gzip coreutils curl unzip

WORKDIR /opt

# Download and set up Blazegraph
RUN wget https://github.com/blazegraph/database/releases/download/BLAZEGRAPH_2_1_6_RC/blazegraph.jar

# Set ownership to the user passed by UID and GID
ARG UID
ARG GID
RUN if [ "${UID:-}" != "" ] && [ "${GID:-}" != "" ]; then \
        chown -R ${UID}:${GID} /opt; \
    fi

RUN chmod u+x blazegraph.jar

# Expose Blazegraph port
# EXPOSE 9999

CMD ["bash"]
