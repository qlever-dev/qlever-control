# Dockerfile
FROM docker.io/openlink/virtuoso-opensource-7:latest

# Build arguments for host UID/GID
ARG UID
ARG GID

# Ensure /opt is owned by user's UID/GID
RUN if [ "${UID:-}" != "" ] && [ "${GID:-}" != "" ]; then \
        chown -R ${UID}:${GID} /opt/virtuoso-opensource; \
    fi