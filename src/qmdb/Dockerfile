# Clone the MillenniumDB repository as the first step
FROM alpine:3.18 AS clone
WORKDIR /mdb-src

RUN apk --no-cache add git
RUN git clone --depth 1 https://github.com/MillenniumDB/MillenniumDB.git .
RUN ls -l /mdb-src

# Build stage
FROM alpine:3.18 AS build
WORKDIR /mdb

# Install necessary build tools and dependencies
RUN apk --no-cache add cmake \
                       make \
                       g++ \
                       openssl-dev \
                       boost1.82-dev \
                       ncurses-dev \
                       icu-dev

# Use files from the cloned repository
COPY --from=clone /mdb-src/src src
COPY --from=clone /mdb-src/CMakeLists.txt CMakeLists.txt
COPY --from=clone /mdb-src/third_party/antlr4-runtime-4.13.1 third_party/antlr4-runtime-4.13.1

# Build MillenniumDB
RUN cmake -B build -D CMAKE_BUILD_TYPE=Release -D CMAKE_INSTALL_PREFIX=./ && \
    cmake --build build -j $(($(getconf _NPROCESSORS_ONLN)-1)) --target install

COPY --from=clone /mdb-src/browser browser

# Final minimal stage (to minimize image size)
FROM alpine:3.18 AS final
WORKDIR /data

# Install runtime dependencies
RUN apk --no-cache add libstdc++ \
                       libgcc \
                       openssl \
                       musl-locales \
                       libncursesw \
                       less \
                       bash \
                       icu-libs

# Copy the binaries and browser from the build stage
COPY --from=build /mdb/build/bin /usr/bin
COPY --from=build /mdb/browser /browser

# Set ownership to the user passed by UID and GID
ARG UID
ARG GID
RUN if [ "${UID:-}" != "" ] && [ "${GID:-}" != "" ]; then \
        chown -R ${UID}:${GID} /data; \
    fi

# Expose necessary volumes and environment variables
VOLUME /data
ENV MDB_BROWSER=/browser

# Default command to run in the container
CMD ["bash"]
