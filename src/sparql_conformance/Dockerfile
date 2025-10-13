FROM node:18
ARG UID
ARG GID

RUN set -eux; \
    if getent group "${GID}" >/dev/null; then \
      echo "Using existing group with GID ${GID}"; \
    else \
      groupadd -g "${GID}" appgroup; \
    fi; \
    if getent passwd "${UID}" >/dev/null; then \
      echo "Using existing user with UID ${UID}"; \
    else \
      useradd -m -u "${UID}" -g "${GID}" appuser; \
    fi

WORKDIR /app

RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*

RUN git clone https://github.com/SIRDNARch/qlever-conformance-website.git .

RUN mkdir -p /public/results

RUN npm install

EXPOSE 3000

USER ${UID}:${GID}

CMD [ "node", "server.js" ]
