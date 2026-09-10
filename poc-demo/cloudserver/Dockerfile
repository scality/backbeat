# Fallback CloudServer image, built from a clone rather than pulled.
#
# The clone is passed in as a named build context so this Dockerfile can
# live here, next to the compose file, rather than inside the clone:
#
#   additional_contexts:
#     src: ${CLOUDSERVER_DIR}
#
# It is the slower path. Prefer the published image; use this only when no
# tag pulls for the machine's architecture. The clone must already have its
# dependencies installed, because this does not run a package manager.
FROM node:22-bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/app
COPY --from=src . /usr/src/app
RUN mkdir -p /usr/src/app/localData /usr/src/app/localMetadata

EXPOSE 8000 8002
CMD ["node", "index.js"]
