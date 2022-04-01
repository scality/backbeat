FROM node:16-slim

WORKDIR /usr/src/app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    ca-certificates \
    build-essential \
    wget \
    bash \
    python3 \
    git \
    jq \
    zlib1g-dev \
    libncurses5-dev \
    libgdbm-dev \
    libnss3-dev \
    libssl-dev \
    libreadline-dev \
    libffi-dev \
    libzstd-dev

ENV DOCKERIZE_VERSION v0.6.1
ENV PYTHON=python3.9
ENV PY_VERSION=3.9.7


RUN wget https://github.com/jwilder/dockerize/releases/download/$DOCKERIZE_VERSION/dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz \
    && tar -C /usr/local/bin -xzvf dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz \
    && rm dockerize-linux-amd64-$DOCKERIZE_VERSION.tar.gz \
    && wget https://www.python.org/ftp/python/$PY_VERSION/Python-$PY_VERSION.tgz \
    && tar -C /usr/local/bin -xzvf Python-$PY_VERSION.tgz \
    && cd /usr/local/bin/Python-$PY_VERSION \
    && ./configure \
    && make \
    && make altinstall


COPY package.json yarn.lock /usr/src/app/
RUN yarn install --ignore-engines --frozen-lockfile --production \
    && rm -rf /var/lib/apt/lists/* \
    && rm -rf ~/.node-gyp \
    && rm -rf /tmp/yarn-*

# Keep the .git directory in order to properly report version
COPY . /usr/src/app

ENTRYPOINT ["/usr/src/app/docker-entrypoint.sh"]

EXPOSE 8900
