FROM node:6
MAINTAINER Vianney Rancurel <vr@scality.com>

RUN apt-get update \
    && apt-get install -y jq netcat python git build-essential supervisor --no-install-recommends \
    && mkdir -p /var/log/supervisor \
    && mkdir -p /opt/backbeat

COPY . /opt/backbeat
WORKDIR /opt/backbeat
RUN npm install

COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf

CMD supervisord -c /etc/supervisor/supervisord.conf -n
