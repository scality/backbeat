FROM node:22

RUN mkdir /app
RUN npm install -g \
    typescript@4.9.5
COPY package.json /app
COPY yarn.lock /app
RUN cd /app && yarn install --network-concurrency 1

COPY tests/utils /app

CMD ["node", "/app/syntheticbucketd.js", "200000", "1000"]
