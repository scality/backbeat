FROM node:16

RUN mkdir /app
COPY * /app
RUN cd /app && yarn install --network-concurrency 1

CMD ["node", "/app/syntheticbucketd.js", "200000", "1000"]
