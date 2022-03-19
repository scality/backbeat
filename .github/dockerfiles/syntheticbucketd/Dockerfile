FROM node:10

RUN mkdir /app
COPY package.json /app
COPY yarn.lock /app
RUN cd /app && yarn install

COPY tests/utils /app

CMD ["node", "/app/syntheticbucketd.js", "200000", "1000"]
