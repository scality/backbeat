FROM node:16

RUN mkdir /app
COPY package.json /app
COPY yarn.lock /app
RUN cd /app && yarn install

COPY tests/utils /app

CMD ["node", "/app/syntheticbucketd.js", "36000000", "1000", "extraaccounts.json"]
