FROM node:24

WORKDIR /app

COPY package.json yarn.lock ./
RUN yarn install --frozen-lockfile

COPY syntheticbucketd.js ./

CMD ["node", "/app/syntheticbucketd.js", "200000", "1000"]
