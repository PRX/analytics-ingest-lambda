FROM lambci/lambda:build-nodejs8.10

MAINTAINER PRX <sysadmin@prx.org>
LABEL org.prx.lambda="true"

WORKDIR /app
EXPOSE 8080

ENTRYPOINT [ "yarn", "run" ]
CMD [ "test" ]

ADD yarn.lock ./
ADD package.json ./
RUN npm install --quiet --global yarn && yarn install
ADD ./bin/getmaxmind.js ./bin/
ADD ./bin/getdatacenters.js ./bin/
RUN yarn geolite
RUN yarn datacenters
ADD . .
RUN yarn run build
