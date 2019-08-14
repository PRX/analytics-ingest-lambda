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

# download geolite database
ADD ./bin/getmaxmind.js ./bin/
RUN yarn geolite

# download datacenter ip lists
ARG S3_ACCESS_KEY_ID
ARG S3_SECRET_ACCESS_KEY
ADD ./bin/getdatacenters.js ./bin/
RUN AWS_ACCESS_KEY_ID=$S3_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY=$S3_SECRET_ACCESS_KEY yarn datacenters

# finish building
ADD . .
RUN yarn build
