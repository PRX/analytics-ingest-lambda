FROM lambci/lambda:build-nodejs10.x

MAINTAINER PRX <sysadmin@prx.org>
LABEL org.prx.lambda="true"

WORKDIR /app
EXPOSE 8080

ENTRYPOINT [ "yarn", "run" ]
CMD [ "test" ]

RUN yum install -y rsync && yum clean all && rm -rf /var/cache/yum
ADD yarn.lock ./
ADD package.json ./
RUN npm install --quiet --global yarn && yarn install

# download dbs (geolite, datacenters, domainthreats)
ADD ./bin ./bin
ARG S3_ACCESS_KEY_ID
ARG S3_SECRET_ACCESS_KEY
RUN AWS_ACCESS_KEY_ID=$S3_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY=$S3_SECRET_ACCESS_KEY yarn dbs

# finish building
ADD . .
RUN yarn build
