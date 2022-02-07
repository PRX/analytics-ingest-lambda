FROM public.ecr.aws/lambda/nodejs:12

MAINTAINER PRX <sysadmin@prx.org>
LABEL org.prx.lambda="true"

WORKDIR /app

ENTRYPOINT [ "yarn", "run" ]
CMD [ "test" ]

RUN yum install -y rsync zip tar xz && yum clean all && rm -rf /var/cache/yum
ADD yarn.lock ./
ADD package.json ./
RUN npm install --quiet --global yarn && yarn install

# download dbs (geolite, datacenters, domainthreats)
ADD ./bin ./bin
ARG MAXMIND_LICENSE_KEY
ARG S3_ACCESS_KEY_ID
ARG S3_SECRET_ACCESS_KEY
RUN MAXMIND_LICENSE_KEY=$MAXMIND_LICENSE_KEY \
    AWS_ACCESS_KEY_ID=$S3_ACCESS_KEY_ID \
    AWS_SECRET_ACCESS_KEY=$S3_SECRET_ACCESS_KEY \
    yarn dbs

# finish building
ADD . .
RUN yarn build
