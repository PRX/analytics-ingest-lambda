FROM public.ecr.aws/lambda/nodejs:12

MAINTAINER PRX <sysadmin@prx.org>
LABEL org.prx.lambda="true"
LABEL org.prx.spire.publish.s3="LAMBDA_ZIP"

WORKDIR /app

ENTRYPOINT [ "yarn", "run" ]
CMD [ "test" ]

RUN yum install -y rsync zip tar xz && yum clean all && rm -rf /var/cache/yum
ADD yarn.lock ./
ADD package.json ./
RUN npm install --quiet --global yarn && yarn install

# download dbs (datacenters, domainthreats)
ADD ./bin ./bin
ARG S3_ACCESS_KEY_ID
ARG S3_SECRET_ACCESS_KEY
RUN AWS_ACCESS_KEY_ID=$S3_ACCESS_KEY_ID \
    AWS_SECRET_ACCESS_KEY=$S3_SECRET_ACCESS_KEY \
    yarn dbs

# finish building
ADD . .
RUN yarn build
