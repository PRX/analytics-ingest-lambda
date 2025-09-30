FROM public.ecr.aws/lambda/nodejs:22

MAINTAINER PRX <sysadmin@prx.org>
LABEL org.prx.lambda="true"
LABEL org.prx.spire.publish.s3="LAMBDA_ZIP"

WORKDIR /app

ENTRYPOINT [ "yarn", "run" ]
CMD [ "test" ]

RUN yum install -y rsync zip tar xz && yum clean all && rm -rf /var/cache/yum
ADD yarn.lock ./
ADD package.json ./
RUN yarn install

# finish building
ADD . .
RUN yarn build
