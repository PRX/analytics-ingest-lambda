FROM public.ecr.aws/lambda/nodejs:22

MAINTAINER PRX <sysadmin@prx.org>
LABEL org.prx.lambda="true"
LABEL org.prx.spire.publish.s3="LAMBDA_ZIP"

WORKDIR /app

ENTRYPOINT [ "yarn", "run" ]
CMD [ "test" ]

RUN dnf install -y rsync zip && dnf clean all
ADD yarn.lock ./
ADD package.json ./
RUN npm install --quiet --global yarn && yarn install

# finish building
ADD . .
RUN yarn build
