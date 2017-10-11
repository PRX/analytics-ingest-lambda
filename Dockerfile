FROM lambci/lambda:build-nodejs6.10

MAINTAINER PRX <sysadmin@prx.org>
LABEL org.prx.lambda="/app/build.zip"

WORKDIR /app
EXPOSE 8080

ENTRYPOINT [ "npm", "run" ]
CMD [ "test" ]

ADD package.json .
RUN npm install
ADD ./bin/getmaxmind.js ./bin/
RUN npm run geolite
ADD . .
RUN npm run build
