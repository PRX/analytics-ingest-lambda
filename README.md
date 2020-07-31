# PRX Metrics Ingest

Lambda to process metrics data coming from one or more kinesis streams, and
send that data to multiple destinations.

# Description

The lambda subscribes to kinesis streams, containing metric event records. These
various metric records are either recognized by an input source in `lib/inputs`,
or ignored and logged as a warning at the end of the lambda execution.

Because of differences in (a) retry logic, or (b) VPC network access, this repo
is actually deployed as **4 different lambdas**, subscribed to one or more kinesis streams.

## BigQuery

Records with type `combined`/`postbytes` will be parsed
into BigQuery table formats, and inserted into their corresponding BigQuery
tables in parallel.  This is called [streaming inserts](https://cloud.google.com/bigquery/streaming-data-into-bigquery),
and in case the insert fails, it will be attempted 2 more times before the Lambda
fails with an error.  And since each insert includes a unique `insertId`, we
don't have any data consistency issues with re-running the inserts.

BigQuery now supports partitioning based on a [specific timestamp field](https://cloud.google.com/bigquery/docs/partitioned-tables#partitioned_tables),
so any inserts streamed to a table will be automatically moved to the correct
daily partition.

Records with the special type `postbytespreview`  will also be inserted into a
"preview" BigQuery table, for running both legacy and IAB 2.0 compliant inserts
in parallel for a program.

Records with type `pixel` will also be inserted into BigQuery, under the dataset/table
indicated in the `record.destination`. Since these destination tables are less
controlled, errors on insert (missing table, bad schema, etc) will be logged and
ignored.

## Pingbacks

Records with type `combined`/`postbytes` and a special `impression[].pings` array will be pinged via
an HTTP GET.  This "ping" does follow redirects, but expects to land on a 200
response afterwards.  Although 500 errors will be retried internally in the
code, any ping failures will be allowed to fail after error/timeout.

Unlike BigQuery, these operations are not idempotent, so we don't want to
over-ping a url.  All errors will be handled internally so Kinesis doesn't
attempt to re-exec the batch of records.

Note that Adzerk `impressionUrl`s now live in this pings array.

### URI Templates

Pingback urls should be valid [RFC 6570](https://tools.ietf.org/html/rfc6570) URI
template.  Valid parameters are:

| Parameter Name    | Description |
| ----------------- | ----------- |
| `ad`              | Adzerk ad id |
| `agent`           | Requester user-agent string |
| `agentmd5`        | An md5'd user-agent string |
| `episode`         | Feeder episode guid |
| `campaign`        | Adzerk campaign id |
| `creative`        | Adzerk creative id |
| `flight`          | Adzerk flight id |
| `ip`              | Request ip address |
| `ipmask`          | Masked ip, with the last octet changed to 0s |
| `listener`        | Unique string for this "listener" |
| `listenerepisode` | Unique string for "listener + url" |
| `podcast`         | Feeder podcast id |
| `randomstr`       | Random string |
| `randomint`       | Random integer |
| `referer`         | Requester http referer |
| `timestamp`       | Epoch milliseconds of request |
| `url`             | Full url of request, including host and query parameters, but _without_ the protocol `https://` |

## Redis

To give some semblance of live metrics, this lambda can also directly `INCR`
the Redis cache used by [castle.prx.org](https://github.com/PRX/castle.prx.org)
and [augury.prx.org](https://github.com/PRX/augury.prx.org).
This operates on type `combined`/`postbytes` records, and like Pingbacks, will
be allowed to fail without retry.

## DynamoDB

When a program in Dovetail is configured to be IAB complaint (with `"bytes": true`),
it will emit kinesis records of type `antebytes` or `antebytespreview`.  Meaning
the bytes haven't been downloaded yet.  These records are inserted into DynamoDB,
and saved until the CDN-bytes are actually downloaded.

This lambda also picks up type `bytes` and `segmentbytes` records, meaning that
the [dovetail-counts-lambda](https://github.com/PRX/dovetail-counts-lambda) has
decided enough of the segment/file-as-a-whole has been downloaded to be counted.
The original `antebytes` records are then retrieved from DynamoDB, and re-emitted
on kinesis with a modified type of `postbytes`.

These `postbytes` records are then processed by the previous 3 lambdas.  Or in
the case of `postbytespreview`, just inserted into BigQuery.  (We don't want to
run pingbacks or increment redis for `"bytes": "preview"` programs).

# Installation

To get started, first run `yarn`.  Then run `yarn dbs` to download the
[GeoLite2 City database](http://dev.maxmind.com/geoip/geoip2/geolite2/), remote
datacenter IP lists, and domain threat lists.

## Unit Tests

And hey, to just run the unit tests locally, you don't need anything!  Just
`yarn test` to your heart's content.

There are some dynamodb tests that use an actual table, and will be skipped.  To
also run these, set `TEST_DDB_TABLE` and `TEST_DDB_ROLE` to something in AWS you
have access to.

## Integration Tests

The integration test simply runs the lambda function against a test-event (the
same way you might in the lambda web console), and outputs the result.

Copy `env-example` to `.env`, and fill in your information. Now when you run
`yarn start`, you should see the test event run 4 times, and do some work for
all of the lambda functions.

## BigQuery

To enable BigQuery inserts, you'll need to first [create a Google Cloud Platform Project](https://cloud.google.com/resource-manager/docs/creating-managing-projects),
create a BigQuery dataset, and create the tables referenced by your `lib/inputs`.
Sorry -- no help on creating the correct table scheme yet!

Then [create a Service Account](https://developers.google.com/identity/protocols/OAuth2ServiceAccount#creatinganaccount) for this app.  Make sure it has BigQuery Data Editor permissions.

## DynamoDB

To enable DynamoDB gets/writes, you'll need to setup a [DynamoDB table](https://docs.aws.amazon.com/dynamodb/index.html#lang/en_us)
that your account has access to.  You can use your local AWS cli credentials, or
setup AWS client/secret environment variables.

You can also optionally access a DynamoDB table in a different account by specifying
a `DDB_ROLE` that the lambda should assume while doing gets/writes.

# Deployment

The 4 lambdas functions are deployed via a Cloudformation stack in the [Infrastructure repo](https://github.com/PRX/Infrastructure/blob/master/stacks/analytics-ingest-lambda.yml):

 - `AnalyticsBigqueryLambda` - insert downloads/impressions/pixels into BigQuery
 - `AnalyticsPingbacksLambda` - ping Adzerk impressions and 3rd-party pingbacks
 - `AnalyticsRedisLambda` - realtime Redis increments
 - `AnalyticsDynamodbLambda` - temporary store for IAB compliant downloads

# Docker

To get started, first make sure you have the MaxMind env vars set up as
these are needed for the docker build. Look for them in the staging
site using AWS 'SSM':

```
aws ssm get-parameter --with-decryption --name /prx/test/analytics-ingest-lambda/MAXMIND_LICENSE_KEY
```

This repo is now dockerized! You'll need some read-only S3 credentials in your
`.env` file for the `bin/getdatacenters.js` script to succeed during build:

```
docker-compose build
docker-compose run test
docker-compose run start
```

And you can easily-ish get the lambda zip built by the Dockerfile:

```
docker ps -a | grep analyticsingestlambda
docker cp {{container-id-here}}:/app/build.zip myzipfile.zip
unzip -l myzipfile.zip
```

# License

[AGPL License](https://www.gnu.org/licenses/agpl-3.0.html)
