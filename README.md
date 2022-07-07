# PRX Metrics Ingest

Lambda to process metrics data coming from one or more kinesis streams, and
send that data to multiple destinations.

# Description

The lambda subscribes to kinesis streams, containing metric event records. These
various metric records are either recognized by an input source in `lib/inputs`,
or ignored and logged as a warning at the end of the lambda execution.

Because of differences in retry logic, this repo
is actually deployed as **3 different lambdas**, subscribed to one or more kinesis streams.

## BigQuery

Records with type `postbytes` will be parsed
into BigQuery table formats, and inserted into their corresponding BigQuery
tables in parallel. This is called [streaming inserts](https://cloud.google.com/bigquery/streaming-data-into-bigquery),
and in case the insert fails, it will be attempted 2 more times before the Lambda
fails with an error. And since each insert includes a unique `insertId`, we
don't have any data consistency issues with re-running the inserts.

BigQuery now supports partitioning based on a [specific timestamp field](https://cloud.google.com/bigquery/docs/partitioned-tables#partitioned_tables),
so any inserts streamed to a table will be automatically moved to the correct
daily partition.

## Pingbacks

Records with type `postbytes` and an `impressions[]` array will POST those
impressions count to the [Dovetail Router](https://github.com/PRX/dovetail-router.prx.org)
Flight Increments API, at `/api/v1/flight_increments/:date`. This gives some
semblance of live flight-impression counts so we can stop serving flights as
close to their goals as possible.

Additionally, records with a special `impression[].pings` array will be pinged via
an HTTP GET. This "ping" does follow redirects, but expects to land on a 200
response afterwards. Although 500 errors will be retried internally in the
code, any ping failures will be allowed to fail after error/timeout.

Unlike BigQuery, these operations are not idempotent, so we don't want to
over-ping a url. All errors will be handled internally so Kinesis doesn't
attempt to re-exec the batch of records.

### URI Templates

Pingback urls should be valid [RFC 6570](https://tools.ietf.org/html/rfc6570) URI
template. Valid parameters are:

| Parameter Name    | Description                                                                                     |
| ----------------- | ----------------------------------------------------------------------------------------------- |
| `ad`              | Ad id (intersection of creative and flight)                                                     |
| `agent`           | Requester user-agent string                                                                     |
| `agentmd5`        | An md5'd user-agent string                                                                      |
| `episode`         | Feeder episode guid                                                                             |
| `campaign`        | Campaign id                                                                                     |
| `creative`        | Creative id                                                                                     |
| `flight`          | Flight id                                                                                       |
| `ip`              | Request ip address                                                                              |
| `ipmask`          | Masked ip, with the last octet changed to 0s                                                    |
| `listener`        | Unique string for this "listener"                                                               |
| `listenerepisode` | Unique string for "listener + url"                                                              |
| `podcast`         | Feeder podcast id                                                                               |
| `randomstr`       | Random string                                                                                   |
| `randomint`       | Random integer                                                                                  |
| `referer`         | Requester http referer                                                                          |
| `timestamp`       | Epoch milliseconds of request                                                                   |
| `url`             | Full url of request, including host and query parameters, but _without_ the protocol `https://` |

## DynamoDB

When a listener requests an episode from [Dovetail Router](https://github.com/PRX/dovetail-router.prx.org),
it will emit kinesis records of type `antebytes`. Meaning
the bytes haven't been downloaded yet. These records are inserted into DynamoDB,
and saved until the CDN-bytes are actually downloaded.

This lambda also picks up type `bytes` and `segmentbytes` records, meaning that
the [dovetail-counts-lambda](https://github.com/PRX/dovetail-counts-lambda) has
decided enough of the segment/file-as-a-whole has been downloaded to be counted.

As both of those records are keyed by the `<listener_episode>.<digest>` of the
request, we avoid a race condition by waiting for _both_ to be present before
logging the real download/impressions. Some example DynamoDB data:

```
+-----------+-----------------------+-------------------------+
| id        | payload               | segments                |
+-----------+-----------------------+-------------------------+
| 1234.abcd | <binary gzipped json> | 1624299980 1624299942.2 |
| 1234.efgh |                       | 1624300094.1            |
| 5678.efgh | <binary gzipped json> |                         |
+-----------+-----------------------+-------------------------+
```

The `segments` [String Set](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.NamingRulesDataTypes.html#HowItWorks.DataTypes)
contains the epoch `timestamp` that came in on each `byte` or `segmentbyte`
record (the time the bytes were actually downloaded from the CDN). And
optionally a `.` and the segment number. This field acts as a gatekeeper, so we
never double-count the same `bytes/segmentbytes` on the same UTC day.

(**NOTE:** a single `antebytes` record _could_ legally be counted twice on 2
different UTC days, if the listener downloaded the episode from the CDN twice
just before and after midnight).

Once we decide to count a segment impression or overall download, the original
`antebytes` is unzipped from the `payload`, we change the type of the record
to `postbytes` and the timestamp to match when the CDN bytes were downloaded,
then re-emit the record to kinesis.

These `postbytes` records are then processed by the previous 2 lambdas.

# Installation

To get started, first run `yarn`. Then run `yarn dbs` to download the
[GeoLite2 City database](http://dev.maxmind.com/geoip/geoip2/geolite2/), remote
datacenter IP lists, and domain threat lists.

## Unit Tests

And hey, to just run the unit tests locally, you don't need anything! Just
`yarn test` to your heart's content.

There are some dynamodb tests that use an actual table, and will be skipped. To
also run these, set `TEST_DDB_TABLE` and `TEST_DDB_ROLE` to something in AWS you
have access to.

## Integration Tests

The integration test simply runs the lambda function against a test-event (the
same way you might in the lambda web console), and outputs the result.

Copy `env-example` to `.env`, and fill in your information. Now when you run
`yarn start`, you should see the test event run 3 times, and do some work for
all of the lambda functions.

## BigQuery

To enable BigQuery inserts, you'll need to first [create a Google Cloud Platform Project](https://cloud.google.com/resource-manager/docs/creating-managing-projects),
create a BigQuery dataset, and create the tables referenced by your `lib/inputs`.
Sorry -- no help on creating the correct table scheme yet!

Then [create a Service Account](https://developers.google.com/identity/protocols/OAuth2ServiceAccount#creatinganaccount) for this app. Make sure it has BigQuery Data Editor permissions.

## DynamoDB

To enable DynamoDB gets/writes, you'll need to setup a [DynamoDB table](https://docs.aws.amazon.com/dynamodb/index.html#lang/en_us)
that your account has access to. You can use your local AWS cli credentials, or
setup AWS client/secret environment variables.

You can also optionally access a DynamoDB table in a different account by specifying
a `DDB_ROLE` that the lambda should assume while doing gets/writes.

# Deployment

The 3 lambdas functions are deployed via a Cloudformation stack in the [Infrastructure repo](https://github.com/PRX/Infrastructure/blob/master/stacks/apps/dovetail-analytics.yml):

- `AnalyticsBigqueryFunction` - insert downloads/impressions into BigQuery
- `AnalyticsPingbacksFunction` - increment flight impressions and 3rd-party pingbacks
- `AnalyticsDynamoDbFunction` - temporary store for IAB compliant downloads

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
