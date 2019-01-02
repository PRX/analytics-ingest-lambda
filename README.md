# PRX Metrics Ingest

Lambda to process metrics data coming from one or more kinesis streams, and
send that data to multiple destinations.

# Description

The lambda subscribes to kinesis streams, containing metric event records. These
various metric records are either recognized by an input source in `lib/inputs`,
or ignored and logged as a warning at the end of the lambda execution.

Because of differences in (a) retry logic, or (b) VPC network access, this repo
is actually deployed as 3 different lambdas.  But all are subscribed to the same
kinesis streams, so they can move records to different destinations.

## BigQuery

Records with type `combined`/`bytes`/`segmentbytes` will be parsed
into BigQuery table formats, and inserted into their corresponding BigQuery
tables in parallel.  This is called [streaming inserts](https://cloud.google.com/bigquery/streaming-data-into-bigquery),
and in case the insert fails, it will be attempted 2 more times before the Lambda
fails with an error.  And since each insert includes a unique `insertId`, we
don't have any data consistency issues with re-running the inserts.

BigQuery now supports partitioning based on a [specific timestamp field](https://cloud.google.com/bigquery/docs/partitioned-tables#partitioned_tables),
so any inserts streamed to a table will be automatically moved to the correct
daily partition.

## Pingbacks

Records with type `combined` and a special `impression[].pings` array will be pinged via
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
| `listener`        | Unique string for this "listener" |
| `listenerepisode` | Unique string for "listener + url" |
| `listenersession` | Unique string for "listener + url + UTCDate" |
| `podcast`         | Feeder podcast id |
| `randomstr`       | Random string |
| `randomint`       | Random integer |
| `referer`         | Requester http referer |
| `timestamp`       | Epoch milliseconds of request |
| `url`             | Full url of request, including host and query parameters, but _without_ the protocol `https://` |

## Redis

To give some semblance of live metrics, this lambda can also directly `INCR`
the Redis cache used by [castle.prx.org](https://github.com/PRX/castle.prx.org).
This operates on type = `combined` records, and like Pingbacks, will be allowed
to fail without retry.

# Installation

To get started, first run an `npm install`.  Then run `npm run geolite` to download
the [GeoLite2 City database](http://dev.maxmind.com/geoip/geoip2/geolite2/).

## Unit Tests

And hey, to just run the unit tests locally, you don't need anything!  Just
`npm test` to your heart's content.

## Integration Tests

The integration test simply runs the lambda function against a test-event (the
same way you might in the lambda web console), and outputs the result.

You'll need to first [create a Google Cloud Platform Project](https://cloud.google.com/resource-manager/docs/creating-managing-projects),
create a BigQuery dataset, and create the tables referenced by your `lib/inputs`.
Sorry -- no help on creating the correct table scheme yet!

Then [create a Service Account](https://developers.google.com/identity/protocols/OAuth2ServiceAccount#creatinganaccount) for this app.  Make sure it has BigQuery Data Editor permissions.

Copy `env-example` to `.env`, and fill in your information. Now when you run
`npm start`, you should see the test event insert some rows into BigQuery.

Note that by default, this will only run the BigQuery table inserts. To instead
run pingbacks, add `PINGBACKS=true` to your `.env` file.

# Deployment

The 3 lambdas functions are deployed via a Cloudformation stack in the [Infrastructure repo](https://github.com/PRX/Infrastructure/blob/master/stacks/analytics-ingest-lambda.yml):

 - `AnalyticsBigqueryLambda` - insert downloads/impressions/bytes into BigQuery
 - `AnalyticsPingbacksLambda` - ping Adzerk impressions and 3rd-party pingbacks
 - `AnalyticsRedisLambda` - realtime Redis increments

# Docker

This repo is now dockerized!

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
