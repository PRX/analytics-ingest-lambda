# PRX Metrics Ingest

Lambda to process metrics data from a kinesis stream, and stream inserts into
one or more BigQuery tables.

# Description

The lambda subscribes to kinesis streams, containing metric event records. These
various metric records are either recognized by an input source in `lib/inputs`,
or ignored and logged as a warning at the end of the lambda execution.

## BigQuery

Records with type `download`/`impression` will be parsed into BigQuery table formats,
and inserted into their corresponding BigQuery tables in parallel.  This is called
[streaming inserts](https://cloud.google.com/bigquery/streaming-data-into-bigquery),
and in case the insert fails, it will be attempted 2 more times before the Lambda
fails with an error.  And since each insert includes a unique `insertId`, we
don't have any data consistency issues with re-running the inserts.

Because the timestamp on the metric may lag behind the lambda execution time,
we also append the BigQuery date partition to the table name, eg
`table_name$20170317`.

## Pingbacks

Non-duplicate records with a special `pingbacks` array will HTTP GET those URLs,
expecting to land on a 200 response after redirects. Although 500 errors will
be retried, any ping failures will be allowed to fail after error/timeout.
Unlike BigQuery, these operations are not idempotent, so we don't want to
over-ping a url.

Similar to Pingbacks, any non-duplicate impression with an `impressionUrl` will
be pinged.  This is normally the "official" Adzerk pixel-tracker from Dovetail.

## Redis

To give some semblance of live metrics, this lambda can also directly `INCR`
the Redis cache used by [castle.prx.org](https://github.com/PRX/castle.prx.org).
This operates on both download and impression records, and like Pingbacks, will
be allowed to fail without retry.

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
Sorry - no help on creating the correct table scheme yet!

Then [create a Service Account](https://developers.google.com/identity/protocols/OAuth2ServiceAccount#creatinganaccount) for this app.  Make sure it has BigQuery Data Editor permissions.

Copy `env-example` to `.env`, and fill in your information. Now when you run
`npm start`, you should see the test event insert some rows into BigQuery.

Note that by default, this will only run the BigQuery table inserts. To instead
run pingbacks, add `PINGBACKS=true` to your `.env` file.

# Deployment

This repo actually represents 2 lambda functions: `analytics-ingest-ENVIRONMENT`
to insert downloads/impressions into bigquery, and `analytics-pingback-ENVIRONMENT`
to manage Adzerk impressions, other HTTP pingbacks, and realtime redis increments.

These need separate functions due to the retry logic of kinesis-triggered lambdas.
If any errors are thrown, that same kinesis segment will be retried over and over,
until they succeed or expire. For BigQuery, this is fine, since the `insertId`
does some de-duping for us. But for pingbacks, we want to just allow these to
fail without re-running the entire kinesis segment. This prevents one misbehaving
pingback from causing a bunch of duplicates GETs on the rest of them.

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
