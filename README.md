# PRX Metrics Ingest

Lambda to process metrics data from a kinesis stream, and stream inserts into
one or more BigQuery tables.

# Description

The lambda subscribes to kinesis streams, containing metric event records. These
various metric records are either recognized by an input source in `lib/inputs`,
or ignored and logged as an error at the end of the lambda execution.

The records are then parsed into BigQuery table formats, and inserted into their
corresponding BigQuery tables in parallel.  This is called
[streaming inserts](https://cloud.google.com/bigquery/streaming-data-into-bigquery),
and in case the insert fails, it will be attempted 2 more times before the Lambda
fails with an error.  And since each insert includes a unique `insertId`, we
don't have any data consistency issues with re-running the inserts.

Because the timestamp on the metric may lag behind the lambda execution time,
we also append the BigQuery date partition to the table name, eg
`table_name$20170317`.

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

# Deployment

## Initial Setup

If you're trying to create a new lambda function, you can use some of node-lambda's
built in functionality to do the initial create.  __YOU SHOULD REALLY ONLY DO THIS
ONCE, TO PREVENT OVERWRITING LAMBDA CONFIGURATIONS__.

Add the following to the end of your `.env` file, altering your role/name/etc to
match your AWS setup:

```
### SETUP ###
AWS_ROLE_ARN=arn:aws:iam::12345678:role/lambda_role_with_kinesis_access
AWS_HANDLER=index.handler
AWS_MEMORY_SIZE=128
AWS_TIMEOUT=30
AWS_DESCRIPTION="Process kinesis metric streams and ship to bigquery"
AWS_RUNTIME=nodejs6.10
EXCLUDE_GLOBS=".env env-example *.log .git .gitignore test"
PACKAGE_DIRECTORY=build
```

Then run `./node_modules/node-lambda deploy -e [development|staging|production]`.

Once deployed, you'll need to explicitly configure the function to add the
required runtime configs from the `env-example`, using the Lambda web console.

## Updating

To update an existing Lambda function, use the "deploy-ENV" scripts in
`package.json`. No `.env` is required, as the environment configs should already
be stored out in AWS.

```
npm run deploy-dev
npm run deploy-staging
npm run deploy-prod
```

# License

[AGPL License](https://www.gnu.org/licenses/agpl-3.0.html)
