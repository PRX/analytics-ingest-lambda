# PRX Analytics Ingest Lambda

## Getting started

PRX uses [asdf](https://asdf-vm.com/) to manage NodeJS - make sure you've got the correct
version of node, and then just run `yarn` to install dependencies.

## Unit Tests

And hey, to just run the unit tests locally, you don't need anything! Just
`yarn test` to your heart's content.

There are some dynamodb tests that use an actual table, and will be skipped. To
also run these, set `TEST_DDB_TABLE` and `TEST_DDB_ROLE` to something in AWS you
have access to.

## Integration Tests

There are also some tests that have external dependencies, located in the
`test/` folder. To run them, use `yarn integration`.

These tests have external AWS/Google dependencies, so you'll need to copy
`env-example` to `.env` and fill in your info.  Those dependencies are:

### BigQuery

To enable BigQuery inserts, you'll need to first [create a Google Cloud Platform Project](https://cloud.google.com/resource-manager/docs/creating-managing-projects),
create a BigQuery dataset, and create the `dt_downloads` and `dt_impressions`
tables required.

Then [create a Service Account](https://developers.google.com/identity/protocols/OAuth2ServiceAccount#creatinganaccount) for this app. Make sure it has BigQuery Data Editor permissions.

Sorry -- no help on creating the correct table scheme yet! But they already
exist in the PRX GCP account, so just use those!

### DynamoDB

To enable DynamoDB gets/writes, you'll need to setup a [DynamoDB table](https://docs.aws.amazon.com/dynamodb/index.html#lang/en_us)
that your account has access to. You can use your local AWS cli credentials, or
setup AWS client/secret environment variables.

You can also optionally access a DynamoDB table in a different account by specifying
a `DDB_ROLE` that the lambda should assume while doing gets/writes.

You'll need 2 tables, for redirect/segment data and frequency caps. Sorry, no help
here for the schemas on those. But they already exist in the PRX AWS account, so
you should just be able to use them here.

## Linting

This project uses [Biome](https://biomejs.dev/) for code formatting and linting. It's a
dependency in `package.json`, so just make sure your editor is configured to run it.

Or run things manually with `yarn lint` and `yarn lint-fix`.

# Deployment

This lambda is built into a zip file using some simple scripting in `package.json`. You
can try it locally by running `yarn build`, and it'll make you a deployable zipfile.
However, it's probably better to let CI build it, as it uses the official AWS lambda
docker image (just in case there are native dependencies to build).

These lambda functions are deployed via a Cloudformation stack in the [Infrastructure repo](https://github.com/PRX/Infrastructure/blob/main/spire/templates/apps/dovetail-analytics.yml) as:

- `AnalyticsDynamoDbFunction`
- `AnalyticsBigqueryFunction`
- `AnalyticsFrequencyFunction`
- `AnalyticsPingbacksFunction`

See that repo for all the alarms/streams/etc goodness.

And you can easily-ish get the lambda zip built by the Dockerfile:

```
docker ps -a | grep analyticsingestlambda
docker cp {{container-id-here}}:/app/build.zip myzipfile.zip
unzip -l myzipfile.zip
```

# License

[AGPL License](https://www.gnu.org/licenses/agpl-3.0.html)
