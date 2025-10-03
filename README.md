# PRX Analytics Ingest Lambda

Lambdas for processing podcast downloads/impressions for PRX Dovetail.

# Description

These lambdas subscribe to kinesis streams, containing metric event records. The types of records seen are:

- `antebytes` - From [Dovetail Router](https://github.com/PRX/dovetail-router.prx.org), this record has information about a request, which ads were inserted, and listener metadata. These requests are normally redirected to the CDN, so we don't know if the listener downloaded the actual bytes of the file yet.
- `bytes` - From the [Counts Lambda](https://github.com/PRX/dovetail-counts-lambda), this indicates that the listener downloaded more than 1 minute of audio and can be counted as an IAB download.
- `segmentbytes` - Similar to bytes, but indicates 100% of a specific ad segment in the audio file was downloaded, and can be counted as an IAB impression.
- `postbytes` - The intersection of the antebytes and bytes/segmentbytes records, indicating we can count downloads/impressions using this upfront redirect metadata.

Because of differences in retry logic, this repo is deployed as 4 different lambdas with different handlers - DynamoDB, Frequency, Pingbacks and BigQuery. This diagram generally describes the data flow around these 4 lambdas (along with the separate [dovetail-counts-lambda](https://github.com/PRX/dovetail-counts-lambda):

<img width="3052" height="2656" alt="image" src="https://github.com/user-attachments/assets/4943cee1-63b6-441d-89ba-d58cbc454a75" />

## DynamoDB Lambda

This lambda temporarily stores Dovetail Router redirect data, and unites it with the
actual counted bytes of CDN downloads. Once all or part of the audio file is downloaded,
we filter the redirect download/impressions to what actually happened and fire a `postbytes`
back onto kinesis.  Which is consumed by the other 3 lambdas.

DynamoDB rows are keyed on the `listenerEpisode` (a hash of IP address, user agent, and the episode guid)
and `digest` (a hash of the specific segments in the audio arrangement they got). The row also
has a `payload` which is just the compressed data receieved from Dovetail Router.

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

Any failures/errors writing to DynamoDB will result in the entire kinesis batch retrying. But since
the output `postbytes` are logged to STDOUT synchronously (and picked up via a
[Subscription Filter](https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/SubscriptionFilters.html))
it's fine to rerun them - they'll no-op the second time.

## BigQuery Lambda

After requests are counted, this lambda inserts them into the `dt_downloads` and `dt_impressions` tables
in BigQuery using [streaming inserts](https://cloud.google.com/bigquery/streaming-data-into-bigquery).

Any errors inserting will result in the entire kinesis batch retrying. To avoid duplicate inserted
rows, they each have a unique [insertId](https://cloud.google.com/dotnet/docs/reference/Google.Cloud.BigQuery.V2/latest/Google.Cloud.BigQuery.V2.BigQueryInsertRow#Google_Cloud_BigQuery_V2_BigQueryInsertRow_InsertId)
which BigQuery will de-duplicate on a best effort basis.

## Frequency Lambda

After requests are counted, this lambda looks for impressions occurring on frequency capped campaigns.
These are inserted into a DynamoDB table that Dovetail Router uses to restrict how often a listener
gets a campaign across any number of requests/episodes.

The frequency table is keyed on Listener ID and Campaign ID, and contains an epoch-milliseconds set
of each impression the listener got this campaign.

```
+----------+---------------+-------------------------+
| campaign | listener      | impressions             |
+----------+---------------+-------------------------+
|   1234   | <some-hash-A> | [1624299980 1624299942] |
|   5678   | <some-hash-B> | [1624300094]            |
|   5678   | <some-hash-B> | [1624300094]            |
+----------+---------------+-------------------------+
```

Since that list of timestamps is a Set, operations here are idempotent, and any failures will retry
the entire kinesis batch.

## Pingbacks Lambda

After requests are counted, this lambda looks for any non-duplicate impressions.

The Flight IDs and counts of those impressions are POST-ed to Dovetail Router, to give it a very
up-to-the-minute idea of how often things served. So it can precisely reach flight goals, and
balance between which flights it serves throughout the day.

Additionally, records with an `impression[].pings` url array will be pinged via
an HTTP GET. This "ping" does follow redirects, but expects to land on a 200
response afterwards. Although 500 errors will be retried internally in the
code, any ping failures will be allowed to fail after error/timeout.

Unlike the other lambdas, these operations are not idempotent, so we don't want to
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
| `campaign`        | Campaign id                                                                                     |
| `creative`        | Creative id                                                                                     |
| `episode`         | Feeder episode guid                                                                             |
| `flight`          | Flight id                                                                                       |
| `ip`              | Request ip address                                                                              |
| `ipmask`          | Masked ip, with the last octet changed to 0s                                                    |
| `ipv4`            | Request ip address, but blank for any ipv6 requests                                             |
| `listener`        | Unique string for this "listener"                                                               |
| `listenerepisode` | Unique string for "listener + url"                                                              |
| `podcast`         | Feeder podcast id                                                                               |
| `randomstr`       | Random string                                                                                   |
| `randomint`       | Random integer                                                                                  |
| `referer`         | Requester http referer                                                                          |
| `timestamp`       | Epoch milliseconds of request                                                                   |
| `url`             | Full url of request, including host and query parameters, but _without_ the protocol `https://` |

# Contributing

See the [Contributing]() file.
