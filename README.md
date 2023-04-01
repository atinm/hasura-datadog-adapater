# Hasura GraphQL Engine Datadog metric adapter (forked from https://github.com/afitzek/hasura-metric-adapter)

[Hasura GraphQL Engine](https://hasura.io) is an awesome piece of software, check it out! There is also a great cloud offering and a pro version, which holds a lot more very useful features.

For projects where one wants to self host the graphql engine, the community edition lacks visibility in terms of metrics. This project can help with this, since it reads and parses the hasura logs and collects and provides Datadog metrics from it.

The logs can expected to be read from a log file.

The application will start a Datadog Statd client that will send Hasura stats to Datadog.

Hasura should be configured to at least emit `http-log`, `webhook-log` and `query-log` log types.

Other envvars needed for configuration:
`DATADOG_ENDPOINT` The datadog statd endpoint (defaults to `localhost:8125`)

`HASURA_GRAPHQL_ENDPOINT` The hasura endpoint (defaults to `http://localhost:8080`)

`HASURA_GRAPHQL_ADMIN_SECRET` The hasura admin secret is optional, if not provided,
some collectors are disabled. 

## Program help

```
metrics 0.1.1
A Datadog Statd metric generator for Hasura based on the log stream

USAGE:
    metrics [OPTIONS] --logfile <logfile>

OPTIONS:
        --collect-interval <collect-interval>
            [env: COLLECT_INTERVAL=] [default: 15000]
        
        --concurrency-limit <concurrency-limit>
            [env: CONCURRENCY_LIMIT=] [default: 0]

        --datadog-endoint <datadog statd endpoint>
            [env: DATADOG_ENDPOINT=] [default: localhost:8125]

        --exclude_collectors <collector>[;collector...]
            [env: EXCLUDE_COLLECTORS=] [possible values: cron-triggers, event-triggers,
            scheduled-events, metadata-inconsistency]

    -h, --help
            Print help information

        --hasura-admin-secret <hasura-admin-secret>
            [env: HASURA_GRAPHQL_ADMIN_SECRET=]

        --hasura-endpoint <hasura-endpoint>
            [env: HASURA_GRAPHQL_ENDPOINT=] [default: http://localhost:8080]

        --histogram-buckets <histogram-buckets>
            [env: HISTOGRAM_BUCKETS=]

    -l, --common-labels <common-labels>
            [env: COMMON_LABELS=] [e.g. COMMON_LABELS="product:hasura;service:graphql-engine"]

        --logfile <logfile>
            [env: LOG_FILE=]

        --metrics-prefix <prefix>
            [env: METRICS_PREFIX=]

        --sleep <sleep>
            [env: SLEEP_TIME=] [default: 1000]

    -V, --version
            Print version information
```

If you want to provide multiple values for some key in ENVIROMENT VARIABLE, they should be separated by `;`, for example:

```
EXCLUDE_COLLECTORS=cron-triggers;event-triggers;scheduled-events
```

## Metrics

- `log_lines_counter`
    This is a counter the counts all parsed log lines. The labels include the
    log type.

- `log_lines_counter_total`
    This is a counter that is the sum of all counted log lines.

- `query_execution_seconds`

    This is a histogram, that stores the query execution time in seconds.
    The labels are:
    - `operation` which holds the operation name of the graphql query or nothing
    if none is provided.
    - `error` which holds the error code if an error was detected or nothing if
    this was successful

    For each label setting there are the following entries:
    - `query_execution_seconds_bucket`
    - `query_execution_seconds_sum`
    - `query_execution_seconds_count`

- `request_counter`

    This is a counter that counts the number of http requests. It provides
    `status` the http status code and `url` the path that was called.

- `request_query_counter`

    This is a counter that counts the number of queries.
    The labels are:
    - `operation` which holds the operation name of the graphql query or nothing
    if none is provided.
    - `error` which holds the error code if an error was detected or nothing if
    this was successful

- `websockets_active`

    This is a gauge that holds the currently active websocket connections.

- `websockets_operations`

    This is a counter that counts the operations (queries) over websockets.
    The labels are:
    - `operation` which holds the operation name of the graphql query or nothing
    if none is provided.
    - `error` which holds the error code if an error was detected or nothing if
    this was successful

- `websockets_operations_active`

    This is a gauge that holds the currently active websocket operations.

- `healthy`

    This is a gauge that is 1 if the instance is healthy or 0 otherwise

- `metadata_version`

    This is a gauge, that holds a `version` label, with the hasura version
    and the value of `1` if that version was detected.

The following metrics are the same as in the project (https://github.com/zolamk/hasura-exporter) and (https://github.com/afitzek/hasura-metric-adapter), also the idea on how to access them is based on it. So all credit for these need to go to @zolamk and @afitzek, I just ported them here. These metrics are disabled if no admin secret is provided. Cron triggers and one off events won't work if the postgres database with the metadata is not accessible as a data source with the 'default' name.

- `pending_cron_triggers`, `processed_cron_triggers`, `successful_cron_triggers`, `failed_cron_triggers`

    These are gauges, that shows the number of (pending, processed, successful, failed) cron triggers labeled with the trigger name

- `pending_event_triggers`, `processed_event_triggers`, `successful_event_triggers`, `failed_event_triggers`

    These are gauges, that shows the number of (pending, processed, successful, failed) event triggers labeled with the trigger name

- `pending_one_off_events`, `processed_one_off_events`, `successful_one_off_events`, `failed_one_off_events`

    These are gauges, that shows the number of (pending, processed, successful, failed) one off events

- `metadata_consistency_status`

    This is a gauge that is 1 if the instance metadata is consistent or 0 otherwise

## Docker Image

Don't use version `v0.1.0` its broken.

The docker image `ghcr.io/afitzek/hasura-metric-adapter:v0.1.6` needs four environment variables to be configured.

`DATADOG_ENDPOINT`: The Datadog Statd endpoint to send metrics to
`LOG_FILE`: The log file, which will hold the hasura logs
`HASURA_GRAPHQL_ENDPOINT` The hasura endpoint (defaults to `http://localhost:8080`)
`HASURA_GRAPHQL_ADMIN_SECRET` The hasura admin secret this is required

## K8S Example

An example k8s setup can be found in [k8s_example.yaml](k8s_example.yaml).

The important pieces of the example are:
- The adapter runs in sidecar mode with the hasura container
- The containers use a shared namespace, so that a named pipe can be accessed in both containers
    `shareProcessNamespace: true`
- The hasura container and the adapter share a common volume called `logs`, which is mounted in both containers as `/tmp/log`.
- Overwrite the hasura command to:
    ```
    "/bin/sh", "-c", "rm -rf /tmp/log/stdout.log && mkfifo /tmp/log/stdout.log && /bin/graphql-engine serve | tee /tmp/log/stdout.log"
    ```
    This creates a named pipe, and pipes the logs of graphql-engine to the stdout for logging and to the named pipe for the metric adapter to collect.

    (an alternative if you can't have shared process namespaces in the pod, is to use a file, but as @Hongbo-Miao pointed out in https://github.com/afitzek/hasura-metric-adapter/issues/11 the log file can become very big)
    ```
    "/bin/sh", "-c", ": > /tmp/log/stdout.log && /bin/graphql-engine serve | tee /tmp/log/stdout.log"
    ```
    This truncates the log file, to not count metrics on container restarts, starts the graphql-engine and pipes the stdout to stdout and the file `/tmp/log/stdout.log`.
- `HASURA_GRAPHQL_ENABLED_LOG_TYPES` includes `http-log`, `webhook-log` and `query-log`.
- The metric adapter is set up to listen on port `9999` and read the log from the shared volume `/tmp/log/stdout.log`.

## Docker-compose

A [docker-compose.yml](docker-compose.yml) is provided, ready to be used, following the same principles used on the construction of the [k8s_example.yaml](k8s_example.yaml).

In order to run it, execute:
```
docker-compose build
docker-compose up
```

# Troubleshooting

- Event Log errors:
    If you see constant errors that the `event_log` is not existing, than you probably don't use
    events. Hasura might only create the `event_log` table when needed, in this case disable
    the appropriate collector. For example `EXCLUDE_COLLECTORS=event-triggers`
