# Iceberg Kafka Connect Sink

## Features
* Commit coordination for centralized Iceberg commits
* Exactly-once delivery semantics
* Multi-table output
* Row mutations (update/delete rows), upsert mode
* Message conversion using the Iceberg schema as the source of truth
* Field name mapping via Iceberg’s column mapping functionality

## Example deployment

This assumes the source topic already exists and is named `tabular-events`.

### Create the landing table
```sql
CREATE TABLE default.tabular_events (
    id STRING,
    type STRING,
    ts TIMESTAMP,
    payload STRING)
PARTITIONED BY (hours(ts))
```

### Create the control topic

If your Kafka cluster has `auto.create.topics.enable` set to `true` (the default), then the control topic will be automatically created. If not, then you will need to create the topic first. The default topic name is `control-<connector name>`:
```bash
bin/kafka-topics  \
  --command-config command-config.props \
  --bootstrap-server ${CONNECT_BOOTSTRAP_SERVERS} \
  --create \
  --topic control-tabular-events-sink \
  --partitions 1
```
*NOTE: Clusters running on Confluent Cloud have `auto.create.topics.enable` set to `false` by default.*

### Start the connector
```bash
curl http://localhost:8083/connectors \
-H 'Content-Type: application/json' \
-H 'Accept: application/json' \
-d @- << EOF
{
"name": "tabular-events-sink",
"config": {
    "connector.class": "io.tabular.iceberg.connect.IcebergSinkConnector",
    "tasks.max": "2",
    "topics": "tabular-events",
    "consumer.override.auto.offset.reset": "latest",
    "iceberg.tables": "default.tabular_events",
    "iceberg.control.commitIntervalMs": "300000",
    "iceberg.catalog": "org.apache.iceberg.rest.RESTCatalog",
    "iceberg.catalog.uri": "https://api.tabular.io/ws",
    "iceberg.catalog.credential": "${TABULAR_TOKEN}",
    "iceberg.catalog.warehouse": "${TABULAR_WAREHOUSE}",
    "iceberg.catalog.http-client.type": "apache"
    }
}
EOF
```
### Check status
```bash
curl http://localhost:8083/connectors/tabular-events-sink/status
```
