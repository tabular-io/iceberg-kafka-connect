# Kafka Connect Iceberg Sink

## Features
* Commit coordination for centralized Iceberg commits
* Exactly-once delivery semantics
* Multi-table output
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

If your Kafka cluster has `auto.create.topics.enable` set to `true` (the default), then the control topic will be automatically created. If not, then you will need to create the topic first:
```bash
bin/kafka-topics  \
  --command-config command-config.props \
  --bootstrap-server ${CONNECT_BOOTSTRAP_SERVERS} \
  --create \
  --topic control-tabular-events \
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
    "iceberg.table": "default.tabular_events",
    "iceberg.table.commitIntervalMs": "300000",
    "iceberg.control.group.id": "cg-control-tabular-events",
    "iceberg.control.topic": "control-tabular-events",
    "iceberg.kafka.bootstrap.servers": "${CONNECT_BOOTSTRAP_SERVERS}",
    "iceberg.kafka.security.protocol": "${CONNECT_SECURITY_PROTOCOL}",
    "iceberg.kafka.sasl.mechanism": "${CONNECT_SASL_MECHANISM}",
    "iceberg.kafka.sasl.jaas.config": "${CONNECT_SASL_JAAS_CONFIG}",
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
