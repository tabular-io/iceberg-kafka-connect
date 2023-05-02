# Kafka Connect Iceberg Sink

## Features
* Commit coordination for centralized Iceberg commits
* Exactly-once delivery semantics
* Message conversion using the Iceberg schema as the source of truth
* Field name mapping via Iceberg’s column mapping functionality

## Example deployment

This assumes the source topic already exists and is named `tabular-events`.

### Create the landing table
```sql
CREATE TABLE bck.tabular_events_kc (
    id STRING,
    type STRING,
    ts TIMESTAMP,
    payload STRING)
PARTITIONED BY (hours(ts))
```
### Start the connector
```bash
curl http://localhost:8083/connectors \
-H 'Content-Type: application/json' \
-H 'Accept: application/json' \
-d @- << EOF
{
"name": "tabular-events-kc",
"config": {
    "connector.class": "io.tabular.iceberg.connect.IcebergSinkConnector",
    "tasks.max": "2",
    "topics": "tabular-events",
    "consumer.override.auto.offset.reset": "latest",
    "transforms": "tabular",
    "transforms.tabular.type": "io.tabular.iceberg.connect.transform.TabularEventTransform",
    "topic.auto.create": "true",
    "iceberg.table": "bck.tabular_events_kc",
    "iceberg.control.group.id": "cg-control-tabular-events",
    "iceberg.control.topic": "control-tabular-events",
    "iceberg.kafka.bootstrap.servers": "${CONNECT_BOOTSTRAP_SERVERS}",
    "iceberg.kafka.security.protocol": "${CONNECT_SECURITY_PROTOCOL}",
    "iceberg.kafka.sasl.mechanism": "${CONNECT_SASL_MECHANISM}",
    "iceberg.kafka.sasl.jaas.config": "${CONNECT_SASL_JAAS_CONFIG}",
    "iceberg.table.commitIntervalMs": "300000",
    "iceberg.catalog": "org.apache.iceberg.rest.RESTCatalog",
    "iceberg.catalog.uri": "https://api.dev.tabular.io/ws",
    "iceberg.catalog.credential": "${TABULAR_TOKEN}",
    "iceberg.catalog.warehouse": "tabular-private-us-west-2",
    "iceberg.catalog.http-client.type": "apache"
    }
}
EOF
```
### Check status
```bash
curl http://localhost:8083/connectors/tabular-events-kc/status
```
