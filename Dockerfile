FROM quay.io/strimzi/kafka:0.34.0-kafka-3.4.0
USER root:root
RUN microdnf install procps jq
COPY kafka-connect-runtime/build/install /opt/kafka/plugins/
USER 1001
