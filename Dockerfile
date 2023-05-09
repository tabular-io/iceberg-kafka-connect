FROM quay.io/strimzi/kafka:0.34.0-kafka-3.4.0
COPY kafka-connect-runtime/build/install /opt/kafka/plugins/
