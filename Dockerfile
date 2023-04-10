FROM confluentinc/cp-kafka-connect
COPY ./build/libs/*-all.jar /usr/share/confluent-hub-components/
