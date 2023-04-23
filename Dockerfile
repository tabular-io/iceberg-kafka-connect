FROM confluentinc/cp-kafka-connect
USER root
RUN yum -y install jq
USER appuser
COPY ./build/libs/*-all.jar /usr/share/confluent-hub-components/
