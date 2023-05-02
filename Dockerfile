FROM confluentinc/cp-kafka-connect
USER root
RUN yum -y install jq
USER appuser
COPY kafka-connect-runtime/build/install /usr/share/confluent-hub-components/
COPY tabular-transforms/build/libs /usr/share/confluent-hub-components/
