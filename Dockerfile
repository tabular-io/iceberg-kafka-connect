FROM confluentinc/cp-kafka-connect
USER root
RUN yum -y install jq
USER appuser
COPY ./build/install/ /usr/share/confluent-hub-components/
