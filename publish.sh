#!/bin/bash

./gradlew -xtest clean build

sha512sum ./kafka-connect-runtime/build/distributions/iceberg-kafka-connect-runtime-0.1.0.zip

aws --profile infra \
  s3 cp ./kafka-connect-runtime/build/distributions/iceberg-kafka-connect-runtime-0.1.0.zip s3://tabular-repository-public/kc/
