#!/bin/bash

./gradlew -xtest clean build

docker buildx build  \
  --platform linux/amd64,linux/arm64 \
  --pull --push \
  -t tabulario/iceberg-kafka-connect:latest \
  -f Dockerfile .

aws --profile infra s3 cp --recursive kafka-connect-runtime/build/distributions/ s3://tabular-repository-public/kc/