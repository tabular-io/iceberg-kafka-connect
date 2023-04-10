#!/bin/bash

# first create the ECR repo by adding it to 01_root/ecr.tf and applying

export AWS_PROFILE=infra

aws ecr get-login-password --region us-west-2 | docker login --username AWS --password-stdin 561839024272.dkr.ecr.us-west-2.amazonaws.com

./gradlew -xtest clean build shadowJar

docker buildx build  \
  --platform linux/amd64,linux/arm64 \
  --pull --push \
  -t 561839024272.dkr.ecr.us-west-2.amazonaws.com/kc-poc:latest \
  -f Dockerfile .
