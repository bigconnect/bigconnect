#!/bin/bash

mvn clean -Pbin-release -DskipTests install

docker login -u bigconnect
docker build -t bigconnect/bigconnect:4.2.1 .
docker push bigconnect/bigconnect:4.2.1
