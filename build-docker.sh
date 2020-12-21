#!/bin/bash

mvn clean

docker build -t bigconnect/bigconnect:4.2.0 .
docker push bigconnect/bigconnect:4.2.0
