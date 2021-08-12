#!/bin/bash

export JAVA_HOME=/opt/jdk-11.0.8+10
mvn -P ossrh-release clean deploy -DskipTests

