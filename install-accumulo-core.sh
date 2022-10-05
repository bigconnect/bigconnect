#!/bin/bash

 mvn install:install-file -Dfile=./lib/accumulo-core-1.10.0-patched.jar -Dpackaging=jar -DgroupId=org.apache.accumulo -DartifactId=accumulo-core -Dversion=1.10.0-bc
