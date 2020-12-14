#!/usr/bin/env bash

BINDIR=$(cd "$(dirname "$0")" && pwd -P)

echo -n "Stopping BigConnect Core... "
BC_PID=$("$JAVA_HOME/bin/jps" -lm | grep "com.mware.bigconnect.BigConnectRunner" | grep -E -o '^[0-9]*')

if [ -z "$BC_PID" ]; then
	echo "no BigConnect Core to stop (could not find BigConnect Core process)"
	exit 1
else
	echo "Stopping BigConnect Core with PID $BC_PID"
	kill -15 "$BC_PID"
	echo "BigConnect Core stopped"
fi
