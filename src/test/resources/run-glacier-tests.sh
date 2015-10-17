#!/bin/bash

set -o errexit
set -o nounset

GLACIER_PROXY_JAR="${PWD}/target/glacier-proxy-1.0-SNAPSHOT-jar-with-dependencies.jar"
GLACIER_PROXY_PORT="8081"
export GLACIER_TEST_CONF="${PWD}/src/test/resources/glacier-tests.conf"

# configure glacier-tests
pushd glacier-tests
./bootstrap
popd

# launch glacier proxy using HTTP and a fixed port
java -jar $GLACIER_PROXY_JAR &
GLACIER_PROXY_PID=$!

# wait for glacier proxy to start
for i in $(seq 30);
do
    if exec 3<>"/dev/tcp/localhost/${GLACIER_PROXY_PORT}";
    then
        exec 3<&-  # Close for read
        exec 3>&-  # Close for write
        break
    fi
    sleep 1
done

# execute glacier-tests
pushd glacier-tests
./virtualenv/bin/nosetests --processes=10
EXIT_CODE=$?
popd

# clean up and return glacier-tests exit code
kill $GLACIER_PROXY_PID
exit $EXIT_CODE
