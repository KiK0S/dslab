#!/bin/bash
cd examples/mp-kv-replication-v2
cargo run -- --impl python/$1 --debug
retVal=$?
if [ $retVal -ne 0 ]; then
    echo "Error"
    exit $retVal
fi
cd ../../
