#!/bin/bash
cd examples/mp-kv-sharding
cargo run -- --impl python/$1 --debug
retVal=$?
if [ $retVal -ne 0 ]; then
    echo "Error"
    exit $retVal
fi
cd ../../