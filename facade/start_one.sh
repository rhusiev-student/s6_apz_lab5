#!/bin/sh
if [ -z "$1" ]; then
    echo "Usage: $0 <num>"
    exit 1
fi
num=$1
port=$((14229 + $num))
podman run --network=consul-net -p $port:$port --rm -ti --name facade-$num -v .:/app:z apz_facade $num
