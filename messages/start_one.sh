#!/bin/sh
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <num>"
    exit 1
fi
num=$1
podman run --network=consul-net --rm -ti --name messages-$num -v .:/app:z apz_messages $num
