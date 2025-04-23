#!/bin/sh

if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <number> <hazelcast_ip>"
    exit 1
fi

num=$1
port=$(($num + 5701))
hazelcast_ip=$2

echo "port: $port"

nohup bash -c "podman run --rm --name=hazelcast-$num --network=hazelcast-network -e HZ_NETWORK_PUBLICADDRESS=$hazelcast_ip:$port -e HZ_CLUSTERNAME=logging -p $port:5701 hazelcast/hazelcast:5.3.8" &>/dev/null & disown

podman run --network=consul-net --rm -i --name logging-$num -v .:/app:z apz_logging $num

podman stop hazelcast-$num
