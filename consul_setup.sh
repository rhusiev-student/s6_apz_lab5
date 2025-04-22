podman run \
    -d \
    --network=consul-net \
    --rm \
    -p 8500:8500 \
    -p 8600:8600/udp \
    --name=badger \
    hashicorp/consul agent -server -ui -node=server-1 -bootstrap-expect=1 -client=0.0.0.0

podman run \
   --network=consul-net \
   --rm \
   --name=fox \
   hashicorp/consul agent -node=client-1 -retry-join=badger

podman run \
   --network=consul-net \
   -p 9001:9001 \
   -d \
   --name=weasel \
   hashicorp/counting-service:0.0.2

podman exec fox /bin/sh -c "echo '{\"service\": {\"name\": \"counting\", \"tags\": [\"go\"], \"port\": 9001}}' >> /consul/config/counting.json"
podman exec fox consul reload
