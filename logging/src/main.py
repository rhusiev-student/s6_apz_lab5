import asyncio
import logging
from hazelcaster import Hazelcaster

import argparse

from grpc import aio
from grpc_generated import logging_pb2_grpc
from log import Logger

import consul

logging.basicConfig()
logging.getLogger("hazelcast").setLevel(logging.ERROR)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


async def serve(hazelcaster: Hazelcaster, port: int):
    logger.info(
        f"Trying to connect to hazelcast at {hazelcaster.hz_ip}:{hazelcaster.port}"
    )
    with hazelcaster:
        logger.info(f"Started hazelcast on port {hazelcaster.port}")
        srv = aio.server()
        logging_pb2_grpc.add_LoggingServiceServicer_to_server(
            Logger(hazelcaster, logger), srv
        )

        listen_address = f"0.0.0.0:{port}"
        srv.add_insecure_port(listen_address)

        logger.info(f"Starting server on {listen_address}")
        await srv.start()
        await srv.wait_for_termination()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("num", type=int)
    args = parser.parse_args()
    num: int = args.num
    port = num + 13228

    consul_client = consul.Consul(host="badger")
    consul_client.agent.service.register(
        name="logging",
        service_id=f"logging-{num}",
        port=port,
        address=f"logging-{num}",
        tags=["logging"],
    )
    logger.info("Registered logging service in consul")
    hazelcast_ip = consul_client.kv.get("hazelcast_ip")[1]["Value"]
    hazelcast_ip = hazelcast_ip.decode("utf-8")
    logger.info(f"Got hazelcast ip from consul: {hazelcast_ip}")

    logger.info("Starting hazelcast logging server")
    hazelcaster = Hazelcaster("logging", num, hz_ip=hazelcast_ip)
    asyncio.run(serve(hazelcaster, port))
