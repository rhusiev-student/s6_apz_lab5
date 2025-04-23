import asyncio
import sys
import uuid
import random

from contextlib import asynccontextmanager

from grpc import aio
import httpx
import uvicorn
from fastapi import FastAPI, Depends, HTTPException, Response, status
from fastapi.responses import PlainTextResponse

from grpc_generated import logging_pb2, logging_pb2_grpc

from kafka import produce_message

from consul.aio import Consul
from consul import Check

import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

MAX_RETRIES = 3
RETRY_DELAY_MS = 500


async def create_grpc_client(logging_url):
    options = [
        ("grpc.enable_http_proxy", 0),
        ("grpc.keepalive_time_ms", 30000),
        ("grpc.connect_timeout_ms", 5000),
        ("grpc.max_receive_message_length", -1),
        ("grpc.max_send_message_length", -1),
    ]
    channel = aio.insecure_channel(logging_url, options=options)
    return logging_pb2_grpc.LoggingServiceStub(channel)


def format_grpc_error(err):
    if hasattr(err, "code") and hasattr(err, "details"):
        return f"gRPC error ({err.code()}): {err.details()}"
    return str(err)


def retry_decorator(max_retries=MAX_RETRIES, delay_ms=RETRY_DELAY_MS, refresh_fn=None):
    def decorator(fn):
        async def wrapper(*args, **kwargs):
            attempts = 0
            while True:
                try:
                    return await fn(*args, **kwargs)
                except Exception as err:
                    print(f"Error on attempt {attempts + 1}: {format_grpc_error(err)}")
                    if attempts >= max_retries:
                        raise
                    if refresh_fn:
                        await refresh_fn(err)
                    attempts += 1
                    await asyncio.sleep(delay_ms / 1000)

        return wrapper

    return decorator


class Client:
    def __init__(
        self,
        consul_client: Consul,
        weird_setup_forced_by_previous_homework: bool = False,
    ):
        self.consul_client = consul_client
        self.weird_setup_forced_by_previous_homework = True

    async def get_possible_addresses(self, service: str) -> list[str]:
        _, services = await self.consul_client.catalog.service(service)
        addresses = [
            f"{service['ServiceAddress']}:{service['ServicePort']}"
            for service in services
        ]
        return addresses


@asynccontextmanager
async def lifespan(app: FastAPI):
    consul_client = Consul(host="badger")
    app.state.client = Client(consul_client, True)

    await consul_client.agent.service.register(
        name="facade",
        service_id=f"facade-{num}",
        port=port,
        address=f"facade-{num}",
        tags=["facade"],
        check=Check.http(
            f"http://facade-{num}:{port}/health",
            timeout="2s",
            interval="5s",
        ),
    )
    kafka_addresses = (await consul_client.kv.get("kafka_addresses"))[1]["Value"]
    kafka_addresses = kafka_addresses.decode("utf-8")
    kafka_topic = (await consul_client.kv.get("kafka_topic"))[1]["Value"]
    kafka_topic = kafka_topic.decode("utf-8")

    app.state.kafka_addresses = kafka_addresses
    app.state.kafka_topic = kafka_topic
    yield
    await consul_client.agent.service.deregister(f"facade-{num}")


app = FastAPI(lifespan=lifespan)


@app.post("/")
async def add_log(message: str):
    client = app.state.client
    log_uuid = str(uuid.uuid4())

    async def refresh_url(_):
        current = app.state.current_url_logging
        total = len(app.state.logging_urls)
        app.state.current_url_logging = (current + 1) % total

    @retry_decorator(refresh_fn=refresh_url)
    async def make_request():
        current = app.state.current_url_logging
        urls = app.state.logging_urls
        grpc_client = await create_grpc_client(urls[current])
        log = logging_pb2.Log(uuid=log_uuid, message=message)
        return await grpc_client.AddLog(log)

    app.state.logging_urls = await client.get_possible_addresses("logging")
    app.state.current_url_logging = random.randint(0, len(app.state.logging_urls) - 1)

    if not app.state.logging_urls:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="No logging service available",
        )

    try:
        await make_request()
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to add log",
        )

    try:
        produce_message(
            {"message": message},
            kafka_addresses=app.state.kafka_addresses,
            topic_name=app.state.kafka_topic,
        )
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to add message",
        )
    return Response(status_code=status.HTTP_200_OK)


@app.get("/")
async def get_logs():
    client = app.state.client
    app.state.messages_urls = await client.get_possible_addresses("messages")
    if not app.state.messages_urls:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="No message service available",
        )
    app.state.current_url_messages = random.randint(0, len(app.state.messages_urls) - 1)

    async def refresh_url(_):
        current = app.state.current_url_messages
        total = len(app.state.messages_urls)
        app.state.current_url_messages = (current + 1) % total

    @retry_decorator(refresh_fn=refresh_url)
    async def make_request():
        current = app.state.current_url_messages
        urls = app.state.messages_urls
        async with httpx.AsyncClient() as ac:
            logger.info(f"Requesting message from {urls[current]}")
            resp = await ac.get(f"http://{urls[current]}")
            resp.raise_for_status()
            return resp.text

    try:
        message_text = await make_request()
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )

    async def refresh_url(_):
        current = app.state.current_url_logging
        total = len(app.state.logging_urls)
        app.state.current_url_logging = (current + 1) % total

    @retry_decorator(refresh_fn=refresh_url)
    async def make_request():
        current = app.state.current_url_logging
        urls = app.state.logging_urls
        grpc_client = await create_grpc_client(urls[current])
        req = logging_pb2.GetLogsRequest()
        return await grpc_client.GetLogs(req)

    app.state.logging_urls = await client.get_possible_addresses("logging")

    if not app.state.logging_urls:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="No logging service available",
        )
    app.state.current_url_logging = random.randint(0, len(app.state.logging_urls) - 1)

    try:
        response = await make_request()
        return PlainTextResponse(f"{message_text}\n{response.logs_string}")
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )


@app.get("/health")
async def health():
    return {"status": "ok"}


def main(port: int):
    uvicorn.run(app, host="0.0.0.0", port=port)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python main.py <number>")
        sys.exit(1)
    try:
        num = int(sys.argv[1])
    except ValueError:
        print("Usage: python main.py <number>")
        sys.exit(1)

    port = 14229 + num

    main(port)
