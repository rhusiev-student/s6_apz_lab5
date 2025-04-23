import asyncio
import sys
import logging
from kafka import KafkaConsumerError, consume_messages
from fastapi import FastAPI
import uvicorn
from threading import Thread
from collections import OrderedDict
from threading import Lock
import consul

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("kafka_consumer.log")],
)

logger = logging.getLogger(__name__)


class SafeOrderedDict:
    def __init__(self, maxsize=1000):
        self.data = OrderedDict()
        self.lock = Lock()
        self.maxsize = maxsize

    def add(self, key, value):
        with self.lock:
            self.data[key] = value
            while len(self.data) > self.maxsize:
                self.data.popitem(last=False)

    def get_all(self):
        with self.lock:
            return list(self.data.items())


message_store = SafeOrderedDict()

app = FastAPI()


@app.get("/")
async def get_messages():
    return {"messages": message_store.get_all()}


@app.get("/health")
async def get_health():
    return {"status": "ok"}


async def kafka_consumer(
    kafka_addresses: str, kafka_topic: str, kafka_consumer_group: str
):
    consumer_gen = consume_messages(
        topic_name=kafka_topic,
        group_id=kafka_consumer_group,
        kafka_addresses=kafka_addresses,
    )
    try:
        message_counter = 0
        for message in consumer_gen:
            if message is None:
                continue
            if isinstance(message, KafkaConsumerError):
                if message.partition is None:
                    logger.critical(
                        "Breaking loop due to consumer error",
                    )
                    break
            else:
                logger.info(f"Received message: {message}")
                message_store.add(message_counter, message)
                message_counter += 1
    except KeyboardInterrupt:
        logger.info("Consumer stopped by user")
        raise
    except Exception as e:
        logger.exception(f"Unexpected error in main consumer loop: {e}")


def run_fastapi(port: int):
    uvicorn.run(app, host="0.0.0.0", port=port)


if __name__ == "__main__":
    logger.info("Starting Kafka consumer application")
    args = sys.argv
    if len(args) != 2:
        logger.error(f"Usage: {args[0]} <num>")
        sys.exit(1)
    num = args[1]

    try:
        num = int(num)
    except ValueError:
        logger.error(f"Invalid number: {num}")
        sys.exit(1)
    port = 12227 + num

    logger.info("Registering service in Consul")

    consul_client = consul.Consul(host="badger")
    consul_client.agent.service.register(
        name="messages",
        service_id=f"messages-{num}",
        port=port,
        address=f"messages-{num}",
        tags=["messages"],
        check=consul.Check.http(
            f"http://messages-{num}:{port}/health",
            timeout="2s",
            interval="5s",
        ),
    )
    kafka_addresses: str = consul_client.kv.get("kafka_addresses")[1]["Value"]
    kafka_addresses = kafka_addresses.decode("utf-8")
    kafka_topic: str = consul_client.kv.get("kafka_topic")[1]["Value"]
    kafka_topic = kafka_topic.decode("utf-8")
    kafka_consumer_group: str = consul_client.kv.get("kafka_consumer_group")[1]["Value"]
    kafka_consumer_group = kafka_consumer_group.decode("utf-8")

    logger.info("Registered service in Consul")

    logger.info("Starting Kafka consumer")

    api_thread = Thread(target=run_fastapi, args=(port,), daemon=True)
    api_thread.start()

    try:
        while True:
            logger.info("Starting to consume messages from Kafka topic 'my-kool-topic'")
            asyncio.run(kafka_consumer(kafka_addresses, kafka_topic, kafka_consumer_group))
            logger.warning("Main function completed, restarting...")
    finally:
        logger.info("Main function stopped by user. Deregistering service")
        consul_client.agent.service.deregister(f"messages-{num}")
        logger.info("Service deregistered")
