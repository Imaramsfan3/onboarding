import asyncio
import json
import logging
import os
from datetime import datetime, timezone
from uuid import uuid4

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from fastapi import FastAPI
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TREATMENT_REQUIRED_TOPIC = os.environ.get("TREATMENT_REQUIRED_TOPIC", "treatment-required")
TREATMENT_RESULTS_TOPIC = os.environ.get("TREATMENT_RESULTS_TOPIC", "treatment-results")

# ---------------------------------------------------------------------------
# FastAPI application
# ---------------------------------------------------------------------------

app = FastAPI(title="Individual Validation Service", version="1.0.0")


class HealthResponse(BaseModel):
    status: str


@app.get("/health", response_model=HealthResponse)
async def health():
    """Health check endpoint."""
    return HealthResponse(status="healthy")


# ---------------------------------------------------------------------------
# Kafka consumer / producer helpers
# ---------------------------------------------------------------------------

async def publish_result(producer: AIOKafkaProducer, message: dict) -> None:
    """Publish a treatment result to the results topic."""
    value = json.dumps(message).encode("utf-8")
    await producer.send_and_wait(TREATMENT_RESULTS_TOPIC, value=value)
    logger.info(
        "Published result for application_id=%s treatment_id=%s",
        message.get("application_id"),
        message.get("treatment_id"),
    )


async def handle_message(msg_value: dict, producer: AIOKafkaProducer) -> None:
    """Process a single consumed message."""
    treatment_type = msg_value.get("treatment_type", "")

    if treatment_type != "VALIDATE_OVERSEAS_INDIVIDUAL":
        logger.debug("Ignoring message with treatment_type=%s", treatment_type)
        return

    application_id = msg_value.get("application_id", "unknown")
    treatment_id = msg_value.get("treatment_id", "unknown")

    logger.info(
        "Performing overseas individual validation for application_id=%s treatment_id=%s",
        application_id,
        treatment_id,
    )

    # Simulate processing delay
    await asyncio.sleep(2)

    result = {
        "event_id": str(uuid4()),
        "application_id": application_id,
        "treatment_id": treatment_id,
        "treatment_type": "VALIDATE_OVERSEAS_INDIVIDUAL",
        "result": "PASS",
        "details": "Individual validation completed successfully",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    await publish_result(producer, result)


async def consume_loop() -> None:
    """Long-running Kafka consumer loop."""
    consumer = AIOKafkaConsumer(
        TREATMENT_REQUIRED_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="individual-validation-group",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
    )

    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    )

    while True:
        try:
            await consumer.start()
            await producer.start()
            logger.info(
                "Kafka consumer started — listening on topic '%s'",
                TREATMENT_REQUIRED_TOPIC,
            )
            async for msg in consumer:
                try:
                    await handle_message(msg.value, producer)
                except Exception:
                    logger.exception("Error processing message: %s", msg.value)
        except Exception:
            logger.exception(
                "Kafka connection error — retrying in 5 seconds"
            )
            await asyncio.sleep(5)
        finally:
            try:
                await consumer.stop()
            except Exception:
                pass
            try:
                await producer.stop()
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Startup / shutdown hooks
# ---------------------------------------------------------------------------

_consumer_task: asyncio.Task | None = None


@app.on_event("startup")
async def startup_event():
    global _consumer_task
    logger.info("Starting Kafka consumer background task")
    _consumer_task = asyncio.create_task(consume_loop())


@app.on_event("shutdown")
async def shutdown_event():
    global _consumer_task
    if _consumer_task is not None:
        _consumer_task.cancel()
        try:
            await _consumer_task
        except asyncio.CancelledError:
            pass
        logger.info("Kafka consumer background task stopped")
