import asyncio
import json
import logging
import os
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Set

import httpx
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("orchestration")

# ---------------------------------------------------------------------------
# Environment variables
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
RULES_SERVICE_URL = os.getenv("RULES_SERVICE_URL", "http://localhost:8001")
CUSTOMER_APPLICATION_TOPIC = os.getenv("CUSTOMER_APPLICATION_TOPIC", "customer-application")
TREATMENT_REQUIRED_TOPIC = os.getenv("TREATMENT_REQUIRED_TOPIC", "treatment-required")
TREATMENT_RESULTS_TOPIC = os.getenv("TREATMENT_RESULTS_TOPIC", "treatment-results")
CUSTOMER_APPLICATION_RESPONSE_TOPIC = os.getenv(
    "CUSTOMER_APPLICATION_RESPONSE_TOPIC", "customer-application-response"
)

# ---------------------------------------------------------------------------
# FSM States
# ---------------------------------------------------------------------------
class FSMState(str, Enum):
    INITIATED = "INITIATED"
    COLLECTING_DATA = "COLLECTING_DATA"
    EVALUATING_RULES = "EVALUATING_RULES"
    TREATMENTS_PENDING = "TREATMENTS_PENDING"
    COMPLETED = "COMPLETED"


# All expected screen types
ALL_SCREEN_TYPES: Set[str] = {"PERSONAL_DETAILS", "ADDRESS_DETAILS", "BUSINESS_DETAILS"}

# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------
class Treatment(BaseModel):
    treatment_id: str
    treatment_type: str
    description: str


class ApplicationState(BaseModel):
    state: FSMState
    screens_received: Set[str] = set()
    application_data: Dict[str, Any] = {}
    treatments_required: List[Treatment] = []
    treatments_completed: List[str] = []


# ---------------------------------------------------------------------------
# In-memory state store
# ---------------------------------------------------------------------------
state_store: Dict[str, ApplicationState] = {}

# ---------------------------------------------------------------------------
# Kafka producer (module-level, initialised during lifespan)
# ---------------------------------------------------------------------------
producer: Optional[AIOKafkaProducer] = None

# ---------------------------------------------------------------------------
# Background task handles
# ---------------------------------------------------------------------------
_background_tasks: List[asyncio.Task] = []


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _serialize(value: Any) -> bytes:
    return json.dumps(value, default=str).encode("utf-8")


def _deserialize(raw: bytes) -> dict:
    return json.loads(raw.decode("utf-8"))


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


async def _publish(topic: str, message: dict) -> None:
    """Publish a JSON message to a Kafka topic via the shared producer."""
    if producer is None:
        logger.error("Producer not initialised – cannot publish to %s", topic)
        return
    await producer.send_and_wait(topic, value=message)
    logger.info("Published to %s: %s", topic, message.get("event_id", ""))


# ---------------------------------------------------------------------------
# Rules service call
# ---------------------------------------------------------------------------
async def call_rules_service(application_id: str, data: dict) -> List[Treatment]:
    """POST to the Rules Service and return a list of Treatment objects."""
    url = f"{RULES_SERVICE_URL}/api/evaluate"
    payload = {"application_id": application_id, "data": data}
    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            response = await client.post(url, json=payload)
            response.raise_for_status()
            body = response.json()
            treatments_raw = body.get("treatments", [])
            return [Treatment(**t) for t in treatments_raw]
        except httpx.HTTPStatusError as exc:
            logger.error(
                "Rules service returned %s for application %s: %s",
                exc.response.status_code,
                application_id,
                exc.response.text,
            )
            return []
        except Exception as exc:
            logger.error(
                "Error calling rules service for application %s: %s",
                application_id,
                exc,
            )
            return []


# ---------------------------------------------------------------------------
# Customer Application Consumer
# ---------------------------------------------------------------------------
async def consume_customer_applications() -> None:
    """Background task: consume from the customer-application topic."""
    consumer = AIOKafkaConsumer(
        CUSTOMER_APPLICATION_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="orchestration-group",
        value_deserializer=_deserialize,
        auto_offset_reset="earliest",
    )

    while True:
        try:
            await consumer.start()
            logger.info(
                "Customer-application consumer started on topic '%s'",
                CUSTOMER_APPLICATION_TOPIC,
            )
            break
        except Exception as exc:
            logger.warning(
                "Failed to start customer-application consumer (%s). Retrying in 5s...",
                exc,
            )
            await asyncio.sleep(5)

    try:
        async for msg in consumer:
            try:
                event: dict = msg.value
                application_id: str = event.get("application_id", "")
                event_type: str = event.get("event_type", "")
                event_data: dict = event.get("data", {})

                if not application_id:
                    logger.warning("Received event without application_id – skipping")
                    continue

                logger.info(
                    "Received event_type=%s for application_id=%s",
                    event_type,
                    application_id,
                )

                # 1. Create new state entry if unseen
                if application_id not in state_store:
                    state_store[application_id] = ApplicationState(state=FSMState.INITIATED)
                    logger.info("Application %s created in INITIATED state", application_id)

                app_state = state_store[application_id]

                # 2. Merge screen data
                app_state.screens_received.add(event_type)
                app_state.application_data.update(event_data)

                # 3. Transition to COLLECTING_DATA
                app_state.state = FSMState.COLLECTING_DATA
                logger.info(
                    "Application %s -> COLLECTING_DATA (screens: %s)",
                    application_id,
                    app_state.screens_received,
                )

                # 4. Check if all screens received
                if ALL_SCREEN_TYPES.issubset(app_state.screens_received):
                    # Transition to EVALUATING_RULES
                    app_state.state = FSMState.EVALUATING_RULES
                    logger.info("Application %s -> EVALUATING_RULES", application_id)

                    treatments = await call_rules_service(
                        application_id, app_state.application_data
                    )

                    if treatments:
                        # Transition to TREATMENTS_PENDING
                        app_state.state = FSMState.TREATMENTS_PENDING
                        app_state.treatments_required = treatments
                        logger.info(
                            "Application %s -> TREATMENTS_PENDING (%d treatments)",
                            application_id,
                            len(treatments),
                        )

                        # Publish a treatment-required event for each treatment
                        for treatment in treatments:
                            treatment_event = {
                                "event_id": str(uuid.uuid4()),
                                "application_id": application_id,
                                "treatment_id": treatment.treatment_id,
                                "treatment_type": treatment.treatment_type,
                                "description": treatment.description,
                                "data": app_state.application_data,
                                "timestamp": _now_iso(),
                            }
                            await _publish(TREATMENT_REQUIRED_TOPIC, treatment_event)
                    else:
                        # No treatments needed – go straight to COMPLETED
                        app_state.state = FSMState.COMPLETED
                        logger.info(
                            "Application %s -> COMPLETED (no treatments needed)",
                            application_id,
                        )
                        complete_event = {
                            "event_id": str(uuid.uuid4()),
                            "application_id": application_id,
                            "status": "COMPLETE",
                            "message": "Application Complete",
                            "timestamp": _now_iso(),
                        }
                        await _publish(
                            CUSTOMER_APPLICATION_RESPONSE_TOPIC, complete_event
                        )

            except Exception as exc:
                logger.exception(
                    "Error processing customer-application message: %s", exc
                )
    finally:
        await consumer.stop()
        logger.info("Customer-application consumer stopped")


# ---------------------------------------------------------------------------
# Treatment Results Consumer
# ---------------------------------------------------------------------------
async def consume_treatment_results() -> None:
    """Background task: consume from the treatment-results topic."""
    consumer = AIOKafkaConsumer(
        TREATMENT_RESULTS_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="orchestration-group",
        value_deserializer=_deserialize,
        auto_offset_reset="earliest",
    )

    while True:
        try:
            await consumer.start()
            logger.info(
                "Treatment-results consumer started on topic '%s'",
                TREATMENT_RESULTS_TOPIC,
            )
            break
        except Exception as exc:
            logger.warning(
                "Failed to start treatment-results consumer (%s). Retrying in 5s...",
                exc,
            )
            await asyncio.sleep(5)

    try:
        async for msg in consumer:
            try:
                event: dict = msg.value
                application_id: str = event.get("application_id", "")
                treatment_id: str = event.get("treatment_id", "")

                if not application_id or not treatment_id:
                    logger.warning(
                        "Treatment result missing application_id or treatment_id – skipping"
                    )
                    continue

                logger.info(
                    "Treatment result received: application_id=%s treatment_id=%s result=%s",
                    application_id,
                    treatment_id,
                    event.get("result", "UNKNOWN"),
                )

                app_state = state_store.get(application_id)
                if app_state is None:
                    logger.warning(
                        "Received treatment result for unknown application %s",
                        application_id,
                    )
                    continue

                # 1. Record completed treatment
                if treatment_id not in app_state.treatments_completed:
                    app_state.treatments_completed.append(treatment_id)

                # 2. Check if all treatments are done
                required_ids = {t.treatment_id for t in app_state.treatments_required}
                completed_ids = set(app_state.treatments_completed)

                if required_ids.issubset(completed_ids):
                    app_state.state = FSMState.COMPLETED
                    logger.info(
                        "Application %s -> COMPLETED (all treatments done)",
                        application_id,
                    )
                    complete_event = {
                        "event_id": str(uuid.uuid4()),
                        "application_id": application_id,
                        "status": "COMPLETE",
                        "message": "Application Complete",
                        "timestamp": _now_iso(),
                    }
                    await _publish(
                        CUSTOMER_APPLICATION_RESPONSE_TOPIC, complete_event
                    )

            except Exception as exc:
                logger.exception(
                    "Error processing treatment-results message: %s", exc
                )
    finally:
        await consumer.stop()
        logger.info("Treatment-results consumer stopped")


# ---------------------------------------------------------------------------
# FastAPI lifespan
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    global producer

    # Start the Kafka producer
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=_serialize,
    )
    while True:
        try:
            await producer.start()
            logger.info("Kafka producer started")
            break
        except Exception as exc:
            logger.warning("Failed to start Kafka producer (%s). Retrying in 5s...", exc)
            await asyncio.sleep(5)

    # Launch background consumer tasks
    task_app = asyncio.create_task(consume_customer_applications())
    task_treatment = asyncio.create_task(consume_treatment_results())
    _background_tasks.extend([task_app, task_treatment])

    yield

    # Shutdown: cancel consumers, stop producer
    for task in _background_tasks:
        task.cancel()
    await asyncio.gather(*_background_tasks, return_exceptions=True)
    _background_tasks.clear()

    if producer is not None:
        await producer.stop()
        logger.info("Kafka producer stopped")


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------
app = FastAPI(title="Orchestration Service (FSM)", lifespan=lifespan)


@app.get("/health")
async def health():
    return {"status": "healthy"}


@app.get("/api/state/{application_id}")
async def get_state(application_id: str):
    app_state = state_store.get(application_id)
    if app_state is None:
        raise HTTPException(status_code=404, detail="Application not found")
    return {
        "application_id": application_id,
        "state": app_state.state.value,
        "screens_received": sorted(app_state.screens_received),
        "application_data": app_state.application_data,
        "treatments_required": [t.model_dump() for t in app_state.treatments_required],
        "treatments_completed": app_state.treatments_completed,
    }
