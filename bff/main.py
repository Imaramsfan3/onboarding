import asyncio
import json
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Dict, Optional

import httpx
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "customer-application")
RESPONSE_TOPIC = os.getenv("RESPONSE_TOPIC", "customer-application-response")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Pydantic Models
# ---------------------------------------------------------------------------


class PersonalDetails(BaseModel):
    application_id: str
    forename: str
    middle_name: Optional[str] = None
    surname: str
    date_of_birth: str


class AddressDetails(BaseModel):
    application_id: str
    address_line_1: str
    address_line_2: Optional[str] = None
    city: str
    county: Optional[str] = None
    postcode: str
    country: str


class BusinessDetails(BaseModel):
    application_id: str
    company_name: str
    company_number: Optional[str] = None
    registered_address: Optional[str] = None
    incorporation_date: Optional[str] = None
    company_type: Optional[str] = None


# ---------------------------------------------------------------------------
# Application State
# ---------------------------------------------------------------------------

application_statuses: Dict[str, dict] = {}

producer: Optional[AIOKafkaProducer] = None
consumer_task: Optional[asyncio.Task] = None

# ---------------------------------------------------------------------------
# FastAPI App
# ---------------------------------------------------------------------------

app = FastAPI(title="Onboarding BFF", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Kafka Helpers
# ---------------------------------------------------------------------------


async def start_producer() -> AIOKafkaProducer:
    """Create and start the Kafka producer."""
    kafka_producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )
    await kafka_producer.start()
    logger.info("Kafka producer started (servers=%s)", KAFKA_BOOTSTRAP_SERVERS)
    return kafka_producer


async def publish_event(event_type: str, application_id: str, data: dict) -> dict:
    """Build a Kafka event envelope and publish it."""
    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "application_id": application_id,
        "data": data,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    if producer is not None:
        await producer.send_and_wait(
            KAFKA_TOPIC,
            value=event,
            key=application_id,
        )
        logger.info(
            "Published %s event for application %s", event_type, application_id
        )
    else:
        logger.warning(
            "Kafka producer not available; event for %s was NOT published",
            application_id,
        )

    return event


# ---------------------------------------------------------------------------
# Response Consumer (background)
# ---------------------------------------------------------------------------


async def consume_responses() -> None:
    """Long-running task that reads from the response topic and updates statuses."""
    consumer = AIOKafkaConsumer(
        RESPONSE_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="bff-response-consumer",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
    )

    while True:
        try:
            await consumer.start()
            logger.info(
                "Kafka response consumer started (topic=%s)", RESPONSE_TOPIC
            )
            async for message in consumer:
                try:
                    payload = message.value
                    app_id = payload.get("application_id")
                    status = payload.get("status")
                    msg = payload.get("message", "")
                    if app_id and status:
                        application_statuses[app_id] = {
                            "status": status,
                            "message": msg,
                        }
                        logger.info(
                            "Updated status for %s -> %s", app_id, status
                        )
                except Exception as exc:
                    logger.error("Error processing response message: %s", exc)
        except Exception as exc:
            logger.error("Response consumer error: %s — retrying in 5s", exc)
            await asyncio.sleep(5)
        finally:
            try:
                await consumer.stop()
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Lifecycle Events
# ---------------------------------------------------------------------------


@app.on_event("startup")
async def on_startup() -> None:
    global producer, consumer_task

    # Start the Kafka producer
    try:
        producer = await start_producer()
    except Exception as exc:
        logger.error("Failed to start Kafka producer: %s", exc)
        producer = None

    # Start the background response consumer
    consumer_task = asyncio.create_task(consume_responses())


@app.on_event("shutdown")
async def on_shutdown() -> None:
    global producer, consumer_task

    if producer is not None:
        await producer.stop()
        logger.info("Kafka producer stopped")

    if consumer_task is not None:
        consumer_task.cancel()
        try:
            await consumer_task
        except asyncio.CancelledError:
            pass
        logger.info("Response consumer task cancelled")


# ---------------------------------------------------------------------------
# Endpoints — Form Submissions
# ---------------------------------------------------------------------------


@app.post("/api/personal-details")
async def submit_personal_details(details: PersonalDetails):
    data = details.model_dump(exclude={"application_id"})
    event = await publish_event(
        "PERSONAL_DETAILS", details.application_id, data
    )
    return {"status": "accepted", "event_id": event["event_id"]}


@app.post("/api/address")
async def submit_address(details: AddressDetails):
    data = details.model_dump(exclude={"application_id"})
    event = await publish_event(
        "ADDRESS_DETAILS", details.application_id, data
    )
    return {"status": "accepted", "event_id": event["event_id"]}


@app.post("/api/business-details")
async def submit_business_details(details: BusinessDetails):
    data = details.model_dump(exclude={"application_id"})
    event = await publish_event(
        "BUSINESS_DETAILS", details.application_id, data
    )
    return {"status": "accepted", "event_id": event["event_id"]}


# ---------------------------------------------------------------------------
# Endpoints — Postcode Lookup
# ---------------------------------------------------------------------------


@app.get("/api/postcode-lookup/{postcode}")
async def postcode_lookup(postcode: str):
    url = f"https://api.postcodes.io/postcodes/{postcode}"
    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            response = await client.get(url)
        except httpx.RequestError as exc:
            raise HTTPException(
                status_code=502,
                detail=f"Error contacting postcodes.io: {exc}",
            )

    if response.status_code != 200:
        raise HTTPException(
            status_code=response.status_code,
            detail="Postcode not found",
        )

    body = response.json()
    result = body.get("result")

    if result is None:
        return {"addresses": []}

    # postcodes.io returns a single result object for an exact postcode lookup.
    # Build a single address suggestion from the available fields.
    address = {
        "address_line_1": result.get("parish", "") or result.get("admin_ward", ""),
        "address_line_2": result.get("admin_district", ""),
        "city": result.get("admin_district", ""),
        "county": result.get("admin_county", ""),
        "postcode": result.get("postcode", postcode),
        "country": result.get("country", ""),
    }

    return {"addresses": [address]}


# ---------------------------------------------------------------------------
# Endpoints — Company Search (mock for demo — Companies House requires API key)
# ---------------------------------------------------------------------------


@app.get("/api/company-search")
async def company_search(q: str = Query(..., min_length=1)):
    # In production this would proxy to Companies House with a valid API key.
    # For this demo we return realistic mock data derived from the query.
    mock_companies = [
        {
            "company_name": f"{q.title()} Ltd",
            "company_number": "12345678",
            "registered_address": "10 Downing Street, London, SW1A 2AA",
            "incorporation_date": "2015-03-12",
            "company_type": "ltd",
        },
        {
            "company_name": f"{q.title()} Holdings PLC",
            "company_number": "87654321",
            "registered_address": "1 Canada Square, Canary Wharf, London, E14 5AB",
            "incorporation_date": "2008-07-22",
            "company_type": "plc",
        },
        {
            "company_name": f"{q.title()} Services Limited",
            "company_number": "11223344",
            "registered_address": "100 New Bridge Street, London, EC4V 6JA",
            "incorporation_date": "2019-11-05",
            "company_type": "ltd",
        },
        {
            "company_name": f"{q.title()} & Partners LLP",
            "company_number": "OC334455",
            "registered_address": "25 Old Broad Street, London, EC2N 1HN",
            "incorporation_date": "2012-01-18",
            "company_type": "llp",
        },
        {
            "company_name": f"{q.title()} International Group PLC",
            "company_number": "55667788",
            "registered_address": "80 Strand, London, WC2R 0DT",
            "incorporation_date": "2003-09-30",
            "company_type": "plc",
        },
    ]

    return {"items": mock_companies, "total_results": len(mock_companies)}


# ---------------------------------------------------------------------------
# Endpoints — Application Status
# ---------------------------------------------------------------------------


@app.get("/api/application-status/{application_id}")
async def get_application_status(application_id: str):
    status = application_statuses.get(
        application_id,
        {"status": "PENDING", "message": "Application is being processed"},
    )
    return {"application_id": application_id, **status}


# ---------------------------------------------------------------------------
# Endpoints — Health
# ---------------------------------------------------------------------------


@app.get("/health")
async def health():
    return {"status": "healthy"}
