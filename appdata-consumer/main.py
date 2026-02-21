import os
import json
import logging
import asyncio
from datetime import datetime
from contextlib import asynccontextmanager

import asyncpg
from aiokafka import AIOKafkaConsumer
from fastapi import FastAPI, HTTPException

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("appdata-consumer")

# ---------------------------------------------------------------------------
# Environment variables
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://appuser:apppass@localhost:5432/appdata")
CUSTOMER_APPLICATION_TOPIC = os.getenv("CUSTOMER_APPLICATION_TOPIC", "customer-application")
TREATMENT_REQUIRED_TOPIC = os.getenv("TREATMENT_REQUIRED_TOPIC", "treatment-required")
TREATMENT_RESULTS_TOPIC = os.getenv("TREATMENT_RESULTS_TOPIC", "treatment-results")

# ---------------------------------------------------------------------------
# Global state
# ---------------------------------------------------------------------------
db_pool: asyncpg.Pool = None
consumer_tasks: list[asyncio.Task] = []

# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------
SQL_CREATE_TABLES = """
CREATE TABLE IF NOT EXISTS applications (
    application_id VARCHAR(255) PRIMARY KEY,
    status VARCHAR(50) DEFAULT 'PENDING',
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS application_events (
    id SERIAL PRIMARY KEY,
    event_id VARCHAR(255) UNIQUE,
    application_id VARCHAR(255),
    event_type VARCHAR(100),
    topic_source VARCHAR(100),
    event_data JSONB,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS personal_details (
    application_id VARCHAR(255) PRIMARY KEY,
    forename VARCHAR(255),
    middle_name VARCHAR(255),
    surname VARCHAR(255),
    date_of_birth VARCHAR(50),
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS address_details (
    application_id VARCHAR(255) PRIMARY KEY,
    address_line_1 VARCHAR(500),
    address_line_2 VARCHAR(500),
    city VARCHAR(255),
    county VARCHAR(255),
    postcode VARCHAR(20),
    country VARCHAR(100),
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS business_details (
    application_id VARCHAR(255) PRIMARY KEY,
    company_name VARCHAR(500),
    company_number VARCHAR(50),
    registered_address VARCHAR(500),
    incorporation_date VARCHAR(50),
    company_type VARCHAR(100),
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS treatments (
    id SERIAL PRIMARY KEY,
    application_id VARCHAR(255),
    treatment_id VARCHAR(255),
    treatment_type VARCHAR(100),
    description TEXT,
    result VARCHAR(50),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);
"""


async def init_db():
    """Create the connection pool and ensure all tables exist."""
    global db_pool
    db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=2, max_size=10)
    async with db_pool.acquire() as conn:
        await conn.execute(SQL_CREATE_TABLES)
    logger.info("Database tables initialised")


async def close_db():
    """Close the connection pool."""
    global db_pool
    if db_pool:
        await db_pool.close()
        logger.info("Database pool closed")


# ---------------------------------------------------------------------------
# Kafka consumer helpers
# ---------------------------------------------------------------------------
def deserialize_message(raw: bytes) -> dict:
    """Deserialize a Kafka message value from JSON bytes to a dict."""
    try:
        return json.loads(raw.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        logger.error("Failed to deserialize message: %s", exc)
        return {}


async def create_consumer(topic: str, group_id: str) -> AIOKafkaConsumer:
    """Create, start, and return an AIOKafkaConsumer for the given topic (with retry)."""
    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=deserialize_message,
    )
    while True:
        try:
            await consumer.start()
            logger.info("Kafka consumer started for topic=%s group=%s", topic, group_id)
            return consumer
        except Exception as exc:
            logger.warning(
                "Failed to start consumer for topic=%s (%s). Retrying in 5s...",
                topic, exc,
            )
            await asyncio.sleep(5)


# ---------------------------------------------------------------------------
# Customer Application consumer logic
# ---------------------------------------------------------------------------
async def handle_customer_application(data: dict):
    """Process a single message from the customer-application topic."""
    application_id = data.get("application_id")
    event_id = data.get("event_id")
    event_type = data.get("event_type", "")

    if not application_id:
        logger.warning("customer-application message missing application_id, skipping")
        return

    async with db_pool.acquire() as conn:
        # Upsert applications table
        await conn.execute(
            """
            INSERT INTO applications (application_id, status, created_at, updated_at)
            VALUES ($1, $2, NOW(), NOW())
            ON CONFLICT (application_id)
            DO UPDATE SET status = EXCLUDED.status, updated_at = NOW()
            """,
            application_id,
            data.get("status", "PENDING"),
        )

        # Insert into application_events
        await conn.execute(
            """
            INSERT INTO application_events (event_id, application_id, event_type, topic_source, event_data, created_at)
            VALUES ($1, $2, $3, $4, $5, NOW())
            ON CONFLICT (event_id) DO NOTHING
            """,
            event_id,
            application_id,
            event_type,
            "customer-application",
            json.dumps(data),
        )

        # Insert detail rows based on event_type
        if event_type == "PERSONAL_DETAILS":
            details = data.get("data", data)
            await conn.execute(
                """
                INSERT INTO personal_details (application_id, forename, middle_name, surname, date_of_birth, created_at)
                VALUES ($1, $2, $3, $4, $5, NOW())
                ON CONFLICT (application_id)
                DO UPDATE SET forename = EXCLUDED.forename,
                              middle_name = EXCLUDED.middle_name,
                              surname = EXCLUDED.surname,
                              date_of_birth = EXCLUDED.date_of_birth
                """,
                application_id,
                details.get("forename"),
                details.get("middle_name"),
                details.get("surname"),
                details.get("date_of_birth"),
            )

        elif event_type == "ADDRESS_DETAILS":
            details = data.get("data", data)
            await conn.execute(
                """
                INSERT INTO address_details (application_id, address_line_1, address_line_2, city, county, postcode, country, created_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
                ON CONFLICT (application_id)
                DO UPDATE SET address_line_1 = EXCLUDED.address_line_1,
                              address_line_2 = EXCLUDED.address_line_2,
                              city = EXCLUDED.city,
                              county = EXCLUDED.county,
                              postcode = EXCLUDED.postcode,
                              country = EXCLUDED.country
                """,
                application_id,
                details.get("address_line_1"),
                details.get("address_line_2"),
                details.get("city"),
                details.get("county"),
                details.get("postcode"),
                details.get("country"),
            )

        elif event_type == "BUSINESS_DETAILS":
            details = data.get("data", data)
            await conn.execute(
                """
                INSERT INTO business_details (application_id, company_name, company_number, registered_address, incorporation_date, company_type, created_at)
                VALUES ($1, $2, $3, $4, $5, $6, NOW())
                ON CONFLICT (application_id)
                DO UPDATE SET company_name = EXCLUDED.company_name,
                              company_number = EXCLUDED.company_number,
                              registered_address = EXCLUDED.registered_address,
                              incorporation_date = EXCLUDED.incorporation_date,
                              company_type = EXCLUDED.company_type
                """,
                application_id,
                details.get("company_name"),
                details.get("company_number"),
                details.get("registered_address"),
                details.get("incorporation_date"),
                details.get("company_type"),
            )

    logger.info("Processed customer-application event_type=%s app=%s", event_type, application_id)


async def consume_customer_application():
    """Long-running task that consumes from the customer-application topic."""
    consumer = None
    try:
        consumer = await create_consumer(
            CUSTOMER_APPLICATION_TOPIC,
            "appdata-customer-application-group",
        )
        async for msg in consumer:
            try:
                await handle_customer_application(msg.value)
            except Exception:
                logger.exception("Error processing customer-application message")
    except asyncio.CancelledError:
        logger.info("customer-application consumer cancelled")
    except Exception:
        logger.exception("customer-application consumer fatal error")
    finally:
        if consumer:
            await consumer.stop()
            logger.info("customer-application consumer stopped")


# ---------------------------------------------------------------------------
# Treatment Required consumer logic
# ---------------------------------------------------------------------------
async def handle_treatment_required(data: dict):
    """Process a single message from the treatment-required topic."""
    application_id = data.get("application_id")
    event_id = data.get("event_id")
    treatment_id = data.get("treatment_id")
    event_type = data.get("event_type", "treatment_required")

    if not application_id:
        logger.warning("treatment-required message missing application_id, skipping")
        return

    async with db_pool.acquire() as conn:
        # Insert into application_events
        await conn.execute(
            """
            INSERT INTO application_events (event_id, application_id, event_type, topic_source, event_data, created_at)
            VALUES ($1, $2, $3, $4, $5, NOW())
            ON CONFLICT (event_id) DO NOTHING
            """,
            event_id,
            application_id,
            event_type,
            "treatment-required",
            json.dumps(data),
        )

        # Insert into treatments with result=NULL
        await conn.execute(
            """
            INSERT INTO treatments (application_id, treatment_id, treatment_type, description, result, created_at, updated_at)
            VALUES ($1, $2, $3, $4, NULL, NOW(), NOW())
            """,
            application_id,
            treatment_id,
            data.get("treatment_type"),
            data.get("description"),
        )

    logger.info("Processed treatment-required treatment_id=%s app=%s", treatment_id, application_id)


async def consume_treatment_required():
    """Long-running task that consumes from the treatment-required topic."""
    consumer = None
    try:
        consumer = await create_consumer(
            TREATMENT_REQUIRED_TOPIC,
            "appdata-treatment-required-group",
        )
        async for msg in consumer:
            try:
                await handle_treatment_required(msg.value)
            except Exception:
                logger.exception("Error processing treatment-required message")
    except asyncio.CancelledError:
        logger.info("treatment-required consumer cancelled")
    except Exception:
        logger.exception("treatment-required consumer fatal error")
    finally:
        if consumer:
            await consumer.stop()
            logger.info("treatment-required consumer stopped")


# ---------------------------------------------------------------------------
# Treatment Results consumer logic
# ---------------------------------------------------------------------------
async def handle_treatment_results(data: dict):
    """Process a single message from the treatment-results topic."""
    application_id = data.get("application_id")
    event_id = data.get("event_id")
    treatment_id = data.get("treatment_id")
    event_type = data.get("event_type", "treatment_results")
    result = data.get("result")

    if not application_id:
        logger.warning("treatment-results message missing application_id, skipping")
        return

    async with db_pool.acquire() as conn:
        # Insert into application_events
        await conn.execute(
            """
            INSERT INTO application_events (event_id, application_id, event_type, topic_source, event_data, created_at)
            VALUES ($1, $2, $3, $4, $5, NOW())
            ON CONFLICT (event_id) DO NOTHING
            """,
            event_id,
            application_id,
            event_type,
            "treatment-results",
            json.dumps(data),
        )

        # Update treatments table: set result and updated_at where treatment_id matches
        await conn.execute(
            """
            UPDATE treatments
            SET result = $1, updated_at = NOW()
            WHERE treatment_id = $2
            """,
            result,
            treatment_id,
        )

    logger.info("Processed treatment-results treatment_id=%s result=%s app=%s", treatment_id, result, application_id)


async def consume_treatment_results():
    """Long-running task that consumes from the treatment-results topic."""
    consumer = None
    try:
        consumer = await create_consumer(
            TREATMENT_RESULTS_TOPIC,
            "appdata-treatment-results-group",
        )
        async for msg in consumer:
            try:
                await handle_treatment_results(msg.value)
            except Exception:
                logger.exception("Error processing treatment-results message")
    except asyncio.CancelledError:
        logger.info("treatment-results consumer cancelled")
    except Exception:
        logger.exception("treatment-results consumer fatal error")
    finally:
        if consumer:
            await consumer.stop()
            logger.info("treatment-results consumer stopped")


# ---------------------------------------------------------------------------
# FastAPI lifespan
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(application: FastAPI):
    """Manage startup and shutdown of database and Kafka consumers."""
    # Startup
    await init_db()

    consumer_tasks.append(asyncio.create_task(consume_customer_application()))
    consumer_tasks.append(asyncio.create_task(consume_treatment_required()))
    consumer_tasks.append(asyncio.create_task(consume_treatment_results()))
    logger.info("All Kafka consumer tasks started")

    yield

    # Shutdown
    for task in consumer_tasks:
        task.cancel()
    await asyncio.gather(*consumer_tasks, return_exceptions=True)
    consumer_tasks.clear()

    await close_db()
    logger.info("Shutdown complete")


app = FastAPI(title="AppData Consumer Service", lifespan=lifespan)


# ---------------------------------------------------------------------------
# JSON serialization helper
# ---------------------------------------------------------------------------
def serialize_row(record: asyncpg.Record) -> dict:
    """Convert an asyncpg Record to a JSON-safe dict."""
    row = dict(record)
    for key, value in row.items():
        if isinstance(value, datetime):
            row[key] = value.isoformat()
    return row


# ---------------------------------------------------------------------------
# API endpoints
# ---------------------------------------------------------------------------
@app.get("/health")
async def health():
    return {"status": "healthy"}


@app.get("/api/applications")
async def list_applications():
    """Return all applications ordered by creation date descending."""
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM applications ORDER BY created_at DESC"
        )
    return [serialize_row(r) for r in rows]


@app.get("/api/applications/{application_id}")
async def get_application(application_id: str):
    """Return a single application with all related detail and treatment records."""
    async with db_pool.acquire() as conn:
        application = await conn.fetchrow(
            "SELECT * FROM applications WHERE application_id = $1",
            application_id,
        )
        if not application:
            raise HTTPException(status_code=404, detail="Application not found")

        personal = await conn.fetchrow(
            "SELECT * FROM personal_details WHERE application_id = $1",
            application_id,
        )
        address = await conn.fetchrow(
            "SELECT * FROM address_details WHERE application_id = $1",
            application_id,
        )
        business = await conn.fetchrow(
            "SELECT * FROM business_details WHERE application_id = $1",
            application_id,
        )
        treatments = await conn.fetch(
            "SELECT * FROM treatments WHERE application_id = $1 ORDER BY created_at DESC",
            application_id,
        )
        events = await conn.fetch(
            "SELECT * FROM application_events WHERE application_id = $1 ORDER BY created_at DESC",
            application_id,
        )

    return {
        "application": serialize_row(application),
        "personal_details": serialize_row(personal) if personal else None,
        "address_details": serialize_row(address) if address else None,
        "business_details": serialize_row(business) if business else None,
        "treatments": [serialize_row(t) for t in treatments],
        "events": [serialize_row(e) for e in events],
    }


@app.get("/api/events/{application_id}")
async def get_events(application_id: str):
    """Return all events for a given application ordered by creation date descending."""
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM application_events WHERE application_id = $1 ORDER BY created_at DESC",
            application_id,
        )
    if not rows:
        raise HTTPException(status_code=404, detail="No events found for this application")
    return [serialize_row(r) for r in rows]
