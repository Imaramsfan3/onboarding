import logging
from uuid import uuid4

from fastapi import FastAPI
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Rules Engine
# ---------------------------------------------------------------------------


class Rule:
    """Base class for all rules. Subclasses must implement ``evaluate``."""

    name: str = "BaseRule"
    description: str = ""

    def evaluate(self, application_data: dict) -> list[dict]:
        """Returns list of treatments if rule fires, empty list otherwise."""
        raise NotImplementedError


class OverseasIndividualRule(Rule):
    """If home location country != UK then create treatment: Validate overseas individual."""

    name = "OverseasIndividualRule"
    description = "If the applicant's country is not the UK, require overseas individual validation."

    def evaluate(self, application_data: dict) -> list[dict]:
        country = application_data.get("country", "").strip().upper()
        uk_values = {
            "UK",
            "UNITED KINGDOM",
            "GB",
            "GREAT BRITAIN",
            "ENGLAND",
            "SCOTLAND",
            "WALES",
            "NORTHERN IRELAND",
        }
        if country and country not in uk_values:
            return [
                {
                    "treatment_id": str(uuid4()),
                    "treatment_type": "VALIDATE_OVERSEAS_INDIVIDUAL",
                    "description": "Validate overseas individual",
                }
            ]
        return []


class RulesEngine:
    """Holds a list of Rule instances and evaluates all of them."""

    def __init__(self) -> None:
        self.rules: list[Rule] = [OverseasIndividualRule()]

    def evaluate(self, application_data: dict) -> list[dict]:
        treatments: list[dict] = []
        for rule in self.rules:
            treatments.extend(rule.evaluate(application_data))
        return treatments


# ---------------------------------------------------------------------------
# FastAPI application
# ---------------------------------------------------------------------------

app = FastAPI(title="Rules Service", version="1.0.0")
engine = RulesEngine()


# --- Request / Response models ---

class EvaluateRequest(BaseModel):
    application_id: str
    data: dict


class TreatmentResponse(BaseModel):
    treatment_id: str
    treatment_type: str
    description: str


class EvaluateResponse(BaseModel):
    application_id: str
    treatments: list[TreatmentResponse]


class HealthResponse(BaseModel):
    status: str


class RuleInfo(BaseModel):
    name: str
    description: str


# --- Endpoints ---

@app.post("/api/evaluate", response_model=EvaluateResponse)
async def evaluate(request: EvaluateRequest):
    """Evaluate application data against all registered rules."""
    logger.info("Evaluating rules for application_id=%s", request.application_id)
    treatments = engine.evaluate(request.data)
    logger.info(
        "Application %s produced %d treatment(s)", request.application_id, len(treatments)
    )
    return EvaluateResponse(application_id=request.application_id, treatments=treatments)


@app.get("/health", response_model=HealthResponse)
async def health():
    """Health check endpoint."""
    return HealthResponse(status="healthy")


@app.get("/api/rules", response_model=list[RuleInfo])
async def list_rules():
    """Return the list of registered rules with descriptions."""
    return [
        RuleInfo(name=rule.name, description=rule.description) for rule in engine.rules
    ]
