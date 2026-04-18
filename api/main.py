import logging
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from agent.agent import run

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="LawFlow API",
    description="Real-time legal intelligence pipeline — query regulatory documents via AI agent",
    version="1.0.0",
)

# =============================================================================
# CORS MIDDLEWARE
# =============================================================================
# Allows the frontend HTML file to call this API from a different port
# without being blocked by the browser's same-origin policy.
# allow_origins=["*"] permits any origin — fine for local development.
# In production restrict to your actual frontend domain.

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# =============================================================================
# REQUEST / RESPONSE MODELS
# =============================================================================
# Pydantic models define the shape of incoming and outgoing JSON.
# FastAPI validates all requests against these models automatically.
# If request body doesn't match FastAPI returns 422 Unprocessable Entity.

class QuestionRequest(BaseModel):
    question: str

    class Config:
        json_schema_extra = {
            "example": {
                "question": "What EPA regulations changed recently about air quality?"
            }
        }


class AnswerResponse(BaseModel):
    question: str
    answer: str


# =============================================================================
# ENDPOINTS
# =============================================================================

@app.get("/health")
async def health():
    """
    Health check endpoint.
    Used by Docker healthcheck and monitoring to verify the API is running.
    Returns 200 with status ok when the service is healthy.
    """
    return {"status": "ok", "service": "LawFlow API"}


@app.post("/query", response_model=AnswerResponse)
async def query(body: QuestionRequest):
    """
    Main query endpoint.
    Receives a plain English question about US federal regulations.
    Passes it to the LangChain agent which searches Qdrant and ArcadeDB
    then synthesizes an answer using Claude.
    Returns the answer with the original question for context.

    Example request:
    {
        "question": "What did the EPA publish about air quality this month?"
    }

    Example response:
    {
        "question": "What did the EPA publish about air quality this month?",
        "answer": "The EPA published two rules this month..."
    }
    """
    if not body.question.strip():
        raise HTTPException(status_code=400, detail="Question cannot be empty")

    logger.info(f"Received question: {body.question}")

    try:
        answer = run(body.question)
        logger.info("Query completed successfully")
        return AnswerResponse(question=body.question, answer=answer)
    except Exception as e:
        logger.error(f"Query failed: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to process question: {str(e)}"
        )