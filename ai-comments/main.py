#!/usr/bin/env python3

import asyncio
import logging
import os
from typing import Any, Dict, List

from fastapi import FastAPI, HTTPException, BackgroundTasks, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from dotenv import load_dotenv
import uvicorn

# Import the main functions using absolute imports
# PYTHONPATH is set to /app in Dockerfile, so we can import directly
import ai_commentary_submit
import ai_commentary_consume
from pushover_notifier import get_notifier

submit_main = ai_commentary_submit.main
consume_main = ai_commentary_consume.main

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = FastAPI(title="AI Commentary Worker", version="1.0.0")

class ProcessBatchJob(BaseModel):
    id: str = Field(..., description="ai_commentary_job_queue.id")
    question_id: str = Field(..., description="questions.id")
    target_level: str = Field(..., description="'full' | 'partial'")


class ProcessBatchRequest(BaseModel):
    worker_id: str
    jobs: List[ProcessBatchJob]


def _require_backend_auth(req: Request) -> None:
    """
    Simple bearer-token auth for the Edge dispatcher -> backend call.
    """
    expected = os.getenv("AI_COMMENTARY_BACKEND_TOKEN")
    if not expected:
        # If unset, refuse requests to avoid accidentally exposing the endpoint.
        raise HTTPException(status_code=500, detail="Backend token is not configured")

    auth = req.headers.get("Authorization") or ""
    token = auth.replace("Bearer ", "")
    if not token or token != expected:
        raise HTTPException(status_code=401, detail="Unauthorized")


@app.get("/")
async def root():
    """Health check endpoint"""
    return {"status": "ok", "service": "ai-commentary-worker"}


@app.get("/health")
async def health():
    """Health check endpoint"""
    return {"status": "healthy", "service": "ai-commentary-worker"}


@app.post("/submit")
async def trigger_submit(background_tasks: BackgroundTasks):
    """
    Trigger the AI commentary submit process.
    This will claim questions and submit batch jobs to enabled providers.
    """
    try:
        logger.info("Triggering AI commentary submit process")
        # Run in background task to avoid blocking
        background_tasks.add_task(run_submit)
        return JSONResponse(
            status_code=202,
            content={
                "status": "accepted",
                "message": "Submit process started in background"
            }
        )
    except Exception as e:
        logger.error(f"Error triggering submit: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/consume")
async def trigger_consume(background_tasks: BackgroundTasks):
    """
    Trigger the AI commentary consume process.
    This will poll batch jobs and write results to Supabase.
    """
    try:
        logger.info("Triggering AI commentary consume process")
        # Run in background task to avoid blocking
        background_tasks.add_task(run_consume)
        return JSONResponse(
            status_code=202,
            content={
                "status": "accepted",
                "message": "Consume process started in background"
            }
        )
    except Exception as e:
        logger.error(f"Error triggering consume: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/run")
async def trigger_both(background_tasks: BackgroundTasks):
    """
    Trigger both submit and consume processes.
    Useful for manual triggers or cron-like behavior via HTTP.
    """
    try:
        logger.info("Triggering both submit and consume processes")
        background_tasks.add_task(run_submit)
        background_tasks.add_task(run_consume)
        return JSONResponse(
            status_code=202,
            content={
                "status": "accepted",
                "message": "Both processes started in background"
            }
        )
    except Exception as e:
        logger.error(f"Error triggering processes: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/process-batch")
async def process_batch(req: Request, payload: ProcessBatchRequest, background_tasks: BackgroundTasks):
    """
    Process an explicit list of claimed queue jobs (Edge dispatcher drives selection).
    """
    _require_backend_auth(req)
    if not payload.jobs:
        return JSONResponse(status_code=200, content={"status": "ok", "processed": 0})

    background_tasks.add_task(run_process_batch, payload.worker_id, [j.model_dump() for j in payload.jobs])
    return JSONResponse(
        status_code=202,
        content={
            "status": "accepted",
            "worker_id": payload.worker_id,
            "jobs": len(payload.jobs),
        },
    )


async def run_submit():
    """Wrapper to run submit in async context"""
    try:
        await submit_main()
    except Exception as e:
        logger.error(f"Error in submit process: {e}", exc_info=True)
        # Send Pushover notification
        notifier = get_notifier()
        await notifier.notify_error(
            context="Submit Process",
            error=e,
            details="The AI commentary submit process encountered a critical error."
        )


async def run_consume():
    """Wrapper to run consume in async context"""
    try:
        await consume_main()
    except Exception as e:
        logger.error(f"Error in consume process: {e}", exc_info=True)
        # Send Pushover notification
        notifier = get_notifier()
        await notifier.notify_error(
            context="Consume Process",
            error=e,
            details="The AI commentary consume process encountered a critical error."
        )

async def run_process_batch(worker_id: str, jobs: List[Dict[str, Any]]) -> None:
    try:
        await ai_commentary_submit.submit_claimed_jobs(worker_id=worker_id, jobs=jobs)
    except Exception as e:
        logger.error(f"Error in process-batch: {e}", exc_info=True)
        notifier = get_notifier()
        await notifier.notify_error(
            context="Process Batch",
            error=e,
            details="The process-batch endpoint encountered a critical error."
        )


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)

