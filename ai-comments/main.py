#!/usr/bin/env python3

import asyncio
import logging
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
from dotenv import load_dotenv
import uvicorn

# Import the main functions
# These modules are in the same package, so we can import them directly
# The relative imports within those modules will work because this directory
# is treated as a package (has __init__.py)
try:
    # Try package-style import first (when run as module)
    from . import ai_commentary_submit, ai_commentary_consume
except ImportError:
    # Fallback for direct script execution
    import ai_commentary_submit
    import ai_commentary_consume

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


async def run_submit():
    """Wrapper to run submit in async context"""
    try:
        await submit_main()
    except Exception as e:
        logger.error(f"Error in submit process: {e}", exc_info=True)


async def run_consume():
    """Wrapper to run consume in async context"""
    try:
        await consume_main()
    except Exception as e:
        logger.error(f"Error in consume process: {e}", exc_info=True)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)

