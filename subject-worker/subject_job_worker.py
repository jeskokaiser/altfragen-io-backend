#!/usr/bin/env python3
"""
Subject Job Worker

HTTP-triggered worker that processes subject assignment and reassignment jobs
from the subject_jobs table. Handles both 'assign' and 'reassign' job types.

Can be triggered via HTTP endpoint or run in polling mode as fallback.

Configuration:
- BATCH_SIZE: Number of questions to process in parallel (default: 2)
- MAX_RETRIES: Maximum retry attempts for OpenAI API calls (default: 3)
- RETRY_DELAY: Initial delay between retries in ms (default: 1000)
- REQUEST_DELAY: Delay between batches in ms (default: 1200)
- CHUNK_SIZE: Number of questions to process per chunk (default: 15)
- CHUNK_DELAY: Delay between chunks in ms (default: 3000)
- POLL_INTERVAL: Seconds to wait between polling for new jobs (fallback mode, default: 60)
"""

import asyncio
import logging
import os
import sys
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
from dotenv import load_dotenv
import uvicorn
from openai import OpenAI

from supabase_client import SupabaseClient

# Load environment variables
load_dotenv()

# Configuration constants
CONFIG = {
    "BATCH_SIZE": 5,
    "MAX_RETRIES": 3,
    "RETRY_DELAY": 1000,  # milliseconds
    "REQUEST_DELAY": 1200,  # milliseconds
    "CHUNK_SIZE": 20,
    "CHUNK_DELAY": 1000,  # milliseconds
    "POLL_INTERVAL": 60,  # seconds (fallback polling mode)
}

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(title="Subject Job Worker", version="1.0.0")

# Global clients (initialized on startup)
supabase: Optional[SupabaseClient] = None
openai_client: Optional[OpenAI] = None


async def retry_with_backoff(
    fn, retries: int = CONFIG["MAX_RETRIES"], delay: int = CONFIG["RETRY_DELAY"]
):
    """Retry a function with exponential backoff. Handles both sync and async functions."""
    try:
        if asyncio.iscoroutinefunction(fn):
            return await fn()
        else:
            # Run sync function in executor to avoid blocking
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(None, fn)
    except Exception as error:
        if retries <= 0:
            raise error
        logger.info(f"Retrying in {delay}ms... ({retries} retries left)")
        await asyncio.sleep(delay / 1000.0)
        return await retry_with_backoff(fn, retries - 1, delay * 2)


async def assign_subject_to_question(
    question: Dict[str, Any],
    available_subjects: List[str],
    openai_client: OpenAI,
    supabase: SupabaseClient,
    user_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Assign a subject to a question using OpenAI.

    Returns a dict with the question data and success status.
    """
    # Build prompt based on job type (assign vs reassign have slightly different prompts)
    # For now, use the simpler assign prompt (English)
    prompt = f"""Du bist ein Fachklassifizierer für akademische Fragen. Wähle anhand der folgenden Frage und der Liste der verfügbaren Fächer das am besten geeignete Fach aus.

Frage: "{question.get('question', '')}"

Verfügbare Fächer: {', '.join(available_subjects)}

Antworte mit NUR dem exakten Fachnamen aus der Liste oben, das am besten zu dieser Frage passt. Füge keine Erklärung oder zusätzlichen Text hinzu."""

    system_prompt = "Du bist ein präziser Fachklassifizierer. Antworte immer nur mit dem exakten Fachnamen aus der Liste oben."

    def call_openai():
        response = openai_client.chat.completions.create(
            model="gpt-5-nano",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": prompt},
            ],


        )
        return response.choices[0].message.content.strip()

    try:
        assigned_subject = await retry_with_backoff(call_openai)

        # Validate that the assigned subject is in the available list
        if assigned_subject not in available_subjects:
            logger.warning(
                f"Invalid subject '{assigned_subject}' for question {question.get('id')}, using first available subject"
            )
            assigned_subject = available_subjects[0]

        # Update the question in the database
        async def update_question():
            await supabase.update_question_subject(
                str(question.get("id")), assigned_subject, user_id
            )

        await retry_with_backoff(update_question)

        logger.info(
            f"Successfully updated question {question.get('id')} with subject: {assigned_subject}"
        )

        return {
            **question,
            "subject": assigned_subject,
            "success": True,
        }

    except Exception as error:
        logger.error(f"Error processing question {question.get('id')}: {error}")
        return {
            **question,
            "subject": available_subjects[0],  # Fallback subject
            "success": False,
            "error": str(error),
        }


async def process_assign_job(
    job: Dict[str, Any],
    supabase: SupabaseClient,
    openai_client: OpenAI,
) -> None:
    """Process an 'assign' type job."""
    job_id = job["id"]
    payload = job.get("payload") or {}
    questions = payload.get("questions", [])
    available_subjects = job.get("available_subjects", [])
    user_id = job.get("user_id")

    if not questions or not available_subjects:
        logger.error(f"Job {job_id}: Missing questions or available_subjects")
        await supabase.update_subject_job_status(
            job_id,
            "failed",
            message="Missing required fields: questions or available_subjects",
        )
        return

    total = len(questions)
    processed = 0
    errors = 0

    logger.info(f"Processing assign job {job_id} with {total} questions")

    # Process questions in chunks
    for chunk_start in range(0, total, CONFIG["CHUNK_SIZE"]):
        chunk = questions[chunk_start : chunk_start + CONFIG["CHUNK_SIZE"]]
        chunk_num = (chunk_start // CONFIG["CHUNK_SIZE"]) + 1
        total_chunks = (total + CONFIG["CHUNK_SIZE"] - 1) // CONFIG["CHUNK_SIZE"]

        logger.info(
            f"Processing chunk {chunk_num}/{total_chunks} ({len(chunk)} questions)"
        )

        # Process questions in smaller batches within each chunk
        for batch_start in range(0, len(chunk), CONFIG["BATCH_SIZE"]):
            batch = chunk[batch_start : batch_start + CONFIG["BATCH_SIZE"]]

            # Process batch with controlled concurrency
            batch_tasks = []
            for idx, question in enumerate(batch):
                # Stagger requests to avoid rate limiting
                async def process_with_delay(q, delay_idx):
                    await asyncio.sleep(delay_idx * 0.3)  # 300ms delay
                    return await assign_subject_to_question(
                        q, available_subjects, openai_client, supabase, user_id
                    )

                batch_tasks.append(process_with_delay(question, idx))

            batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)

            # Process results
            for result in batch_results:
                if isinstance(result, Exception):
                    logger.error(f"Batch processing error: {result}")
                    errors += 1
                else:
                    if not result.get("success", False):
                        errors += 1
                processed += 1

            # Update progress
            await supabase.update_subject_job_status(
                job_id,
                "processing",
                progress=processed,
                errors=errors,
                message=f"Processed {processed}/{total} questions ({errors} errors)",
            )

            logger.info(f"Progress: {processed}/{total} questions processed ({errors} errors)")

            # Delay between batches
            if batch_start + CONFIG["BATCH_SIZE"] < len(chunk):
                await asyncio.sleep(CONFIG["REQUEST_DELAY"] / 1000.0)

        # Delay between chunks
        if chunk_start + CONFIG["CHUNK_SIZE"] < total:
            logger.info(
                f"Completed chunk, waiting {CONFIG['CHUNK_DELAY']}ms before next chunk..."
            )
            await asyncio.sleep(CONFIG["CHUNK_DELAY"] / 1000.0)

    success_count = total - errors
    logger.info(
        f"Completed processing: {success_count}/{total} questions successfully updated, {errors} errors"
    )

    # Final progress update
    await supabase.update_subject_job_status(
        job_id,
        "completed",
        progress=processed,
        errors=errors,
        message=f"Successfully processed {success_count} out of {total} questions"
        + (f" ({errors} questions had errors and used fallback subjects)" if errors > 0 else ""),
        result={
            "total": total,
            "successful": success_count,
            "errors": errors,
            "processed": processed,
        },
    )


async def process_reassign_job(
    job: Dict[str, Any],
    supabase: SupabaseClient,
    openai_client: OpenAI,
) -> None:
    """Process a 'reassign' type job."""
    job_id = job["id"]
    payload = job.get("payload") or {}
    # For reassign jobs, data is stored directly in job fields, not in payload
    exam_name = job.get("exam_name")
    university_id = job.get("university_id")
    only_null_subjects = job.get("only_null_subjects", False)
    available_subjects = job.get("available_subjects", [])

    if not exam_name or not available_subjects:
        logger.error(f"Job {job_id}: Missing exam_name or available_subjects")
        await supabase.update_subject_job_status(
            job_id,
            "failed",
            message="Missing required fields: exam_name or available_subjects",
        )
        return

    # Fetch matching questions
    logger.info(
        f"Fetching questions for reassign job {job_id}: exam_name={exam_name}, university_id={university_id}, only_null_subjects={only_null_subjects}"
    )
    questions = await supabase.fetch_questions_for_reassign_job(
        exam_name, university_id, only_null_subjects
    )

    if not questions:
        logger.warning(f"Job {job_id}: No questions found matching criteria")
        await supabase.update_subject_job_status(
            job_id,
            "completed",
            progress=0,
            errors=0,
            message="No questions found matching the criteria",
            result={"total": 0, "successful": 0, "errors": 0, "processed": 0},
        )
        return

    total = len(questions)
    processed = 0
    errors = 0

    logger.info(f"Processing reassign job {job_id} with {total} questions")

    # Update job with total count
    await supabase.update_subject_job_status(
        job_id,
        "processing",
        progress=0,
        errors=0,
        message=f"Processing {total} questions for exam: {exam_name}"
        + (" (null subjects only)" if only_null_subjects else ""),
    )

    # Process questions in chunks (same logic as assign)
    for chunk_start in range(0, total, CONFIG["CHUNK_SIZE"]):
        chunk = questions[chunk_start : chunk_start + CONFIG["CHUNK_SIZE"]]
        chunk_num = (chunk_start // CONFIG["CHUNK_SIZE"]) + 1
        total_chunks = (total + CONFIG["CHUNK_SIZE"] - 1) // CONFIG["CHUNK_SIZE"]

        logger.info(
            f"Processing chunk {chunk_num}/{total_chunks} ({len(chunk)} questions)"
        )

        # Process questions in smaller batches within each chunk
        for batch_start in range(0, len(chunk), CONFIG["BATCH_SIZE"]):
            batch = chunk[batch_start : batch_start + CONFIG["BATCH_SIZE"]]

            # Process batch with controlled concurrency
            batch_tasks = []
            for idx, question in enumerate(batch):
                # Stagger requests to avoid rate limiting
                async def process_with_delay(q, delay_idx):
                    await asyncio.sleep(delay_idx * 0.3)  # 300ms delay
                    return await assign_subject_to_question(
                        q, available_subjects, openai_client, supabase, None
                    )

                batch_tasks.append(process_with_delay(question, idx))

            batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)

            # Process results
            for result in batch_results:
                if isinstance(result, Exception):
                    logger.error(f"Batch processing error: {result}")
                    errors += 1
                else:
                    if not result.get("success", False):
                        errors += 1
                processed += 1

            # Update progress
            await supabase.update_subject_job_status(
                job_id,
                "processing",
                progress=processed,
                errors=errors,
                message=f"Processed {processed}/{total} questions ({errors} errors)",
            )

            logger.info(f"Progress: {processed}/{total} questions processed ({errors} errors)")

            # Delay between batches
            if batch_start + CONFIG["BATCH_SIZE"] < len(chunk):
                await asyncio.sleep(CONFIG["REQUEST_DELAY"] / 1000.0)

        # Delay between chunks
        if chunk_start + CONFIG["CHUNK_SIZE"] < total:
            logger.info(
                f"Completed chunk, waiting {CONFIG['CHUNK_DELAY']}ms before next chunk..."
            )
            await asyncio.sleep(CONFIG["CHUNK_DELAY"] / 1000.0)

    success_count = total - errors
    logger.info(
        f"Completed processing: {success_count}/{total} questions successfully updated, {errors} errors"
    )

    # Final progress update
    await supabase.update_subject_job_status(
        job_id,
        "completed",
        progress=processed,
        errors=errors,
        message=f"Successfully processed {success_count} out of {total} questions"
        + (f" ({errors} questions had errors and used fallback subjects)" if errors > 0 else "")
        + (" (filtered for null subjects only)" if only_null_subjects else ""),
        result={
            "total": total,
            "successful": success_count,
            "errors": errors,
            "processed": processed,
        },
    )


async def process_job_by_id(job_id: str):
    """Process a specific job by ID."""
    if not supabase or not openai_client:
        logger.error("Clients not initialized")
        return

    try:
        # Fetch the job from database
        pending_jobs = await supabase.fetch_pending_subject_jobs()
        job = next((j for j in pending_jobs if j["id"] == job_id), None)

        if not job:
            # Try to fetch any job with this ID (might already be processing)
            logger.warning(f"Job {job_id} not found in pending jobs, may already be processed")
            return

        job_type = job.get("type", "unknown")

        # Mark job as processing
        await supabase.update_subject_job_status(
            job_id, "processing", message="Starting processing"
        )

        # Process based on job type
        if job_type == "assign":
            await process_assign_job(job, supabase, openai_client)
        elif job_type == "reassign":
            await process_reassign_job(job, supabase, openai_client)
        else:
            logger.error(f"Unknown job type: {job_type}")
            await supabase.update_subject_job_status(
                job_id,
                "failed",
                message=f"Unknown job type: {job_type}",
            )

    except Exception as error:
        logger.error(f"Fatal error processing job {job_id}: {error}", exc_info=True)
        if supabase:
            await supabase.update_subject_job_status(
                job_id,
                "failed",
                message=f"Processing failed: {str(error)}",
            )


# FastAPI endpoints
@app.get("/")
async def root():
    """Health check endpoint"""
    return {"status": "ok", "service": "subject-job-worker"}


@app.get("/health")
async def health():
    """Health check endpoint"""
    return {"status": "healthy", "service": "subject-job-worker"}


@app.post("/process-job/{job_id}")
async def trigger_process_job(job_id: str, background_tasks: BackgroundTasks):
    """
    Trigger processing of a specific job by ID.
    This endpoint is called by Edge Functions when a new job is created.
    """
    try:
        logger.info(f"Received request to process job {job_id}")
        # Run in background task to avoid blocking
        background_tasks.add_task(process_job_by_id, job_id)
        return JSONResponse(
            status_code=202,
            content={
                "status": "accepted",
                "message": f"Job {job_id} queued for processing",
                "job_id": job_id
            }
        )
    except Exception as e:
        logger.error(f"Error triggering job processing: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/process-pending")
async def trigger_process_pending(background_tasks: BackgroundTasks):
    """
    Process all pending jobs. Useful as a fallback or manual trigger.
    """
    try:
        logger.info("Received request to process all pending jobs")
        background_tasks.add_task(process_all_pending_jobs)
        return JSONResponse(
            status_code=202,
            content={
                "status": "accepted",
                "message": "Processing all pending jobs in background"
            }
        )
    except Exception as e:
        logger.error(f"Error triggering pending jobs processing: {e}")
        raise HTTPException(status_code=500, detail=str(e))


async def process_all_pending_jobs():
    """Process all pending jobs (fallback/polling mode)."""
    if not supabase or not openai_client:
        logger.error("Clients not initialized")
        return

    try:
        pending_jobs = await supabase.fetch_pending_subject_jobs()

        if not pending_jobs:
            logger.debug("No pending jobs found")
            return

        logger.info(f"Found {len(pending_jobs)} pending job(s)")

        for job in pending_jobs:
            await process_job_by_id(job["id"])

    except Exception as error:
        logger.error(f"Error processing pending jobs: {error}", exc_info=True)


async def polling_loop():
    """Background polling loop as fallback if HTTP triggers fail."""
    while True:
        try:
            await asyncio.sleep(CONFIG["POLL_INTERVAL"])
            if supabase and openai_client:
                await process_all_pending_jobs()
        except Exception as error:
            logger.error(f"Error in polling loop: {error}", exc_info=True)
            await asyncio.sleep(CONFIG["POLL_INTERVAL"])


@app.on_event("startup")
async def startup_event():
    """Initialize clients on startup."""
    global supabase, openai_client
    try:
        supabase = SupabaseClient()
        openai_api_key = os.getenv("OPENAI_API_KEY")
        if not openai_api_key:
            logger.error("OPENAI_API_KEY not set in environment")
            raise RuntimeError("OPENAI_API_KEY not configured")
        openai_client = OpenAI(api_key=openai_api_key)
        logger.info("Subject job worker started and ready")
        
        # Start background polling loop as fallback
        asyncio.create_task(polling_loop())
        logger.info(f"Started background polling loop (interval: {CONFIG['POLL_INTERVAL']}s)")
    except Exception as e:
        logger.error(f"Failed to initialize clients: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8001)

