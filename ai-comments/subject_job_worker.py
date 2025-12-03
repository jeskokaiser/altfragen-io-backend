#!/usr/bin/env python3
"""
Subject Job Worker

Long-running worker that processes subject assignment and reassignment jobs
from the subject_jobs table. Handles both 'assign' and 'reassign' job types.

Configuration:
- BATCH_SIZE: Number of questions to process in parallel (default: 2)
- MAX_RETRIES: Maximum retry attempts for OpenAI API calls (default: 3)
- RETRY_DELAY: Initial delay between retries in ms (default: 1000)
- REQUEST_DELAY: Delay between batches in ms (default: 1200)
- CHUNK_SIZE: Number of questions to process per chunk (default: 15)
- CHUNK_DELAY: Delay between chunks in ms (default: 3000)
- POLL_INTERVAL: Seconds to wait between polling for new jobs (default: 5)
"""

import asyncio
import logging
import os
import sys
from typing import Any, Dict, List, Optional

from openai import OpenAI

from supabase_client import SupabaseClient

# Configuration constants
CONFIG = {
    "BATCH_SIZE": 2,
    "MAX_RETRIES": 3,
    "RETRY_DELAY": 1000,  # milliseconds
    "REQUEST_DELAY": 1200,  # milliseconds
    "CHUNK_SIZE": 15,
    "CHUNK_DELAY": 3000,  # milliseconds
    "POLL_INTERVAL": 5,  # seconds
}

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


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
    prompt = f"""You are a subject classifier for academic questions. Given the following question and list of available subjects, select the most appropriate subject.

Question: "{question.get('question', '')}"

Available subjects: {', '.join(available_subjects)}

Please respond with ONLY the exact subject name from the list above that best matches this question. Do not add any explanation or additional text."""

    system_prompt = "You are a precise subject classifier. Always respond with only the exact subject name from the provided list."

    def call_openai():
        response = openai_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": prompt},
            ],
            temperature=0.1,
            max_tokens=50,
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
    payload = job.get("payload", {})
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
    payload = job.get("payload", {})
    exam_name = payload.get("exam_name") or job.get("exam_name")
    university_id = payload.get("university_id") or job.get("university_id")
    only_null_subjects = payload.get("only_null_subjects", False) or job.get(
        "only_null_subjects", False
    )
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


async def main_loop():
    """Main worker loop that polls for jobs and processes them."""
    # Initialize clients
    supabase = SupabaseClient()
    openai_api_key = os.getenv("OPENAI_API_KEY")
    if not openai_api_key:
        logger.error("OPENAI_API_KEY not set in environment")
        sys.exit(1)
    openai_client = OpenAI(api_key=openai_api_key)

    logger.info("Subject job worker started")

    while True:
        try:
            # Fetch pending jobs
            pending_jobs = await supabase.fetch_pending_subject_jobs()

            if not pending_jobs:
                logger.debug(f"No pending jobs, sleeping for {CONFIG['POLL_INTERVAL']}s")
                await asyncio.sleep(CONFIG["POLL_INTERVAL"])
                continue

            logger.info(f"Found {len(pending_jobs)} pending job(s)")

            # Process each job
            for job in pending_jobs:
                job_id = job["id"]
                job_type = job.get("type", "unknown")

                try:
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
                    await supabase.update_subject_job_status(
                        job_id,
                        "failed",
                        message=f"Processing failed: {str(error)}",
                    )

        except Exception as error:
            logger.error(f"Error in main loop: {error}", exc_info=True)
            await asyncio.sleep(CONFIG["POLL_INTERVAL"])


if __name__ == "__main__":
    try:
        asyncio.run(main_loop())
    except KeyboardInterrupt:
        logger.info("Worker stopped by user")
    except Exception as error:
        logger.error(f"Fatal error: {error}", exc_info=True)
        sys.exit(1)

