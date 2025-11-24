import asyncio
import json
import logging
from typing import Any, Dict, List

from openai import OpenAI
from google import genai
from mistralai import Mistral

from supabase_client import SupabaseClient
from openai_batch import load_batch_results
from gemini_batch import parse_inline_responses
from mistral_batch import parse_results_file


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("ai_commentary_consume")


async def process_openai_batches(supabase: SupabaseClient) -> None:
    client = OpenAI()
    jobs = await supabase.get_open_batch_jobs(provider="openai")
    if not jobs:
        logger.info("No open OpenAI batch jobs.")
        return

    # Get settings to check which models are enabled
    settings = await supabase.fetch_settings()
    models_enabled = settings.get("models_enabled") or {}

    for job in jobs:
        batch_id = job["batch_id"]
        logger.info("Checking OpenAI batch %s", batch_id)
        batch = client.batches.retrieve(batch_id)
        status = batch.status
        if status in {"validating", "in_progress", "finalizing"}:
            logger.info("OpenAI batch %s still in status %s", batch_id, status)
            continue
        if status != "completed":
            logger.warning("OpenAI batch %s finished with status %s", batch_id, status)
            await supabase.update_batch_job(
                batch_id=batch_id,
                provider="openai",
                status=status,
            )
            continue

        output_file_id = batch.output_file_id
        if not output_file_id:
            logger.warning("OpenAI batch %s has no output_file_id", batch_id)
            await supabase.update_batch_job(
                batch_id=batch_id,
                provider="openai",
                status="failed",
            )
            continue

        results = load_batch_results(client, output_file_id)
        success_count = 0
        for qid, payload in results.items():
            if "error" in payload:
                logger.error("OpenAI error for question %s: %s", qid, payload["error"])
                await supabase.update_question_status(qid, "failed", set_processed_at=False)
                continue
            await supabase.upsert_comments(qid, {"chatgpt": payload})
            
            # Check if all enabled models have completed before marking as completed
            if await supabase.check_all_models_completed(qid, models_enabled):
                await supabase.update_question_status(qid, "completed", set_processed_at=True)
                logger.info("Question %s: all enabled models completed", qid)
            else:
                logger.debug("Question %s: waiting for other models to complete", qid)
            success_count += 1

        await supabase.update_batch_job(
            batch_id=batch_id,
            provider="openai",
            status="completed",
            output_file_id=output_file_id,
        )
        logger.info(
            "Processed OpenAI batch %s with %d successful question(s).",
            batch_id,
            success_count,
        )


async def process_gemini_batches(supabase: SupabaseClient) -> None:
    client = genai.Client()
    jobs = await supabase.get_open_batch_jobs(provider="gemini")
    if not jobs:
        logger.info("No open Gemini batch jobs.")
        return

    # Get settings to check which models are enabled
    settings = await supabase.fetch_settings()
    models_enabled = settings.get("models_enabled") or {}

    for job in jobs:
        job_name = job["batch_id"]
        question_ids: List[str] = [str(qid) for qid in (job.get("question_ids") or [])]
        logger.info("Checking Gemini batch %s", job_name)
        batch_job = client.batches.get(name=job_name)
        state_name = batch_job.state.name
        if state_name in {
            "JOB_STATE_PENDING",
            "JOB_STATE_RUNNING",
        }:
            logger.info("Gemini batch %s still in state %s", job_name, state_name)
            continue

        if state_name != "JOB_STATE_SUCCEEDED":
            logger.warning("Gemini batch %s finished with state %s", job_name, state_name)
            await supabase.update_batch_job(
                batch_id=job_name,
                provider="gemini",
                status=state_name,
            )
            continue

        results = parse_inline_responses(batch_job, question_ids)
        success_count = 0
        for qid, payload in results.items():
            if "error" in payload:
                logger.error("Gemini error for question %s: %s", qid, payload["error"])
                await supabase.update_question_status(qid, "failed", set_processed_at=False)
                continue
            await supabase.upsert_comments(qid, {"gemini": payload})
            
            # Check if all enabled models have completed before marking as completed
            if await supabase.check_all_models_completed(qid, models_enabled):
                await supabase.update_question_status(qid, "completed", set_processed_at=True)
                logger.info("Question %s: all enabled models completed", qid)
            else:
                logger.debug("Question %s: waiting for other models to complete", qid)
            success_count += 1

        await supabase.update_batch_job(
            batch_id=job_name,
            provider="gemini",
            status="completed",
        )
        logger.info(
            "Processed Gemini batch %s with %d successful question(s).",
            job_name,
            success_count,
        )


async def process_mistral_batches(supabase: SupabaseClient) -> None:
    import os
    mistral_api_key = os.getenv("MISTRAL_API_KEY")
    if not mistral_api_key:
        logger.error("MISTRAL_API_KEY environment variable is not set, skipping Mistral batches")
        return
    client = Mistral(api_key=mistral_api_key)
    jobs = await supabase.get_open_batch_jobs(provider="mistral")
    if not jobs:
        logger.info("No open Mistral batch jobs.")
        return

    # Get settings to check which models are enabled
    settings = await supabase.fetch_settings()
    models_enabled = settings.get("models_enabled") or {}

    from pathlib import Path
    import tempfile

    for job in jobs:
        job_id = job["batch_id"]
        logger.info("Checking Mistral batch %s", job_id)
        batch_job = client.batch.jobs.get(job_id=job_id)
        status = batch_job.status
        logger.info(f"Mistral batch {job_id} status: {status}")
        logger.info(f"Batch job details: total_requests={getattr(batch_job, 'total_requests', 'N/A')}, "
                   f"succeeded_requests={getattr(batch_job, 'succeeded_requests', 'N/A')}, "
                   f"failed_requests={getattr(batch_job, 'failed_requests', 'N/A')}")
        
        if status in {"QUEUED", "RUNNING"}:
            logger.info("Mistral batch %s still in status %s", job_id, status)
            continue

        if status != "SUCCESS":
            logger.warning("Mistral batch %s finished with status %s", job_id, status)
            await supabase.update_batch_job(
                batch_id=job_id,
                provider="mistral",
                status=status,
            )
            continue

        output_file_id = batch_job.output_file
        if not output_file_id:
            logger.warning("Mistral batch %s has no output_file", job_id)
            await supabase.update_batch_job(
                batch_id=job_id,
                provider="mistral",
                status="FAILED",
            )
            continue

        # Download results into a temporary file
        logger.info(f"Downloading Mistral batch output file {output_file_id}")
        output_stream = client.files.download(file_id=output_file_id)
        tmp_dir = Path(tempfile.mkdtemp())
        result_path = tmp_dir / f"mistral_{job_id}.jsonl"
        
        bytes_written = 0
        with result_path.open("wb") as f:
            for chunk in output_stream.stream:
                f.write(chunk)
                bytes_written += len(chunk)
        
        logger.info(f"Downloaded {bytes_written} bytes to {result_path}")
        logger.info(f"File size: {result_path.stat().st_size} bytes")
        
        # Log first few lines for debugging
        with result_path.open("r", encoding="utf-8") as f:
            first_lines = [f.readline() for _ in range(3)]
            logger.info(f"First 3 lines of output file (raw):\n{''.join(first_lines)}")
            # Try to parse and show structure
            for i, line in enumerate(first_lines, 1):
                if line.strip():
                    try:
                        parsed = json.loads(line)
                        logger.info(f"Line {i} parsed structure (keys): {list(parsed.keys())}")
                        logger.debug(f"Line {i} full structure: {json.dumps(parsed, indent=2, default=str)[:2000]}")
                    except Exception as e:
                        logger.warning(f"Line {i} could not be parsed: {e}")

        results = parse_results_file(result_path)
        logger.info(f"Parsed {len(results)} results from batch output")
        success_count = 0
        error_count = 0
        
        for qid, payload in results.items():
            if "error" in payload:
                logger.error("Mistral error for question %s: %s", qid, payload["error"])
                await supabase.update_question_status(qid, "failed", set_processed_at=False)
                error_count += 1
                continue
            
            try:
                logger.debug(f"Upserting Mistral comments for question {qid}")
                logger.info(f"Question {qid}: payload chosen_answer={repr(payload.get('chosen_answer'))} (type: {type(payload.get('chosen_answer')).__name__})")
                logger.debug(f"Question {qid}: full payload keys: {list(payload.keys())}")
                await supabase.upsert_comments(qid, {"mistral": payload})
                logger.debug(f"Successfully upserted comments for question {qid}")
                
                # Check if all enabled models have completed before marking as completed
                if await supabase.check_all_models_completed(qid, models_enabled):
                    await supabase.update_question_status(qid, "completed", set_processed_at=True)
                    logger.info("Question %s: all enabled models completed", qid)
                else:
                    logger.debug("Question %s: waiting for other models to complete", qid)
                success_count += 1
            except Exception as e:
                logger.error(f"Failed to upsert comments for question {qid}: {e}", exc_info=True)
                error_count += 1
        
        if error_count > 0:
            logger.warning(f"Mistral batch {job_id}: {error_count} errors, {success_count} successes")

        await supabase.update_batch_job(
            batch_id=job_id,
            provider="mistral",
            status="SUCCESS",
            output_file_id=output_file_id,
        )
        logger.info(
            "Processed Mistral batch %s with %d successful question(s).",
            job_id,
            success_count,
        )


async def main() -> None:
    supabase = SupabaseClient()
    try:
        await process_openai_batches(supabase)
        await process_gemini_batches(supabase)
        await process_mistral_batches(supabase)
    finally:
        await supabase.close()


if __name__ == "__main__":
    asyncio.run(main())






