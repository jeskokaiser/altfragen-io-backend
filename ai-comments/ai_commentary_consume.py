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
from pushover_notifier import get_notifier
from quota_detector import is_quota_error, extract_quota_message


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("ai_commentary_consume")


async def handle_quota_error_consume(
    supabase: SupabaseClient,
    api_name: str,
    error: Exception,
) -> None:
    """
    Handle quota/credit errors by disabling the feature and sending notifications.
    
    Args:
        supabase: Supabase client instance
        api_name: Name of the API that ran out of credits
        error: The exception that occurred
    """
    if is_quota_error(error, api_name):
        logger.error(
            "Quota/credit error detected for %s: %s. Disabling feature.",
            api_name,
            error,
        )
        
        # Disable the feature
        disabled = await supabase.disable_feature()
        
        # Send notification
        notifier = get_notifier()
        quota_msg = extract_quota_message(error, api_name)
        await notifier.notify_critical(
            context="API Credits Exhausted",
            message=f"{api_name} has run out of credits/quota",
            details=f"{quota_msg}. AI commentary feature has been automatically disabled."
        )
        
        if disabled:
            logger.info("AI commentary feature has been disabled due to quota error")
        else:
            logger.warning("Failed to disable feature in database, but notification was sent")


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
        try:
            batch = client.batches.retrieve(batch_id)
        except Exception as e:
            logger.error("Failed to retrieve OpenAI batch %s: %s", batch_id, e, exc_info=True)
            await handle_quota_error_consume(supabase, "OpenAI", e)
            continue
        status = batch.status
        if status in {"validating", "in_progress", "finalizing"}:
            logger.info("OpenAI batch %s still in status %s", batch_id, status)
            continue
        if status != "completed":
            logger.warning("OpenAI batch %s finished with status %s", batch_id, status)
            
            # Check if status indicates quota error
            if status in {"failed", "expired", "cancelled"}:
                # Check error details if available
                error_info = getattr(batch, "errors", None) or getattr(batch, "error", None)
                if error_info:
                    error_str = str(error_info)
                    if is_quota_error(Exception(error_str), "OpenAI"):
                        await handle_quota_error_consume(supabase, "OpenAI", Exception(error_str))
            
            notifier = get_notifier()
            await notifier.notify_warning(
                context="OpenAI Batch Status",
                message=f"OpenAI batch {batch_id} finished with status: {status}"
            )
            await supabase.update_batch_job(
                batch_id=batch_id,
                provider="openai",
                status=status,
            )
            continue

        output_file_id = batch.output_file_id
        if not output_file_id:
            logger.warning("OpenAI batch %s has no output_file_id", batch_id)
            notifier = get_notifier()
            await notifier.notify_critical(
                context="OpenAI Batch Processing",
                message=f"OpenAI batch {batch_id} has no output file",
                details="Batch completed but output file is missing"
            )
            await supabase.update_batch_job(
                batch_id=batch_id,
                provider="openai",
                status="failed",
            )
            continue

        try:
            results = load_batch_results(client, output_file_id)
        except Exception as e:
            logger.error("Failed to load OpenAI batch results for %s: %s", batch_id, e, exc_info=True)
            await handle_quota_error_consume(supabase, "OpenAI", e)
            continue
        success_count = 0
        error_count = 0
        for qid, payload in results.items():
            if "error" in payload:
                logger.error("OpenAI error for question %s: %s", qid, payload["error"])
                await supabase.update_question_status(qid, "failed", set_processed_at=False)
                error_count += 1
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
        
        # Notify if high error rate
        total_processed = success_count + error_count
        if total_processed > 0 and error_count / total_processed > 0.2:  # More than 20% errors
            notifier = get_notifier()
            await notifier.notify_warning(
                context="OpenAI Batch Processing",
                message=f"High error rate in batch {batch_id}: {error_count}/{total_processed} questions failed ({error_count/total_processed*100:.1f}%)"
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
        try:
            batch_job = client.batches.get(name=job_name)
        except Exception as e:
            logger.error("Failed to retrieve Gemini batch %s: %s", job_name, e, exc_info=True)
            await handle_quota_error_consume(supabase, "Gemini", e)
            continue
        state_name = batch_job.state.name
        if state_name in {
            "JOB_STATE_PENDING",
            "JOB_STATE_RUNNING",
        }:
            logger.info("Gemini batch %s still in state %s", job_name, state_name)
            continue

        if state_name != "JOB_STATE_SUCCEEDED":
            logger.warning("Gemini batch %s finished with state %s", job_name, state_name)
            
            # Check if state indicates quota error
            if state_name in {"JOB_STATE_FAILED", "JOB_STATE_CANCELLED"}:
                error_info = getattr(batch_job, "error", None)
                if error_info:
                    error_str = str(error_info)
                    if is_quota_error(Exception(error_str), "Gemini"):
                        await handle_quota_error_consume(supabase, "Gemini", Exception(error_str))
            
            notifier = get_notifier()
            await notifier.notify_warning(
                context="Gemini Batch Status",
                message=f"Gemini batch {job_name} finished with state: {state_name}"
            )
            await supabase.update_batch_job(
                batch_id=job_name,
                provider="gemini",
                status=state_name,
            )
            continue

        results = parse_inline_responses(batch_job, question_ids)
        success_count = 0
        error_count = 0
        for qid, payload in results.items():
            if "error" in payload:
                logger.error("Gemini error for question %s: %s", qid, payload["error"])
                await supabase.update_question_status(qid, "failed", set_processed_at=False)
                error_count += 1
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
        
        # Notify if high error rate
        total_processed = success_count + error_count
        if total_processed > 0 and error_count / total_processed > 0.2:  # More than 20% errors
            notifier = get_notifier()
            await notifier.notify_warning(
                context="Gemini Batch Processing",
                message=f"High error rate in batch {job_name}: {error_count}/{total_processed} questions failed ({error_count/total_processed*100:.1f}%)"
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
        try:
            batch_job = client.batch.jobs.get(job_id=job_id)
        except Exception as e:
            logger.error("Failed to retrieve Mistral batch %s: %s", job_id, e, exc_info=True)
            await handle_quota_error_consume(supabase, "Mistral", e)
            continue
        status = batch_job.status
        
        if status in {"QUEUED", "RUNNING"}:
            continue

        if status != "SUCCESS":
            logger.warning("Mistral batch %s finished with status %s", job_id, status)
            
            # Check if status indicates quota error
            if status in {"FAILED", "CANCELLED"}:
                error_info = getattr(batch_job, "error", None)
                if error_info:
                    error_str = str(error_info)
                    if is_quota_error(Exception(error_str), "Mistral"):
                        await handle_quota_error_consume(supabase, "Mistral", Exception(error_str))
            
            notifier = get_notifier()
            await notifier.notify_warning(
                context="Mistral Batch Status",
                message=f"Mistral batch {job_id} finished with status: {status}"
            )
            await supabase.update_batch_job(
                batch_id=job_id,
                provider="mistral",
                status=status,
            )
            continue

        output_file_id = batch_job.output_file
        if not output_file_id:
            logger.warning("Mistral batch %s has no output_file", job_id)
            notifier = get_notifier()
            await notifier.notify_critical(
                context="Mistral Batch Processing",
                message=f"Mistral batch {job_id} has no output file",
                details="Batch completed but output file is missing"
            )
            await supabase.update_batch_job(
                batch_id=job_id,
                provider="mistral",
                status="FAILED",
            )
            continue

        # Download results into a temporary file
        output_stream = client.files.download(file_id=output_file_id)
        tmp_dir = Path(tempfile.mkdtemp())
        result_path = tmp_dir / f"mistral_{job_id}.jsonl"
        
        with result_path.open("wb") as f:
            for chunk in output_stream.stream:
                f.write(chunk)

        results = parse_results_file(result_path)
        success_count = 0
        error_count = 0
        
        for qid, payload in results.items():
            if "error" in payload:
                logger.error("Mistral error for question %s: %s", qid, payload["error"])
                await supabase.update_question_status(qid, "failed", set_processed_at=False)
                error_count += 1
                continue
            
            try:
                await supabase.upsert_comments(qid, {"mistral": payload})
                
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
            
            # Notify if high error rate
            total_processed = success_count + error_count
            if total_processed > 0 and error_count / total_processed > 0.2:  # More than 20% errors
                notifier = get_notifier()
                await notifier.notify_warning(
                    context="Mistral Batch Processing",
                    message=f"High error rate in batch {job_id}: {error_count}/{total_processed} questions failed ({error_count/total_processed*100:.1f}%)"
                )

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
    except Exception as e:
        logger.error(f"Critical error in consume process: {e}", exc_info=True)
        notifier = get_notifier()
        await notifier.notify_error(
            context="Consume Process",
            error=e,
            details="The consume process encountered a critical error while processing batches"
        )
        raise
    finally:
        await supabase.close()


if __name__ == "__main__":
    asyncio.run(main())






