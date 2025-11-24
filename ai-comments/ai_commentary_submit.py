import asyncio
import logging
from typing import Any, Dict, List

from openai import OpenAI
from google import genai
from mistralai import Mistral

from supabase_client import SupabaseClient
from openai_batch import submit_batch as submit_openai_batch
from gemini_batch import submit_batch as submit_gemini_batch
from mistral_batch import submit_batch as submit_mistral_batch
from perplexity_instant import generate_commentary as generate_perplexity_commentary
from deepseek_instant import generate_commentary as generate_deepseek_commentary
from pushover_notifier import get_notifier
from quota_detector import is_quota_error, extract_quota_message


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("ai_commentary_submit")


async def handle_quota_error(
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


async def main() -> None:
    supabase = SupabaseClient()
    try:
        settings = await supabase.fetch_settings()
        if not settings["feature_enabled"]:
            logger.info("AI commentary feature is disabled; exiting.")
            return

        batch_size = int(settings["batch_size"])
        processing_delay_minutes = int(settings["processing_delay_minutes"])
        models_enabled: Dict[str, Any] = settings["models_enabled"] or {}

        ids_to_process, ids_to_cleanup = await supabase.find_candidates(
            batch_size=batch_size,
            processing_delay_minutes=processing_delay_minutes,
        )

        if not ids_to_process and not ids_to_cleanup:
            logger.info("No candidate questions to process or clean up.")
            return

        if ids_to_cleanup:
            logger.info("Cleaning up %d completed questions.", len(ids_to_cleanup))
            await supabase.cleanup_completed(ids_to_cleanup)

        if not ids_to_process:
            logger.info("Nothing left to process after cleanup.")
            return

        claimed_questions = await supabase.claim_questions(ids_to_process)
        if not claimed_questions:
            logger.info("No questions claimed for processing.")
            return

        logger.info("Claimed %d questions for processing.", len(claimed_questions))

        # Submit provider-specific batch jobs based on models_enabled.
        # Each model is wrapped in try-except to ensure failures don't stop other models.
        
        # OpenAI / ChatGPT
        if models_enabled.get("chatgpt"):
            try:
                logger.info("Submitting OpenAI batch for %d questions.", len(claimed_questions))
                client = OpenAI()
                batch_id, input_file_id, question_ids = submit_openai_batch(
                    claimed_questions, client=client
                )
                await supabase.create_batch_job(
                    provider="openai",
                    batch_id=batch_id,
                    input_file_id=input_file_id,
                    question_ids=question_ids,
                )
                logger.info("Created OpenAI batch %s.", batch_id)
            except Exception as e:
                logger.error("Failed to submit OpenAI batch: %s", e, exc_info=True)
                # Check for quota error and disable feature if needed
                await handle_quota_error(supabase, "OpenAI", e)
                
                notifier = get_notifier()
                await notifier.notify_error(
                    context="OpenAI Batch Submission",
                    error=e,
                    details=f"Failed to submit batch for {len(claimed_questions)} questions"
                )
                # Continue with other models

        # Gemini
        if models_enabled.get("gemini"):
            try:
                logger.info("Submitting Gemini batch for %d questions.", len(claimed_questions))
                gemini_client = genai.Client()
                job_name, question_ids = submit_gemini_batch(
                    claimed_questions, client=gemini_client
                )
                await supabase.create_batch_job(
                    provider="gemini",
                    batch_id=job_name,
                    question_ids=question_ids,
                )
                logger.info("Created Gemini batch %s.", job_name)
            except Exception as e:
                logger.error("Failed to submit Gemini batch: %s", e, exc_info=True)
                # Check for quota error and disable feature if needed
                await handle_quota_error(supabase, "Gemini", e)
                
                notifier = get_notifier()
                await notifier.notify_error(
                    context="Gemini Batch Submission",
                    error=e,
                    details=f"Failed to submit batch for {len(claimed_questions)} questions"
                )
                # Continue with other models

        # Mistral
        if models_enabled.get("mistral"):
            try:
                logger.info("Submitting Mistral batch for %d questions.", len(claimed_questions))
                import os
                mistral_api_key = os.getenv("MISTRAL_API_KEY")
                if not mistral_api_key:
                    logger.error("MISTRAL_API_KEY environment variable is not set, skipping Mistral batch")
                else:
                    mistral_client = Mistral(api_key=mistral_api_key)
                    job_id, question_ids = submit_mistral_batch(
                        claimed_questions, client=mistral_client
                    )
                    await supabase.create_batch_job(
                        provider="mistral",
                        batch_id=job_id,
                        question_ids=question_ids,
                    )
                    logger.info("Created Mistral batch %s.", job_id)
            except Exception as e:
                logger.error("Failed to submit Mistral batch: %s", e, exc_info=True)
                # Check for quota error and disable feature if needed
                await handle_quota_error(supabase, "Mistral", e)
                
                notifier = get_notifier()
                await notifier.notify_error(
                    context="Mistral Batch Submission",
                    error=e,
                    details=f"Failed to submit batch for {len(claimed_questions)} questions"
                )
                # Continue with other models

        # Perplexity and Deepseek: instant API calls (no batch discount)
        # Process these immediately and save results
        # These are wrapped in try-except to ensure failures don't stop the process
        instant_models = []
        if models_enabled.get("perplexity"):
            instant_models.append(("perplexity", generate_perplexity_commentary))
        if models_enabled.get("deepseek"):
            instant_models.append(("deepseek", generate_deepseek_commentary))

        if instant_models:
            logger.info(
                "Processing %d questions with instant APIs: %s",
                len(claimed_questions),
                ", ".join(name for name, _ in instant_models),
            )

            # Track failures for notification purposes
            model_failure_counts: Dict[str, int] = {name: 0 for name, _ in instant_models}
            total_calls = {name: 0 for name, _ in instant_models}

            # Process all questions with all instant models in parallel
            async def process_question_with_instant_models(question: Dict[str, Any]) -> None:
                answer_comments: Dict[str, Any] = {}
                errors: Dict[str, str] = {}

                # Call all instant models in parallel with individual error handling
                async def call_model_safely(model_name: str, generate_fn) -> tuple[str, Any]:
                    """Call a model and return (model_name, result_or_exception)"""
                    nonlocal model_failure_counts, total_calls
                    total_calls[model_name] += 1
                    try:
                        result = await generate_fn(question)
                        return (model_name, result)
                    except Exception as e:
                        model_failure_counts[model_name] += 1
                        logger.error(
                            "%s error for question %s: %s",
                            model_name,
                            question["id"],
                            e,
                            exc_info=True,
                        )
                        # Check for quota error and disable feature if needed
                        await handle_quota_error(supabase, model_name.capitalize(), e)
                        return (model_name, e)

                # Call all models in parallel
                tasks = [
                    call_model_safely(model_name, generate_fn)
                    for model_name, generate_fn in instant_models
                ]
                results = await asyncio.gather(*tasks, return_exceptions=False)

                # Process results
                for model_name, result in results:
                    if isinstance(result, Exception):
                        errors[model_name] = str(result)
                        # Create error response similar to TS function
                        answer_comments[model_name] = {
                            "chosen_answer": None,
                            "general_comment": f"Fehler: {result}",
                            "comment_a": f"Fehler: {result}",
                            "comment_b": f"Fehler: {result}",
                            "comment_c": f"Fehler: {result}",
                            "comment_d": f"Fehler: {result}",
                            "comment_e": f"Fehler: {result}",
                            "processing_status": "failed",
                        }
                    else:
                        answer_comments[model_name] = {
                            **result,
                            "processing_status": "completed",
                        }

                # Upsert comments to database (even if some models failed)
                if answer_comments:
                    try:
                        await supabase.upsert_comments(question["id"], answer_comments)
                    except Exception as e:
                        logger.error(
                            "Failed to upsert comments for question %s: %s",
                            question["id"],
                            e,
                            exc_info=True,
                        )
                        # Don't re-raise, continue processing

                # Check if all enabled models have completed
                # (This handles the case where only instant models are enabled,
                #  or where instant models complete before batch jobs)
                try:
                    if await supabase.check_all_models_completed(question["id"], models_enabled):
                        await supabase.update_question_status(
                            question["id"], "completed", set_processed_at=True
                        )
                        logger.info(
                            "Question %s: all enabled models completed (instant APIs)",
                            question["id"],
                        )
                    else:
                        logger.debug(
                            "Question %s: waiting for other models to complete",
                            question["id"],
                        )
                except Exception as e:
                    logger.error(
                        "Failed to check/update status for question %s: %s",
                        question["id"],
                        e,
                        exc_info=True,
                    )
                    # Don't re-raise, continue processing

            # Process all questions concurrently (with some rate limiting)
            # Use semaphore to limit concurrent API calls
            semaphore = asyncio.Semaphore(3)  # Max 3 concurrent questions

            async def process_with_semaphore(question: Dict[str, Any]) -> None:
                try:
                    async with semaphore:
                        await process_question_with_instant_models(question)
                except Exception as e:
                    logger.error(
                        "Failed to process question %s with instant models: %s",
                        question.get("id"),
                        e,
                        exc_info=True,
                    )
                    # Send notification for critical processing errors
                    notifier = get_notifier()
                    await notifier.notify_error(
                        context="Instant API Processing",
                        error=e,
                        details=f"Failed to process question {question.get('id')} with instant models"
                    )
                    # Continue processing other questions

            results = await asyncio.gather(
                *[process_with_semaphore(q) for q in claimed_questions],
                return_exceptions=True
            )
            
            # Count successful vs failed
            successful = sum(1 for r in results if not isinstance(r, Exception))
            failed = len(results) - successful
            
            logger.info(
                "Completed instant API processing: %d successful, %d failed out of %d questions.",
                successful,
                failed,
                len(claimed_questions),
            )
            
            # Send notifications for high failure rates per model
            notifier = get_notifier()
            for model_name, _ in instant_models:
                failures = model_failure_counts[model_name]
                total = total_calls[model_name]
                
                if total > 0:
                    failure_rate = failures / total
                    if failure_rate > 0.2:  # More than 20% failures
                        await notifier.notify_warning(
                            context=f"{model_name.capitalize()} Instant API",
                            message=f"High failure rate: {failures}/{total} calls failed ({failure_rate*100:.1f}%)"
                        )
                    elif failures > 0 and total <= 3:  # If all or most calls failed on small batch
                        await notifier.notify_error(
                            context=f"{model_name.capitalize()} Instant API",
                            error=Exception(f"{failures} out of {total} calls failed"),
                            details=f"Processing {len(claimed_questions)} questions"
                        )

    finally:
        await supabase.close()


if __name__ == "__main__":
    asyncio.run(main())






