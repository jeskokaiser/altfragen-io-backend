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


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("ai_commentary_submit")


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
        # OpenAI / ChatGPT
        if models_enabled.get("chatgpt"):
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

        # Gemini
        if models_enabled.get("gemini"):
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

        # Mistral
        if models_enabled.get("mistral"):
            logger.info("Submitting Mistral batch for %d questions.", len(claimed_questions))
            mistral_client = Mistral()
            job_id, question_ids = submit_mistral_batch(
                claimed_questions, client=mistral_client
            )
            await supabase.create_batch_job(
                provider="mistral",
                batch_id=job_id,
                question_ids=question_ids,
            )
            logger.info("Created Mistral batch %s.", job_id)

        # Perplexity and Deepseek: instant API calls (no batch discount)
        # Process these immediately and save results
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

            # Process all questions with all instant models in parallel
            async def process_question_with_instant_models(question: Dict[str, Any]) -> None:
                answer_comments: Dict[str, Any] = {}
                errors: Dict[str, str] = {}

                # Call all instant models in parallel
                tasks = []
                for model_name, generate_fn in instant_models:
                    tasks.append(
                        (model_name, generate_fn(question))
                    )

                results = await asyncio.gather(
                    *[task[1] for task in tasks],
                    return_exceptions=True
                )

                for idx, (model_name, _) in enumerate(instant_models):
                    result = results[idx]
                    if isinstance(result, Exception):
                        logger.error(
                            "%s error for question %s: %s",
                            model_name,
                            question["id"],
                            result,
                        )
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

                # Upsert comments to database
                if answer_comments:
                    await supabase.upsert_comments(question["id"], answer_comments)

                # Check if all enabled models have completed
                # (This handles the case where only instant models are enabled,
                #  or where instant models complete before batch jobs)
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

            # Process all questions concurrently (with some rate limiting)
            # Use semaphore to limit concurrent API calls
            semaphore = asyncio.Semaphore(3)  # Max 3 concurrent questions

            async def process_with_semaphore(question: Dict[str, Any]) -> None:
                async with semaphore:
                    await process_question_with_instant_models(question)

            await asyncio.gather(
                *[process_with_semaphore(q) for q in claimed_questions],
                return_exceptions=True
            )

            logger.info(
                "Completed instant API processing for %d questions.",
                len(claimed_questions),
            )

    finally:
        await supabase.close()


if __name__ == "__main__":
    asyncio.run(main())






