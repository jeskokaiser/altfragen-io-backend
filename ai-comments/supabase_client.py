import os
import asyncio
from typing import Any, Dict, List, Optional, Tuple

from supabase import create_client, Client


class SupabaseClient:
    """
    Async wrapper around the official Supabase Python SDK.
    
    Uses the official supabase-py SDK (sync) and wraps calls in async executors
    to maintain async compatibility while using the maintained SDK.
    """

    def __init__(self) -> None:
        url = os.getenv("SUPABASE_URL")
        key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
        if not url or not key:
            raise RuntimeError(
                "SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set in the environment."
            )

        # Create sync Supabase client
        self._client: Client = create_client(url.rstrip("/"), key)

    async def close(self) -> None:
        """Close any open connections (SDK handles this internally)"""
        # The official SDK doesn't require explicit closing
        pass

    async def _run_sync(self, func, *args, **kwargs):
        """Run a sync function in an async executor"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, lambda: func(*args, **kwargs))

    # -------------------------------------------------------------------------
    # Settings
    # -------------------------------------------------------------------------

    async def fetch_settings(self) -> Dict[str, Any]:
        """
        Fetch the single row from ai_commentary_settings.
        Mirrors the `.single()` behaviour from supabase-js.
        """
        def _fetch():
            response = self._client.table("ai_commentary_settings").select("*").limit(1).execute()
            if not response.data:
                raise RuntimeError("ai_commentary_settings: no row found")
            return response.data[0]

        row = await self._run_sync(_fetch)
        return {
            "batch_size": row.get("batch_size") or 5,
            "processing_delay_minutes": row.get("processing_delay_minutes") or 60,
            "models_enabled": row.get("models_enabled") or {},
            "feature_enabled": bool(row.get("feature_enabled")),
        }

    # -------------------------------------------------------------------------
    # Candidate question selection
    # -------------------------------------------------------------------------

    async def _select_questions(
        self,
        status: str,
        queued_before_iso: str,
        limit: int,
    ) -> List[Dict[str, Any]]:
        def _select():
            return (
                self._client.table("questions")
                .select("id,ai_commentary_status")
                .eq("ai_commentary_status", status)
                .lt("ai_commentary_queued_at", queued_before_iso)
                .limit(limit)
                .execute()
            ).data

        return await self._run_sync(_select)

    async def _select_questions_with_commentary(
        self, limit: int
    ) -> List[Dict[str, Any]]:
        def _select():
            return (
                self._client.table("ai_answer_comments")
                .select("question_id")
                .limit(limit)
                .execute()
            ).data

        return await self._run_sync(_select)

    async def _select_questions_with_summaries(self) -> List[Dict[str, Any]]:
        def _select():
            return (
                self._client.table("ai_commentary_summaries")
                .select("question_id")
                .execute()
            ).data

        return await self._run_sync(_select)

    async def _select_questions_by_ids(
        self, ids: List[int]
    ) -> List[Dict[str, Any]]:
        if not ids:
            return []
        
        def _select():
            return (
                self._client.table("questions")
                .select("id,ai_commentary_status")
                .in_("id", ids)
                .in_("ai_commentary_status", ["completed", "processing"])
                .execute()
            ).data

        return await self._run_sync(_select)

    async def _select_existing_comments(
        self, question_ids: List[str]
    ) -> List[Dict[str, Any]]:
        if not question_ids:
            return []
        
        def _select():
            return (
                self._client.table("ai_answer_comments")
                .select(
                    "question_id,"
                    "openai_general_comment,gemini_general_comment,chatgpt_general_comment,"
                    "mistral_general_comment,perplexity_general_comment,deepseek_general_comment,"
                    "gemini_new_general_comment"
                )
                .in_("question_id", question_ids)
                .execute()
            ).data

        return await self._run_sync(_select)

    async def _select_existing_summaries(
        self, question_ids: List[str]
    ) -> List[Dict[str, Any]]:
        if not question_ids:
            return []
        
        def _select():
            return (
                self._client.table("ai_commentary_summaries")
                .select("question_id")
                .in_("question_id", question_ids)
                .execute()
            ).data

        return await self._run_sync(_select)

    async def find_candidates(
        self,
        batch_size: int,
        processing_delay_minutes: int,
    ) -> Tuple[List[str], List[str]]:
        """
        Reproduce the TS logic to find candidate questions and decide which
        IDs to process vs clean up.

        Returns (ids_to_process, ids_to_cleanup).
        """
        from datetime import datetime, timedelta, timezone

        now = datetime.now(timezone.utc)

        delay_threshold = now - timedelta(minutes=processing_delay_minutes)
        stuck_threshold = now - timedelta(minutes=30)

        delay_iso = delay_threshold.isoformat()
        stuck_iso = stuck_threshold.isoformat()

        pending_candidates = await self._select_questions(
            status="pending",
            queued_before_iso=delay_iso,
            limit=batch_size,
        )
        stuck_candidates = await self._select_questions(
            status="processing",
            queued_before_iso=stuck_iso,
            limit=batch_size,
        )

        # Questions that have commentary but no summaries
        questions_with_commentary = await self._select_questions_with_commentary(
            limit=batch_size * 2
        )
        questions_with_summaries = await self._select_questions_with_summaries()

        summary_ids = {
            row["question_id"] for row in questions_with_summaries or []
        }
        needing_summary_ids = [
            row["question_id"]
            for row in (questions_with_commentary or [])
            if row.get("question_id") not in summary_ids
        ][:batch_size]

        commentary_only_questions: List[Dict[str, Any]] = []
        if needing_summary_ids:
            commentary_only_questions = await self._select_questions_by_ids(
                needing_summary_ids
            )

        candidate_questions = []
        candidate_questions.extend(pending_candidates or [])
        candidate_questions.extend(stuck_candidates or [])
        candidate_questions.extend(commentary_only_questions or [])

        if not candidate_questions:
            return [], []

        candidate_ids: List[str] = [str(q["id"]) for q in candidate_questions]

        existing_comments = await self._select_existing_comments(candidate_ids)
        existing_summaries = await self._select_existing_summaries(candidate_ids)

        # Questions with errors in any general comment field (contains "Fehler:")
        questions_with_errors = set()
        for comment in existing_comments or []:
            for key in [
                "openai_general_comment",
                "gemini_general_comment",
                "chatgpt_general_comment",
                "mistral_general_comment",
                "perplexity_general_comment",
                "deepseek_general_comment",
                "gemini_new_general_comment",
            ]:
                value = comment.get(key)
                if isinstance(value, str) and "Fehler:" in value:
                    questions_with_errors.add(str(comment["question_id"]))
                    break

        existing_question_ids = {
            *(str(c["question_id"]) for c in (existing_comments or [])),
            *(str(s["question_id"]) for s in (existing_summaries or [])),
        }

        commentary_only_ids = {str(q["id"]) for q in commentary_only_questions or []}

        ids_to_process_raw: List[str] = []
        ids_to_cleanup: List[str] = []

        for qid in candidate_ids:
            if qid in existing_question_ids:
                if qid in questions_with_errors:
                    ids_to_process_raw.append(qid)
                elif qid in commentary_only_ids:
                    ids_to_process_raw.append(qid)
                else:
                    ids_to_cleanup.append(qid)
            else:
                ids_to_process_raw.append(qid)

        ids_to_process = ids_to_process_raw[:batch_size]
        return ids_to_process, ids_to_cleanup

    # -------------------------------------------------------------------------
    # Claiming & status updates
    # -------------------------------------------------------------------------

    async def cleanup_completed(self, ids_to_cleanup: List[str]) -> None:
        if not ids_to_cleanup:
            return
        
        from datetime import datetime, timezone

        def _cleanup():
            self._client.table("questions").update({
                "ai_commentary_status": "completed",
                "ai_commentary_processed_at": datetime.now(timezone.utc).isoformat(),
            }).in_("id", ids_to_cleanup).eq("ai_commentary_status", "pending").execute()

        await self._run_sync(_cleanup)

    async def claim_questions(self, ids_to_process: List[str]) -> List[Dict[str, Any]]:
        if not ids_to_process:
            return []
        
        def _claim():
            # Update the questions first (only update those that are pending or processing)
            self._client.table("questions").update({
                "ai_commentary_status": "processing"
            }).in_("id", ids_to_process).in_("ai_commentary_status", ["pending", "processing"]).execute()
            
            # Then fetch the updated questions
            return (
                self._client.table("questions")
                .select("*")
                .in_("id", ids_to_process)
                .eq("ai_commentary_status", "processing")
                .execute()
            ).data

        return await self._run_sync(_claim)

    async def check_all_models_completed(
        self,
        question_id: str,
        models_enabled: Dict[str, Any],
    ) -> bool:
        """
        Check if all enabled models have completed commentary for a question.

        Returns True if all enabled models have non-null general_comment fields
        (and they don't contain "Fehler:").
        """
        def _check():
            response = (
                self._client.table("ai_answer_comments")
                .select(
                    "chatgpt_general_comment,gemini_new_general_comment,"
                    "mistral_general_comment,perplexity_general_comment,"
                    "deepseek_general_comment"
                )
                .eq("question_id", question_id)
                .limit(1)
                .execute()
            )
            return response.data

        data = await self._run_sync(_check)
        if not data:
            return False

        comment_row = data[0]

        # Map model names to database column names
        model_columns = {
            "chatgpt": "chatgpt_general_comment",
            "gemini": "gemini_new_general_comment",
            "mistral": "mistral_general_comment",
            "perplexity": "perplexity_general_comment",
            "deepseek": "deepseek_general_comment",
        }

        # Check each enabled model
        for model_name, enabled in models_enabled.items():
            if not enabled:
                continue

            column = model_columns.get(model_name)
            if not column:
                continue

            comment = comment_row.get(column)
            if not comment or not isinstance(comment, str):
                return False

            # Check for errors
            if "Fehler:" in comment:
                return False

        return True

    async def update_question_status(
        self,
        question_id: str,
        status: str,
        set_processed_at: bool = False,
    ) -> None:
        from datetime import datetime, timezone

        def _update():
            payload: Dict[str, Any] = {"ai_commentary_status": status}
            if set_processed_at and status == "completed":
                payload["ai_commentary_processed_at"] = datetime.now(
                    timezone.utc
                ).isoformat()

            self._client.table("questions").update(payload).eq("id", question_id).execute()

        await self._run_sync(_update)

    # -------------------------------------------------------------------------
    # Answer comments upsert
    # -------------------------------------------------------------------------

    async def upsert_comments(
        self,
        question_id: str,
        answer_comments: Dict[str, Any],
    ) -> None:
        """
        Upsert into ai_answer_comments using the same column mapping
        as the TS function's insertData object for all 5 models.

        answer_comments is expected to be a dict:
        {
          "chatgpt": {...} | None,
          "gemini": {...} | None,
          "mistral": {...} | None,
          "perplexity": {...} | None,
          "deepseek": {...} | None,
        }
        """
        chatgpt = answer_comments.get("chatgpt") or {}
        gemini = answer_comments.get("gemini") or {}
        mistral = answer_comments.get("mistral") or {}
        perplexity = answer_comments.get("perplexity") or {}
        deepseek = answer_comments.get("deepseek") or {}

        payload: Dict[str, Any] = {
            "question_id": question_id,
            # ChatGPT (OpenAI / chatgpt)
            "chatgpt_chosen_answer": chatgpt.get("chosen_answer"),
            "chatgpt_general_comment": chatgpt.get("general_comment"),
            "chatgpt_comment_a": chatgpt.get("comment_a"),
            "chatgpt_comment_b": chatgpt.get("comment_b"),
            "chatgpt_comment_c": chatgpt.get("comment_c"),
            "chatgpt_comment_d": chatgpt.get("comment_d"),
            "chatgpt_comment_e": chatgpt.get("comment_e"),
            "chatgpt_regenerated_question": chatgpt.get("regenerated_question"),
            "chatgpt_regenerated_option_a": chatgpt.get("regenerated_option_a"),
            "chatgpt_regenerated_option_b": chatgpt.get("regenerated_option_b"),
            "chatgpt_regenerated_option_c": chatgpt.get("regenerated_option_c"),
            "chatgpt_regenerated_option_d": chatgpt.get("regenerated_option_d"),
            "chatgpt_regenerated_option_e": chatgpt.get("regenerated_option_e"),
            # Gemini
            "gemini_chosen_answer": gemini.get("chosen_answer"),
            "gemini_new_general_comment": gemini.get("general_comment"),
            "gemini_new_comment_a": gemini.get("comment_a"),
            "gemini_new_comment_b": gemini.get("comment_b"),
            "gemini_new_comment_c": gemini.get("comment_c"),
            "gemini_new_comment_d": gemini.get("comment_d"),
            "gemini_new_comment_e": gemini.get("comment_e"),
            "gemini_regenerated_question": gemini.get("regenerated_question"),
            "gemini_regenerated_option_a": gemini.get("regenerated_option_a"),
            "gemini_regenerated_option_b": gemini.get("regenerated_option_b"),
            "gemini_regenerated_option_c": gemini.get("regenerated_option_c"),
            "gemini_regenerated_option_d": gemini.get("regenerated_option_d"),
            "gemini_regenerated_option_e": gemini.get("regenerated_option_e"),
            # Mistral
            "mistral_chosen_answer": mistral.get("chosen_answer"),
            "mistral_general_comment": mistral.get("general_comment"),
            "mistral_comment_a": mistral.get("comment_a"),
            "mistral_comment_b": mistral.get("comment_b"),
            "mistral_comment_c": mistral.get("comment_c"),
            "mistral_comment_d": mistral.get("comment_d"),
            "mistral_comment_e": mistral.get("comment_e"),
            # Perplexity
            "perplexity_chosen_answer": perplexity.get("chosen_answer"),
            "perplexity_general_comment": perplexity.get("general_comment"),
            "perplexity_comment_a": perplexity.get("comment_a"),
            "perplexity_comment_b": perplexity.get("comment_b"),
            "perplexity_comment_c": perplexity.get("comment_c"),
            "perplexity_comment_d": perplexity.get("comment_d"),
            "perplexity_comment_e": perplexity.get("comment_e"),
            # Deepseek
            "deepseek_chosen_answer": deepseek.get("chosen_answer"),
            "deepseek_general_comment": deepseek.get("general_comment"),
            "deepseek_comment_a": deepseek.get("comment_a"),
            "deepseek_comment_b": deepseek.get("comment_b"),
            "deepseek_comment_c": deepseek.get("comment_c"),
            "deepseek_comment_d": deepseek.get("comment_d"),
            "deepseek_comment_e": deepseek.get("comment_e"),
        }

        def _upsert():
            self._client.table("ai_answer_comments").upsert(
                payload, on_conflict="question_id"
            ).execute()

        await self._run_sync(_upsert)

    # -------------------------------------------------------------------------
    # Batch job tracking (ai_commentary_batch_jobs)
    # -------------------------------------------------------------------------

    async def create_batch_job(
        self,
        provider: str,
        batch_id: str,
        question_ids: List[str],
        input_file_id: Optional[str] = None,
    ) -> None:
        """
        Insert a row into ai_commentary_batch_jobs.

        NOTE: The table must be created manually in Supabase.
        """
        def _create():
            payload = {
                "provider": provider,
                "batch_id": batch_id,
                "status": "pending",
                "question_ids": question_ids,
                "input_file_id": input_file_id,
            }
            self._client.table("ai_commentary_batch_jobs").insert(payload).execute()

        await self._run_sync(_create)

    async def update_batch_job(
        self,
        batch_id: str,
        provider: str,
        status: str,
        output_file_id: Optional[str] = None,
        error_file_id: Optional[str] = None,
    ) -> None:
        def _update():
            payload: Dict[str, Any] = {"status": status}
            if output_file_id is not None:
                payload["output_file_id"] = output_file_id
            if error_file_id is not None:
                payload["error_file_id"] = error_file_id

            self._client.table("ai_commentary_batch_jobs").update(payload).eq(
                "batch_id", batch_id
            ).eq("provider", provider).execute()

        await self._run_sync(_update)

    async def get_open_batch_jobs(self, provider: str) -> List[Dict[str, Any]]:
        """
        Return batch jobs for a provider that are not in a final state.
        """
        def _get():
            return (
                self._client.table("ai_commentary_batch_jobs")
                .select("*")
                .eq("provider", provider)
                .in_(
                    "status",
                    [
                        "pending",
                        "validating",
                        "in_progress",
                        "finalizing",
                        "QUEUED",
                        "RUNNING",
                    ],
                )
                .execute()
            ).data

        return await self._run_sync(_get)
