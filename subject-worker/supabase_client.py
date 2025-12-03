import os
import asyncio
from typing import Any, Dict, List, Optional
from datetime import datetime, timezone

from supabase import create_client, Client


class SupabaseClient:
    """
    Async wrapper around the official Supabase Python SDK for subject job processing.
    
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

    async def _run_sync(self, func, *args, **kwargs):
        """Run a sync function in an async executor"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, lambda: func(*args, **kwargs))

    # -------------------------------------------------------------------------
    # Subject job management (subject_jobs)
    # -------------------------------------------------------------------------

    async def fetch_pending_subject_jobs(self) -> List[Dict[str, Any]]:
        """
        Fetch all pending subject jobs (both assign and reassign types),
        ordered by created_at ascending.
        """
        def _fetch():
            return (
                self._client.table("subject_jobs")
                .select("*")
                .eq("status", "pending")
                .order("created_at", desc=False)
                .execute()
            ).data

        return await self._run_sync(_fetch)

    async def update_subject_job_status(
        self,
        job_id: str,
        status: str,
        progress: Optional[int] = None,
        errors: Optional[int] = None,
        message: Optional[str] = None,
        result: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Update a subject job's status and optionally progress, errors, message, and result.
        """
        def _update():
            payload: Dict[str, Any] = {
                "status": status,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
            if progress is not None:
                payload["progress"] = progress
            if errors is not None:
                payload["errors"] = errors
            if message is not None:
                payload["message"] = message
            if result is not None:
                payload["result"] = result

            self._client.table("subject_jobs").update(payload).eq("id", job_id).execute()

        await self._run_sync(_update)

    async def fetch_questions_for_reassign_job(
        self,
        exam_name: str,
        university_id: Optional[str] = None,
        only_null_subjects: bool = False,
    ) -> List[Dict[str, Any]]:
        """
        Fetch questions matching the reassign job criteria.
        Returns all matching questions with their full data.
        """
        def _fetch():
            query = (
                self._client.table("questions")
                .select("*")
                .eq("exam_name", exam_name)
            )

            if university_id and university_id != "all":
                query = query.eq("university_id", university_id)

            response = query.execute()
            questions = response.data or []

            # Filter for null subjects on client side if requested
            if only_null_subjects:
                questions = [
                    q for q in questions
                    if not q.get("subject") or str(q.get("subject", "")).strip() == ""
                ]

            return questions

        return await self._run_sync(_fetch)

    async def update_question_subject(
        self,
        question_id: str,
        subject: str,
        user_id: Optional[str] = None,
    ) -> None:
        """
        Update a question's subject field.
        If user_id is provided, also filter by user_id for safety.
        """
        def _update():
            query = self._client.table("questions").update({"subject": subject}).eq("id", question_id)
            if user_id:
                query = query.eq("user_id", user_id)
            query.execute()

        await self._run_sync(_update)

