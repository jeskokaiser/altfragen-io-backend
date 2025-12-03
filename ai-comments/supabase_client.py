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
    
    async def disable_feature(self) -> bool:
        """
        Disable the AI commentary feature by setting feature_enabled to False.
        Returns True if successful, False otherwise.
        """
        def _update():
            response = self._client.table("ai_commentary_settings").update({
                "feature_enabled": False
            }).execute()
            return len(response.data) > 0
        
        try:
            result = await self._run_sync(_update)
            return result
        except Exception as e:
            # Log but don't raise - this is a safety mechanism
            import logging
            logger = logging.getLogger(__name__)
            logger.error(f"Failed to disable feature in database: {e}", exc_info=True)
            return False

    # -------------------------------------------------------------------------
    # Candidate question selection
    # -------------------------------------------------------------------------

    async def _select_questions(
        self,
        status: str,
        processed_before_iso: Optional[str],
        limit: int,
    ) -> List[Dict[str, Any]]:
        def _select():
            query = (
                self._client.table("questions")
                # We include visibility and user_id so we can:
                # - prioritise university-visible questions
                # - gate private questions based on the owner's premium status
                .select("id,ai_commentary_status,visibility,user_id")
                .eq("ai_commentary_status", status)
            )

            # Use ai_commentary_processed_at as the relevant timestamp for
            # delay / stuck calculations instead of the legacy queued_at.
            # If processed_before_iso is None, we don't apply any time filter.
            if processed_before_iso is not None:
                query = query.lt("ai_commentary_processed_at", processed_before_iso)

            return query.limit(limit).execute().data

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

    async def _get_premium_users(
        self,
        user_ids: List[str],
    ) -> Dict[str, bool]:
        """
        Fetch premium status for a list of user IDs.

        Returns a mapping of user_id -> is_premium (bool).
        Missing users default to False (non-premium).
        """
        # Deduplicate and filter out empty values
        unique_ids = sorted({uid for uid in user_ids if uid})
        if not unique_ids:
            return {}

        def _select():
            return (
                self._client.table("profiles")
                .select("id,is_premium")
                .in_("id", unique_ids)
                .execute()
            ).data

        rows = await self._run_sync(_select)
        premium_map: Dict[str, bool] = {}
        for row in rows or []:
            uid = str(row.get("id"))
            premium_map[uid] = bool(row.get("is_premium"))
        return premium_map

    async def _get_private_quota(
        self,
        user_ids: List[str],
        month_start: str,
    ) -> Dict[str, Dict[str, int]]:
        """
        Fetch private AI commentary quota usage for a list of users for the
        given calendar month (identified by its first day as ISO date).

        Returns a mapping:
          user_id -> {
            "free_used_count": int,
            "paid_credits_remaining": int,
          }

        Missing users default to zero usage and zero credits.
        """
        unique_ids = sorted({uid for uid in user_ids if uid})
        if not unique_ids:
            return {}

        def _select():
            return (
                self._client.table("user_private_ai_quota")
                .select("user_id,free_used_count,paid_credits_remaining,month_start")
                .in_("user_id", unique_ids)
                .eq("month_start", month_start)
                .execute()
            ).data

        rows = await self._run_sync(_select)
        quota_map: Dict[str, Dict[str, int]] = {}
        for row in rows or []:
            uid = str(row.get("user_id"))
            quota_map[uid] = {
                "free_used_count": int(row.get("free_used_count") or 0),
                "paid_credits_remaining": int(row.get("paid_credits_remaining") or 0),
            }
        return quota_map

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

        # Always prioritise questions that are currently marked as "pending".
        # For pending questions we *ignore* the delay threshold and pick up to
        # batch_size rows regardless of ai_commentary_queued_at, so that:
        # - newly created questions are processed immediately
        # - manually re-queued questions (status reset to 'pending') are not
        #   filtered out just because queued_at is NULL or very recent.
        pending_candidates = await self._select_questions(
            status="pending",
            processed_before_iso=None,
            limit=batch_size,
        )

        # Only take "processing" (stuck) questions if there is remaining capacity
        # in the current batch. This guarantees that, whenever pending questions
        # exist, they fill the batch first. For stuck detection, we now compare
        # against ai_commentary_processed_at instead of the legacy queued_at.
        remaining_capacity = max(
            0, batch_size - len(pending_candidates or [])
        )
        if remaining_capacity > 0:
            stuck_candidates = await self._select_questions(
                status="processing",
                processed_before_iso=stuck_iso,
                limit=remaining_capacity,
            )
        else:
            stuck_candidates = []

        candidate_questions: List[Dict[str, Any]] = []
        candidate_questions.extend(pending_candidates or [])
        candidate_questions.extend(stuck_candidates or [])

        if not candidate_questions:
            return [], []

        # Build a small metadata map so we can prioritise / gate later.
        # visibility defaults to 'private' if not set (historic rows)
        question_meta: Dict[str, Dict[str, Any]] = {}
        for q in candidate_questions:
            qid = str(q["id"])
            question_meta[qid] = {
                "visibility": (q.get("visibility") or "private"),
                "user_id": q.get("user_id"),
            }

        ids_to_process_raw: List[str] = []
        ids_to_cleanup: List[str] = []

        # IMPORTANT BEHAVIOUR (simplified):
        # - Any question that is currently "pending" or "processing" and made it
        #   into candidate_questions should be (re)processed.
        #   This allows:
        #   - newly created questions to run immediately
        #   - manually re-queued questions (status reset to 'pending')
        #   - partially processed questions (some models done, others missing)
        for question in candidate_questions:
            qid = str(question["id"])
            ids_to_process_raw.append(qid)

        # ------------------------------------------------------------------
        # Prioritisation & gating:
        # - University-visible questions are always processed first
        # - Private questions are only processed for premium users
        #   (full vs overflow processing is enforced in a separate step)
        # - Non-premium users' private questions are never processed at all
        # ------------------------------------------------------------------

        # Split into university vs private based on visibility
        uni_ids: List[str] = []
        private_ids: List[str] = []
        for qid in ids_to_process_raw:
            meta = question_meta.get(qid, {})
            visibility = meta.get("visibility") or "private"
            if visibility == "university":
                uni_ids.append(qid)
            else:
                private_ids.append(qid)

        # Resolve premium status for owners of private questions
        private_user_ids = [
            str(question_meta[qid].get("user_id"))
            for qid in private_ids
            if question_meta.get(qid, {}).get("user_id")
        ]
        premium_map = await self._get_premium_users(private_user_ids)

        eligible_private_ids: List[str] = []
        for qid in private_ids:
            meta = question_meta.get(qid, {})
            user_id = meta.get("user_id")
            # If we don't know the owner, treat as non-premium for safety
            if not user_id:
                import logging
                logging.getLogger(__name__).warning(
                    "Skipping private question %s for AI commentary: missing user_id",
                    qid,
                )
                continue
            user_id_str = str(user_id)

            # Explicitly skip private questions of non-premium users
            if not premium_map.get(user_id_str, False):
                import logging
                logging.getLogger(__name__).info(
                    "Skipping private question %s for non-premium user %s",
                    qid,
                    user_id_str,
                )
                continue

            eligible_private_ids.append(qid)

        # Final ordering: all university questions first, then eligible private
        prioritised_ids: List[str] = uni_ids + eligible_private_ids
        ids_to_process = prioritised_ids[:batch_size]
        return ids_to_process, ids_to_cleanup

    async def classify_quota_for_questions(
        self,
        questions: List[Dict[str, Any]],
        models_enabled: Dict[str, Any],
    ) -> Dict[str, Dict[str, Any]]:
        """
        Classify questions into full-slot vs overflow processing based on
        per-user monthly quota and existing comments.

        Returns a mapping:
          question_id -> {
            "user_id": str | None,
            "is_full_slot": bool,
          }

        Behaviour:
        - University-visible questions are always treated as full-slot.
        - Private questions of non-premium users are never expected here
          (they are filtered out in find_candidates), but are treated as
          overflow for safety.
        - For premium private questions, we allocate up to
          BASE_MONTHLY_FREE_LIMIT free full slots per calendar month plus any
          paid credits, on a per-user basis.
        - Within a user, questions are ordered backlog-first:
          questions that already have some comments (older overflow) are
          prioritised before brand-new questions.
        """
        from datetime import datetime, timezone

        if not questions:
            return {}

        # Identify current calendar month by its first day in UTC
        now = datetime.now(timezone.utc)
        month_start = datetime(now.year, now.month, 1, tzinfo=timezone.utc).date().isoformat()

        BASE_MONTHLY_FREE_LIMIT = 100

        # Build basic metadata
        classification: Dict[str, Dict[str, Any]] = {}
        question_ids: List[str] = []
        private_user_ids: List[str] = []

        for q in questions:
            qid = str(q.get("id"))
            if not qid:
                continue
            question_ids.append(qid)

            visibility = (q.get("visibility") or "private")
            user_id = q.get("user_id")

            classification[qid] = {
                "user_id": str(user_id) if user_id else None,
                "visibility": visibility,
                "is_full_slot": False,  # default, will be updated below
            }

            if visibility == "private" and user_id:
                private_user_ids.append(str(user_id))

        # Resolve premium status and quota for owners of private questions
        premium_map = await self._get_premium_users(private_user_ids)
        quota_map = await self._get_private_quota(private_user_ids, month_start)

        # Load existing comments to prioritise backlog questions
        existing_comments = await self._select_existing_comments(question_ids)
        comments_by_qid: Dict[str, Dict[str, Any]] = {}
        for row in existing_comments or []:
            qid = str(row.get("question_id"))
            comments_by_qid[qid] = row

        # Pre-compute whether a question already has any comments at all
        has_any_comments: Dict[str, bool] = {
            qid: qid in comments_by_qid for qid in question_ids
        }

        # University-visible questions are always full-slot
        for q in questions:
            qid = str(q.get("id"))
            meta = classification.get(qid)
            if not meta:
                continue
            if meta.get("visibility") == "university":
                meta["is_full_slot"] = True

        # Allocate per-user full slots for premium private questions
        # in a backlog-first order.
        # Group questions by user
        per_user_questions: Dict[str, List[str]] = {}
        for qid, meta in classification.items():
            user_id = meta.get("user_id")
            if not user_id:
                continue
            if meta.get("visibility") != "private":
                continue
            if not premium_map.get(user_id, False):
                # Non-premium user: keep is_full_slot=False
                continue
            per_user_questions.setdefault(user_id, []).append(qid)

        for user_id, qids in per_user_questions.items():
            quota = quota_map.get(user_id, {"free_used_count": 0, "paid_credits_remaining": 0})
            free_used = int(quota.get("free_used_count") or 0)
            paid_remaining = int(quota.get("paid_credits_remaining") or 0)

            remaining_free = max(0, BASE_MONTHLY_FREE_LIMIT - free_used)
            slots_remaining = remaining_free + paid_remaining
            if slots_remaining <= 0:
                # All of this user's private questions become overflow this month
                continue

            # Sort user's questions:
            # - First those that already have any comments (backlog / overflow from
            #   previous processing), then those without comments.
            # - Within each group, sort by ai_commentary_processed_at, then created_at,
            #   then id as a final tie breaker.
            def sort_key(qid: str):
                # Prefer questions that already have comments
                any_comments = has_any_comments.get(qid, False)
                # Retrieve original question row
                # (find first matching question in the provided list)
                base = next((q for q in questions if str(q.get("id")) == qid), None)
                processed_at = base.get("ai_commentary_processed_at") if base else None
                created_at = base.get("created_at") if base else None
                return (
                    0 if any_comments else 1,
                    processed_at or "",
                    created_at or "",
                    qid,
                )

            sorted_qids = sorted(qids, key=sort_key)

            for qid in sorted_qids:
                if slots_remaining <= 0:
                    break
                meta = classification.get(qid)
                if not meta:
                    continue
                meta["is_full_slot"] = True
                slots_remaining -= 1

        return classification

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
        Check if all enabled models have completed commentary for a question without errors.

        Returns True if:
        1. All enabled models have non-null general_comment fields
        2. None of the comments contain "Fehler:" (error indicator)
        3. All answers were successfully written to ai_answer_comments
        """
        def _check():
            response = (
                self._client.table("ai_answer_comments")
                .select(
                    "chatgpt_general_comment,gemini_new_general_comment,"
                    "mistral_general_comment,perplexity_general_comment,"
                    "deepseek_general_comment,processing_status"
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

        # Check processing_status - if it's "failed", return False
        processing_status = comment_row.get("processing_status")
        if processing_status == "failed":
            return False

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
        def _update():
            from datetime import datetime, timezone

            # Read existing state first so we can decide whether this is the
            # first time a question is marked as completed (for quota updates).
            existing_resp = (
                self._client.table("questions")
                .select("ai_commentary_status,ai_commentary_processed_at,visibility,user_id")
                .eq("id", question_id)
                .limit(1)
                .execute()
            )
            existing_rows = existing_resp.data or []
            existing_row = existing_rows[0] if existing_rows else None

            first_time_completed = False
            if status == "completed" and existing_row is not None:
                prev_status = existing_row.get("ai_commentary_status")
                prev_processed_at = existing_row.get("ai_commentary_processed_at")
                first_time_completed = (
                    prev_status != "completed" or prev_processed_at is None
                )

            payload: Dict[str, Any] = {"ai_commentary_status": status}
            if set_processed_at and status == "completed":
                payload["ai_commentary_processed_at"] = datetime.now(
                    timezone.utc
                ).isoformat()

            self._client.table("questions").update(payload).eq("id", question_id).execute()
            
            # If status is "completed", also update processing_status in ai_answer_comments
            if status == "completed":
                self._client.table("ai_answer_comments").update({
                    "processing_status": "completed"
                }).eq("question_id", question_id).execute()

            # If this is the first time a private question is completed, update
            # the per-user monthly quota and paid credits.
            if first_time_completed and existing_row is not None:
                visibility = existing_row.get("visibility") or "private"
                user_id = existing_row.get("user_id")
                if visibility == "private" and user_id:
                    from datetime import datetime as dt_mod, timezone as tz_mod

                    now = dt_mod.now(tz_mod.utc)
                    month_start = dt_mod(
                        now.year, now.month, 1, tzinfo=tz_mod.utc
                    ).date().isoformat()

                    BASE_MONTHLY_FREE_LIMIT = 100

                    # Fetch current quota for this user/month
                    quota_resp = (
                        self._client.table("user_private_ai_quota")
                        .select("id,free_used_count,paid_credits_remaining")
                        .eq("user_id", user_id)
                        .eq("month_start", month_start)
                        .limit(1)
                        .execute()
                    )
                    quota_rows = quota_resp.data or []

                    if quota_rows:
                        quota_row = quota_rows[0]
                        free_used = int(quota_row.get("free_used_count") or 0)
                        paid_remaining = int(quota_row.get("paid_credits_remaining") or 0)

                        # Prefer consuming from free monthly quota, then from paid credits.
                        if free_used < BASE_MONTHLY_FREE_LIMIT:
                            new_free_used = free_used + 1
                            new_paid_remaining = paid_remaining
                        elif paid_remaining > 0:
                            new_free_used = free_used
                            new_paid_remaining = paid_remaining - 1
                        else:
                            # Quota already exhausted; do not modify row.
                            new_free_used = free_used
                            new_paid_remaining = paid_remaining

                        if (
                            new_free_used != free_used
                            or new_paid_remaining != paid_remaining
                        ):
                            self._client.table("user_private_ai_quota").update(
                                {
                                    "free_used_count": new_free_used,
                                    "paid_credits_remaining": new_paid_remaining,
                                }
                            ).eq("id", quota_row["id"]).execute()
                    else:
                        # No quota row yet for this month: create one and consume from free quota.
                        self._client.table("user_private_ai_quota").insert(
                            {
                                "user_id": user_id,
                                "month_start": month_start,
                                "free_used_count": 1,
                                "paid_credits_remaining": 0,
                            }
                        ).execute()

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
        
        Each model dict can optionally include "model_version" key.
        """
        chatgpt = answer_comments.get("chatgpt") or {}
        gemini = answer_comments.get("gemini") or {}
        mistral = answer_comments.get("mistral") or {}
        perplexity = answer_comments.get("perplexity") or {}
        deepseek = answer_comments.get("deepseek") or {}

        # Check if any model in the current update has errors
        # We'll determine the final processing_status after merging with existing data
        has_errors_in_update = False
        for model_data in [chatgpt, gemini, mistral, perplexity, deepseek]:
            if model_data.get("processing_status") == "failed":
                has_errors_in_update = True
                break
            # Also check for error strings in general_comment
            general_comment = model_data.get("general_comment", "")
            if isinstance(general_comment, str) and "Fehler:" in general_comment:
                has_errors_in_update = True
                break
        
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
            "chatgpt_model_version": chatgpt.get("model_version"),
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
            "gemini_model_version": gemini.get("model_version"),
            # Mistral
            "mistral_chosen_answer": mistral.get("chosen_answer"),
            "mistral_general_comment": mistral.get("general_comment"),
            "mistral_comment_a": mistral.get("comment_a"),
            "mistral_comment_b": mistral.get("comment_b"),
            "mistral_comment_c": mistral.get("comment_c"),
            "mistral_comment_d": mistral.get("comment_d"),
            "mistral_comment_e": mistral.get("comment_e"),
            "mistral_model_version": mistral.get("model_version"),
            # Perplexity
            "perplexity_chosen_answer": perplexity.get("chosen_answer"),
            "perplexity_general_comment": perplexity.get("general_comment"),
            "perplexity_comment_a": perplexity.get("comment_a"),
            "perplexity_comment_b": perplexity.get("comment_b"),
            "perplexity_comment_c": perplexity.get("comment_c"),
            "perplexity_comment_d": perplexity.get("comment_d"),
            "perplexity_comment_e": perplexity.get("comment_e"),
            "perplexity_model_version": perplexity.get("model_version"),
            # Deepseek
            "deepseek_chosen_answer": deepseek.get("chosen_answer"),
            "deepseek_general_comment": deepseek.get("general_comment"),
            "deepseek_comment_a": deepseek.get("comment_a"),
            "deepseek_comment_b": deepseek.get("comment_b"),
            "deepseek_comment_c": deepseek.get("comment_c"),
            "deepseek_comment_d": deepseek.get("comment_d"),
            "deepseek_comment_e": deepseek.get("comment_e"),
            "deepseek_model_version": deepseek.get("model_version"),
        }

        def _upsert():
            # Check if a row exists for this question_id and get existing data
            existing = (
                self._client.table("ai_answer_comments")
                .select("*")
                .eq("question_id", question_id)
                .limit(1)
                .execute()
            )
            
            if existing.data and len(existing.data) > 0:
                # Merge with existing data - only update fields that have non-None values in payload
                existing_row = existing.data[0]
                update_payload = {}
                
                # Only include fields from payload that have non-None values
                # This preserves existing data for models that weren't processed in this call
                # For model_version fields: only update if new value is not None (preserve existing)
                for key, value in payload.items():
                    if key != "id" and key != "question_id" and key != "processing_status":
                        if key.endswith("_model_version"):
                            # Only update model_version if new value is not None (preserve existing)
                            if value is not None:
                                update_payload[key] = value
                        elif value is not None:
                            update_payload[key] = value
                
                # If the current update has errors, set processing_status to "failed"
                # Otherwise, leave it as-is (it will be set to "completed" by update_question_status
                # when all models complete successfully)
                if has_errors_in_update:
                    update_payload["processing_status"] = "failed"
                
                # Only update if there are fields to update
                if update_payload:
                    existing_id = existing.data[0]["id"]
                    self._client.table("ai_answer_comments").update(update_payload).eq("id", existing_id).execute()
            else:
                # Insert new row - include all payload fields
                # For new rows, set processing_status based on current update
                # If there are errors, set to "failed", otherwise leave as default ("completed")
                # It will be updated to "completed" by update_question_status when all models complete
                new_payload = {**payload}
                if has_errors_in_update:
                    new_payload["processing_status"] = "failed"
                # Otherwise, use the default from the database (which is "completed")
                # This will be corrected by update_question_status when all models complete
                self._client.table("ai_answer_comments").insert(new_payload).execute()

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
        from datetime import datetime, timezone

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
