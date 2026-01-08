import os
import asyncio
from typing import Any, Dict, List

from supabase import create_client, Client


class SupabaseClient:
    """
    Async wrapper around the official Supabase Python SDK for OCR service.
    
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
        pass

    async def _run_sync(self, func, *args, **kwargs):
        """Run a sync function in an async executor"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, lambda: func(*args, **kwargs))

    async def insert_questions(self, questions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Insert questions into the questions table.
        
        Args:
            questions: List of question dictionaries with database column names
            
        Returns:
            List of inserted question records
        """
        def _insert():
            response = self._client.table("questions").insert(questions).execute()
            return response.data

        return await self._run_sync(_insert)

    async def upload_file_to_storage(self, bucket: str, filename: str, content: bytes, content_type: str) -> str:
        """
        Upload a file to Supabase storage and return the public URL.
        
        Args:
            bucket: Storage bucket name
            filename: Name for the file in storage
            content: File content as bytes
            content_type: MIME type of the file
            
        Returns:
            Public URL of the uploaded file
        """
        def _upload():
            response = self._client.storage.from_(bucket).upload(
                filename,
                content,
                file_options={"content-type": content_type, "upsert": "true"}
            )
            if hasattr(response, 'error') and response.error:
                raise Exception(f"Storage upload error: {response.error}")
            return self._client.storage.from_(bucket).get_public_url(filename)

        return await self._run_sync(_upload)

