import json
import os
import logging
from typing import Any, Dict

import httpx

from prompts import SYSTEM_PROMPT_WITHOUT_REGENERATING, build_user_prompt


logger = logging.getLogger("deepseek_instant")


async def generate_commentary(
    question: Dict[str, Any],
    timeout: float = 60.0,
) -> Dict[str, Any]:
    """
    Generate commentary for a question using Deepseek API (instant call).

    Returns a dict with the commentary fields or raises an exception.
    """
    api_key = os.getenv("DEEPSEEK_API_KEY")
    if not api_key:
        raise RuntimeError("DEEPSEEK_API_KEY not configured")

    async with httpx.AsyncClient(timeout=timeout) as client:
        response = await client.post(
            "https://api.deepseek.com/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json={
                "model": "deepseek-chat",
                "messages": [
                    {"role": "system", "content": SYSTEM_PROMPT_WITHOUT_REGENERATING.strip()},
                    {"role": "user", "content": build_user_prompt(question)},
                ],
                "temperature": 0.7,
                "max_tokens": 4096,
                "stream": False,
            },
        )

        if not response.is_success:
            error_text = response.text
            logger.error(
                "Deepseek API error: %d - %s", response.status_code, error_text
            )
            raise RuntimeError(
                f"Deepseek API error: {response.status_code} - {error_text}"
            )

        data = response.json()
        if not data.get("choices") or not data["choices"][0] or not data["choices"][0].get("message"):
            logger.error("Unexpected Deepseek response structure: %s", data)
            raise RuntimeError("Unexpected Deepseek response structure")

        try:
            content = data["choices"][0]["message"]["content"]
            if "```json" in content:
                content = content.replace("```json", "").replace("```", "").strip()

            parsed_content = json.loads(content)
            required_fields = [
                "chosen_answer",
                "general_comment",
                "comment_a",
                "comment_b",
                "comment_c",
                "comment_d",
                "comment_e",
            ]
            for field in required_fields:
                if not parsed_content.get(field):
                    parsed_content[field] = (
                        None if field == "chosen_answer" else "Keine Bewertung verfügbar."
                    )

            return parsed_content
        except json.JSONDecodeError as parse_error:
            logger.error("Failed to parse Deepseek response: %s", parse_error)
            raise RuntimeError("Failed to parse Deepseek response as JSON") from parse_error

