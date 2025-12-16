import json
import tempfile
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from openai import OpenAI

from prompts import SYSTEM_PROMPT_WITH_REGENERATING, build_user_prompt


# Model version constant
MODEL_VERSION = "gpt-5.2"


JSON_SCHEMA: Dict[str, Any] = {
    "name": "answer_comments_with_choice_and_regen",
    "schema": {
        "type": "object",
        "properties": {
            "chosen_answer": {
                "type": "string",
                "description": "Ein Buchstabe von A bis E für die gewählte beste Antwort",
            },
            "general_comment": {
                "type": "string",
                "description": "Allgemeiner Kommentar zur Frage",
            },
            "comment_a": {
                "type": "string",
                "description": "Kurzer Kommentar zu Antwort A",
            },
            "comment_b": {
                "type": "string",
                "description": "Kurzer Kommentar zu Antwort B",
            },
            "comment_c": {
                "type": "string",
                "description": "Kurzer Kommentar zu Antwort C",
            },
            "comment_d": {
                "type": "string",
                "description": "Kurzer Kommentar zu Antwort D",
            },
            "comment_e": {
                "type": "string",
                "description": "Kurzer Kommentar zu Antwort E",
            },
            "regenerated_question": {
                "type": "string",
                "description": "Neu formulierte, gut lesbare Version der Frage",
            },
            "regenerated_option_a": {
                "type": "string",
                "description": "Neu formulierte Antwortoption A",
            },
            "regenerated_option_b": {
                "type": "string",
                "description": "Neu formulierte Antwortoption B",
            },
            "regenerated_option_c": {
                "type": "string",
                "description": "Neu formulierte Antwortoption C",
            },
            "regenerated_option_d": {
                "type": "string",
                "description": "Neu formulierte Antwortoption D",
            },
            "regenerated_option_e": {
                "type": "string",
                "description": "Neu formulierte Antwortoption E",
            },
        },
        "required": [
            "chosen_answer",
            "general_comment",
            "comment_a",
            "comment_b",
            "comment_c",
            "comment_d",
            "comment_e",
        ],
        "additionalProperties": False,
    },
}


# build_prompt is now imported from prompts module


def build_batch_jsonl(
    questions: Iterable[Dict[str, Any]],
    model: str = MODEL_VERSION,
) -> Tuple[Path, List[str]]:
    """
    Build a temporary JSONL file for the OpenAI Batch API and return the path
    plus the ordered list of question IDs used.
    """
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".jsonl")
    path = Path(tmp.name)
    question_ids: List[str] = []

    try:
        for q in questions:
            qid = str(q["id"])  # Handle UUIDs as strings
            question_ids.append(qid)
            body = {
                "model": model,
                "messages": [
                    {"role": "system", "content": SYSTEM_PROMPT_WITH_REGENERATING.strip()},
                    {"role": "user", "content": build_user_prompt(q)},
                ],
                "response_format": {
                    "type": "json_schema",
                    "json_schema": JSON_SCHEMA,
                },
            }
            line = {
                "custom_id": f"q-{qid}",
                "method": "POST",
                "url": "/v1/chat/completions",
                "body": body,
            }
            tmp.write(json.dumps(line).encode("utf-8"))
            tmp.write(b"\n")
    finally:
        tmp.close()

    return path, question_ids


def submit_batch(
    questions: Iterable[Dict[str, Any]],
    client: Optional[OpenAI] = None,
) -> Tuple[str, str, List[str]]:
    """
    Create an OpenAI Batch job for the given questions.

    Returns (batch_id, input_file_id, question_ids).
    """
    if client is None:
        client = OpenAI()

    jsonl_path, question_ids = build_batch_jsonl(questions)

    with open(jsonl_path, "rb") as f:
        batch_input_file = client.files.create(file=f, purpose="batch")

    batch = client.batches.create(
        input_file_id=batch_input_file.id,
        endpoint="/v1/chat/completions",
        completion_window="24h",
        metadata={"description": "ai_commentary"},
    )

    return batch.id, batch_input_file.id, question_ids


def parse_batch_output_line(line: str) -> Tuple[str, Dict[str, Any], bool]:
    """
    Parse a single JSONL line from the batch output file.

    Returns (question_id, commentary_dict, is_error).
    """
    obj = json.loads(line)
    custom_id = obj.get("custom_id", "")
    if not custom_id.startswith("q-"):
        raise ValueError(f"Unexpected custom_id: {custom_id}")
    qid = custom_id.split("-", 1)[1]  # Extract UUID string, not int

    error = obj.get("error")
    if error:
        # Return an error marker; callers can mark this question as failed.
        return qid, {"error": error}, True

    response = obj.get("response") or {}
    body = response.get("body") or {}
    choices = body.get("choices") or []
    if not choices:
        return qid, {"error": "no choices in response"}, True
    message = choices[0].get("message") or {}
    content = message.get("content")
    if isinstance(content, dict):
        commentary = content
    else:
        # Fallback: try to parse text as JSON
        try:
            commentary = json.loads(content)
        except Exception:
            return qid, {"error": "invalid JSON in content"}, True

    return qid, commentary, False


def load_batch_results(
    client: OpenAI,
    output_file_id: str,
) -> Dict[str, Dict[str, Any]]:
    """
    Download and parse the batch output file into a mapping:
    question_id -> commentary dict (or error info).
    """
    file_response = client.files.content(output_file_id)
    text = file_response.text
    results: Dict[str, Dict[str, Any]] = {}
    for line in text.splitlines():
        if not line.strip():
            continue
        qid, commentary, is_error = parse_batch_output_line(line)
        results[qid] = commentary
    return results







