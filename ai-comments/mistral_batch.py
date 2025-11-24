import json
import os
import tempfile
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

from mistralai import File, Mistral


def build_prompt(question: Dict[str, Any]) -> str:
    return (
        "Analysiere diese Multiple-Choice-Frage und erstelle Kommentare für jede "
        "Antwortmöglichkeit als JSON-Objekt mit den Feldern "
        "chosen_answer, general_comment, comment_a, comment_b, comment_c, "
        "comment_d, comment_e:\n\n"
        f"Frage: {question.get('question')}\n"
        f"A) {question.get('option_a')}\n"
        f"B) {question.get('option_b')}\n"
        f"C) {question.get('option_c')}\n"
        f"D) {question.get('option_d')}\n"
        f"E) {question.get('option_e')}\n"
    )


def build_batch_file(
    questions: Iterable[Dict[str, Any]],
    model: str = "mistral-medium-latest",
) -> Tuple[Path, List[str]]:
    """
    Build an in-memory JSONL batch file as described in the Mistral docs.
    Mistral batch API expects each line to be: {"custom_id": "...", "body": {...}}
    The body contains the actual request body (messages, max_tokens, etc.)
    """
    buffer = BytesIO()
    question_ids: List[str] = []

    for idx, q in enumerate(questions):
        qid = str(q["id"])  # Handle UUIDs as strings
        question_ids.append(qid)
        # Mistral batch API format: only custom_id and body
        # The body contains the actual chat completions request
        request = {
            "custom_id": f"q-{qid}",
            "body": {
                "max_tokens": 4096,
                "messages": [
                    {
                        "role": "user",
                        "content": build_prompt(q),
                    }
                ],
            },
        }
        buffer.write(json.dumps(request).encode("utf-8"))
        buffer.write(b"\n")

    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".jsonl")
    path = Path(tmp.name)
    try:
        tmp.write(buffer.getvalue())
    finally:
        tmp.close()
    return path, question_ids


def submit_batch(
    questions: Iterable[Dict[str, Any]],
    client: "Mistral | None" = None,
    model: str = "mistral-small-latest",
) -> Tuple[str, List[str]]:
    """
    Create a Mistral batch job from questions.

    Returns (job_id, question_ids).
    """
    if client is None:
        api_key = os.getenv("MISTRAL_API_KEY")
        if not api_key:
            raise RuntimeError("MISTRAL_API_KEY environment variable is not set")
        client = Mistral(api_key=api_key)

    jsonl_path, question_ids = build_batch_file(questions, model=model)

    with open(jsonl_path, "rb") as f:
        file_obj = File(file_name="ai_commentary.jsonl", content=f.read())
    batch_data = client.files.upload(file=file_obj)

    created_job = client.batch.jobs.create(
        input_files=[batch_data.id],
        model=model,
        endpoint="/v1/chat/completions",
        metadata={"job_type": "ai_commentary"},
    )

    return created_job.id, question_ids


def parse_results_file(path: Path) -> Dict[str, Dict[str, Any]]:
    """
    Parse the downloaded batch results file into question_id -> commentary dict.
    """
    results: Dict[str, Dict[str, Any]] = {}
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            obj = json.loads(line)
            custom_id = obj.get("custom_id", "")
            if not custom_id.startswith("q-"):
                continue
            qid = custom_id.split("-", 1)[1]  # Extract UUID string, not int

            error = obj.get("error")
            if error:
                results[qid] = {"error": error}
                continue

            response = obj.get("response") or {}
            body = response.get("body") or {}
            choices = body.get("choices") or []
            if not choices:
                results[qid] = {"error": "no choices"}
                continue
            message = choices[0].get("message") or {}
            content = message.get("content")
            try:
                commentary = json.loads(content)
            except Exception as exc:
                commentary = {"error": f"parse error: {exc}"}
            results[qid] = commentary
    return results







