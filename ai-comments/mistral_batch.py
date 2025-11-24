import json
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
) -> Tuple[Path, List[int]]:
    """
    Build an in-memory JSONL batch file as described in the Mistral docs.
    """
    buffer = BytesIO()
    question_ids: List[int] = []

    for idx, q in enumerate(questions):
        qid = int(q["id"])
        question_ids.append(qid)
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
) -> Tuple[str, List[int]]:
    """
    Create a Mistral batch job from questions.

    Returns (job_id, question_ids).
    """
    if client is None:
        client = Mistral()

    jsonl_path, question_ids = build_batch_file(questions)

    with open(jsonl_path, "rb") as f:
        file_obj = File(file_name="ai_commentary.jsonl", content=f.read())
    batch_data = client.files.upload(file=file_obj, purpose="batch")

    created_job = client.batch.jobs.create(
        input_files=[batch_data.id],
        model=model,
        endpoint="/v1/chat/completions",
        metadata={"job_type": "ai_commentary"},
    )

    return created_job.id, question_ids


def parse_results_file(path: Path) -> Dict[int, Dict[str, Any]]:
    """
    Parse the downloaded batch results file into question_id -> commentary dict.
    """
    results: Dict[int, Dict[str, Any]] = {}
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            obj = json.loads(line)
            custom_id = obj.get("custom_id", "")
            if not custom_id.startswith("q-"):
                continue
            qid = int(custom_id.split("-", 1)[1])

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







