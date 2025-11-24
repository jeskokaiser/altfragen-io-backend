import json
from typing import Any, Dict, Iterable, List, Tuple

from google import genai


def build_prompt(question: Dict[str, Any]) -> str:
    return (
        "Analysiere diese Multiple-Choice-Frage und erstelle Kommentare für jede "
        "Antwortmöglichkeit:\n\n"
        f"Frage: {question.get('question')}\n"
        f"A) {question.get('option_a')}\n"
        f"B) {question.get('option_b')}\n"
        f"C) {question.get('option_c')}\n"
        f"D) {question.get('option_d')}\n"
        f"E) {question.get('option_e')}\n\n"
        "Erstelle:\n"
        "1. Einen kurzen, aber gehaltvollen Überblick (3–5 Sätze)...\n"
        "2. Kommentar für jede Antwortoption (A–E)...\n"
    )


JSON_SCHEMA: Dict[str, Any] = {
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
}


def build_inline_requests(
    questions: Iterable[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], List[int]]:
    inline_requests: List[Dict[str, Any]] = []
    question_ids: List[int] = []
    for q in questions:
        qid = int(q["id"])
        question_ids.append(qid)
        prompt = build_prompt(q)
        inline_requests.append(
            {
                "contents": [
                    {
                        "parts": [{"text": prompt}],
                        "role": "user",
                    }
                ],
                "config": {
                    "response_mime_type": "application/json",
                    "response_schema": JSON_SCHEMA,
                    # Encode the question id into metadata so we can map back.
                    "metadata": {"question_id": str(qid)},
                },
            }
        )
    return inline_requests, question_ids


def submit_batch(
    questions: Iterable[Dict[str, Any]],
    client: "genai.Client | None" = None,
    model: str = "models/gemini-2.5-flash",
) -> Tuple[str, List[int]]:
    """
    Create a Gemini Batch job using inline requests.

    Returns (job_name, question_ids).
    """
    if client is None:
        client = genai.Client()

    inline_requests, question_ids = build_inline_requests(questions)

    inline_batch_job = client.batches.create(
        model=model,
        src=inline_requests,
        config={"display_name": "ai_commentary_gemini"},
    )

    return inline_batch_job.name, question_ids


def parse_inline_responses(
    batch_job: Any,
    original_question_ids: List[int],
) -> Dict[int, Dict[str, Any]]:
    """
    Given a finished batch_job with inlined_responses, map them back to
    question IDs and return a dict question_id -> commentary dict (or error).
    """
    results: Dict[int, Dict[str, Any]] = {}
    dest = getattr(batch_job, "dest", None)
    inlined = getattr(dest, "inlined_responses", None) if dest else None
    if not inlined:
        return results

    for idx, inline_response in enumerate(inlined):
        # Map by position as a robust fallback; metadata may not be available
        qid = original_question_ids[idx] if idx < len(original_question_ids) else None
        if qid is None:
            continue

        if getattr(inline_response, "error", None):
            results[qid] = {"error": str(inline_response.error)}
            continue

        response = getattr(inline_response, "response", None)
        if not response:
            results[qid] = {"error": "no response"}
            continue

        # For structured output with response_mime_type=application/json,
        # response.text should already be JSON.
        try:
            text = response.text if hasattr(response, "text") else str(response)
            payload = json.loads(text)
            results[qid] = payload
        except Exception as exc:
            results[qid] = {"error": f"parse error: {exc}"}

    return results







