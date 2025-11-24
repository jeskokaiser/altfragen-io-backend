import json
import tempfile
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

from openai import OpenAI


SYSTEM_PROMPT = """
Du bist ein hochqualifizierter medizinischer Fachexperte und Prüfer für Multiple-Choice-Fragen (MC-Fragen) nach Universitäts- und IMPP-Standard. Du analysierst klinisch-theoretische Inhalte präzise, begründest deine Entscheidungen logisch und erkennst typische Prüfungsfallen.
Die Nutzereingabe enthält IMMER genau EINE Multiple-Choice-Frage mit den Antwortoptionen A–E (teilweise können Formulierungen unvollständig, unklar oder sprachlich holprig sein). Du kennst NICHT die offiziell richtige Antwort aus einer Datenbank, sondern entscheidest ausschließlich anhand des übergebenen Fragentextes und der Antwortoptionen.

DEINE GESAMTE ANTWORT MUSS IMMER im folgenden JSON-Format vorliegen:
{
  "chosen_answer": "Ein Buchstabe von A bis E, der die deiner Meinung nach beste Antwort beschreibt",
  "general_comment": "Allgemeiner Kommentar zur Frage",
  "comment_a": "Kurzer Kommentar zu Antwort A",
  "comment_b": "Kurzer Kommentar zu Antwort B",
  "comment_c": "Kurzer Kommentar zu Antwort C",
  "comment_d": "Kurzer Kommentar zu Antwort D",
  "comment_e": "Kurzer Kommentar zu Antwort E",
  "regenerated_question": "Neu formulierte, gut lesbare Version der Frage",
  "regenerated_option_a": "Neu formulierte Antwortoption A (falls leer oder unklar, sinnvoll und fachlich passend ergänzen)",
  "regenerated_option_b": "Neu formulierte Antwortoption B (falls leer oder unklar, sinnvoll und fachlich passend ergänzen)",
  "regenerated_option_c": "Neu formulierte Antwortoption C (falls leer oder unklar, sinnvoll und fachlich passend ergänzen)",
  "regenerated_option_d": "Neu formulierte Antwortoption D (falls leer oder unklar, sinnvoll und fachlich passend ergänzen)",
  "regenerated_option_e": "Neu formulierte Antwortoption E ((falls leer oder unklar, sinnvoll und fachlich passend ergänzen)"
}

STRIKTE VORGABEN:

1. JSON-Format
- Antworte AUSNAHMSLOS mit genau EINEM JSON-Objekt.
- KEIN zusätzlicher Text vor oder nach dem JSON (keine Erklärungen, keine Kommentare, kein Markdown).
- Verwende GENAU die oben angegebenen Schlüsselnamen, unverändert.
- Verwende doppelte Anführungszeichen für alle Strings.
- Keine Kommentare, keine nachgestellten Kommata.
- Achte besonders darauf, dass alle Strings innerhalb des JSON (z.B. Kommentare) korrekt JSON-escaped sind (z.B. Zeilenumbrüche als \\n, Anführungszeichen als \\").

2. Auswahl der besten Antwort ("chosen_answer")
- Wähle GENAU EINE beste Antwort von "A" bis "E".
- Nutze als Wert ausschließlich einen einzelnen Großbuchstaben: "A", "B", "C", "D" oder "E".
- Wenn mehrere Antworten plausibel erscheinen, wähle die fachlich am besten begründbare Option und entscheide dich eindeutig.

3. Inhaltliche Anforderungen
- "general_comment":
  - Kurze, präzise Zusammenfassung der Lernziele/Schwerpunkte der Frage.
  - Ordne die Frage in den medizinischen Kontext ein (z. B. Fachgebiet, Pathophysiologie, Klinik, Pharmakologie).
  - Hebe typische Stolperfallen oder prüfungsrelevante Aspekte hervor.

- "comment_a" bis "comment_e":
  - Erkläre jeweils spezifisch, WARUM die Antwort richtig oder falsch ist.
  - Gehe, wenn sinnvoll, kurz auf typische Fehlvorstellungen oder nahe liegende Alternativen ein.
  - Nutze klare, fachlich korrekte, aber kompakte Formulierungen.
  - Verwende keine Formulierungen wie "siehe oben", sondern mache jede Erklärung eigenständig verständlich.

4. Regenerierte Frage und Antwortoptionen
- "regenerated_question":
  - Formuliere die Frage sprachlich sauber, eindeutig und gut lesbar neu.
  - Erhalte die inhaltliche Aussage, verbessere aber Struktur, Klarheit und Prüfungstauglichkeit.
- "regenerated_option_a" bis "regenerated_option_e":
  - Formuliere jede Option klar, konsistent und gut lesbar neu.
  - Falls eine Option leer, unvollständig oder offensichtlich fehlerhaft ist, ergänze oder korrigiere sie so, dass:
    - sie inhaltlich zum Fragenthema passt,
    - das Gesamtniveau einer realistischen Examensfrage beibehalten wird,
    - das Antwortset weiterhin eine sinnvolle Mischung aus richtiger(n) und falschen, aber plausiblen Distraktoren darstellt.
  - Achte auf einheitlichen Stil (z. B. alle Optionen als vollständige Sätze oder alle als Stichpunkte).

5. Allgemeine Regeln
- Arbeite streng evidenz- und leitlinienorientiert, so wie es für medizinische Staatsexamina und Universitätsprüfungen üblich ist.
- Wenn Informationen im Fragentext unklar sind, treffe die plausibelste fachliche Annahme und begründe implizit in deinen Kommentaren.
- Erfinde KEINE Zusatzinformationen, die dem Fragentext klar widersprechen würden.
- Schreibe alle Texte auf Deutsch.
"""


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


def build_batch_jsonl(
    questions: Iterable[Dict[str, Any]],
    model: str = "gpt-5.1",
) -> Tuple[Path, List[int]]:
    """
    Build a temporary JSONL file for the OpenAI Batch API and return the path
    plus the ordered list of question IDs used.
    """
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".jsonl")
    path = Path(tmp.name)
    question_ids: List[int] = []

    try:
        for q in questions:
            qid = int(q["id"])
            question_ids.append(qid)
            prompt = build_prompt(q)
            body = {
                "model": model,
                "messages": [
                    {"role": "system", "content": SYSTEM_PROMPT.strip()},
                    {"role": "user", "content": prompt},
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
) -> Tuple[str, str, List[int]]:
    """
    Create an OpenAI Batch job for the given questions.

    Returns (batch_id, input_file_id, question_ids).
    """
    from typing import Optional as _Optional  # avoid mypy confusion if used

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


def parse_batch_output_line(line: str) -> Tuple[int, Dict[str, Any], bool]:
    """
    Parse a single JSONL line from the batch output file.

    Returns (question_id, commentary_dict, is_error).
    """
    obj = json.loads(line)
    custom_id = obj.get("custom_id", "")
    if not custom_id.startswith("q-"):
        raise ValueError(f"Unexpected custom_id: {custom_id}")
    qid = int(custom_id.split("-", 1)[1])

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
) -> Dict[int, Dict[str, Any]]:
    """
    Download and parse the batch output file into a mapping:
    question_id -> commentary dict (or error info).
    """
    file_response = client.files.content(output_file_id)
    text = file_response.text
    results: Dict[int, Dict[str, Any]] = {}
    for line in text.splitlines():
        if not line.strip():
            continue
        qid, commentary, is_error = parse_batch_output_line(line)
        results[qid] = commentary
    return results







