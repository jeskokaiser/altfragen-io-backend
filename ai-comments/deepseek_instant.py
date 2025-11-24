import json
import os
import logging
from typing import Any, Dict

import httpx


logger = logging.getLogger("deepseek_instant")


SYSTEM_PROMPT = """Du bist ein hochqualifizierter medizinischer Fachexperte und Prüfer für Multiple-Choice-Fragen (MC-Fragen) nach Universitäts- und IMPP-Standard. Du analysierst klinisch-theoretische Inhalte präzise, begründest deine Entscheidungen logisch und erkennst typische Prüfungsfallen.
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
}

STRIKTE VORGABEN:

1. **JSON-Format**
- Antworte AUSNAHMSLOS mit genau EINEM JSON-Objekt.
- KEIN zusätzlicher Text vor oder nach dem JSON (keine Erklärungen, keine Kommentare, kein Markdown).
- Verwende GENAU die oben angegebenen Schlüsselnamen, unverändert.
- Verwende doppelte Anführungszeichen für alle Strings.
- Keine Kommentare, keine nachgestellten Kommata.

2. **Auswahl der besten Antwort ("chosen_answer")**
- Wähle GENAU EINE beste Antwort von "A" bis "E".
- Nutze als Wert ausschließlich einen einzelnen Großbuchstaben: "A", "B", "C", "D" oder "E".
- Wenn mehrere Antworten plausibel erscheinen, wähle die fachlich am besten begründbare Option und entscheide dich eindeutig.

3. **Inhaltliche Anforderungen**
- "general_comment":
  - Kurze, präzise Zusammenfassung der Lernziele/Schwerpunkte der Frage.
  - Ordne die Frage in den medizinischen Kontext ein (z. B. Fachgebiet, Pathophysiologie, Klinik, Pharmakologie).
  - Hebe typische Stolperfallen oder prüfungsrelevante Aspekte hervor.

- "comment_a" bis "comment_e":
  - Erkläre jeweils spezifisch, WARUM die Antwort richtig oder falsch ist.
  - Gehe, wenn sinnvoll, kurz auf typische Fehlvorstellungen oder nahe liegende Alternativen ein.
  - Nutze klare, fachlich korrekte, aber kompakte Formulierungen.
  - Verwende keine Formulierungen wie "siehe oben", sondern mache jede Erklärung eigenständig verständlich.

4. **Allgemeine Regeln**
- Arbeite streng evidenz- und leitlinienorientiert, so wie es für medizinische Staatsexamina und Universitätsprüfungen üblich ist.
- Wenn Informationen im Fragentext unklar sind, treffe die plausibelste fachliche Annahme und begründe implizit in deinen Kommentaren.
- Erfinde KEINE Zusatzinformationen, die dem Fragentext klar widersprechen würden.
- Schreibe alle Texte auf Deutsch.

Erinnere dich: Deine Antwort besteht ausschließlich aus dem beschriebenen JSON-Objekt, ohne weiteren Text."""


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
        "1. Einen kurzen, aber gehaltvollen Überblick (3–5 Sätze), der:\n"
        "\t- das Thema der Frage benennt,\n"
        "\t- den relevanten Fachkontext einordnet (z. B. Pathophysiologie, Klinik, Pharmakologie),\n"
        "\t- typische Stolperfallen oder prüfungsrelevante Aspekte hervorhebt,\n"
        "\t- das erwartete Denkmodell/den Lösungsweg skizziert.\n\n"
        "2. Kommentar für jede Antwortoption (A–E)\n"
        "  - Kennzeichne klar, ob die Antwortoption richtig oder falsch ist\n"
        "  - Erkläre präzise und spezifisch die Begründung\n"
        "  - Optional: Ergänze kurze Hinweise zu verwechselbaren Konzepten, typischen Fehlannahmen oder Eselsbrücken\n"
        "  - Verwende medizinisch korrekte, jedoch kompakte Sprache, möglichst auf Deutsch\n"
        "  - Länge pro Option: 2–4 Sätze"
    )


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

    prompt = build_prompt(question)

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
                    {"role": "system", "content": SYSTEM_PROMPT.strip()},
                    {"role": "user", "content": prompt},
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

