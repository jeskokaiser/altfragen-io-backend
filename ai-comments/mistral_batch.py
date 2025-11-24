import json
import os
import re
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
    Build an in-memory JSONL batch file for Mistral batch API.
    
    According to Mistral docs, format should be:
    {"custom_id": "...", "body": {"messages": [...], "max_tokens": ...}}
    
    However, the file upload endpoint validates against fine-tuning format.
    We'll keep the correct format and track the custom IDs separately.
    """
    buffer = BytesIO()
    question_ids: List[str] = []
    
    # Store mapping for later retrieval
    custom_id_mapping = {}

    for idx, q in enumerate(questions):
        qid = str(q["id"])  # Handle UUIDs as strings
        question_ids.append(qid)
        custom_id = f"q-{qid}"
        custom_id_mapping[idx] = custom_id
        
        # Mistral batch format as per documentation
        request = {
            "custom_id": custom_id,
            "body": {
                "messages": [
                    {
                        "role": "user",
                        "content": build_prompt(q),
                    }
                ],
                "max_tokens": 4096,
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
    model: str = "mistral-medium-latest",
) -> Tuple[str, List[str]]:
    """
    Create a Mistral batch job from questions.

    Returns (job_id, question_ids).
    """
    api_key = os.getenv("MISTRAL_API_KEY")
    if not api_key:
        raise RuntimeError("MISTRAL_API_KEY environment variable is not set")
    
    if client is None:
        client = Mistral(api_key=api_key)

    jsonl_path, question_ids = build_batch_file(questions, model=model)

    # Read the file content
    with open(jsonl_path, "rb") as f:
        file_content = f.read()
    
    # The Mistral SDK files.upload() validates against fine-tuning schema,
    # but we need batch schema. Try different approaches:
    
    import httpx
    import logging
    
    logger = logging.getLogger(__name__)
    
    # Prepare file for upload
    files = {
        'file': ('batch_input.jsonl', file_content, 'application/x-jsonlines')
    }
    headers = {
        'Authorization': f'Bearer {api_key}',
    }
    
    file_uploaded = False
    batch_data = None
    last_error = None
    
    # Approach 1: Try raw API with explicit batch purpose
    with httpx.Client() as http_client:
        try:
            logger.info("Attempting to upload batch file with purpose=batch")
            response = http_client.post(
                'https://api.mistral.ai/v1/files',
                headers=headers,
                files=files,
                data={'purpose': 'batch'}
            )
            logger.info(f"Upload response status: {response.status_code}")
            if response.status_code in [200, 201]:
                batch_data_dict = response.json()
                class FileResponse:
                    def __init__(self, id):
                        self.id = id
                batch_data = FileResponse(batch_data_dict.get('id'))
                file_uploaded = True
                logger.info(f"Successfully uploaded batch file with ID: {batch_data.id}")
            else:
                logger.warning(f"Upload failed with status {response.status_code}: {response.text}")
                last_error = f"HTTP {response.status_code}: {response.text}"
        except Exception as e:
            logger.warning(f"Raw API with purpose=batch failed: {e}")
            last_error = str(e)
    
    # Approach 2: Try without purpose parameter  
    if not file_uploaded:
        with httpx.Client() as http_client:
            try:
                logger.info("Attempting to upload batch file without purpose parameter")
                response = http_client.post(
                    'https://api.mistral.ai/v1/files',
                    headers=headers,
                    files=files
                )
                logger.info(f"Upload response status: {response.status_code}")
                if response.status_code in [200, 201]:
                    batch_data_dict = response.json()
                    class FileResponse:
                        def __init__(self, id):
                            self.id = id
                    batch_data = FileResponse(batch_data_dict.get('id'))
                    file_uploaded = True
                    logger.info(f"Successfully uploaded batch file with ID: {batch_data.id}")
                else:
                    logger.warning(f"Upload failed with status {response.status_code}: {response.text}")
                    last_error = f"HTTP {response.status_code}: {response.text}"
            except Exception as e:
                logger.warning(f"Raw API without purpose failed: {e}")
                last_error = str(e)
    
    # Approach 3: Last resort - use SDK (will likely fail with validation error)
    if not file_uploaded:
        logger.info("Falling back to SDK file upload method")
        try:
            file_obj = File(file_name="batch_input.jsonl", content=file_content)
            batch_data = client.files.upload(file=file_obj)
            logger.info(f"Successfully uploaded batch file with SDK, ID: {batch_data.id}")
        except Exception as e:
            logger.error(f"SDK upload failed: {e}")
            # Re-raise with more context
            raise RuntimeError(f"Failed to upload batch file to Mistral. Last error: {last_error or e}") from e

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
    
    Mistral batch output format should be similar to OpenAI:
    {
      "custom_id": "q-<uuid>",
      "response": {
        "body": {
          "choices": [{
            "message": {
              "content": "<json string>"
            }
          }]
        }
      }
    }
    """
    import logging
    logger = logging.getLogger(__name__)
    
    results: Dict[str, Dict[str, Any]] = {}
    line_count = 0
    
    with path.open("r", encoding="utf-8") as f:
        for line_num, line in enumerate(f, 1):
            if not line.strip():
                continue
            line_count += 1
            
            try:
                obj = json.loads(line)
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse line {line_num} as JSON: {e}")
                logger.debug(f"Line content: {line[:200]}...")
                continue
            
            # Log first line structure for debugging
            if line_num == 1:
                logger.info(f"First line structure (keys): {list(obj.keys())}")
                logger.debug(f"First line full structure: {json.dumps(obj, indent=2, default=str)[:1000]}...")
                
            custom_id = obj.get("custom_id", "")
            if not custom_id.startswith("q-"):
                logger.warning(f"Line {line_num}: custom_id '{custom_id}' doesn't start with 'q-', skipping")
                continue
            qid = custom_id.split("-", 1)[1]  # Extract UUID string, not int

            # Check for errors
            error = obj.get("error")
            if error:
                logger.error(f"Question {qid} has error: {error}")
                results[qid] = {"error": error}
                continue

            # Parse response structure
            response = obj.get("response")
            if not response:
                logger.warning(f"Question {qid}: no 'response' field in result. Keys: {list(obj.keys())}")
                logger.debug(f"Full object structure: {json.dumps(obj, indent=2, default=str)[:2000]}")
                results[qid] = {"error": "no response field"}
                continue
                
            # Log response structure for debugging
            logger.debug(f"Question {qid}: response type={type(response)}")
            if isinstance(response, dict):
                logger.debug(f"Question {qid}: response keys: {list(response.keys())}")
                body = response.get("body") or response
            else:
                logger.debug(f"Question {qid}: response is not a dict: {type(response)}")
                body = response
                
            if isinstance(body, dict):
                logger.debug(f"Question {qid}: body keys: {list(body.keys())}")
            else:
                logger.debug(f"Question {qid}: body is not a dict: {type(body)}")
                
            choices = body.get("choices") or []
            if not choices:
                logger.warning(f"Question {qid}: no choices in response. Body keys: {list(body.keys()) if isinstance(body, dict) else 'not a dict'}")
                logger.debug(f"Question {qid}: Full response structure: {json.dumps(response, indent=2, default=str)[:2000]}")
                results[qid] = {"error": "no choices"}
                continue
                
            message = choices[0].get("message") or {}
            content = message.get("content")
            
            if not content:
                logger.warning(f"Question {qid}: no content in message. Message keys: {list(message.keys())}")
                logger.debug(f"Full message object: {json.dumps(message, indent=2)}")
                results[qid] = {"error": "no content"}
                continue
            
            # Log content details for debugging
            logger.debug(f"Question {qid}: content type={type(content)}, length={len(str(content)) if content else 0}")
            if content:
                logger.debug(f"Question {qid}: content preview (first 500 chars): {str(content)[:500]}")
            
            # Parse JSON content
            # Mistral often returns JSON wrapped in markdown code blocks, so we need to extract it
            try:
                if isinstance(content, dict):
                    commentary = content
                elif isinstance(content, str):
                    content_stripped = content.strip()
                    if not content_stripped:
                        logger.error(f"Question {qid}: content is empty string")
                        results[qid] = {"error": "empty content"}
                        continue
                    
                    # Try to extract JSON from markdown code blocks
                    # Pattern to match ```json ... ``` blocks
                    json_block_pattern = r'```(?:json)?\s*(\{.*?\})\s*```'
                    matches = re.findall(json_block_pattern, content_stripped, re.DOTALL)
                    
                    if matches:
                        # Use the first JSON block found
                        json_str = matches[0]
                        logger.debug(f"Question {qid}: extracted JSON from markdown code block")
                        commentary = json.loads(json_str)
                    else:
                        # Try to find JSON object directly in the text
                        # Look for { ... } pattern
                        json_obj_pattern = r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}'
                        json_matches = re.findall(json_obj_pattern, content_stripped, re.DOTALL)
                        
                        if json_matches:
                            # Try to parse the largest match (likely the main JSON object)
                            json_str = max(json_matches, key=len)
                            logger.debug(f"Question {qid}: extracted JSON object from text")
                            commentary = json.loads(json_str)
                        else:
                            # Last resort: try parsing the whole content as JSON
                            logger.debug(f"Question {qid}: attempting to parse entire content as JSON")
                            commentary = json.loads(content_stripped)
                else:
                    logger.error(f"Question {qid}: unexpected content type: {type(content)}")
                    results[qid] = {"error": f"unexpected content type: {type(content)}"}
                    continue
                    
                results[qid] = commentary
                logger.debug(f"Successfully parsed commentary for question {qid}")
            except json.JSONDecodeError as exc:
                logger.error(f"Question {qid}: failed to parse content as JSON: {exc}")
                logger.error(f"Content (first 500 chars): {str(content)[:500]}")
                # Try one more time with a more aggressive extraction
                try:
                    # Try to find any JSON-like structure
                    json_match = re.search(r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}', str(content), re.DOTALL)
                    if json_match:
                        json_str = json_match.group(0)
                        commentary = json.loads(json_str)
                        results[qid] = commentary
                        logger.info(f"Question {qid}: successfully extracted JSON on retry")
                    else:
                        results[qid] = {"error": f"parse error: {exc}"}
                except Exception as retry_exc:
                    logger.error(f"Question {qid}: retry also failed: {retry_exc}")
                    results[qid] = {"error": f"parse error: {exc}"}
            except Exception as exc:
                logger.error(f"Question {qid}: unexpected error parsing content: {exc}", exc_info=True)
                results[qid] = {"error": f"parse error: {exc}"}
    
    logger.info(f"Parsed {len(results)} results from {line_count} lines in batch output file")
    return results







