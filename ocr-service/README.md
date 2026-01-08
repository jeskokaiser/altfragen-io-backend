OCR Question Extraction Service
===============================

This service processes PDF, PNG, and JPEG files using Mistral OCR API to extract multiple-choice questions and automatically saves them to the Supabase database.

Features
--------

- Accepts PDF, PNG, and JPEG file uploads
- Uses Mistral OCR API with JSON schema mode for structured question extraction
- Automatically saves extracted questions to Supabase `questions` table
- Validates and parses question data before insertion
- Admin-only access (enforced at frontend level)

Directory layout
----------------

- `main.py` – FastAPI application with OCR processing endpoint
- `supabase_client.py` – Supabase client wrapper for database operations
- `requirements.txt` – Python dependencies
- `Dockerfile` – Container configuration

Environment variables
---------------------

The service expects the following variables:

- `MISTRAL_API_KEY` – Mistral API key for OCR API
- `SUPABASE_URL` – e.g. `https://<project>.supabase.co`
- `SUPABASE_SERVICE_ROLE_KEY` – **service role key**, used server-side only
- `OCR_SERVICE_PORT` – Port number (default: 8002)

API Endpoints
-------------

### POST `/process`

Processes a document file and extracts questions.

**Request:**
- `file`: Multipart file (PDF, PNG, or JPEG)
- `userId`: User ID (string)
- `visibility`: 'private' or 'university' (string)
- `universityId`: University ID (optional string)
- `examName`: Exam name (optional string)
- `examYear`: Exam year (optional string)
- `examSemester`: Exam semester 'WS' or 'SS' (optional string)
- `subject`: Subject name (optional string)

**Response:**
```json
{
  "success": true,
  "questions_extracted": 5,
  "message": "Questions extracted and saved successfully"
}
```

**Error Response:**
```json
{
  "success": false,
  "error": "Error message"
}
```

Running locally
---------------

```bash
cd ocr-service
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m uvicorn main:app --host 0.0.0.0 --port 8002
```

Docker
------

Build and run with Docker Compose (see main `docker-compose.yml`):

```bash
docker-compose up altfragen_ocr
```

The service will be available at `http://api.altfragen.io/ocr-service/process` when routed through Caddy.

