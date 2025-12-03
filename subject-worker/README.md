# Subject Job Worker

HTTP-triggered worker service that processes subject assignment and reassignment jobs from the `subject_jobs` table in Supabase.

## Overview

This service handles both `assign` and `reassign` job types:
- **assign**: Assigns subjects to a list of questions provided in the job payload
- **reassign**: Fetches questions matching criteria (exam_name, university_id, etc.) and reassigns their subjects

## Architecture

- **FastAPI HTTP server** that exposes endpoints for job processing
- **Event-driven**: Triggered by Edge Functions when jobs are created
- **Background processing**: Jobs are processed asynchronously to avoid blocking HTTP requests

## API Endpoints

### `GET /` or `GET /health`
Health check endpoint.

**Response:**
```json
{
  "status": "ok",
  "service": "subject-job-worker"
}
```

### `POST /process-job/{job_id}`
Trigger processing of a specific job by ID. Called by Edge Functions when a new job is created.

**Response:**
```json
{
  "status": "accepted",
  "message": "Job {job_id} queued for processing",
  "job_id": "uuid"
}
```

### `POST /process-pending`
Process all pending jobs. Useful as a fallback or manual trigger.

**Response:**
```json
{
  "status": "accepted",
  "message": "Processing all pending jobs in background"
}
```

## Configuration

Environment variables (set via `.env` file):
- `SUPABASE_URL` - Supabase project URL
- `SUPABASE_SERVICE_ROLE_KEY` - Service role key for Supabase
- `OPENAI_API_KEY` - OpenAI API key for subject classification

## Running the Service

### Docker Compose
```bash
docker-compose up -d altfragen_subject_worker
```

### Development
```bash
cd subject-worker
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m uvicorn subject_job_worker:app --host 0.0.0.0 --port 8001
```

## Caddy Routing

The service is exposed via Caddy at:
- `https://api.altfragen.io/subject-worker/*`

## Integration

Edge Functions (`assign-subjects` and `reassign-subjects`) automatically trigger this worker when creating jobs by calling:
```
POST https://api.altfragen.io/subject-worker/process-job/{jobId}
```

