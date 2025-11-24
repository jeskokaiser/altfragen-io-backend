# AI Comments Docker/Caddy Integration

This document describes how the ai-comments service is integrated into the Docker Compose and Caddy setup.

## Architecture

The ai-comments service runs as a FastAPI HTTP server that exposes endpoints to trigger the AI commentary processing workflows.

## Components

### Docker Setup

- **Service Name**: `altfragen_ai`
- **Container Name**: `altfragen_ai`
- **Port**: 8000 (internal)
- **Build Context**: `./ai-comments`
- **Network**: `backend`

### Caddy Routing

The Caddyfile routes all requests matching `/ai*` to the `altfragen_ai:8000` service.

Example URLs:
- `https://api.altfragen.io/ai/health` - Health check
- `https://api.altfragen.io/ai/submit` - Trigger submit process
- `https://api.altfragen.io/ai/consume` - Trigger consume process
- `https://api.altfragen.io/ai/run` - Trigger both processes

## API Endpoints

### GET `/` or `/health`
Health check endpoint.

**Response:**
```json
{
  "status": "ok",
  "service": "ai-commentary-worker"
}
```

### POST `/submit`
Triggers the AI commentary submit process. This will:
- Fetch settings from Supabase
- Find candidate questions to process
- Claim questions for processing
- Submit batch jobs to enabled providers (OpenAI, Gemini, Mistral)
- Process instant APIs (Perplexity, Deepseek) immediately

**Response:**
```json
{
  "status": "accepted",
  "message": "Submit process started in background"
}
```

### POST `/consume`
Triggers the AI commentary consume process. This will:
- Poll open batch jobs from all providers
- Download and parse results
- Write results to Supabase
- Update question statuses

**Response:**
```json
{
  "status": "accepted",
  "message": "Consume process started in background"
}
```

### POST `/run`
Triggers both submit and consume processes in sequence.

**Response:**
```json
{
  "status": "accepted",
  "message": "Both processes started in background"
}
```

## Environment Variables

The service requires the following environment variables (set via `.env` file):

- `SUPABASE_URL` - Supabase project URL
- `SUPABASE_SERVICE_ROLE_KEY` - Service role key for Supabase
- `OPENAI_API_KEY` - OpenAI API key (for batch API)
- `GEMINI_API_KEY` - Google Gemini API key
- `MISTRAL_API_KEY` - Mistral API key
- `PERPLEXITY_API_KEY` - Perplexity API key (for instant API)
- `DEEPSEEK_API_KEY` - Deepseek API key (for instant API)

## Running the Service

### Development

```bash
cd ai-comments
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m uvicorn main:app --host 0.0.0.0 --port 8000
```

### Docker Compose

```bash
# Build and start all services
docker-compose up -d

# View logs
docker-compose logs -f altfragen_ai

# Rebuild after changes
docker-compose build altfragen_ai
docker-compose up -d altfragen_ai
```

## Cron Integration

You can set up cron jobs to automatically trigger the processes:

```bash
# Submit new batch jobs every 10 minutes
*/10 * * * * curl -X POST https://api.altfragen.io/ai/submit

# Consume completed batch jobs every 10 minutes
*/10 * * * * curl -X POST https://api.altfragen.io/ai/consume
```

Or use the combined endpoint:

```bash
# Run both every 10 minutes
*/10 * * * * curl -X POST https://api.altfragen.io/ai/run
```

## File Structure

```
ai-comments/
├── Dockerfile              # Container definition
├── main.py                 # FastAPI HTTP server
├── requirements.txt        # Python dependencies
├── __init__.py            # Package marker
├── supabase_client.py     # Supabase REST client
├── ai_commentary_submit.py # Submit workflow
├── ai_commentary_consume.py # Consume workflow
├── openai_batch.py        # OpenAI batch API helpers
├── gemini_batch.py        # Gemini batch API helpers
├── mistral_batch.py       # Mistral batch API helpers
├── perplexity_instant.py  # Perplexity instant API
└── deepseek_instant.py    # Deepseek instant API
```

## Troubleshooting

### Import Errors

If you see import errors related to relative imports, ensure:
1. The `PYTHONPATH` environment variable is set to `/app` in the Dockerfile
2. The service is run with `python -m uvicorn` (not just `uvicorn`)
3. All files are present in the container (check with `docker-compose exec altfragen_ai ls -la /app`)

### Connection Issues

If the service isn't reachable:
1. Check that the container is running: `docker-compose ps`
2. Check Caddy logs: `docker-compose logs caddy`
3. Verify the Caddyfile routing: `cat caddy/Caddyfile`
4. Test direct connection: `docker-compose exec altfragen_ai curl http://localhost:8000/health`

