# External Trigger Guide for AI Commentary Service

This guide explains how to trigger the AI commentary processing service externally via HTTP endpoints.

## Base URL

All endpoints are available at:
```
https://api.altfragen.io/ai
```

## Available Endpoints

### 1. Health Check
**GET** `/health` or `/`

Check if the service is running.

**Example:**
```bash
curl https://api.altfragen.io/ai/health
```

**Response:**
```json
{
  "status": "healthy",
  "service": "ai-commentary-worker"
}
```

### 2. Submit Process
**POST** `/submit`

Triggers the AI commentary submit process. This will:
- Fetch settings from Supabase
- Find candidate questions to process
- Claim questions for processing
- Submit batch jobs to enabled providers (OpenAI, Gemini, Mistral)
- Process instant APIs (Perplexity, Deepseek) immediately

**Example:**
```bash
curl -X POST https://api.altfragen.io/ai/submit
```

**Response:**
```json
{
  "status": "accepted",
  "message": "Submit process started in background"
}
```

### 3. Consume Process
**POST** `/consume`

Triggers the AI commentary consume process. This will:
- Poll open batch jobs from all providers
- Download and parse results
- Write results to Supabase
- Update question statuses

**Example:**
```bash
curl -X POST https://api.altfragen.io/ai/consume
```

**Response:**
```json
{
  "status": "accepted",
  "message": "Consume process started in background"
}
```

### 4. Run Both Processes
**POST** `/run`

Triggers both submit and consume processes in sequence.

**Example:**
```bash
curl -X POST https://api.altfragen.io/ai/run
```

**Response:**
```json
{
  "status": "accepted",
  "message": "Both processes started in background"
}
```

## Trigger Methods

### Method 1: Using cURL (Command Line)

#### Basic Trigger
```bash
# Submit new batch jobs
curl -X POST https://api.altfragen.io/ai/submit

# Consume completed batch jobs
curl -X POST https://api.altfragen.io/ai/consume

# Run both
curl -X POST https://api.altfragen.io/ai/run
```

#### With Verbose Output
```bash
curl -X POST -v https://api.altfragen.io/ai/submit
```

#### With Error Handling
```bash
curl -X POST -f https://api.altfragen.io/ai/submit || echo "Request failed"
```

### Method 2: Using HTTPie

```bash
# Install HTTPie if needed: pip install httpie

# Submit
http POST https://api.altfragen.io/ai/submit

# Consume
http POST https://api.altfragen.io/ai/consume

# Run both
http POST https://api.altfragen.io/ai/run
```

### Method 3: Using Python (requests)

```python
import requests

# Submit
response = requests.post("https://api.altfragen.io/ai/submit")
print(response.json())

# Consume
response = requests.post("https://api.altfragen.io/ai/consume")
print(response.json())

# Run both
response = requests.post("https://api.altfragen.io/ai/run")
print(response.json())
```

### Method 4: Using JavaScript/Node.js (fetch)

```javascript
// Submit
fetch('https://api.altfragen.io/ai/submit', {
  method: 'POST'
})
.then(response => response.json())
.then(data => console.log(data));

// Consume
fetch('https://api.altfragen.io/ai/consume', {
  method: 'POST'
})
.then(response => response.json())
.then(data => console.log(data));

// Run both
fetch('https://api.altfragen.io/ai/run', {
  method: 'POST'
})
.then(response => response.json())
.then(data => console.log(data));
```

### Method 5: Using Cron (Automated Scheduling)

Set up cron jobs to automatically trigger the processes:

#### Edit crontab
```bash
crontab -e
```

#### Add cron jobs
```cron
# Submit new batch jobs every 10 minutes
*/10 * * * * curl -X POST -s https://api.altfragen.io/ai/submit > /dev/null 2>&1

# Consume completed batch jobs every 10 minutes
*/10 * * * * curl -X POST -s https://api.altfragen.io/ai/consume > /dev/null 2>&1
```

Or use the combined endpoint:
```cron
# Run both processes every 10 minutes
*/10 * * * * curl -X POST -s https://api.altfragen.io/ai/run > /dev/null 2>&1
```

#### With Logging
```cron
# Submit with logging
*/10 * * * * curl -X POST https://api.altfragen.io/ai/submit >> /var/log/ai_submit.log 2>&1

# Consume with logging
*/10 * * * * curl -X POST https://api.altfragen.io/ai/consume >> /var/log/ai_consume.log 2>&1
```

### Method 6: Using GitHub Actions (CI/CD)

```yaml
name: Trigger AI Commentary

on:
  schedule:
    # Run every 10 minutes
    - cron: '*/10 * * * *'
  workflow_dispatch: # Allow manual trigger

jobs:
  trigger:
    runs-on: ubuntu-latest
    steps:
      - name: Trigger Submit
        run: |
          curl -X POST https://api.altfragen.io/ai/submit
      
      - name: Trigger Consume
        run: |
          curl -X POST https://api.altfragen.io/ai/consume
```

### Method 7: Using Make.com / Zapier / n8n (No-Code Automation)

#### Make.com Scenario:
1. Add "HTTP Request" module
2. Method: `POST`
3. URL: `https://api.altfragen.io/ai/submit` (or `/consume` or `/run`)
4. Set up schedule or trigger

#### Zapier:
1. Create a new Zap
2. Choose "Schedule by Zapier" as trigger
3. Add "Webhooks by Zapier" as action
4. Method: `POST`
5. URL: `https://api.altfragen.io/ai/submit`

### Method 8: Using Supabase Edge Functions / Webhooks

You can trigger from Supabase Edge Functions or database webhooks:

```typescript
// Supabase Edge Function example
Deno.serve(async (req) => {
  const response = await fetch('https://api.altfragen.io/ai/submit', {
    method: 'POST',
  });
  return new Response(JSON.stringify(await response.json()));
});
```

## Recommended Schedule

For optimal performance, we recommend:

1. **Submit**: Every 10-15 minutes
   - Finds and claims new questions
   - Submits batch jobs to providers

2. **Consume**: Every 10-15 minutes
   - Polls batch jobs for completion
   - Writes results to database

3. **Combined**: Every 10-15 minutes (if using `/run`)
   - Runs both processes in sequence

## Monitoring

### Check Service Status
```bash
curl https://api.altfragen.io/ai/health
```

### View Logs (if running via Docker)
```bash
docker-compose logs -f altfragen_ai
```

### Test Endpoint
```bash
# Test with verbose output
curl -X POST -v https://api.altfragen.io/ai/submit

# Test with timing
time curl -X POST https://api.altfragen.io/ai/submit
```

## Error Handling

All endpoints return HTTP status codes:

- **202 Accepted**: Request accepted, process started in background
- **500 Internal Server Error**: Error occurred (check logs)

Example error handling in bash:
```bash
response=$(curl -s -w "\n%{http_code}" -X POST https://api.altfragen.io/ai/submit)
http_code=$(echo "$response" | tail -n1)
body=$(echo "$response" | sed '$d')

if [ "$http_code" -eq 202 ]; then
  echo "Success: $body"
else
  echo "Error ($http_code): $body"
fi
```

## Security Considerations

Currently, the endpoints are **publicly accessible**. If you need to secure them:

1. **Add API Key Authentication** (modify `main.py`):
```python
from fastapi import Header, HTTPException

API_KEY = os.getenv("API_KEY", "your-secret-key")

@app.post("/submit")
async def trigger_submit(
    background_tasks: BackgroundTasks,
    x_api_key: str = Header(...)
):
    if x_api_key != API_KEY:
        raise HTTPException(status_code=403, detail="Invalid API key")
    # ... rest of code
```

2. **Use Caddy Authentication** (add to Caddyfile):
```caddy
handle_path /ai* {
    basicauth {
        username JDJhJDE0JEVCNmdaNEg2Ti5IVGRFc...
    }
    reverse_proxy altfragen_ai:8000
}
```

3. **IP Whitelisting** (add to Caddyfile):
```caddy
handle_path /ai* {
    @allowed {
        remote_ip 192.168.1.0/24
    }
    handle @allowed {
        reverse_proxy altfragen_ai:8000
    }
    respond "Forbidden" 403
}
```

## Troubleshooting

### Service Not Responding
1. Check if container is running: `docker-compose ps`
2. Check service logs: `docker-compose logs altfragen_ai`
3. Check Caddy logs: `docker-compose logs caddy`
4. Test direct connection: `docker-compose exec altfragen_ai curl http://localhost:8000/health`

### Connection Timeout
1. Verify DNS resolution: `nslookup api.altfragen.io`
2. Check firewall rules
3. Verify Caddy is listening on ports 80/443

### 502 Bad Gateway
1. Check if `altfragen_ai` container is running
2. Verify network connectivity between Caddy and service
3. Check service logs for errors

## Example Scripts

### Bash Script for Automated Triggering
```bash
#!/bin/bash
# trigger_ai_commentary.sh

API_URL="https://api.altfragen.io/ai"
LOG_FILE="/var/log/ai_commentary_trigger.log"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" >> "$LOG_FILE"
}

trigger_submit() {
    log "Triggering submit process..."
    response=$(curl -s -X POST "$API_URL/submit")
    log "Submit response: $response"
}

trigger_consume() {
    log "Triggering consume process..."
    response=$(curl -s -X POST "$API_URL/consume")
    log "Consume response: $response"
}

# Run both
trigger_submit
sleep 5
trigger_consume
```

Make it executable:
```bash
chmod +x trigger_ai_commentary.sh
```

## Support

For issues or questions:
1. Check service logs: `docker-compose logs -f altfragen_ai`
2. Verify environment variables are set correctly
3. Ensure Supabase connection is working
4. Check that API keys for AI providers are valid

