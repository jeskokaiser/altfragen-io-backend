Python AI Commentary Worker
===========================

This package contains an asynchronous Python worker that replaces the Supabase
Edge Function `process-ai-commentary` for generating AI commentary on
questions using OpenAI, Gemini, and Mistral **Batch APIs**.

The worker:

- Reads `ai_commentary_settings` from Supabase (including `batch_size`).
- Selects pending / stuck / summary-missing `questions` up to `batch_size`.
- Submits batch jobs to OpenAI, Gemini, and Mistral.
- Polls batch jobs and writes results into `ai_answer_comments`.
- Updates `questions.ai_commentary_status` to `completed` / `failed`.

Directory layout
----------------

- `supabase_client.py` – async Supabase REST helper.
- `openai_batch.py` – helpers for OpenAI Batch API.
- `gemini_batch.py` – helpers for Gemini Batch API.
- `mistral_batch.py` – helpers for Mistral Batch API.
- `ai_commentary_submit.py` – selects/claims questions and submits batch jobs.
- `ai_commentary_consume.py` – polls batch jobs and writes results to Supabase.
- `requirements.txt` – Python dependencies for the worker.

Environment variables
---------------------

The worker expects the following variables on your VPS:

- `SUPABASE_URL` – e.g. `https://<project>.supabase.co`
- `SUPABASE_SERVICE_ROLE_KEY` – **service role key**, used server-side only
- `OPENAI_API_KEY` – for OpenAI Batch API
- `GEMINI_API_KEY` – for Gemini Batch API
- `MISTRAL_API_KEY` – for Mistral Batch API
- `PUSHOVER_USER_KEY` – (optional) Pushover user key for error notifications
- `PUSHOVER_API_TOKEN` – (optional) Pushover API token for error notifications

Installation
------------

From the project root:

```bash
cd python_worker
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Running locally
---------------

Always run the scripts as modules so that package-relative imports work:

```bash
cd /path/to/altfragen-io

# Submit batch jobs for all enabled models
python -m python_worker.ai_commentary_submit

# Poll and consume finished batch jobs
python -m python_worker.ai_commentary_consume
```

Required database table for batch jobs
--------------------------------------

The worker tracks provider batch jobs in a new table `ai_commentary_batch_jobs`.
You must create this table yourself in Supabase (e.g. via SQL editor or
migration). One possible schema is:

```sql
create table if not exists ai_commentary_batch_jobs (
  id uuid primary key default gen_random_uuid(),
  provider text not null, -- 'openai' | 'gemini' | 'mistral'
  batch_id text not null,
  status text not null default 'pending',
  question_ids uuid[] not null,
  input_file_id text,
  output_file_id text,
  error_file_id text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);
```

> Note: The worker never creates or alters database tables itself; you remain
> in full control of migrations.

Cron examples (VPS)
-------------------

Example cron configuration (assuming `python` and venv are set up properly):

```cron
*/10 * * * * cd /path/to/altfragen-io && . python_worker/.venv/bin/activate && python -m python_worker.ai_commentary_submit >> /var/log/ai_commentary_submit.log 2>&1
*/10 * * * * cd /path/to/altfragen-io && . python_worker/.venv/bin/activate && python -m python_worker.ai_commentary_consume >> /var/log/ai_commentary_consume.log 2>&1
```

Adjust paths and intervals as needed for your environment.

Pushover Notifications
----------------------

The worker can send push notifications via Pushover when errors occur. To enable:

1. Create a Pushover account at https://pushover.net
2. Create an application to get an API token
3. Get your user key from your account dashboard
4. Set the environment variables:
   - `PUSHOVER_USER_KEY` – Your Pushover user key
   - `PUSHOVER_API_TOKEN` – Your Pushover application API token

Notifications are sent for:
- Critical errors in submit/consume processes
- Batch job submission failures
- Batch job status issues (non-successful completions)
- Missing output files from batch jobs
- High error rates (>20%) in batch processing

If Pushover credentials are not set, notifications are silently disabled and the worker continues to function normally.

Automatic Feature Disabling on Quota Errors
-------------------------------------------

The worker automatically detects when any AI API runs out of credits/quota and will:

1. **Detect quota errors** from all supported APIs (OpenAI, Gemini, Mistral, Perplexity, DeepSeek)
2. **Automatically disable** the AI commentary feature by setting `feature_enabled = FALSE` in `ai_commentary_settings`
3. **Send a Pushover notification** alerting you that the feature has been disabled

Quota errors are detected by analyzing error messages for patterns like:
- "quota", "credit", "insufficient funds", "billing"
- HTTP status codes 429 (Too Many Requests) or 402 (Payment Required)
- API-specific error codes (e.g., OpenAI's `insufficient_quota`, Gemini's `resource_exhausted`)

Once disabled, the feature will remain off until manually re-enabled in the Supabase settings. This prevents the system from continuing to attempt API calls when credits are exhausted.






