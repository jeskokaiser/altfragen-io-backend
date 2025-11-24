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






