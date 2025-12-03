# Supabase Metrics Dashboard

Self-hosted Prometheus and Grafana setup for monitoring Supabase metrics.

## Prerequisites

- Docker and Docker Compose installed
- Supabase project with service role key
- Access to your Supabase project reference ID

## Setup Instructions

### 1. Configure Environment Variables

Add the following variables to your `.env` file in the main `altfragen-io-backend` directory:

```bash
# Supabase Configuration
SUPABASE_PROJECT_REF=your-project-ref
SUPABASE_SERVICE_ROLE_KEY=your-service-role-key

# Grafana Configuration
GRAFANA_ADMIN_USER=admin-
GRAFANA_ADMIN_PASSWORD=change-this-password
GRAFANA_ROOT_URL=https://api.altfragen.io/metrics
```

Edit the values:
- `SUPABASE_PROJECT_REF`: Your Supabase project reference (found in project settings)
- `SUPABASE_SERVICE_ROLE_KEY`: Your service role key (found in project API settings)
- `GRAFANA_ADMIN_PASSWORD`: Change the default admin password
- `GRAFANA_ROOT_URL`: Set to your domain URL (e.g., `https://api.altfragen.io/metrics`)

**Note:** The `.env` file should be in `/Users/jeskokaiser/Desktop/altfragen-io-backend/` (the parent directory), not in the `supabase-metrics` folder.

### 2. Run Setup Script

Run the setup script to download the Supabase Grafana dashboard:

```bash
./setup.sh
```

This will:
- Create `.env` from `.env.example` if it doesn't exist
- Download the Supabase Grafana dashboard JSON
- Create the Docker network if needed

**Note:** If the script fails to download the dashboard, you can download it manually:
```bash
mkdir -p grafana/dashboards
curl -o grafana/dashboards/supabase.json https://raw.githubusercontent.com/supabase/supabase-grafana/main/dashboards/supabase.json
```

### 3. Start the Services

From the main `altfragen-io-backend` directory:

```bash
cd /Users/jeskokaiser/Desktop/altfragen-io-backend
docker-compose up -d
```

This will start all services including:
- Prometheus on port 9090 (scraping Supabase metrics every 60 seconds)
- Grafana on port 3000 (accessible via Caddy at `/metrics`)

### 4. Access Grafana

1. Navigate to `https://api.altfragen.io/metrics` (or your configured domain)
2. Login with:
   - Username: `admin` (or your configured `GRAFANA_ADMIN_USER`)
   - Password: The password you set in `.env`

### 5. Verify Dashboard

The Supabase dashboard should be automatically imported. If not:

1. Go to Dashboards → Browse
2. Look for the "Supabase" folder
3. Open the Supabase dashboard

## Configuration Details

### Prometheus

- Scrapes metrics from Supabase every 60 seconds
- Stores metrics for 30 days
- Accessible at `http://localhost:9090` (internal only)

### Grafana

- Pre-configured with Prometheus as data source
- Dashboard auto-provisioned from `grafana/dashboards/`
- Accessible via Caddy reverse proxy at `/metrics`

### Caddy Integration

The Caddyfile has been updated to proxy `/metrics*` requests to Grafana. Make sure to restart Caddy after updating:

```bash
cd /Users/jeskokaiser/Desktop/altfragen-io-backend
docker-compose restart caddy
```

## Troubleshooting

### Metrics not appearing

1. Verify your `SUPABASE_PROJECT_REF` and `SUPABASE_SERVICE_ROLE_KEY` are correct
2. Check Prometheus targets: `http://localhost:9090/targets`
3. Check Prometheus logs: `docker logs supabase_prometheus`

### Dashboard not loading

1. Verify the dashboard JSON is in `grafana/dashboards/supabase.json`
2. Check Grafana logs: `docker logs supabase_grafana`
3. Verify the data source is configured: Grafana → Configuration → Data Sources

### Cannot access via Caddy

1. Ensure the services are on the same Docker network (`backend`)
2. Restart Caddy: `docker-compose restart caddy`
3. Check Caddy logs: `docker logs caddy_reverse_proxy`

## Updating the Dashboard

To update to the latest Supabase dashboard:

```bash
cd /Users/jeskokaiser/Desktop/altfragen-io-backend/supabase-metrics/grafana/dashboards
curl -o supabase.json https://raw.githubusercontent.com/supabase/supabase-grafana/main/dashboards/supabase.json
cd ../..
docker-compose restart supabase_grafana
```

## Stopping Services

From the main `altfragen-io-backend` directory:

```bash
docker-compose down
```

To also remove volumes (this will delete all stored metrics and Grafana data):

```bash
docker-compose down -v
```

**Note:** This will stop ALL services. To stop only metrics services:
```bash
docker-compose stop supabase_prometheus supabase_grafana
```

