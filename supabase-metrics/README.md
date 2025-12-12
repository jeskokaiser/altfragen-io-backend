# Supabase Metrics Monitoring with Prometheus & Grafana

This setup provides self-hosted monitoring for your Supabase project using Prometheus and Grafana.

## Architecture

1. **Prometheus** scrapes Supabase metrics from `https://<project-ref>.supabase.co/customer/v1/privileged/metrics` every 60 seconds
2. **Grafana** reads from Prometheus and renders dashboards/alerts
3. (Optional) **Alertmanager** can be added for notifications

## Prerequisites

- Docker and Docker Compose installed
- Supabase project reference ID
- Supabase service role key (from Project Settings > API keys)

## Setup

### 1. Environment Variables

Add the following variables to your `.env` file:

```bash
# Supabase Configuration
SUPABASE_PROJECT_REF=your-project-ref
SUPABASE_SERVICE_ROLE_KEY=your-service-role-key

# Grafana Configuration (optional, defaults shown)
GRAFANA_ADMIN_USER=admin
GRAFANA_ADMIN_PASSWORD=admin
GRAFANA_ROOT_URL=http://localhost:3000
```

**Important**: 
- Replace `your-project-ref` with your Supabase project reference (found in your project URL or dashboard sidebar)
- Replace `your-service-role-key` with your service role secret from Project Settings > API keys
- Change the default Grafana admin password for production use

### 2. Start Services

```bash
docker-compose up -d prometheus grafana
```

### 3. Access Services

- **Prometheus**: http://localhost:9090
- **Grafana**: http://localhost:3000
  - Default login: `admin` / `admin` (change on first login)

#### Optional: Expose via Caddy Reverse Proxy

If you want to access Grafana through your existing `metrics.altfragen.io` domain, update `caddy/Caddyfile`:

```caddy
metrics.altfragen.io {
    encode gzip
    reverse_proxy grafana:3000
}
```

Then restart Caddy: `docker-compose restart caddy`

**Note**: Your current Caddyfile points to an external Grafana instance. You can either:
- Replace it with the new Grafana instance (as shown above)
- Keep both and use different subdomains
- Use the new Grafana only locally

### 4. Configure Grafana Data Source

The Prometheus data source is automatically configured via provisioning. If you need to verify:

1. Go to **Connections → Data sources** in Grafana
2. You should see "Prometheus" already configured pointing to `http://prometheus:9090`
3. Click "Save & test" to verify the connection

### 5. Import Supabase Dashboards

1. Go to **Dashboards → New → Import** in Grafana
2. Import the Supabase dashboard JSON from [supabase-grafana/dashboard.json](https://github.com/supabase/supabase-grafana/blob/main/dashboard.json)
   - Or use dashboard ID: `13639` from Grafana.com
3. Select your Prometheus datasource when prompted

You'll now have over 200 production-ready panels covering:
- CPU usage
- IO metrics
- WAL (Write-Ahead Logging)
- Replication status
- Index bloat
- Query throughput

### 6. Configure Alerting (Optional)

1. Copy `alerts.example.yml` to `alerts.yml` and customize thresholds
2. Uncomment the `rule_files` section in `prometheus.yml.template`
3. Update the volume mount in `docker-compose.yml` to include alerts.yml
4. Restart Prometheus: `docker-compose restart prometheus`

## Testing the Setup

### Test Prometheus Scraping

```bash
# Check if Prometheus is scraping successfully
curl http://localhost:9090/api/v1/targets

# Query a metric
curl 'http://localhost:9090/api/v1/query?query=up'
```

### Test Supabase Metrics Endpoint Directly

```bash
curl https://<project-ref>.supabase.co/customer/v1/privileged/metrics \
  --user "service_role:<service-role-key>"
```

## Configuration Files

- `prometheus.yml.template` - Prometheus configuration with Supabase scrape job
- `grafana/provisioning/datasources/prometheus.yml` - Auto-configures Prometheus data source in Grafana
- `alerts.example.yml` - Example alerting rules (optional)

## Data Retention

- Prometheus data is retained for **30 days** by default
- Data is stored in the `prometheus_data` Docker volume
- Adjust retention in `docker-compose.yml` by modifying `--storage.tsdb.retention.time`

## Multiple Projects

To monitor multiple Supabase projects, add additional scrape jobs to `prometheus.yml.template`:

```yaml
scrape_configs:
  - job_name: 'supabase-project-1'
    # ... configuration ...
  - job_name: 'supabase-project-2'
    # ... configuration ...
```

## Security Notes

1. **Rotate service role keys** regularly and update the Prometheus configuration
2. **Change default Grafana credentials** immediately after first login
3. **Use environment variables** for secrets, never commit them to version control
4. **Restrict network access** - consider using firewall rules to limit access to Prometheus/Grafana ports

## Troubleshooting

### Prometheus can't scrape Supabase

- Verify `SUPABASE_PROJECT_REF` and `SUPABASE_SERVICE_ROLE_KEY` are set correctly
- Check Prometheus logs: `docker-compose logs prometheus`
- Test the Supabase endpoint directly with curl (see above)

### Grafana can't connect to Prometheus

- Ensure both services are on the same Docker network (`metrics`)
- Check Grafana logs: `docker-compose logs grafana`
- Verify the Prometheus URL in Grafana data source settings

### Metrics not appearing

- Wait at least 60 seconds after starting Prometheus (scrape interval)
- Check Prometheus targets: http://localhost:9090/targets
- Verify the Supabase project is active and accessible

## References

- [Supabase Metrics API Documentation](https://supabase.com/docs/guides/telemetry/metrics/grafana-self-hosted)
- [Prometheus Documentation](https://prometheus.io/docs/)
- [Grafana Documentation](https://grafana.com/docs/)

