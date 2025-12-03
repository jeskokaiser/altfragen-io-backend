# Troubleshooting White Page Issue

## Quick Tests

### 1. Test Direct Grafana Access

First, test if Grafana is accessible directly (bypassing Caddy):

```bash
# From your VPS, test local access
curl -I http://localhost:3000

# Or from your local machine, if you have SSH port forwarding:
# ssh -L 3000:localhost:3000 user@your-vps
# Then access http://localhost:3000 in your browser
```

### 2. Check Dashboard File

Verify the dashboard file exists and is valid:

```bash
cd /opt/altfragen-io-backend/supabase-metrics
ls -lh grafana/dashboards/supabase.json
head -20 grafana/dashboards/supabase.json
```

If the file is missing or empty, download it:

```bash
mkdir -p grafana/dashboards
curl -o grafana/dashboards/supabase.json \
  https://raw.githubusercontent.com/supabase/supabase-grafana/main/dashboard.json
```

### 3. Check Caddy Access Logs

Enable access logging in Caddy to see if requests are reaching it:

```bash
# Check if there are any access logs
docker logs caddy_reverse_proxy | grep -i "GET\|POST\|metrics"
```

### 4. Test Caddy Routing

Test if Caddy can reach Grafana:

```bash
# From inside the Caddy container
docker exec caddy_reverse_proxy wget -O- http://supabase_grafana:3000/api/health
```

### 5. Check Grafana Configuration

Verify Grafana environment variables:

```bash
docker exec supabase_grafana env | grep GF_
```

Should show:
- `GF_SERVER_ROOT_URL=https://api.altfragen.io/metrics`
- `GF_SERVER_SERVE_FROM_SUBPATH=true`

### 6. Test with curl

Test the full path through Caddy:

```bash
curl -I https://api.altfragen.io/metrics
curl -v https://api.altfragen.io/metrics/api/health
```

## Common Issues

### Issue: White page, no errors
- **Cause**: Grafana subpath configuration mismatch
- **Fix**: Ensure `GF_SERVER_ROOT_URL` ends with `/metrics` and `GF_SERVER_SERVE_FROM_SUBPATH=true`

### Issue: Dashboard not loading
- **Cause**: Dashboard JSON file missing or invalid
- **Fix**: Re-download the dashboard file using the setup script

### Issue: 404 errors
- **Cause**: Caddy not routing correctly
- **Fix**: Restart Caddy and verify the Caddyfile syntax

