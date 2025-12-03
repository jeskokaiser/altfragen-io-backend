#!/bin/bash
echo "=== Testing Grafana Access ==="
echo ""

echo "1. Testing direct Grafana access (bypassing Caddy):"
docker exec supabase_grafana wget -q -O- http://localhost:3000/api/health 2>/dev/null && echo "✅ Grafana is responding" || echo "❌ Grafana not responding"

echo ""
echo "2. Testing Caddy -> Grafana routing:"
docker exec caddy_reverse_proxy wget -q -O- http://supabase_grafana:3000/api/health 2>/dev/null && echo "✅ Caddy can reach Grafana" || echo "❌ Caddy cannot reach Grafana"

echo ""
echo "3. Testing full path through Caddy (from host):"
curl -s -o /dev/null -w "HTTP Status: %{http_code}\n" https://api.altfragen.io/metrics/api/health

echo ""
echo "4. Checking Grafana environment:"
docker exec supabase_grafana env | grep "GF_SERVER"

echo ""
echo "5. Checking if dashboard file exists:"
docker exec supabase_grafana ls -lh /var/lib/grafana/dashboards/ 2>/dev/null || echo "Dashboard directory not found"

echo ""
echo "6. Testing root path:"
curl -s -I https://api.altfragen.io/metrics | head -5

