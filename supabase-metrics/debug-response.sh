#!/bin/bash
echo "=== Detailed Response Debugging ==="
echo ""

echo "1. What Grafana returns directly (full response with headers):"
echo "--------------------------------------------"
curl -v http://localhost:3000 2>&1 | head -40
echo ""

echo "2. What Grafana returns through Caddy (full response):"
echo "--------------------------------------------"
curl -v https://api.altfragen.io/metrics 2>&1 | head -50
echo ""

echo "3. Testing from inside Caddy container to Grafana:"
echo "--------------------------------------------"
docker exec caddy_reverse_proxy wget -O- --server-response http://supabase_grafana:3000 2>&1 | head -30
echo ""

echo "4. Check if it's a redirect issue:"
echo "--------------------------------------------"
curl -L -v https://api.altfragen.io/metrics 2>&1 | grep -E "(< HTTP|Location:|< )" | head -20
echo ""

echo "5. Get response with max redirects:"
echo "--------------------------------------------"
curl -L --max-redirs 5 -v https://api.altfragen.io/metrics 2>&1 | tail -50

