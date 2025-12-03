#!/bin/bash
echo "=== Debugging HTML Response ==="
echo ""

echo "1. Getting full HTML response from /metrics:"
echo "--------------------------------------------"
curl -s https://api.altfragen.io/metrics | head -50
echo ""
echo ""

echo "2. Checking what assets Grafana is trying to load:"
echo "--------------------------------------------"
curl -s https://api.altfragen.io/metrics | grep -E "(href=|src=)" | head -10
echo ""

echo "3. Testing asset path (public build files):"
echo "--------------------------------------------"
curl -I https://api.altfragen.io/metrics/public/build/app*.js 2>&1 | head -5
echo ""

echo "4. Testing login page directly:"
echo "--------------------------------------------"
curl -s https://api.altfragen.io/metrics/login | head -30
echo ""

echo "5. Checking response headers:"
echo "--------------------------------------------"
curl -I https://api.altfragen.io/metrics 2>&1 | grep -E "(Content-Type|Location|X-)"

