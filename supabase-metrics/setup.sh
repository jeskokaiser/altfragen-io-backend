#!/bin/bash
set -e

echo "Setting up Supabase Metrics Dashboard..."

# Check if .env exists in parent directory
if [ ! -f ../.env ]; then
    echo "⚠️  .env file not found in parent directory!"
    echo "Please create a .env file in the parent directory with the following content:"
    echo ""
    echo "SUPABASE_PROJECT_REF=your-project-ref"
    echo "SUPABASE_SERVICE_ROLE_KEY=your-service-role-key"
    echo "GRAFANA_ADMIN_USER=admin"
    echo "GRAFANA_ADMIN_PASSWORD=change-this-password"
    echo "GRAFANA_ROOT_URL=https://api.altfragen.io/metrics"
    echo ""
    exit 1
fi

# Download Supabase Grafana dashboard
echo "Downloading Supabase Grafana dashboard..."
mkdir -p grafana/dashboards
curl -s -o grafana/dashboards/supabase.json \
  https://raw.githubusercontent.com/supabase/supabase-grafana/main/dashboards/supabase.json

if [ $? -eq 0 ]; then
    echo "✅ Dashboard downloaded successfully"
else
    echo "❌ Failed to download dashboard. Please download manually:"
    echo "   curl -o grafana/dashboards/supabase.json https://raw.githubusercontent.com/supabase/supabase-grafana/main/dashboards/supabase.json"
    exit 1
fi

# Check if backend network exists
if ! docker network inspect backend > /dev/null 2>&1; then
    echo "Creating Docker network 'backend'..."
    docker network create backend
fi

echo "✅ Setup complete!"
echo ""
echo "Next steps:"
echo "1. Edit .env in the parent directory and set your SUPABASE_PROJECT_REF and SUPABASE_SERVICE_ROLE_KEY"
echo "2. From the parent directory, run: docker-compose up -d"
echo "3. Access Grafana at: https://api.altfragen.io/metrics"

