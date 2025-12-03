#!/bin/sh
set -e

# Generate Prometheus config from template
# Replace environment variables in the template
sed "s|\${SUPABASE_PROJECT_REF}|${SUPABASE_PROJECT_REF}|g; s|\${SUPABASE_SERVICE_ROLE_KEY}|${SUPABASE_SERVICE_ROLE_KEY}|g" \
  /etc/prometheus/prometheus.yml.template > /etc/prometheus/prometheus.yml

# Start Prometheus
exec /bin/prometheus "$@"

