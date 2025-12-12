#!/bin/sh
set -e

# Substitute environment variables in prometheus.yml using sed
# This works with any shell and doesn't require additional packages
sed -i "s|\${SUPABASE_PROJECT_REF}|${SUPABASE_PROJECT_REF}|g" /etc/prometheus/prometheus.yml
sed -i "s|\${SUPABASE_SERVICE_ROLE_KEY}|${SUPABASE_SERVICE_ROLE_KEY}|g" /etc/prometheus/prometheus.yml

# Start Prometheus with the substituted config
exec /bin/prometheus "$@"

