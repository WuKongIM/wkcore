#!/usr/bin/env bash
set -euo pipefail

required_files=(
  "ui/index.html"
  "ui/dashboard.html"
  "ui/nodes.html"
  "ui/placeholder/groups.html"
  "ui/placeholder/network.html"
  "ui/placeholder/topology.html"
  "ui/placeholder/connections.html"
  "ui/assets/styles.css"
  "ui/assets/data.js"
  "ui/assets/app.js"
)

for file in "${required_files[@]}"; do
  [[ -f "$file" ]] || { echo "missing: $file" >&2; exit 1; }
done
