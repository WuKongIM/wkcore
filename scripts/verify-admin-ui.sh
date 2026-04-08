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

html_files=(
  "ui/index.html"
  "ui/dashboard.html"
  "ui/nodes.html"
  "ui/placeholder/groups.html"
  "ui/placeholder/network.html"
  "ui/placeholder/topology.html"
  "ui/placeholder/connections.html"
)

for file in "${html_files[@]}"; do
  grep -q 'tailwindcss.com' "$file" || { echo "missing tailwind cdn: $file" >&2; exit 1; }
  grep -q '\./assets/styles.css\|../assets/styles.css' "$file" || { echo "missing styles reference: $file" >&2; exit 1; }
  grep -q '\./assets/app.js\|../assets/app.js' "$file" || { echo "missing app.js reference: $file" >&2; exit 1; }
done

! rg -n "<svg" ui >/dev/null || { echo "inline svg is forbidden" >&2; exit 1; }
rg -n "unpkg.com/lucide-static@latest/icons/" ui >/dev/null || { echo "missing lucide static cdn usage" >&2; exit 1; }

for file in ui/placeholder/*.html; do
  grep -q '该页面将在下一阶段扩展' "$file" || { echo "missing placeholder copy: $file" >&2; exit 1; }
done
