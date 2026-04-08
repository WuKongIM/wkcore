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

dashboard_files=("ui/index.html" "ui/dashboard.html")
for file in "${dashboard_files[@]}"; do
  grep -q 'data-page="dashboard"' "$file" || { echo "missing dashboard page marker: $file" >&2; exit 1; }
done

grep -q 'renderDashboard' ui/assets/app.js || { echo "missing renderDashboard" >&2; exit 1; }
grep -q '在线节点数' ui/assets/data.js || { echo "missing dashboard metric copy" >&2; exit 1; }
grep -q '风险摘要' ui/assets/app.js || { echo "missing risk section" >&2; exit 1; }
grep -q '集群快照' ui/assets/app.js || { echo "missing cluster snapshot" >&2; exit 1; }

grep -q 'data-page="nodes"' ui/nodes.html || { echo "missing nodes marker" >&2; exit 1; }
grep -q '节点ID' ui/assets/app.js || { echo "missing node id header" >&2; exit 1; }
grep -q 'RPC Latency' ui/assets/app.js || { echo "missing latency header" >&2; exit 1; }
grep -q '查看详情' ui/assets/app.js || { echo "missing drawer trigger" >&2; exit 1; }
grep -q 'data-node-drawer' ui/assets/app.js || { echo "missing drawer container" >&2; exit 1; }
grep -q 'Follower replication lag > threshold' ui/assets/data.js || { echo "missing degraded hint" >&2; exit 1; }

grep -q 'pageHref' ui/assets/app.js || { echo "missing relative path helper" >&2; exit 1; }
grep -q 'dashboard.html' ui/assets/data.js || { echo "missing dashboard nav target" >&2; exit 1; }
grep -q 'nodes.html' ui/assets/data.js || { echo "missing nodes nav target" >&2; exit 1; }
grep -q 'placeholder/groups.html' ui/assets/data.js || { echo "missing groups nav target" >&2; exit 1; }
grep -q 'data-base="."' ui/index.html || { echo "index missing base marker" >&2; exit 1; }
grep -q 'data-base="."' ui/dashboard.html || { echo "dashboard missing base marker" >&2; exit 1; }
grep -q 'data-base="."' ui/nodes.html || { echo "nodes missing base marker" >&2; exit 1; }
for file in ui/placeholder/*.html; do
  grep -q 'data-base=".."' "$file" || { echo "placeholder missing base marker: $file" >&2; exit 1; }
done

grep -q '\-\-accent-primary' ui/assets/styles.css || { echo "missing accent token" >&2; exit 1; }
grep -q 'drawer' ui/assets/styles.css || { echo "missing drawer styles" >&2; exit 1; }
