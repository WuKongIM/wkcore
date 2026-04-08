#!/usr/bin/env bash
set -euo pipefail

required_files=(
  "ui/index.html"
  "ui/dashboard.html"
  "ui/groups.html"
  "ui/network.html"
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
  "ui/groups.html"
  "ui/network.html"
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
grep -q 'groups.html' ui/assets/data.js || { echo "missing groups nav target" >&2; exit 1; }
grep -q 'network.html' ui/assets/data.js || { echo "missing network nav target" >&2; exit 1; }
grep -q 'data-base="."' ui/index.html || { echo "index missing base marker" >&2; exit 1; }
grep -q 'data-base="."' ui/dashboard.html || { echo "dashboard missing base marker" >&2; exit 1; }
grep -q 'data-base="."' ui/groups.html || { echo "groups missing base marker" >&2; exit 1; }
grep -q 'data-base="."' ui/network.html || { echo "network missing base marker" >&2; exit 1; }
grep -q 'data-base="."' ui/nodes.html || { echo "nodes missing base marker" >&2; exit 1; }
for file in ui/placeholder/*.html; do
  grep -q 'data-base=".."' "$file" || { echo "placeholder missing base marker: $file" >&2; exit 1; }
done

grep -q '\-\-accent-primary' ui/assets/styles.css || { echo "missing accent token" >&2; exit 1; }
grep -q 'drawer' ui/assets/styles.css || { echo "missing drawer styles" >&2; exit 1; }

grep -q 'data-page="groups"' ui/groups.html || { echo "missing groups page marker" >&2; exit 1; }
grep -q 'renderGroups' ui/assets/app.js || { echo "missing renderGroups" >&2; exit 1; }
grep -q '分区管理' ui/assets/app.js || { echo "missing groups heading" >&2; exit 1; }
grep -q 'Group ID' ui/assets/app.js || { echo "missing groups table header" >&2; exit 1; }
grep -q 'Leader 节点' ui/assets/app.js || { echo "missing leader column" >&2; exit 1; }
grep -q '查看 Group' ui/assets/app.js || { echo "missing group drawer trigger" >&2; exit 1; }
grep -q 'data-group-drawer' ui/assets/app.js || { echo "missing group drawer container" >&2; exit 1; }
grep -q 'group-17' ui/assets/data.js || { echo "missing groups sample data" >&2; exit 1; }

grep -q 'data-page="network"' ui/network.html || { echo "missing network page marker" >&2; exit 1; }
grep -q 'renderNetwork' ui/assets/app.js || { echo "missing renderNetwork" >&2; exit 1; }
grep -q '网络监控' ui/assets/app.js || { echo "missing network heading" >&2; exit 1; }
grep -q 'RPC 链路健康' ui/assets/app.js || { echo "missing network section" >&2; exit 1; }
grep -q '链路矩阵' ui/assets/app.js || { echo "missing link matrix section" >&2; exit 1; }
grep -q '查看链路' ui/assets/app.js || { echo "missing network drawer trigger" >&2; exit 1; }
grep -q 'data-link-drawer' ui/assets/app.js || { echo "missing network drawer container" >&2; exit 1; }
grep -q 'node1→node3' ui/assets/data.js || { echo "missing network sample data" >&2; exit 1; }
