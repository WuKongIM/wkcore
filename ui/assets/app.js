function icon(name) {
  return `https://unpkg.com/lucide-static@latest/icons/${name}.svg`;
}

function pageHref(path) {
  const base = document.body.dataset.base || ".";
  return `${base}/${path}`;
}

function currentPageMeta() {
  const page = document.body.dataset.page;
  return window.ADMIN_UI_DATA.pages[page] || {
    title: "WuKongIM Admin",
    description: "",
  };
}

function queryStatusFilter() {
  const params = new URLSearchParams(window.location.search);
  return params.get("status");
}

function renderSidebar(currentKey) {
  const groups = window.ADMIN_UI_DATA.nav
    .map((group) => {
      const items = group.items
        .map((item) => {
          const activeClass = item.key === currentKey ? " is-active" : "";
          return `
            <a class="nav-item${activeClass}" href="${pageHref(item.path)}">
              <img class="nav-icon" src="${icon(item.icon)}" alt="" />
              <span>${item.text}</span>
            </a>
          `;
        })
        .join("");

      return `
        <section class="nav-group">
          <div class="nav-group-title">${group.label}</div>
          <div class="nav-list">${items}</div>
        </section>
      `;
    })
    .join("");

  return `
    <div class="brand">
      <div class="brand-mark">W</div>
      <div class="brand-copy">
        <h1>WuKongIM</h1>
        <p>Distributed Admin</p>
      </div>
    </div>
    <div class="nav-groups">${groups}</div>
    <div class="sidebar-footer">
      <a class="logout-button" href="#">
        <img class="nav-icon" src="${icon("log-out")}" alt="" />
        <span>退出登录</span>
      </a>
    </div>
  `;
}

function renderTopbar(meta) {
  return `
    <div class="topbar-meta">
      <div class="cluster-chip">
        <span class="chip-dot"></span>
        <div class="meta-copy">
          <strong>${meta.name} / ${meta.deployment}</strong>
          <span>${meta.window} · ${meta.refresh}</span>
        </div>
      </div>
    </div>
    <div class="topbar-actions">
      <div class="surface-pill search-pill">
        <img class="nav-icon" src="${icon("search")}" alt="" />
        <span>搜索节点 / Group / 地址</span>
      </div>
      <div class="surface-pill">
        <img class="nav-icon" src="${icon("refresh-cw")}" alt="" />
        <span>刷新</span>
      </div>
      <div class="surface-pill">
        <img class="nav-icon" src="${icon("clock-3")}" alt="" />
        <span>${meta.window}</span>
      </div>
    </div>
  `;
}

function renderPlaceholder(title, copy, tags) {
  const tagHtml = (tags || [])
    .map((tag) => `<span class="tag">${tag}</span>`)
    .join("");

  return `
    <section class="page-shell">
      <header class="page-header">
        <h1>${title}</h1>
        <p>${copy}</p>
      </header>
      <section class="panel placeholder-panel">
        <div class="placeholder-card">
          <h2>${title}</h2>
          <p>${copy}</p>
          ${tagHtml ? `<div class="placeholder-tags">${tagHtml}</div>` : ""}
        </div>
      </section>
    </section>
  `;
}

function renderStubPage(meta) {
  return `
    <section class="page-shell">
      <header class="page-header">
        <h1>${meta.title}</h1>
        <p>${meta.description}</p>
      </header>
      <section class="panel skeleton-panel">
        <div class="skeleton-title"></div>
        <div class="skeleton-line"></div>
        <div class="skeleton-line short"></div>
        <div class="skeleton-line"></div>
      </section>
    </section>
  `;
}

function renderDashboard() {
  const dashboard = window.ADMIN_UI_DATA.dashboard;
  const windowPills = dashboard.windows
    .map((label, index) => {
      const activeClass = index === 0 ? " is-active" : "";
      return `<button class="window-pill${activeClass}" type="button">${label}</button>`;
    })
    .join("");

  const metrics = dashboard.metrics
    .map((item) => {
      const toneClass = item.tone ? ` ${item.tone}` : "";
      return `
        <article class="metric-card${toneClass}">
          <span class="metric-label">${item.label}</span>
          <strong class="metric-value">${item.value}</strong>
          <span class="metric-hint">${item.hint}</span>
        </article>
      `;
    })
    .join("");

  const risks = dashboard.risks
    .map((risk) => {
      return `
        <a class="risk-item" href="${pageHref(risk.path)}">
          <span class="risk-level ${risk.level}">${risk.level}</span>
          <div class="risk-copy">
            <strong>${risk.title}</strong>
            <span>${risk.detail}</span>
          </div>
          <img class="nav-icon" src="${icon("arrow-up-right")}" alt="" />
        </a>
      `;
    })
    .join("");

  const snapshotCells = dashboard.snapshot
    .map((tone) => `<div class="snapshot-cell ${tone}"></div>`)
    .join("");

  const trendBars = dashboard.trend
    .map(
      (point) => `
        <div class="trend-bar">
          <span class="trend-fill" style="height: ${point.value}%;"></span>
          <label>${point.label}</label>
        </div>
      `,
    )
    .join("");

  const hotNodes = dashboard.hotNodes
    .map(
      (node) => `
        <div class="rank-row">
          <div>
            <strong>${node.name}</strong>
            <span>${node.meta}</span>
          </div>
          <span class="inline-badge ${node.status}">${node.status}</span>
        </div>
      `,
    )
    .join("");

  return `
    <section data-view="dashboard" class="page-shell">
      <header class="page-header">
        <div>
          <h1>Dashboard</h1>
          <p>把集群健康、异常线索和节点承载情况放在一个首屏里，先判断，再下钻。</p>
        </div>
        <div class="window-pills">${windowPills}</div>
      </header>

      <section class="metric-grid">${metrics}</section>

      <section class="dashboard-grid">
        <article class="panel content-panel">
          <div class="section-head">
            <div>
              <h2>风险摘要</h2>
              <p>按优先级列出当前最值得处理的线索</p>
            </div>
            <a class="section-link" href="${pageHref("nodes.html")}">进入节点管理</a>
          </div>
          <div class="risk-list">${risks}</div>
        </article>

        <article class="panel content-panel">
          <div class="section-head">
            <div>
              <h2>集群快照</h2>
              <p>用轻量热度矩阵观察 Group 分布与健康差异</p>
            </div>
          </div>
          <div class="snapshot-grid">${snapshotCells}</div>
          <div class="snapshot-legend">
            <span><i class="legend-dot accent"></i>高负载</span>
            <span><i class="legend-dot warning"></i>风险</span>
            <span><i class="legend-dot healthy"></i>健康</span>
          </div>
        </article>
      </section>

      <section class="dashboard-grid lower-grid">
        <article class="panel content-panel">
          <div class="section-head">
            <div>
              <h2>连接趋势</h2>
              <p>最近 30 分钟内的连接波动</p>
            </div>
          </div>
          <div class="trend-chart">${trendBars}</div>
        </article>

        <article class="panel content-panel">
          <div class="section-head">
            <div>
              <h2>异常节点排行</h2>
              <p>优先查看需要下钻的节点</p>
            </div>
          </div>
          <div class="rank-list">${hotNodes}</div>
        </article>
      </section>
    </section>
  `;
}

function roleBadge(role) {
  return role === "Leader"
    ? `<span class="role-badge leader">${role}</span>`
    : `<span class="role-badge follower">${role}</span>`;
}

function statusBadge(status) {
  return `<span class="inline-badge ${status}">${status}</span>`;
}

function renderGroupDrawer(group) {
  if (!group) {
    return `
      <div class="drawer-head">
        <div>
          <h2>Group 详情</h2>
          <p>选择 Group 后查看详情</p>
        </div>
      </div>
    `;
  }

  const members = group.detail.members.map((member) => `<span class="mini-tag">${member}</span>`).join("");
  const indicators = group.detail.indicators
    .map(
      (item) => `
        <div class="drawer-metric">
          <span>${item.label}</span>
          <strong>${item.value}</strong>
        </div>
      `,
    )
    .join("");
  const events = group.detail.events.map((event) => `<li>${event}</li>`).join("");

  return `
    <div class="drawer-head">
      <div>
        <h2>${group.id}</h2>
        <p>${group.leader} · ${group.status} · ${group.term}</p>
      </div>
      <button class="icon-button" type="button" data-close-group-drawer>关闭</button>
    </div>
    <div class="drawer-body">
      <section class="drawer-section">
        <h3>Group 概览</h3>
        <p>${group.detail.summary}</p>
        <p>Leader 节点 ${group.leader} · Epoch ${group.detail.epoch} · Replica ${group.replicas}</p>
      </section>
      <section class="drawer-section">
        <h3>成员状态</h3>
        <div class="mini-tags">${members}</div>
      </section>
      <section class="drawer-section">
        <h3>关键指标</h3>
        <div class="drawer-metrics">${indicators}</div>
      </section>
      <section class="drawer-section">
        <h3>热点 Channel</h3>
        <p>${group.channels}</p>
      </section>
      <section class="drawer-section">
        <h3>最近事件</h3>
        <ul class="drawer-list">${events}</ul>
      </section>
    </div>
  `;
}

function renderLinkDrawer(link) {
  if (!link) {
    return `
      <div class="drawer-head">
        <div>
          <h2>链路详情</h2>
          <p>选择链路后查看详情</p>
        </div>
      </div>
    `;
  }

  const metrics = link.detail.metrics
    .map(
      (item) => `
        <div class="drawer-metric">
          <span>${item.label}</span>
          <strong>${item.value}</strong>
        </div>
      `,
    )
    .join("");
  const events = link.detail.events.map((event) => `<li>${event}</li>`).join("");

  return `
    <div class="drawer-head">
      <div>
        <h2>${link.id}</h2>
        <p>${link.source} → ${link.target} · ${link.status}</p>
      </div>
      <button class="icon-button" type="button" data-close-link-drawer>关闭</button>
    </div>
    <div class="drawer-body">
      <section class="drawer-section">
        <h3>链路摘要</h3>
        <p>${link.detail.summary}</p>
        <p>RTT ${link.rtt} · Retry ${link.retry} · Packet ${link.packet}</p>
      </section>
      <section class="drawer-section">
        <h3>关键指标</h3>
        <div class="drawer-metrics">${metrics}</div>
      </section>
      <section class="drawer-section">
        <h3>最近事件</h3>
        <ul class="drawer-list">${events}</ul>
      </section>
    </div>
  `;
}

function renderConnectionDrawer(connection) {
  if (!connection) {
    return `
      <div class="drawer-head">
        <div>
          <h2>连接详情</h2>
          <p>选择连接后查看详情</p>
        </div>
      </div>
    `;
  }

  const metrics = connection.detail.metrics
    .map(
      (item) => `
        <div class="drawer-metric">
          <span>${item.label}</span>
          <strong>${item.value}</strong>
        </div>
      `,
    )
    .join("");
  const tags = connection.detail.tags.map((tag) => `<span class="mini-tag">${tag}</span>`).join("");
  const events = connection.detail.events.map((event) => `<li>${event}</li>`).join("");

  return `
    <div class="drawer-head">
      <div>
        <h2>${connection.id}</h2>
        <p>${connection.uid} · ${connection.device} · ${connection.status}</p>
      </div>
      <button class="icon-button" type="button" data-close-connection-drawer>关闭</button>
    </div>
    <div class="drawer-body">
      <section class="drawer-section">
        <h3>连接摘要</h3>
        <p>${connection.detail.summary}</p>
        <p>Node ${connection.node} · Client ${connection.client} · Connected ${connection.since}</p>
      </section>
      <section class="drawer-section">
        <h3>关键指标</h3>
        <div class="drawer-metrics">${metrics}</div>
      </section>
      <section class="drawer-section">
        <h3>会话标签</h3>
        <div class="mini-tags">${tags}</div>
      </section>
      <section class="drawer-section">
        <h3>最近事件</h3>
        <ul class="drawer-list">${events}</ul>
      </section>
    </div>
  `;
}

function renderChannelDrawer(channel) {
  if (!channel) {
    return `
      <div class="drawer-head">
        <div>
          <h2>频道详情</h2>
          <p>选择频道后查看详情</p>
        </div>
      </div>
    `;
  }

  const metrics = channel.detail.metrics
    .map(
      (item) => `
        <div class="drawer-metric">
          <span>${item.label}</span>
          <strong>${item.value}</strong>
        </div>
      `,
    )
    .join("");
  const tags = channel.detail.tags.map((tag) => `<span class="mini-tag">${tag}</span>`).join("");
  const events = channel.detail.events.map((event) => `<li>${event}</li>`).join("");

  return `
    <div class="drawer-head">
      <div>
        <h2>${channel.id}</h2>
        <p>${channel.type} · ${channel.status} · ${channel.group}</p>
      </div>
      <button class="icon-button" type="button" data-close-channel-drawer>关闭</button>
    </div>
    <div class="drawer-body">
      <section class="drawer-section">
        <h3>频道摘要</h3>
        <p>${channel.detail.summary}</p>
        <p>Leader ${channel.leader} · Group ${channel.group} · Subscriber ${channel.subscribers}</p>
      </section>
      <section class="drawer-section">
        <h3>关键指标</h3>
        <div class="drawer-metrics">${metrics}</div>
      </section>
      <section class="drawer-section">
        <h3>频道标签</h3>
        <div class="mini-tags">${tags}</div>
      </section>
      <section class="drawer-section">
        <h3>运行摘要</h3>
        <p>Ingress ${channel.ingress} · Backlog ${channel.backlog} · 最近活跃 ${channel.activeAt}</p>
      </section>
      <section class="drawer-section">
        <h3>最近事件</h3>
        <ul class="drawer-list">${events}</ul>
      </section>
    </div>
  `;
}

function renderTopologyDrawer(node) {
  if (!node) {
    return `
      <div class="drawer-head">
        <div>
          <h2>拓扑节点详情</h2>
          <p>选择节点后查看详情</p>
        </div>
      </div>
    `;
  }

  const metrics = node.detail.metrics
    .map(
      (item) => `
        <div class="drawer-metric">
          <span>${item.label}</span>
          <strong>${item.value}</strong>
        </div>
      `,
    )
    .join("");
  const links = node.detail.links.map((item) => `<span class="mini-tag">${item}</span>`).join("");
  const events = node.detail.events.map((event) => `<li>${event}</li>`).join("");

  return `
    <div class="drawer-head">
      <div>
        <h2>${node.name}</h2>
        <p>${node.role} · ${node.status} · ${node.groups} groups</p>
      </div>
      <button class="icon-button" type="button" data-close-topology-drawer>关闭</button>
    </div>
    <div class="drawer-body">
      <section class="drawer-section">
        <h3>节点概览</h3>
        <p>${node.detail.summary}</p>
        <p>Outbound ${node.outbound} · Groups ${node.groups}</p>
      </section>
      <section class="drawer-section">
        <h3>关键指标</h3>
        <div class="drawer-metrics">${metrics}</div>
      </section>
      <section class="drawer-section">
        <h3>关键链路</h3>
        <div class="mini-tags">${links}</div>
      </section>
      <section class="drawer-section">
        <h3>最近事件</h3>
        <ul class="drawer-list">${events}</ul>
      </section>
    </div>
  `;
}

function renderChannels() {
  const channels = window.ADMIN_UI_DATA.channels;
  const metrics = channels.metrics
    .map(
      (item) => `
        <article class="metric-card">
          <span class="metric-label">${item.label}</span>
          <strong class="metric-value">${item.value}</strong>
          <span class="metric-hint">${item.hint}</span>
        </article>
      `,
    )
    .join("");

  const typeMix = channels.typeMix
    .map(
      (item) => `
        <div class="drawer-metric">
          <span>${item.label}</span>
          <strong>${item.value}</strong>
        </div>
      `,
    )
    .join("");

  const watchlist = channels.watchlist
    .map(
      (item) => `
        <div class="rank-row">
          <div>
            <strong>${item.id}</strong>
            <span>${item.summary}</span>
          </div>
          <span class="inline-badge ${item.status}">${item.status}</span>
        </div>
      `,
    )
    .join("");

  const rows = channels.rows
    .map(
      (channel) => `
        <tr>
          <td>
            <strong>${channel.id}</strong>
            <div class="cell-subtle">${channel.note}</div>
          </td>
          <td>${channel.type}</td>
          <td>${channel.leader}</td>
          <td>${channel.group}</td>
          <td>${channel.subscribers}</td>
          <td>${channel.ingress}</td>
          <td>${channel.backlog}</td>
          <td>${statusBadge(channel.status)}</td>
          <td>${channel.activeAt}</td>
          <td><button class="table-link" type="button" data-open-channel="${channel.id}">查看频道</button></td>
        </tr>
      `,
    )
    .join("");

  return `
    <section data-view="channels" class="page-shell">
      <header class="page-header">
        <div>
          <h1>频道管理</h1>
          <p>从 Channel 维度查看 Leader、Group、订阅规模和消息积压，优先定位热点与异常频道。</p>
        </div>
      </header>

      <section class="panel content-panel">
        <div class="section-head">
          <div>
            <h2>频道运行概况</h2>
            <p>先看活跃规模、热点数量和当前积压</p>
          </div>
        </div>
        <div class="metric-grid section-metrics">${metrics}</div>
      </section>

      <section class="dashboard-grid lower-grid">
        <article class="panel content-panel">
          <div class="section-head">
            <div>
              <h2>频道类型分布</h2>
              <p>区分 room / dm / stream / system 的当前规模</p>
            </div>
          </div>
          <div class="drawer-metrics section-metrics">${typeMix}</div>
        </article>

        <article class="panel content-panel">
          <div class="section-head">
            <div>
              <h2>重点频道</h2>
              <p>优先跟进 backlog 增长或持续高写入的频道</p>
            </div>
          </div>
          <div class="rank-list">${watchlist}</div>
        </article>
      </section>

      <section class="panel toolbar-panel">
        <div class="toolbar-row">
          <div class="surface-pill search-pill">
            <img class="nav-icon" src="${icon("search")}" alt="" />
            <span>搜索频道ID / 类型 / Group / Leader</span>
          </div>
          <div class="toolbar-actions">
            <button class="filter-pill" type="button">类型：全部</button>
            <button class="filter-pill" type="button">状态：全部</button>
            <button class="filter-pill" type="button">排序：Backlog</button>
          </div>
        </div>
      </section>

      <section class="panel table-panel">
        <div class="section-head table-head">
          <div>
            <h2>频道列表</h2>
            <p>按 Leader、Group、订阅量和消息速率快速筛查热点频道</p>
          </div>
        </div>
        <table class="node-table">
          <thead>
            <tr>
              <th>频道ID</th>
              <th>类型</th>
              <th>Leader 节点</th>
              <th>所属 Group</th>
              <th>订阅数</th>
              <th>消息速率</th>
              <th>Backlog</th>
              <th>状态</th>
              <th>最近活跃</th>
              <th>操作</th>
            </tr>
          </thead>
          <tbody>${rows}</tbody>
        </table>
      </section>

      <aside class="drawer" data-channel-drawer hidden></aside>
    </section>
  `;
}

function renderTopology() {
  const topology = window.ADMIN_UI_DATA.topology;
  const metrics = topology.metrics
    .map(
      (item) => `
        <article class="metric-card">
          <span class="metric-label">${item.label}</span>
          <strong class="metric-value">${item.value}</strong>
          <span class="metric-hint">${item.hint}</span>
        </article>
      `,
    )
    .join("");

  const nodes = topology.nodes
    .map(
      (node) => `
        <button
          class="topology-node ${node.status}"
          type="button"
          data-open-topology-node="${node.id}"
          style="left:${node.x};top:${node.y};"
        >
          <strong>${node.name}</strong>
          <span>${node.role}</span>
        </button>
      `,
    )
    .join("");

  const flows = topology.flows
    .map(
      (flow) => `
        <div class="flow-row">
          <div class="flow-copy">
            <strong>${flow.label}</strong>
          </div>
          <div class="flow-bar">
            <span class="flow-fill ${flow.tone}" style="width:${flow.width};"></span>
          </div>
        </div>
      `,
    )
    .join("");

  const incidents = topology.incidents
    .map(
      (item) => `
        <div class="rank-row">
          <div>
            <strong>${item.name}</strong>
            <span>${item.summary}</span>
          </div>
          <span class="inline-badge ${item.status}">${item.status}</span>
        </div>
      `,
    )
    .join("");

  return `
    <section data-view="topology" class="page-shell">
      <header class="page-header">
        <div>
          <h1>拓扑视图</h1>
          <p>把节点、Leader 分布和 Group 流向放在同一个视图里，帮助定位集中热点和链路风险。</p>
        </div>
      </header>

      <section class="metric-grid">${metrics}</section>

      <section class="dashboard-grid">
        <article class="panel content-panel">
          <div class="section-head">
            <div>
              <h2>节点拓扑总览</h2>
              <p>点击节点查看角色、负载和关键链路上下文</p>
            </div>
          </div>
          <div class="topology-canvas">
            <div class="topology-link link-a"></div>
            <div class="topology-link link-b"></div>
            <div class="topology-link link-c"></div>
            ${nodes}
          </div>
        </article>

        <article class="panel content-panel">
          <div class="section-head">
            <div>
              <h2>热点清单</h2>
              <p>优先处理拓扑中的风险点和集中热点</p>
            </div>
          </div>
          <div class="rank-list">${incidents}</div>
        </article>
      </section>

      <section class="dashboard-grid lower-grid">
        <article class="panel content-panel">
          <div class="section-head">
            <div>
              <h2>Group 流向</h2>
              <p>观察热点 Group 在节点间的主要方向</p>
            </div>
          </div>
          <div class="flow-list">${flows}</div>
        </article>

        <article class="panel content-panel">
          <div class="section-head">
            <div>
              <h2>角色分布</h2>
              <p>当前 Leader 与 Follower 在节点间的分布情况</p>
            </div>
          </div>
          <div class="drawer-metrics section-metrics">
            ${topology.nodes
              .map(
                (node) => `
                  <div class="drawer-metric">
                    <span>${node.name}</span>
                    <strong>${node.groups}</strong>
                    <span>${node.role}</span>
                  </div>
                `,
              )
              .join("")}
          </div>
        </article>
      </section>

      <aside class="drawer" data-topology-drawer hidden></aside>
      <div class="topology-actions">
        <button class="table-link" type="button" data-open-topology-node="topo-node-3">查看拓扑节点</button>
      </div>
    </section>
  `;
}

function renderConnections() {
  const connections = window.ADMIN_UI_DATA.connections;
  const metrics = connections.metrics
    .map(
      (item) => `
        <article class="metric-card">
          <span class="metric-label">${item.label}</span>
          <strong class="metric-value">${item.value}</strong>
          <span class="metric-hint">${item.hint}</span>
        </article>
      `,
    )
    .join("");

  const deviceMix = connections.deviceMix
    .map(
      (item) => `
        <div class="drawer-metric">
          <span>${item.label}</span>
          <strong>${item.value}</strong>
        </div>
      `,
    )
    .join("");

  const nodeLoad = connections.nodeLoad
    .map(
      (row) => `
        <div class="rank-row">
          <div>
            <strong>${row.node}</strong>
            <span>${row.connections} 连接</span>
          </div>
          <span class="inline-badge ${row.status}">${row.status}</span>
        </div>
      `,
    )
    .join("");

  const trendBars = connections.trend
    .map(
      (point) => `
        <div class="trend-bar">
          <span class="trend-fill" style="height: ${point.value}%;"></span>
          <label>${point.label}</label>
        </div>
      `,
    )
    .join("");

  const rows = connections.rows
    .map(
      (row) => `
        <tr>
          <td><strong>${row.id}</strong></td>
          <td>${row.uid}</td>
          <td>${row.device}</td>
          <td>${row.node}</td>
          <td>${statusBadge(row.status)}</td>
          <td>${row.latency}</td>
          <td>${row.client}</td>
          <td>${row.since}</td>
          <td><button class="table-link" type="button" data-open-connection="${row.id}">查看连接</button></td>
        </tr>
      `,
    )
    .join("");

  return `
    <section data-view="connections" class="page-shell">
      <header class="page-header">
        <div>
          <h1>在线连接</h1>
          <p>从连接、设备和节点三个维度查看当前在线会话，优先定位高风险连接。</p>
        </div>
      </header>

      <section class="panel content-panel">
        <div class="section-head">
          <div>
            <h2>活跃连接概况</h2>
            <p>先看总体规模、异常连接数量和多端在线占比</p>
          </div>
        </div>
        <div class="metric-grid section-metrics">${metrics}</div>
      </section>

      <section class="dashboard-grid lower-grid">
        <article class="panel content-panel">
          <div class="section-head">
            <div>
              <h2>设备分布</h2>
              <p>按设备类型看当前连接结构</p>
            </div>
          </div>
          <div class="drawer-metrics section-metrics">${deviceMix}</div>
        </article>
        <article class="panel content-panel">
          <div class="section-head">
            <div>
              <h2>节点承载</h2>
              <p>结合节点状态看连接负载是否偏斜</p>
            </div>
          </div>
          <div class="rank-list">${nodeLoad}</div>
        </article>
      </section>

      <section class="dashboard-grid">
        <article class="panel content-panel">
          <div class="section-head">
            <div>
              <h2>活跃会话趋势</h2>
              <p>最近 30 分钟在线连接数变化</p>
            </div>
          </div>
          <div class="trend-chart">${trendBars}</div>
        </article>

        <article class="panel content-panel">
          <div class="section-head">
            <div>
              <h2>高风险连接</h2>
              <p>优先查看 warning / degraded 会话</p>
            </div>
          </div>
          <div class="risk-list">
            ${connections.rows
              .filter((row) => row.status !== "online")
              .map(
                (row) => `
                  <div class="risk-item compact">
                    <span class="risk-level ${row.status === "degraded" ? "critical" : "warning"}">${row.status}</span>
                    <div class="risk-copy">
                      <strong>${row.id}</strong>
                      <span>${row.note} · ${row.device} · ${row.node}</span>
                    </div>
                    <button class="table-link" type="button" data-open-connection="${row.id}">查看连接</button>
                  </div>
                `,
              )
              .join("")}
          </div>
        </article>
      </section>

      <section class="panel toolbar-panel">
        <div class="toolbar-row">
          <div class="surface-pill search-pill">
            <img class="nav-icon" src="${icon("search")}" alt="" />
            <span>搜索连接ID / UID / 设备 / 节点</span>
          </div>
          <div class="toolbar-actions">
            <button class="filter-pill" type="button">状态：全部</button>
            <button class="filter-pill" type="button">设备：全部</button>
            <button class="filter-pill" type="button">排序：Latency</button>
          </div>
        </div>
      </section>

      <section class="panel table-panel">
        <div class="section-head table-head">
          <div>
            <h2>连接会话列表</h2>
            <p>按连接状态、设备和节点快速筛查在线会话</p>
          </div>
        </div>
        <table class="node-table">
          <thead>
            <tr>
              <th>连接ID</th>
              <th>UID</th>
              <th>设备</th>
              <th>节点</th>
              <th>状态</th>
              <th>RTT</th>
              <th>Client</th>
              <th>在线时长</th>
              <th>操作</th>
            </tr>
          </thead>
          <tbody>${rows}</tbody>
        </table>
      </section>

      <aside class="drawer" data-connection-drawer hidden></aside>
    </section>
  `;
}

function renderNetwork() {
  const network = window.ADMIN_UI_DATA.network;
  const metrics = network.metrics
    .map(
      (item) => `
        <article class="metric-card">
          <span class="metric-label">${item.label}</span>
          <strong class="metric-value">${item.value}</strong>
          <span class="metric-hint">${item.hint}</span>
        </article>
      `,
    )
    .join("");

  const linkRows = network.links
    .map(
      (link) => `
        <div class="risk-item compact">
          <span class="risk-level ${link.status === "degraded" ? "critical" : link.status === "warning" ? "warning" : "info"}">${link.status}</span>
          <div class="risk-copy">
            <strong>${link.id}</strong>
            <span>${link.note} · RTT ${link.rtt} · Retry ${link.retry}</span>
          </div>
          <button class="table-link" type="button" data-open-link="${link.id}">查看链路</button>
        </div>
      `,
    )
    .join("");

  const matrixCells = network.matrix
    .map((tone) => `<div class="matrix-cell ${tone || "empty"}"></div>`)
    .join("");

  const trendBars = network.trend
    .map(
      (point) => `
        <div class="trend-bar">
          <span class="trend-fill" style="height: ${point.value}%;"></span>
          <label>${point.label}</label>
        </div>
      `,
    )
    .join("");

  const tableRows = network.links
    .map(
      (link) => `
        <tr>
          <td><strong>${link.id}</strong></td>
          <td>${statusBadge(link.status)}</td>
          <td>${link.rtt}</td>
          <td>${link.retry}</td>
          <td>${link.packet}</td>
          <td>${link.note}</td>
          <td><button class="table-link" type="button" data-open-link="${link.id}">查看链路</button></td>
        </tr>
      `,
    )
    .join("");

  return `
    <section data-view="network" class="page-shell">
      <header class="page-header">
        <div>
          <h1>网络监控</h1>
          <p>从节点到节点观察 RPC 链路健康、延迟抖动和重试热点，优先定位慢链路。</p>
        </div>
      </header>

      <section class="metric-grid">${metrics}</section>

      <section class="dashboard-grid">
        <article class="panel content-panel">
          <div class="section-head">
            <div>
              <h2>RPC 链路健康</h2>
              <p>先看最值得下钻的慢链路和抖动来源</p>
            </div>
          </div>
          <div class="risk-list">${linkRows}</div>
        </article>

        <article class="panel content-panel">
          <div class="section-head">
            <div>
              <h2>链路矩阵</h2>
              <p>横向对比 node 间的链路状态</p>
            </div>
          </div>
          <div class="network-matrix-head">
            <span></span><span>node1</span><span>node2</span><span>node3</span>
          </div>
          <div class="network-matrix-body">
            <span>node1</span><span>node2</span><span>node3</span>
            ${matrixCells}
          </div>
        </article>
      </section>

      <section class="dashboard-grid lower-grid">
        <article class="panel content-panel">
          <div class="section-head">
            <div>
              <h2>最近抖动趋势</h2>
              <p>链路 jitter 峰值在 10:15 前后出现明显抬升</p>
            </div>
          </div>
          <div class="trend-chart">${trendBars}</div>
        </article>

        <article class="panel content-panel">
          <div class="section-head">
            <div>
              <h2>链路清单</h2>
              <p>以 RTT、Retry 和 packet 行为快速排序</p>
            </div>
          </div>
          <section class="panel table-panel inset-table">
            <table class="node-table">
              <thead>
                <tr>
                  <th>链路</th>
                  <th>状态</th>
                  <th>RTT</th>
                  <th>Retry</th>
                  <th>Packet</th>
                  <th>备注</th>
                  <th>操作</th>
                </tr>
              </thead>
              <tbody>${tableRows}</tbody>
            </table>
          </section>
        </article>
      </section>

      <aside class="drawer" data-link-drawer hidden></aside>
    </section>
  `;
}

function renderGroups() {
  const groups = window.ADMIN_UI_DATA.groups;
  const metrics = groups.metrics
    .map(
      (item) => `
        <article class="metric-card">
          <span class="metric-label">${item.label}</span>
          <strong class="metric-value">${item.value}</strong>
          <span class="metric-hint">${item.hint}</span>
        </article>
      `,
    )
    .join("");

  const heatmap = groups.heatmap
    .map((tone) => `<div class="snapshot-cell ${tone}"></div>`)
    .join("");

  const rows = groups.rows
    .map(
      (group) => `
        <tr>
          <td><strong>${group.id}</strong></td>
          <td>${group.leader}</td>
          <td>${group.replicas}</td>
          <td>${statusBadge(group.status)}</td>
          <td>${group.lag}</td>
          <td>${group.throughput}</td>
          <td>${group.term}</td>
          <td><button class="table-link" type="button" data-open-group="${group.id}">查看 Group</button></td>
        </tr>
      `,
    )
    .join("");

  const incidents = groups.rows
    .filter((group) => group.status !== "online")
    .map(
      (group) => `
        <div class="rank-row">
          <div>
            <strong>${group.id}</strong>
            <span>Leader 节点 ${group.leader} · lag ${group.lag} · ${group.channels}</span>
          </div>
          <span class="inline-badge ${group.status}">${group.status}</span>
        </div>
      `,
    )
    .join("");

  return `
    <section data-view="groups" class="page-shell">
      <header class="page-header">
        <div>
          <h1>分区管理</h1>
          <p>查看 Group 的 Leader 分布、复制状态与当前热度，作为集群调度和排障入口。</p>
        </div>
      </header>

      <section class="metric-grid">${metrics}</section>

      <section class="dashboard-grid lower-grid">
        <article class="panel content-panel">
          <div class="section-head">
            <div>
              <h2>Leader 热度矩阵</h2>
              <p>快速观察 Group 分布、风险点和热点集中位置</p>
            </div>
          </div>
          <div class="snapshot-grid">${heatmap}</div>
          <div class="snapshot-legend">
            <span><i class="legend-dot accent"></i>热点</span>
            <span><i class="legend-dot warning"></i>异常</span>
            <span><i class="legend-dot healthy"></i>健康</span>
          </div>
        </article>
        <article class="panel content-panel">
          <div class="section-head">
            <div>
              <h2>风险 Group</h2>
              <p>优先看状态不是 online 的分区</p>
            </div>
          </div>
          <div class="rank-list">${incidents}</div>
        </article>
      </section>

      <section class="panel toolbar-panel">
        <div class="toolbar-row">
          <div class="surface-pill search-pill">
            <img class="nav-icon" src="${icon("search")}" alt="" />
            <span>搜索 Group ID / Leader / Replica</span>
          </div>
          <div class="toolbar-actions">
            <button class="filter-pill" type="button">状态：全部</button>
            <button class="filter-pill" type="button">Leader：全部</button>
            <button class="filter-pill" type="button">排序：Lag</button>
          </div>
        </div>
      </section>

      <section class="panel table-panel">
        <table class="node-table">
          <thead>
            <tr>
              <th>Group ID</th>
              <th>Leader 节点</th>
              <th>Replica</th>
              <th>状态</th>
              <th>Commit Lag</th>
              <th>Write Throughput</th>
              <th>当前 Term</th>
              <th>操作</th>
            </tr>
          </thead>
          <tbody>${rows}</tbody>
        </table>
      </section>

      <aside class="drawer" data-group-drawer hidden></aside>
    </section>
  `;
}

function renderNodeDrawer(node) {
  if (!node) {
    return `
      <div class="drawer-head">
        <div>
          <h2>节点详情</h2>
          <p>选择节点后查看详情</p>
        </div>
      </div>
    `;
  }

  const metrics = node.detail.metrics
    .map(
      (metric) => `
        <div class="drawer-metric">
          <span>${metric.label}</span>
          <strong>${metric.value}</strong>
        </div>
      `,
    )
    .join("");
  const groups = node.detail.groups.map((group) => `<span class="mini-tag">${group}</span>`).join("");
  const events = node.detail.events.map((event) => `<li>${event}</li>`).join("");

  return `
    <div class="drawer-head">
      <div>
        <h2>${node.name}</h2>
        <p>${node.address} · ${node.role} · ${node.status}</p>
      </div>
      <button class="icon-button" type="button" data-close-node-drawer>关闭</button>
    </div>
    <div class="drawer-body">
      <section class="drawer-section">
        <h3>基础信息</h3>
        <p>${node.detail.summary}</p>
      </section>
      <section class="drawer-section">
        <h3>近 15 分钟负载</h3>
        <div class="drawer-metrics">${metrics}</div>
      </section>
      <section class="drawer-section">
        <h3>重点 Group</h3>
        <div class="mini-tags">${groups}</div>
      </section>
      <section class="drawer-section">
        <h3>最近异常事件</h3>
        <ul class="drawer-list">${events}</ul>
      </section>
      <section class="drawer-section">
        <h3>网络指标摘要</h3>
        <p>RPC Latency ${node.latency} · 连接数 ${node.connections} · Group 数 ${node.groups}</p>
      </section>
    </div>
  `;
}

function renderNodes() {
  const status = queryStatusFilter();
  const nodes = status
    ? window.ADMIN_UI_DATA.nodes.filter((node) => node.status === status)
    : window.ADMIN_UI_DATA.nodes;

  const activeFilter = status
    ? `<span class="active-filter">当前筛选：status=${status}</span>`
    : `<span class="active-filter subtle">默认显示全部节点</span>`;

  const rows = nodes
    .map((node) => {
      return `
        <tr>
          <td>${node.id}</td>
          <td>
            <strong>${node.name}</strong>
            <div class="cell-subtle">${node.address}</div>
            ${node.note ? `<div class="cell-alert">${node.note}</div>` : ""}
          </td>
          <td>${roleBadge(node.role)}</td>
          <td>${statusBadge(node.status)}</td>
          <td>${node.groups}</td>
          <td>${node.connections}</td>
          <td>${node.latency}</td>
          <td><button class="table-link" type="button" data-open-node="${node.id}">查看详情</button></td>
        </tr>
      `;
    })
    .join("");

  return `
    <section data-view="nodes" class="page-shell">
      <header class="page-header">
        <div>
          <h1>节点管理</h1>
          <p>主表就是入口，先看状态，再看承载，再进入节点详情抽屉。</p>
        </div>
        ${activeFilter}
      </header>

      <section class="panel toolbar-panel">
        <div class="toolbar-row">
          <div class="surface-pill search-pill">
            <img class="nav-icon" src="${icon("search")}" alt="" />
            <span>搜索节点ID / 地址 / 角色</span>
          </div>
          <div class="toolbar-actions">
            <button class="filter-pill" type="button">角色：全部</button>
            <button class="filter-pill" type="button">状态：全部</button>
            <button class="filter-pill" type="button">排序：Latency</button>
          </div>
        </div>
      </section>

      <section class="panel table-panel">
        <table class="node-table">
          <thead>
            <tr>
              <th>节点ID</th>
              <th>地址</th>
              <th>角色</th>
              <th>状态</th>
              <th>Group 数</th>
              <th>连接数</th>
              <th>RPC Latency</th>
              <th>操作</th>
            </tr>
          </thead>
          <tbody>${rows}</tbody>
        </table>
      </section>

      <aside class="drawer" data-node-drawer hidden></aside>
    </section>
  `;
}

function bindNodeDrawer() {
  const drawer = document.querySelector("[data-node-drawer]");
  if (!drawer) return;

  const open = (id) => {
    const node = window.ADMIN_UI_DATA.nodes.find((item) => String(item.id) === String(id));
    drawer.innerHTML = renderNodeDrawer(node);
    drawer.hidden = false;
    document.body.classList.add("drawer-open");
    const closeButton = drawer.querySelector("[data-close-node-drawer]");
    if (closeButton) {
      closeButton.addEventListener("click", close);
    }
  };

  const close = () => {
    drawer.hidden = true;
    drawer.innerHTML = "";
    document.body.classList.remove("drawer-open");
  };

  document.querySelectorAll("[data-open-node]").forEach((button) => {
    button.addEventListener("click", () => open(button.dataset.openNode));
  });
}

function bindGroupDrawer() {
  const drawer = document.querySelector("[data-group-drawer]");
  if (!drawer) return;

  const open = (id) => {
    const group = window.ADMIN_UI_DATA.groups.rows.find((item) => item.id === id);
    drawer.innerHTML = renderGroupDrawer(group);
    drawer.hidden = false;
    document.body.classList.add("drawer-open");
    const closeButton = drawer.querySelector("[data-close-group-drawer]");
    if (closeButton) {
      closeButton.addEventListener("click", close);
    }
  };

  const close = () => {
    drawer.hidden = true;
    drawer.innerHTML = "";
    document.body.classList.remove("drawer-open");
  };

  document.querySelectorAll("[data-open-group]").forEach((button) => {
    button.addEventListener("click", () => open(button.dataset.openGroup));
  });
}

function bindLinkDrawer() {
  const drawer = document.querySelector("[data-link-drawer]");
  if (!drawer) return;

  const open = (id) => {
    const link = window.ADMIN_UI_DATA.network.links.find((item) => item.id === id);
    drawer.innerHTML = renderLinkDrawer(link);
    drawer.hidden = false;
    document.body.classList.add("drawer-open");
    const closeButton = drawer.querySelector("[data-close-link-drawer]");
    if (closeButton) {
      closeButton.addEventListener("click", close);
    }
  };

  const close = () => {
    drawer.hidden = true;
    drawer.innerHTML = "";
    document.body.classList.remove("drawer-open");
  };

  document.querySelectorAll("[data-open-link]").forEach((button) => {
    button.addEventListener("click", () => open(button.dataset.openLink));
  });
}

function bindConnectionDrawer() {
  const drawer = document.querySelector("[data-connection-drawer]");
  if (!drawer) return;

  const open = (id) => {
    const connection = window.ADMIN_UI_DATA.connections.rows.find((item) => item.id === id);
    drawer.innerHTML = renderConnectionDrawer(connection);
    drawer.hidden = false;
    document.body.classList.add("drawer-open");
    const closeButton = drawer.querySelector("[data-close-connection-drawer]");
    if (closeButton) {
      closeButton.addEventListener("click", close);
    }
  };

  const close = () => {
    drawer.hidden = true;
    drawer.innerHTML = "";
    document.body.classList.remove("drawer-open");
  };

  document.querySelectorAll("[data-open-connection]").forEach((button) => {
    button.addEventListener("click", () => open(button.dataset.openConnection));
  });
}

function bindChannelDrawer() {
  const drawer = document.querySelector("[data-channel-drawer]");
  if (!drawer) return;

  const open = (id) => {
    const channel = window.ADMIN_UI_DATA.channels.rows.find((item) => item.id === id);
    drawer.innerHTML = renderChannelDrawer(channel);
    drawer.hidden = false;
    document.body.classList.add("drawer-open");
    const closeButton = drawer.querySelector("[data-close-channel-drawer]");
    if (closeButton) {
      closeButton.addEventListener("click", close);
    }
  };

  const close = () => {
    drawer.hidden = true;
    drawer.innerHTML = "";
    document.body.classList.remove("drawer-open");
  };

  document.querySelectorAll("[data-open-channel]").forEach((button) => {
    button.addEventListener("click", () => open(button.dataset.openChannel));
  });
}

function bindTopologyDrawer() {
  const drawer = document.querySelector("[data-topology-drawer]");
  if (!drawer) return;

  const open = (id) => {
    const node = window.ADMIN_UI_DATA.topology.nodes.find((item) => item.id === id);
    drawer.innerHTML = renderTopologyDrawer(node);
    drawer.hidden = false;
    document.body.classList.add("drawer-open");
    const closeButton = drawer.querySelector("[data-close-topology-drawer]");
    if (closeButton) {
      closeButton.addEventListener("click", close);
    }
  };

  const close = () => {
    drawer.hidden = true;
    drawer.innerHTML = "";
    document.body.classList.remove("drawer-open");
  };

  document.querySelectorAll("[data-open-topology-node]").forEach((button) => {
    button.addEventListener("click", () => open(button.dataset.openTopologyNode));
  });
}

function renderCurrentPage() {
  const page = document.body.dataset.page;
  const meta = currentPageMeta();

  if (page === "topology") {
    return renderTopology();
  }

  if (page === "connections") {
    return renderConnections();
  }

  if (page === "channels") {
    return renderChannels();
  }

  if (page === "dashboard") {
    return renderDashboard();
  }

  if (page === "network") {
    return renderNetwork();
  }

  if (page === "groups") {
    return renderGroups();
  }

  if (page === "nodes") {
    return renderNodes();
  }

  if (["groups", "network", "topology", "connections"].includes(page)) {
    return renderPlaceholder(meta.title, meta.description, meta.tags);
  }

  return renderStubPage(meta);
}

document.addEventListener("DOMContentLoaded", () => {
  const page = document.body.dataset.page;
  document.title = `${currentPageMeta().title} | WuKongIM Admin`;
  document.querySelector("[data-sidebar]").innerHTML = renderSidebar(page);
  document.querySelector("[data-topbar]").innerHTML = renderTopbar(window.ADMIN_UI_DATA.cluster);
  document.querySelector("[data-page-root]").innerHTML = renderCurrentPage();
  bindTopologyDrawer();
  bindChannelDrawer();
  bindConnectionDrawer();
  bindLinkDrawer();
  bindGroupDrawer();
  bindNodeDrawer();
});
