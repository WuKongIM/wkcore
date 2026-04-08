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

function renderCurrentPage() {
  const page = document.body.dataset.page;
  const meta = currentPageMeta();

  if (page === "dashboard") {
    return renderDashboard();
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
  bindNodeDrawer();
});
