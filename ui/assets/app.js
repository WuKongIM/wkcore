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

function renderCurrentPage() {
  const page = document.body.dataset.page;
  const meta = currentPageMeta();

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
});
