"use strict";
/*
  Firefox向け純CSS/JS/HTML版
  - innerHTMLは使わず、createElement/appendChildのみ
  - 2行省略は line-height + max-height
  - 棒グラフは transform(scaleX)
  - PCでホーム2枚パネルの高さを揃える（grid align-items: stretch + 子panel height:100%）
*/

(() => {
  const B = (window.browser || window.chrome);

  // Keys
  const KEY_LOG = "rt_daily_log";
  const KEY_DETAILS = "rt_details";

  // State
  let lastDetails = {};
  let lastLogs = {};
  let lastSiteEnable = {};
  let lastLive = { total: 0, daily: 0 };

  // Utils
  const $ = sel => document.querySelector(sel);
  function $frag(){ return document.createDocumentFragment(); }
  function clear(el){ if (el) el.textContent = ""; }
  function fmt(ms) {
    const s = Math.max(0, Math.floor((ms || 0) / 1000));
    const h = Math.floor(s / 3600);
    const m = Math.floor((s % 3600) / 60);
    const sec = s % 60;
    const pad = n => String(n).padStart(2, "0");
    return `${h}:${pad(m)}:${pad(sec)}`;
  }
  function shortId(id, n = 8) { return String(id || "").slice(0, n); }
  function median(arr) {
    if (!arr || !arr.length) return 0;
    const a = [...arr].sort((x, y) => x - y);
    const n = a.length;
    if (n % 2 === 1) return a[(n - 1) / 2];
    return Math.round((a[n / 2 - 1] + a[n / 2]) / 2);
  }
  function showToast(msg, ms = 1600) {
    const t = $("#toast");
    if (!t) return;
    t.textContent = msg;
    t.classList.add("show");
    window.clearTimeout(showToast._timer);
    showToast._timer = window.setTimeout(() => t.classList.remove("show"), ms);
  }
  function shortDate(ts){
    if (!ts) return "-";
    const d = new Date(ts);
    const mm = String(d.getMonth()+1).padStart(2,'0');
    const dd = String(d.getDate()).padStart(2,'0');
    const hh = String(d.getHours()).padStart(2,'0');
    const mi = String(d.getMinutes()).padStart(2,'0');
    return `${mm}/${dd} ${hh}:${mi}`;
  }

  // Theme
  const THEME_KEY = "rt_theme";
  function applyTheme(theme) {
    const root = document.documentElement;
    const value = theme || localStorage.getItem(THEME_KEY) || "system";
    root.setAttribute("data-theme", value);
    const el = document.querySelector(`.segmented input[value="${value}"]`);
    if (el) el.checked = true;
  }
  function bindTheme() {
    document.querySelectorAll(".segmented input[name='theme']").forEach(r => {
      r.addEventListener("change", () => {
        localStorage.setItem(THEME_KEY, r.value);
        applyTheme(r.value);
        showToast(`テーマ: ${r.value}`);
      });
    });
    applyTheme();
  }

  // Views
  function switchView(view) {
    document.querySelectorAll(".tab").forEach(b => {
      const is = b.dataset.view === view;
      b.classList.toggle("active", is);
      b.setAttribute("aria-selected", String(is));
      if (is) b.focus({ preventScroll: true });
    });
    document.querySelectorAll(".view").forEach(v => v.classList.add("hidden"));
    const target = document.getElementById(`view-${view}`);
    if (target) target.classList.remove("hidden");
    try {
      const map = {home:"ホーム", library:"ライブラリ", work:"作品ページ", log:"ログ", stats:"統計", settings:"設定"};
      const suffix = map[view] || "";
      document.title = suffix ? `ブックタイマー — ${suffix}` : "ブックタイマー";
    } catch {}
  }
  function bindTabKeyboard() {
    const tabs = Array.from(document.querySelectorAll(".toolbar .tab"));
    const order = tabs.map(t => t.dataset.view);
    const toolbar = document.querySelector(".toolbar");
    if (!toolbar) return;
    toolbar.addEventListener("keydown", (e) => {
      if (!["ArrowLeft", "ArrowRight", "Home", "End"].includes(e.key)) return;
      e.preventDefault();
      const current = tabs.findIndex(t => t.getAttribute("aria-selected") === "true");
      let next = current;
      if (e.key === "ArrowRight") next = (current + 1) % tabs.length;
      if (e.key === "ArrowLeft") next = (current - 1 + tabs.length) % tabs.length;
      if (e.key === "Home") next = 0;
      if (e.key === "End") next = tabs.length - 1;
      switchView(order[next]);
    });
  }

  // Home: live
  function renderLive(live) {
    const tEl = $("#liveTotal");
    const dEl = $("#liveToday");
    if (tEl) tEl.textContent = fmt((live && live.total) || 0);
    if (dEl) dEl.textContent = fmt((live && live.daily) || 0);
  }

  // Home: recent works
  function renderRecentWorks(details) {
    const container = document.getElementById("recentWorks");
    if (!container) return;
    clear(container);

    const rows = [];
    Object.entries(details).forEach(([day, list]) => (list || []).forEach(r => rows.push({ day, ...r })));
    rows.sort((a, b) => (b.ts || 0) - (a.ts || 0));

    const byWorkTitle = new Map();
    for (const r of rows) {
      const k = `${r.site}|${r.workTitle}`;
      if (!byWorkTitle.has(k)) byWorkTitle.set(k, r);
    }

    const f = $frag();
    const items = Array.from(byWorkTitle.values()).slice(0, 8);
    if (!items.length) {
      const card = document.createElement("div");
      card.className = "card";
      const title = document.createElement("div");
      title.className = "title";
      const text = document.createElement("span");
      text.className = "text truncate-2";
      text.textContent = "まだ記録がありません";
      title.appendChild(text);
      card.appendChild(title);
      f.appendChild(card);
    } else {
      items.forEach(r => {
        const card = document.createElement("div");
        card.className = "card";
        card.dataset.workKey = `${r.site}|${r.workTitle}`;

        const title = document.createElement("div");
        title.className = "title";
        const text = document.createElement("span");
        text.className = "text truncate-2";
        text.textContent = `[${r.site}] ${r.workTitle || "(no title)"}`;
        title.appendChild(text);

        const meta = document.createElement("div");
        meta.className = "meta";
        meta.textContent = `最終更新: ${r.ts ? shortDate(r.ts) : "-"}`;

        const actions = document.createElement("div");
        actions.className = "actions";

        const btnLibrary = document.createElement("button");
        btnLibrary.className = "secondary";
        btnLibrary.textContent = "ライブラリで表示";
        btnLibrary.addEventListener("click", (e) => {
          e.stopPropagation();
          switchView("library");
          setTimeout(() => highlightWork(card.dataset.workKey), 10);
        });

        const btnOpen = document.createElement("button");
        btnOpen.className = "secondary";
        btnOpen.textContent = "作品ページへ";
        btnOpen.addEventListener("click", (e) => {
          e.stopPropagation();
          openWorkPage(card.dataset.workKey);
        });

        actions.appendChild(btnLibrary);
        actions.appendChild(btnOpen);

        card.appendChild(title);
        card.appendChild(meta);
        card.appendChild(actions);
        card.addEventListener("click", () => openWorkPage(card.dataset.workKey));
        f.appendChild(card);
      });
    }
    container.appendChild(f);
  }

  // Month summary
  function renderMonthSummary(logs) {
    const container = document.getElementById("monthSummary");
    if (!container) return;
    clear(container);

    const now = new Date();
    const ym = `${now.getFullYear()}-${String(now.getMonth() + 1).padStart(2, "0")}`;
    const prev = new Date(now.getFullYear(), now.getMonth() - 1, 1);
    const pYm = `${prev.getFullYear()}-${String(prev.getMonth() + 1).padStart(2, "0")}`;
    let totalMonth = 0, days = 0;
    let totalPrev = 0, pDays = 0;
    Object.entries(logs).forEach(([day, ms]) => {
      if (day.startsWith(ym)) { totalMonth += (ms || 0); days++; }
      if (day.startsWith(pYm)) { totalPrev += (ms || 0); pDays++; }
    });
    const avg = days ? Math.round(totalMonth / days) : 0;
    const prevAvg = pDays ? Math.round(totalPrev / pDays) : 0;
    const diff = avg - prevAvg;

    const items = [
      { label: "今月合計", value: fmt(totalMonth) },
      { label: "日平均", value: fmt(avg) },
      { label: "対象日数", value: `${days}日` },
      { label: "前月比（平均差）", value: `${diff >= 0 ? "+" : ""}${fmt(diff)}` }
    ];

    const f = $frag();
    items.forEach(it => {
      const card = document.createElement("div");
      card.className = "card";
      const title = document.createElement("div");
      title.className = "title";
      title.textContent = it.label;
      const meta = document.createElement("div");
      meta.className = "meta mono";
      meta.textContent = it.value;
      card.appendChild(title);
      card.appendChild(meta);
      f.appendChild(card);
    });
    container.appendChild(f);
  }

  // Log: daily
  function renderDaily(logs) {
    const tbody = document.querySelector("#tableDaily tbody");
    if (!tbody) return;
    clear(tbody);
    const f = $frag();
    Object.entries(logs).sort(([a],[b]) => a.localeCompare(b)).forEach(([day, ms]) => {
      const tr = document.createElement("tr");
      const tdDay = document.createElement("td"); tdDay.textContent = day;
      const tdMs = document.createElement("td"); tdMs.className = "mono"; tdMs.textContent = fmt(ms || 0);
      tr.appendChild(tdDay); tr.appendChild(tdMs);
      f.appendChild(tr);
    });
    tbody.appendChild(f);

    const bars = document.getElementById("dailyBars");
    if (bars) {
      clear(bars);
      const days = Object.entries(logs).sort(([a],[b]) => a.localeCompare(b));
      const max = Math.max(1, ...days.map(([, v]) => v || 0));
      const bf = $frag();
      days.slice(-30).forEach(([day, ms]) => {
        const wrap = document.createElement("div"); wrap.className = "bar";
        const label = document.createElement("div"); label.className = "label"; label.textContent = day.slice(5);
        const gauge = document.createElement("div"); gauge.className = "gauge";
        const fill = document.createElement("div"); fill.className = "fill";
        gauge.appendChild(fill);
        const ratio = Math.max(0, Math.min(1, ((ms || 0) / max)));
        requestAnimationFrame(() => { fill.style.transform = `scaleX(${ratio})`; });
        const value = document.createElement("div"); value.className = "value mono"; value.textContent = fmt(ms || 0);
        wrap.appendChild(label); wrap.appendChild(gauge); wrap.appendChild(value);
        bf.appendChild(wrap);
      });
      bars.appendChild(bf);
    }
  }

  // Details
  function renderDetails(details) {
    const tbody = document.querySelector("#tableDetails tbody");
    if (!tbody) return;
    clear(tbody);
    const rows = [];
    Object.entries(details).forEach(([day, list]) => (list || []).forEach(r => rows.push({ day, ...r })));
    rows.sort((a, b) => (b.ts || 0) - (a.ts || 0));
    const f = $frag();
    rows.forEach(r => {
      const tr = document.createElement("tr");
      const tdDay = document.createElement("td"); tdDay.textContent = r.day || "";
      const tdSite = document.createElement("td"); tdSite.textContent = r.site || "";
      const tdWork = document.createElement("td"); tdWork.className = "mono"; tdWork.textContent = r.workTitle || "";
      const tdEp = document.createElement("td"); tdEp.className = "mono"; tdEp.textContent = r.episodeTitle || "";
      const tdMs = document.createElement("td"); tdMs.className = "mono"; tdMs.textContent = fmt(r.ms || 0);
      const tdTs = document.createElement("td"); tdTs.textContent = (r.ts ? new Date(r.ts).toLocaleString() : "");
      const tdSess = document.createElement("td"); tdSess.className = "mono"; tdSess.textContent = shortId(r.sessionId || "no-session");
      tr.appendChild(tdDay); tr.appendChild(tdSite); tr.appendChild(tdWork); tr.appendChild(tdEp); tr.appendChild(tdMs); tr.appendChild(tdTs); tr.appendChild(tdSess);
      f.appendChild(tr);
    });
    tbody.appendChild(f);
  }

  // Aggregates: work
  function renderWorkAggregates(details) {
    const workTbody = document.querySelector("#tableAggWork tbody");
    if (!workTbody) return;
    clear(workTbody);
    const byWork = {};
    Object.values(details).forEach(list => {
      (list || []).forEach(r => {
        const k = `${r.site}|${r.workTitle || ""}`;
        byWork[k] = (byWork[k] || 0) + (r.ms || 0);
      });
    });
    const f = $frag();
    Object.entries(byWork)
      .map(([k, ms]) => {
        const [site, workTitle] = k.split("|");
        return { site, workTitle, ms };
      })
      .sort((a, b) => b.ms - a.ms)
      .forEach(row => {
        const tr = document.createElement("tr");
        const tdSite = document.createElement("td"); tdSite.textContent = row.site;
        const tdWork = document.createElement("td"); tdWork.className = "mono"; tdWork.textContent = row.workTitle || "";
        const tdMs = document.createElement("td"); tdMs.className = "mono"; tdMs.textContent = fmt(row.ms);
        tr.appendChild(tdSite); tr.appendChild(tdWork); tr.appendChild(tdMs);
        f.appendChild(tr);
      });
    workTbody.appendChild(f);
  }

  // Aggregates: episode sessions
  function renderAggregates(details) {
    const epTbody = document.querySelector("#tableAggEpisode tbody");
    if (!epTbody) return;
    clear(epTbody);
    const bySessionTotal = {};
    const labelByEpisode = {};
    Object.values(details).forEach(list => {
      (list || []).forEach(r => {
        const epKey = `${r.site}|${r.workTitle}|${r.episodeTitle}`;
        labelByEpisode[epKey] = labelByEpisode[epKey] || {
          site: r.site || "",
          workTitle: r.workTitle || "",
          episodeTitle: r.episodeTitle || ""
        };
        const sessKey = `${epKey}|${r.sessionId || "no-session"}`;
        bySessionTotal[sessKey] = (bySessionTotal[sessKey] || 0) + (r.ms || 0);
      });
    });
    const statsByEpisode = {};
    Object.entries(bySessionTotal).forEach(([k, ms]) => {
      const parts = k.split("|");
      const epKey = parts.slice(0, 3).join("|");
      const sessionId = parts[3];
      if (!statsByEpisode[epKey]) {
        const lbl = labelByEpisode[epKey] || {};
        statsByEpisode[epKey] = { totals: [], sessions: [], label: lbl };
      }
      statsByEpisode[epKey].totals.push(ms);
      statsByEpisode[epKey].sessions.push({ sessionId, ms });
    });

    const f = $frag();
    Object.values(statsByEpisode)
      .map(v => {
        const totals = v.totals;
        return {
          ...v.label,
          count: totals.length,
          total: totals.reduce((s, x) => s + x, 0),
          min: totals.length ? Math.min(...totals) : 0,
          max: totals.length ? Math.max(...totals) : 0,
          median: median(totals),
          sessions: v.sessions
        };
      })
      .sort((a, b) => b.total - a.total)
      .forEach(row => {
        const tr = document.createElement("tr");
        const td = document.createElement("td"); td.setAttribute("colspan", "3");
        const detailsEl = document.createElement("details");
        const summary = document.createElement("summary");
        summary.textContent = `[${row.site}] ${row.episodeTitle} / ${row.workTitle} — 中央 ${fmt(row.median)} / 最小 ${fmt(row.min)} / 最大 ${fmt(row.max)} （${row.count}回）`;
        const divSessions = document.createElement("div"); divSessions.className = "sessions";
        row.sessions.sort((a, b) => b.ms - a.ms).forEach(s => {
          const div = document.createElement("div");
          div.className = "mono";
          div.textContent = `session ${shortId(s.sessionId)}: ${fmt(s.ms)}`;
          divSessions.appendChild(div);
        });
        detailsEl.appendChild(summary);
        detailsEl.appendChild(divSessions);
        td.appendChild(detailsEl);
        tr.appendChild(td);
        f.appendChild(tr);
      });
    epTbody.appendChild(f);
  }

  // Library grouping
  function groupByWork(details) {
    const byWork = {};
    Object.values(details).forEach(list => {
      (list || []).forEach(r => {
        const key = `${r.site}|${r.workTitle}`;
        if (!byWork[key]) {
          byWork[key] = { site: r.site, workTitle: r.workTitle, episodes: [], latestTs: 0, totalMs: 0 };
        }
        byWork[key].episodes.push(r);
        byWork[key].latestTs = Math.max(byWork[key].latestTs, r.ts || 0);
        byWork[key].totalMs += (r.ms || 0);
      });
    });
    return byWork;
  }

  function renderLibrary(details) {
    const container = document.getElementById("libraryShelf");
    if (!container) return;
    clear(container);
    const grouped = groupByWork(details);

    const q = (document.getElementById("filterText")?.value || "").trim().toLowerCase();
    const sortKey = document.getElementById("sortLibrary")?.value || "recent";

    let works = Object.values(grouped);
    if (q) works = works.filter(w => (w.workTitle || "").toLowerCase().includes(q));
    works.sort((a, b) => {
      if (sortKey === "total") return b.totalMs - a.totalMs;
      if (sortKey === "title") return (a.workTitle || "").localeCompare(b.workTitle || "");
      return (b.latestTs - a.latestTs);
    });

    const f = $frag();
    if (!works.length) {
      const empty = document.createElement("div");
      empty.className = "empty";
      empty.textContent = "表示できる作品がありません";
      f.appendChild(empty);
    } else {
      works.forEach(w => {
        const detailsEl = document.createElement("details");
        detailsEl.dataset.workKey = `${w.site}|${w.workTitle}`;

        const summary = document.createElement("summary");
        summary.className = "lib-summary";

        const leftTitle = document.createElement("span");
        leftTitle.className = "lib-title";
        leftTitle.textContent = `[${w.site}] ${w.workTitle}`;

        const meta = document.createElement("span");
        meta.className = "lib-meta";

        const metaFrag = $frag();
        const metaItems = [
          { icon: "⏱", label: "合計", value: fmt(w.totalMs) },
          { icon: "📅", label: "最終", value: shortDate(w.latestTs) },
          { icon: "#", label: "件数", value: String(w.episodes.length) }
        ];
        metaItems.forEach(mi => {
          const item = document.createElement("span");
          item.className = "meta-item";

          const ic = document.createElement("span");
          ic.className = "icon"; ic.setAttribute("aria-hidden", "true");
          ic.textContent = mi.icon;

          const lab = document.createElement("span");
          lab.className = "label"; lab.textContent = mi.label;

          const val = document.createElement("span");
          val.className = "value mono"; val.textContent = mi.value;

          item.appendChild(ic); item.appendChild(lab); item.appendChild(val);
          metaFrag.appendChild(item);
        });
        meta.appendChild(metaFrag);

        const actions = document.createElement("span");
        actions.className = "lib-actions";

        const focusBtn = document.createElement("button");
        focusBtn.className = "secondary";
        focusBtn.textContent = "ライブラリで表示";
        focusBtn.addEventListener("click", (e) => {
          e.preventDefault(); e.stopPropagation();
          detailsEl.open = true;
          detailsEl.scrollIntoView({ behavior: "smooth", block: "center" });
          detailsEl.classList.add("highlight");
          setTimeout(() => detailsEl.classList.remove("highlight"), 2000);
        });

        const openBtn = document.createElement("button");
        openBtn.className = "secondary";
        openBtn.textContent = "作品ページへ";
        openBtn.addEventListener("click", (e) => {
          e.preventDefault(); e.stopPropagation();
          openWorkPage(detailsEl.dataset.workKey);
        });

        actions.appendChild(focusBtn);
        actions.appendChild(openBtn);

        summary.appendChild(leftTitle);
        summary.appendChild(actions);
        summary.appendChild(meta);

        const list = document.createElement("div"); list.className = "episodes";
        w.episodes.sort((a, b) => a.ts - b.ts).forEach(ep => {
          const row = document.createElement("div"); row.className = "episode";
          const aEl = document.createElement("a");
          aEl.href = ep.url || "#"; aEl.target = "_blank"; aEl.rel = "noopener";
          aEl.textContent = ep.episodeTitle || "(no title)";
          const time = document.createElement("span"); time.className = "mono"; time.textContent = fmt(ep.ms || 0);
          const when = document.createElement("span"); when.className = "mono"; when.textContent = shortDate(ep.ts || 0);
          row.appendChild(aEl); row.appendChild(time); row.appendChild(when);
          list.appendChild(row);
        });

        detailsEl.appendChild(summary);
        detailsEl.appendChild(list);
        f.appendChild(detailsEl);
      });
    }
    container.appendChild(f);
  }

  function highlightWork(workKey) {
    const el = document.querySelector(`[data-work-key="${CSS.escape(workKey)}"]`);
    if (!el) return;
    el.open = true;
    el.scrollIntoView({ behavior: "smooth", block: "center" });
    el.classList.add("highlight");
    setTimeout(() => el.classList.remove("highlight"), 2000);
  }

  // Work page
  function openWorkPage(workKey) {
    const grouped = groupByWork(lastDetails);
    const w = grouped[workKey];
    if (!w) { showToast("作品データが見つかりません"); return; }

    const titleEl = document.getElementById("workPageTitle");
    const metaEl = document.getElementById("workMeta");
    if (titleEl) titleEl.textContent = `[${w.site}] ${w.workTitle}`;
    if (metaEl) metaEl.textContent = `合計 ${fmt(w.totalMs)} ／ 最終 ${shortDate(w.latestTs)} ／ ${w.episodes.length}件`;

    const listEl = document.getElementById("workEpisodes");
    clear(listEl);
    const ef = $frag();
    w.episodes.sort((a, b) => b.ts - a.ts).forEach(ep => {
      const row = document.createElement("div"); row.className = "episode";
      const aEl = document.createElement("a");
      aEl.href = ep.url || "#"; aEl.target = "_blank"; aEl.rel = "noopener";
      aEl.textContent = ep.episodeTitle || "(no title)";
      const time = document.createElement("span"); time.className = "mono"; time.textContent = fmt(ep.ms || 0);
      const when = document.createElement("span"); when.className = "mono"; when.textContent = shortDate(ep.ts || 0);
      row.appendChild(aEl); row.appendChild(time); row.appendChild(when);
      ef.appendChild(row);
    });
    listEl.appendChild(ef);

    const totalsBySession = {};
    w.episodes.forEach(ep => {
      const sid = ep.sessionId || "no-session";
      totalsBySession[sid] = (totalsBySession[sid] || 0) + (ep.ms || 0);
    });
    const totalsArr = Object.values(totalsBySession);
    const cards = document.getElementById("workInnerStats");
    clear(cards);
    const cf = $frag();
    [
      { label: "合計", value: fmt(w.totalMs) },
      { label: "中央値（セッション）", value: fmt(median(totalsArr)) },
      { label: "最小（セッション）", value: fmt(totalsArr.length ? Math.min(...totalsArr) : 0) },
      { label: "最大（セッション）", value: fmt(totalsArr.length ? Math.max(...totalsArr) : 0) },
    ].forEach(it => {
      const card = document.createElement("div");
      card.className = "card";
      const title = document.createElement("div");
      title.className = "title";
      title.textContent = it.label;
      const meta = document.createElement("div");
      meta.className = "meta mono";
      meta.textContent = it.value;
      card.appendChild(title);
      card.appendChild(meta);
      cf.appendChild(card);
    });
    cards.appendChild(cf);

    switchView("work");
  }

  // Site toggles
  function renderToggles(cfg) {
    const div = document.getElementById("siteToggles");
    if (!div) return;
    clear(div);
    const sites = Object.keys(cfg).length ? Object.keys(cfg) : ["kakuyomu.jp", "syosetu.org", "pixiv.net", "syosetu.com"];
    const f = $frag();
    sites.forEach(domain => {
      const label = document.createElement("label");
      label.style.display = "inline-flex";
      label.style.alignItems = "center";
      label.style.gap = "8px";
      const input = document.createElement("input");
      input.type = "checkbox";
      input.checked = !!cfg[domain];
      input.addEventListener("change", (e) => {
        try {
          B.runtime.sendMessage({ type: "set-site-enable", domain, enabled: e.target.checked }, (resp) => {
            showToast(resp && resp.ok ? `${domain}: ${e.target.checked ? "有効化" : "無効化"}` : "更新に失敗しました");
          });
        } catch {}
      });
      const span = document.createElement("span");
      span.textContent = ` ${domain}`;
      label.appendChild(input);
      label.appendChild(span);
      f.appendChild(label);
    });
    div.appendChild(f);
  }

  // Composite render
  function renderAll() {
    renderLive(lastLive);
    renderRecentWorks(lastDetails);
    renderMonthSummary(lastLogs);
    renderDaily(lastLogs);
    renderDetails(lastDetails);
    renderWorkAggregates(lastDetails);
    renderAggregates(lastDetails);
    renderLibrary(lastDetails);
    renderToggles(lastSiteEnable);
  }

  // Load
  function load() {
    try {
      B?.runtime?.sendMessage({ type: "export-store" }, (resp) => {
        const snap = (resp && resp.ok && resp.snapshot) ? resp.snapshot : {};
        lastLogs = snap[KEY_LOG] || {};
        lastDetails = snap[KEY_DETAILS] || {};
        renderAll();
      });
    } catch {}
    try {
      B?.runtime?.sendMessage({ type: "get-stats" }, (live) => {
        lastLive = (live && typeof live === "object") ? live : { total: 0, daily: 0 };
        renderLive(lastLive);
      });
    } catch {}
    try {
      B?.runtime?.sendMessage({ type: "get-site-enable" }, (cfg) => {
        lastSiteEnable = cfg || {};
        renderToggles(lastSiteEnable);
      });
    } catch {}
  }

  // Live updates
  try {
    B?.runtime?.onMessage?.addListener((msg) => {
      if (msg?.type === "live-update" && msg.payload) {
        lastLive = {
          total: msg.payload.total ?? lastLive.total,
          daily: msg.payload.daily ?? lastLive.daily
        };
        renderLive(lastLive);
      } else if (msg?.type === "export-store") {
        const snap = (msg.snapshot || {});
        lastLogs = snap[KEY_LOG] || lastLogs;
        lastDetails = snap[KEY_DETAILS] || lastDetails;
        renderAll();
      } else if (msg?.type === "import-store") {
        showToast("データをインポートしました");
        load();
      }
    });
  } catch {}

  // Actions
  document.getElementById("resetToday")?.addEventListener("click", () => {
    if (!confirm("今日のログを消去します。よろしいですか？")) return;
    try {
      B.runtime.sendMessage({ type: "reset-today" }, (resp) => {
        if (!(resp && resp.ok)) alert("reset-today に失敗しました");
        else showToast("今日のログをリセットしました");
        load();
      });
    } catch { load(); }
  });
  document.getElementById("resetAll")?.addEventListener("click", () => {
    if (!confirm("全てのログを消去します。よろしいですか？")) return;
    try {
      B.runtime.sendMessage({ type: "reset-all" }, (resp) => {
        if (!(resp && resp.ok)) alert("reset-all に失敗しました");
        else showToast("全ログを削除しました");
        load();
      });
    } catch { load(); }
  });
  document.getElementById("backToLibrary")?.addEventListener("click", () => {
    switchView("library");
  });

  // Toolbar navigation
  document.querySelectorAll(".toolbar .tab").forEach(btn => {
    btn.addEventListener("click", () => switchView(btn.dataset.view));
  });
  bindTabKeyboard();

  // Filters in library
  document.getElementById("filterText")?.addEventListener("input", () => renderLibrary(lastDetails));
  document.getElementById("sortLibrary")?.addEventListener("change", () => renderLibrary(lastDetails));

  // Init
  document.addEventListener("DOMContentLoaded", () => {
    bindTheme();
    switchView("home");
    load();
  });
})();
