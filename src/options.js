"use strict";
/*
Options UI
- Firefox/Chrome 両対応のため browser/chrome エイリアス使用
- background.js の既存メッセージ: export-store / get-stats / get-site-enable / set-site-enable / live-update / reset-today / reset-all に対応
*/

(() => {
  const B = (window.browser || window.chrome);

  // ---------------------------
  // Constants / Config
  // ---------------------------
  const KEY_LOG = "rt_daily_log";
  const KEY_DETAILS = "rt_details";
  const KEY_SITE_ENABLE = "rt_site_enable";

  const SITE = {
    KAKUYOMU: "kakuyomu.jp",
    HAMELN: "syosetu.org",
    PIXIV: "pixiv.net",
    NAROU: "syosetu.com"
  };

  // ---------------------------
  // Utilities (DOM / format)
  // ---------------------------

  function el(tag, { className, attrs, text, children, on } = {}) {
    const node = document.createElement(tag);
    if (className) node.className = className;
    if (attrs) {
      for (const [k, v] of Object.entries(attrs)) {
        if (v === undefined || v === null) continue;
        node.setAttribute(k, String(v));
      }
    }
    if (text !== undefined && text !== null) node.textContent = String(text);
    if (children && children.length) node.append(...children);
    if (on) {
      for (const [type, handler] of Object.entries(on)) node.addEventListener(type, handler);
    }
    return node;
  }

  function clear(elm) { if (!elm) return; elm.textContent = ""; }
  function frag() { return document.createDocumentFragment(); }

  function fmt(ms) {
    const s = Math.max(0, Math.floor((ms || 0) / 1000));
    const h = Math.floor(s / 3600);
    const m = Math.floor((s % 3600) / 60);
    const sec = s % 60;
    const pad = n => String(n).padStart(2, "0");
    return `${h}:${pad(m)}:${pad(sec)}`;
  }

  function median(arr) {
    if (!arr || !arr.length) return 0;
    const a = [...arr].sort((x, y) => x - y);
    const n = a.length;
    if (n % 2 === 1) return a[(n - 1) / 2];
    return Math.round((a[n / 2 - 1] + a[n / 2]) / 2);
  }

  function shortId(id, n = 8) { return String(id || "").slice(0, n); }

  // ---------------------------
  // State
  // ---------------------------
  let lastDetails = {};
  let lastLogs = {};
  let lastSiteEnable = {};
  let lastLive = { total: 0, daily: 0 };

  // ---------------------------
  // Renderers
  // ---------------------------

  function renderDaily(logs) {
    const tbody = document.querySelector("#tableDaily tbody");
    if (!tbody) return;
    clear(tbody);

    const f = frag();
    Object.entries(logs).sort().forEach(([day, ms]) => {
      const tr = el("tr", {
        children: [
          el("td", { text: day }),
          el("td", { className: "mono", text: fmt(ms || 0) })
        ]
      });
      f.appendChild(tr);
    });
    tbody.appendChild(f);
  }

  function renderDetails(details) {
    const tbody = document.querySelector("#tableDetails tbody");
    if (!tbody) return;
    clear(tbody);

    const rows = [];
    Object.entries(details).forEach(([day, list]) => {
      (list || []).forEach(r => rows.push({ day, ...r }));
    });
    rows.sort((a, b) => b.ts - a.ts);

    const f = frag();
    rows.forEach(r => {
      const tr = el("tr", {
        children: [
          el("td", { text: r.day || "" }),
          el("td", { text: r.site || "" }),
          el("td", { className: "mono", text: r.workTitle || r.workId || "" }),
          el("td", { className: "mono", text: r.episodeTitle || r.episodeKey || "" }),
          el("td", { className: "mono", text: fmt(r.ms || 0) }),
          el("td", { text: (r.ts ? new Date(r.ts).toLocaleString() : "") }),
          el("td", { className: "mono", text: shortId(r.sessionId || "no-session") })
        ]
      });
      f.appendChild(tr);
    });
    tbody.appendChild(f);
  }

  function renderWorkAggregates(details) {
    const workTbody = document.querySelector("#tableAggWork tbody");
    if (!workTbody) return;
    clear(workTbody);

    const byWork = {};
    const titleByWork = {};

    Object.values(details).forEach(list => {
      (list || []).forEach(r => {
        const k = `${r.site}|${r.workId}`;
        byWork[k] = (byWork[k] || 0) + (r.ms || 0);
        if (r.workTitle) titleByWork[k] = r.workTitle;
      });
    });

    const f = frag();
    Object.entries(byWork)
      .map(([k, ms]) => {
        const [site, workId] = k.split("|");
        return { site, workId, workTitle: titleByWork[k] || "", ms };
      })
      .sort((a, b) => b.ms - a.ms)
      .forEach(row => {
        const tr = el("tr", {
          children: [
            el("td", { text: row.site }),
            el("td", { className: "mono", text: row.workTitle || row.workId || "" }),
            el("td", { className: "mono", text: fmt(row.ms) })
          ]
        });
        f.appendChild(tr);
      });
    workTbody.appendChild(f);
  }

  function renderAggregates(details) {
    const epTbody = document.querySelector("#tableAggEpisode tbody");
    if (!epTbody) return;
    clear(epTbody);

    const bySessionTotal = {};
    const labelByEpisode = {};

    Object.values(details).forEach(list => {
      (list || []).forEach(r => {
        const epKey = `${r.site}|${r.workId}|${r.episodeKey}`;
        labelByEpisode[epKey] = labelByEpisode[epKey] || {
          site: r.site || "",
          workId: r.workId || "",
          episodeKey: r.episodeKey || "",
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

    const f = frag();
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
        const tr = el("tr");
        const td = el("td", { attrs: { colspan: 3 } });

        const detailsEl = el("details");
        const summary = el("summary", {
          text: `[${row.site}] ${row.episodeTitle || row.episodeKey || ""} / ${row.workTitle || row.workId || ""} — 中央 ${fmt(row.median)} / 最小 ${fmt(row.min)} / 最大 ${fmt(row.max)} （${row.count}回）`
        });

        const divSessions = el("div", { className: "sessions" });
        row.sessions
          .sort((a, b) => b.ms - a.ms)
          .forEach(s => {
            const div = el("div", {
              className: "mono",
              text: `session ${shortId(s.sessionId)}: ${fmt(s.ms)}`
            });
            divSessions.appendChild(div);
          });

        detailsEl.append(summary, divSessions);
        td.appendChild(detailsEl);
        tr.appendChild(td);
        f.appendChild(tr);
      });
    epTbody.appendChild(f);
  }

  function renderToggles(cfg) {
    const div = document.getElementById("siteToggles");
    if (!div) return;
    clear(div);

    const sites = [SITE.KAKUYOMU, SITE.HAMELN, SITE.PIXIV, SITE.NAROU];
    const f = frag();

    sites.forEach(domain => {
      const id = `tgl-${domain}`;
      const label = el("label", {
        children: [
          el("input", {
            attrs: { type: "checkbox", id, ...(cfg[domain] ? { checked: "" } : {}) },
            on: {
              change: (e) => {
                B.runtime.sendMessage(
                  { type: "set-site-enable", domain, enabled: e.target.checked },
                  () => {}
                );
              }
            }
          }),
          el("span", { text: ` ${domain}` })
        ]
      });
      f.appendChild(label);
    });

    div.appendChild(f);
  }

  function renderLive(live) {
    const tEl = document.getElementById("liveTotal");
    const dEl = document.getElementById("liveToday");
    if (tEl) tEl.textContent = fmt((live && live.total) || 0);
    if (dEl) dEl.textContent = fmt((live && live.daily) || 0);
  }

  // ---------------------------
  // Data loading / events
  // ---------------------------

  function load() {
    // 詳細・ログは export-store から取得
    try {
      B.runtime.sendMessage({ type: "export-store" }, (resp) => {
        const snap = (resp && resp.ok && resp.snapshot) ? resp.snapshot : {};
        lastLogs = snap[KEY_LOG] || {};
        lastDetails = snap[KEY_DETAILS] || {};
        renderDaily(lastLogs);
        renderDetails(lastDetails);
        renderAggregates(lastDetails);
        renderWorkAggregates(lastDetails);
      });
    } catch {}

    // ライブ値は get-stats
    try {
      B.runtime.sendMessage({ type: "get-stats" }, (live) => {
        lastLive = (live && typeof live === "object") ? live : { total: 0, daily: 0 };
        renderLive(lastLive);
      });
    } catch {}

    // サイト有効設定
    try {
      B.runtime.sendMessage({ type: "get-site-enable" }, (cfg) => {
        lastSiteEnable = cfg || {};
        renderToggles(lastSiteEnable);
      });
    } catch {}
  }

  // リセットボタン
  document.getElementById("resetToday")?.addEventListener("click", () => {
    if (!confirm("今日のログを消去します。よろしいですか？")) return;
    try {
      B.runtime.sendMessage({ type: "reset-today" }, (resp) => {
        if (!(resp && resp.ok)) {
          alert("reset-today に失敗しました");
        }
        load();
      });
    } catch { load(); }
  });

  document.getElementById("resetAll")?.addEventListener("click", () => {
    if (!confirm("全てのログを消去します。よろしいですか？")) return;
    try {
      B.runtime.sendMessage({ type: "reset-all" }, (resp) => {
        if (!(resp && resp.ok)) {
          alert("reset-all に失敗しました");
        }
        load();
      });
    } catch { load(); }
  });

  // ライブ更新は live-update で反映
  B.runtime.onMessage.addListener((msg) => {
    if (msg && msg.type === "live-update") {
      lastLive = msg.payload || lastLive;
      renderLive(lastLive);
    }
  });

  // Init
  load();
})();
