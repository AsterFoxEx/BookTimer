"use strict";
/**
 * options.js — ブックタイマー（拡張オプション UI）
 * 方針:
 * - UI/UX: 軽快・最小構造・視認性重視。ユーザーが「いま」を把握できる。
 * - グラフ: 横軸は固定（0..10, 現在=10）。縦軸は最大値+2分を基準に狭く、ホイール/ピンチで上下のみ拡縮。
 * - 役割分離: データ→整形→描画、UI→イベント→レンダリング。過剰な再描画や重複リスナーを抑制。
 * 依存:
 * - Firefox: window.browser / Chrome: window.chrome
 */

/* =========================================================
   Globals / Bridge
========================================================= */
(() => {
  const B = (window.browser || window.chrome);

  // Storage keys（拡張側と一致）
  const KEY_LOG = "rt_daily_log";       // { "YYYY-MM-DD": ms, ... }
  const KEY_DETAILS = "rt_details";     // { "YYYY-MM-DD": [ { site, workTitle, episodeTitle, url, ms, ts, sessionId }, ... ], ... }
  const THEME_KEY = "rt_theme";         // "system" | "light" | "dark"

  // State
  let lastLive = { total: 0, daily: 0 };
  let lastLogs = {};
  let lastDetails = {};
  let lastSiteEnable = {};

  // Render cache
  const renderCache = {
    live: "",
    logs: "",
    details: "",
  };

  /* =========================================================
     Utilities
  ========================================================= */
  const $ = (sel, root = document) => root.querySelector(sel);
  const $$ = (sel, root = document) => Array.from(root.querySelectorAll(sel));
  const clear = (el) => { if (el) el.textContent = ""; };
  const frag = () => document.createDocumentFragment();
  const pad2 = (n) => String(n).padStart(2, "0");

  function fmt(ms) {
    const s = Math.max(0, Math.floor((ms || 0) / 1000));
    const h = Math.floor(s / 3600);
    const m = Math.floor((s % 3600) / 60);
    const sec = s % 60;
    return `${h}:${pad2(m)}:${pad2(sec)}`;
  }
  function formatShort(ms) {
    // 軽快表示: h>0 => hhm, else mm:ss
    const s = Math.max(0, Math.floor((ms || 0) / 1000));
    const h = Math.floor(s / 3600);
    const m = Math.floor((s % 3600) / 60);
    const sec = s % 60;
    if (h > 0) return `${h}h${m}`;
    return `${m}:${pad2(sec)}`;
  }
  function formatReadable(ms) {
    const s = Math.max(0, Math.floor((ms || 0) / 1000));
    const h = Math.floor(s / 3600);
    const m = Math.floor((s % 3600) / 60);
    const sec = s % 60;
    if (h > 0) return `${h}時間${m}分`;
    return `${m}分${sec}秒`;
  }
  function shortDate(ts) {
    if (!ts) return "-";
    const d = new Date(ts);
    return `${pad2(d.getMonth() + 1)}/${pad2(d.getDate())} ${pad2(d.getHours())}:${pad2(d.getMinutes())}`;
  }
  function median(arr) {
    if (!arr || !arr.length) return 0;
    const a = [...arr].sort((x, y) => x - y);
    const n = a.length;
    return n % 2 ? a[(n - 1) / 2] : Math.round((a[n / 2 - 1] + a[n / 2]) / 2);
  }
  function clamp(v, min, max) { return Math.max(min, Math.min(max, v)); }

  // Toast
  function showToast(msg, ms = 1600, type) {
    const t = $("#toast");
    if (!t) return;
    t.textContent = msg;
    t.className = `toast show${type ? " " + type : ""}`;
    clearTimeout(showToast._timer);
    showToast._timer = setTimeout(() => t.classList.remove("show"), ms);
  }

  // Tooltip（Canvas 用 DOM オーバレイ）
  function createTooltip(container) {
    let el = container.querySelector(".chart-tooltip");
    if (!el) {
      el = document.createElement("div");
      el.className = "chart-tooltip mono";
      Object.assign(el.style, {
        position: "fixed",
        zIndex: 80,
        pointerEvents: "none",
        background: "var(--surface)",
        border: "1px solid var(--border)",
        borderRadius: "8px",
        boxShadow: "var(--shadow-2)",
        padding: "6px 8px",
        fontSize: "13px",
        whiteSpace: "pre",
        display: "none",
      });
      document.body.appendChild(el);
    }
    return {
      show(text, x, y) {
        el.textContent = text;
        el.style.left = `${x + 10}px`;
        el.style.top = `${y + 10}px`;
        el.style.display = "block";
      },
      hide() { el.style.display = "none"; }
    };
  }

  /* =========================================================
     Theme
  ========================================================= */
  function applyTheme(theme) {
    const root = document.documentElement;
    const value = theme || localStorage.getItem(THEME_KEY) || "system";
    root.setAttribute("data-theme", value);
    // UI state
    const input = document.querySelector(`.segmented input[value="${value}"]`);
    if (input) input.checked = true;
  }
  function bindTheme() {
    document.querySelectorAll(".segmented input[name='theme']").forEach(r => {
      r.addEventListener("change", () => {
        localStorage.setItem(THEME_KEY, r.value);
        applyTheme(r.value);
        const labelText = r.labels?.[0]?.textContent || r.value;
        showToast(`テーマを ${labelText} に変更しました`, 1600, "success");
      });
    });
    applyTheme();
  }

  /* =========================================================
     Tabs / Views
  ========================================================= */
  function switchView(view) {
    const isWorkPage = view === "work";
    const activeTabKey = isWorkPage ? "library" : view;
    $$(".toolbar .tab").forEach(btn => {
      const active = btn.dataset.view === activeTabKey;
      btn.classList.toggle("active", active);
      btn.setAttribute("aria-selected", String(active));
      if (active && !isWorkPage) btn.focus({ preventScroll: true });
    });
    $$(".view").forEach(v => v.classList.add("hidden"));
    const target = document.getElementById(`view-${view}`);
    if (target) target.classList.remove("hidden");
    try {
      const titles = { home: "ホーム", library: "ライブラリ", work: "作品ページ", settings: "設定" };
      document.title = `ブックタイマー — ${titles[view] || ""}`;
    } catch {}
  }
  function bindTabs() {
    $$(".toolbar .tab").forEach(btn => {
      btn.addEventListener("click", () => switchView(btn.dataset.view));
    });
    const toolbar = $(".toolbar");
    const tabs = $$(".toolbar .tab");
    const order = tabs.map(t => t.dataset.view);
    toolbar?.addEventListener("keydown", (e) => {
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

  /* =========================================================
     Home: Summary
  ========================================================= */
  function renderHomeSummary(live, logs) {
    const key = JSON.stringify(live);
    if (key === renderCache.live) return;
    renderCache.live = key;

    $("#liveTotal") && ($("#liveTotal").textContent = fmt(live?.total || 0));
    $("#liveToday") && ($("#liveToday").textContent = fmt(live?.daily || 0));

    const now = new Date();
    const ym = `${now.getFullYear()}-${pad2(now.getMonth() + 1)}`;
    let totalMonthMs = 0, dayCount = 0;
    Object.entries(logs).forEach(([day, ms]) => {
      if (day.startsWith(ym)) {
        totalMonthMs += (ms || 0);
        if (ms > 0) dayCount++;
      }
    });
    const avgMonthMs = dayCount ? (totalMonthMs / dayCount) : 0;
    $("#liveMonthTotal") && ($("#liveMonthTotal").textContent = fmt(totalMonthMs));
    $("#liveMonthAvg") && ($("#liveMonthAvg").textContent = fmt(avgMonthMs));
  }

  /* =========================================================
     Home: Recent works
  ========================================================= */
  function renderRecentWorks(details) {
    const key = JSON.stringify(details);
    if (key === renderCache.details) return;
    renderCache.details = key;

    const container = $("#recentWorks");
    if (!container) return;
    clear(container);

    const rows = [];
    Object.entries(details).forEach(([day, list]) => (list || []).forEach(r => rows.push({ day, ...r })));
    rows.sort((a, b) => (b.ts || 0) - (a.ts || 0));

    const pick = new Map();
    for (const r of rows) {
      const k = `${r.site}|${r.workTitle}`;
      if (!pick.has(k)) pick.set(k, r);
    }
    const items = Array.from(pick.values()).slice(0, 8);

    if (!items.length) {
      const empty = document.createElement("div");
      empty.className = "empty";
      empty.textContent = "まだ記録がありません";
      container.appendChild(empty);
      return;
    }

    const f = frag();
    items.forEach(r => {
      const card = document.createElement("div");
      card.className = "card";
      card.dataset.workKey = `${r.site}|${r.workTitle}`;
      card.setAttribute("role", "button");
      card.setAttribute("tabindex", "0");

      const title = document.createElement("div");
      title.className = "title";
      const text = document.createElement("span");
      text.className = "text truncate-1";
      text.textContent = `[${r.site}] ${r.workTitle || "(no title)"}`;
      title.appendChild(text);

      const meta = document.createElement("div");
      meta.className = "meta";
      meta.textContent = `最終更新: ${r.ts ? shortDate(r.ts) : "-"}`;

      const actions = document.createElement("div");
      actions.className = "actions";
      const btnOpen = document.createElement("button");
      btnOpen.className = "small";
      btnOpen.textContent = "作品ページへ";
      btnOpen.addEventListener("click", (e) => {
        e.stopPropagation();
        openWorkPage(card.dataset.workKey);
      });
      actions.append(btnOpen);

      card.append(title, meta, actions);
      card.addEventListener("click", (e) => {
        if (e.target.tagName === "BUTTON") return;
        openWorkPage(card.dataset.workKey);
      });
      card.addEventListener("keydown", (e) => {
        if (e.key === "Enter" || e.key === " ") openWorkPage(card.dataset.workKey);
      });

      f.appendChild(card);
    });
    container.appendChild(f);
  }

/**
 * 月次トレンドグラフ（縦軸は「最大値（baseTop）」をスケールで可変）
 * - キャンバス自体は固定（DPRのみ適用）。スクロールで要素全体が拡大しない
 * - wheel/pinch で yScaleFactor を更新 → topMax を再計算 → 軸ラベルとデータが連動
 * - 欠損は 0 として線で結ぶ（連続）。点は非ゼロのみ。今日を軽く強調
 * - 空データ時はフォールバック（DOM）を重複なく表示
 */
/**
 * 月次トレンドグラフ一式（縦軸は最大値を可変、PC/スマホの操作に自然対応）
 * - 要素を拡大しない：キャンバスは常に固定サイズ（DPRのみ）。ズームは「縦軸最大値 topMax」の変更で表現
 * - 操作は統一された Pointer/Touch/Wheel：
 *   - Wheel（PC）/ Pinch（スマホ2本指）で縦軸ズーム
 *   - Drag（1本指/左ドラッグ）で縦軸ズーム（指数スケールで滑らか）
 *   - Tap/Click で最寄り点のツールチップ表示、Long-press で固定表示
 * - 欠損は 0 として連続線、点は非ゼロのみ。今日を軽く強調
 * - 空データ時はフォールバックを重複なく表示
 */
function renderMonthTrendGraph(logs) {
  const container = $("#monthTrendChart");
  if (!container) return;

  if (!container._graph) {
    const canvas = document.createElement("canvas");
    canvas.style.display = "block";
    canvas.style.position = "relative";
    container.appendChild(canvas);

    container._graph = {
      cacheKey: "",
      canvas,
      ctx: null,
      lastSize: { w: 0, h: 0, dpr: 1 },

      // Interaction state
      yScaleFactor: 0.375, // baseTop に乗算される縦軸スケール
      isMobile: window.innerWidth <= 599,
      gesture: { touching: false, lastDistY: 0 },
      tooltip: createTooltip(container),
      suppressTooltip: false,
      lastHoverTs: 0,
      hoverThrottleMs: 60,
      tipLocked: false, // 長押しで固定されたか

      // Data
      data: [],
      today: 0,
      baseTop: 0, // 最大値+バッファ（最低10分）
    };

    const g = container._graph;

    // Wheel: 縦軸ズーム（要素拡大はしない）
    container.addEventListener(
      "wheel",
      (e) => {
        e.preventDefault();
        if (g.tipLocked) g.tipLocked = false;
        const factor = e.deltaY > 0 ? 1.1 : 0.9;
        g.yScaleFactor = clamp(g.yScaleFactor * factor, 0.2, 5);
        drawMonthTrend(container, g);
      },
      { passive: false }
    );

    // Pinch（2本指）: 縦距離ベースでズーム
    container.addEventListener(
      "touchstart",
      (e) => {
        g.gesture.touching = true;
        g.suppressTooltip = true;
        if (e.touches.length === 2) {
          g.gesture.lastDistY = Math.abs(e.touches[0].clientY - e.touches[1].clientY);
        }
        // long-press 予約
        if (e.touches.length === 1) {
          const t = e.touches[0];
          g._lpTmr = window.setTimeout(() => {
            const hit = findNearestPoint(g, t.clientX, t.clientY);
            if (hit) {
              g.tipLocked = true;
              g.tooltip.show(`${hit.day}日\n${formatShort(hit.ms)}`, t.clientX, t.clientY);
            }
          }, 500);
        }
      },
      { passive: true }
    );
    container.addEventListener(
      "touchmove",
      (e) => {
        if (!g.gesture.touching) return;
        if (e.touches.length === 2) {
          const distY = Math.abs(e.touches[0].clientY - e.touches[1].clientY);
          const factor = distY / (g.gesture.lastDistY || distY);
          g.gesture.lastDistY = distY;
          g.yScaleFactor = clamp(g.yScaleFactor * factor, 0.2, 5);
          drawMonthTrend(container, g);
        }
      },
      { passive: true }
    );
    container.addEventListener(
      "touchend",
      (e) => {
        g.gesture.touching = false;
        window.clearTimeout(g._lpTmr);
        g._lpTmr = null;
        setTimeout(() => {
          g.suppressTooltip = false;
        }, 80);
        g.gesture.lastDistY = 0;

        // タップで最近傍点のツールチップ（固定でなければ）
        if (e.changedTouches.length === 1 && !g.tipLocked) {
          const t = e.changedTouches[0];
          const hit = findNearestPoint(g, t.clientX, t.clientY);
          if (hit) g.tooltip.show(`${hit.day}日\n${formatShort(hit.ms)}`, t.clientX, t.clientY);
          else g.tooltip.hide();
        }
      },
      { passive: true }
    );

    // Pointer-based drag scaling（PC/タッチ/ペン共通の縦ドラッグズーム）
    enableVerticalDragScaling(container, g, drawMonthTrend);

    // ホバー（PC）：軽いスロットル＋抑制考慮
    canvas.onmousemove = (e) => {
      const tNow = performance.now();
      if (tNow - g.lastHoverTs < g.hoverThrottleMs) return;
      g.lastHoverTs = tNow;
      if (g.suppressTooltip || !g.ctx || g.tipLocked) {
        if (!g.tipLocked) g.tooltip.hide();
        return;
      }
      const hit = findNearestPoint(g, e.clientX, e.clientY);
      if (hit) g.tooltip.show(`${hit.day}日\n${formatShort(hit.ms)}`, e.clientX, e.clientY);
      else g.tooltip.hide();
    };
    canvas.onmouseleave = () => {
      if (!g.tipLocked) g.tooltip.hide();
    };

    // クリックで固定/解除
    canvas.addEventListener("click", (e) => {
      const hit = findNearestPoint(g, e.clientX, e.clientY);
      if (hit) {
        g.tipLocked = true;
        g.tooltip.show(`${hit.day}日\n${formatShort(hit.ms)}`, e.clientX, e.clientY);
      } else {
        g.tipLocked = false;
        g.tooltip.hide();
      }
    });

    // リサイズ
    window.addEventListener(
      "resize",
      () => {
        g.isMobile = window.innerWidth <= 599;
        drawMonthTrend(container, g);
      },
      { passive: true }
    );
  }

  // データ更新
  const key = JSON.stringify(logs);
  if (key !== container._graph.cacheKey) {
    container._graph.cacheKey = key;
  }

  // 整形＋描画
  hydrateMonthTrend(container._graph, logs);
  drawMonthTrend(container, container._graph);
}

/* ===================== core: data ===================== */
function hydrateMonthTrend(g, logs) {
  const now = new Date();
  const year = now.getFullYear();
  const month = now.getMonth();
  const today = now.getDate();
  g.today = today;
  const ym = `${year}-${pad2(month + 1)}`;

  const normalizeDay = (d) => 10 * (d / today); // 今日=10
  const arr = [];
  let totalMs = 0,
    nonZeroCount = 0,
    maxRaw = 0;

  for (let d = 1; d <= today; d++) {
    const keyDay = `${ym}-${pad2(d)}`;
    const ms = logs[keyDay] || 0;
    if (ms > 0) {
      totalMs += ms;
      nonZeroCount++;
    }
    maxRaw = Math.max(maxRaw, ms);
    arr.push({ day: d, ms, xNorm: normalizeDay(d) });
  }
  g.data = arr;

  // 縦軸基準（最大値+2分、最低10分）
  const BUFFER_MS = 2 * 60 * 1000;
  const MIN_TOP = 10 * 60 * 1000;
  g.baseTop = Math.max(MIN_TOP, maxRaw + BUFFER_MS);

  // 説明（非ゼロ平均）
  const desc = $("#chart-desc-id");
  if (desc) {
    const avgMs = nonZeroCount ? totalMs / nonZeroCount : 0;
    desc.textContent = `今月の読書推移。平均 ${formatReadable(avgMs)}、最大 ${formatReadable(
      g.baseTop
    )}、${today}日分。横軸は現在=10で固定、縦軸は調整可能。`;
  }
}

/* ===================== utils ========================= */
// Short format: 1h15m / 8m / 30s
function formatShort(ms) {
  const s = Math.round(ms / 1000);
  const h = Math.floor(s / 3600);
  const m = Math.floor((s % 3600) / 60);
  const sec = s % 60;
  if (h > 0) return `${h}h${m}m`;
  if (m > 0) return `${m}m`;
  return `${sec}s`;
}

// Readable format: 1時間15分 / 8分 / 30秒
function formatReadable(ms) {
  const s = Math.round(ms / 1000);
  const h = Math.floor(s / 3600);
  const m = Math.floor((s % 3600) / 60);
  const sec = s % 60;
  const parts = [];
  if (h > 0) parts.push(`${h}時間`);
  if (m > 0) parts.push(`${m}分`);
  if (sec && parts.length === 0) parts.push(`${sec}秒`);
  return parts.join("") || "0分";
}

function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value));
}

/* ===================== tooltip ======================= */
function createTooltip(host) {
  const tip = document.createElement("div");
  tip.style.position = "absolute";
  tip.style.background = "var(--surface)";
  tip.style.color = "var(--text)";
  tip.style.border = "1px solid var(--border)";
  tip.style.borderRadius = "10px";
  tip.style.boxShadow = "var(--shadow-2)";
  tip.style.padding = "8px 10px";
  tip.style.font = "12px/1.5 ui-sans-serif, system-ui, -apple-system, Segoe UI, Roboto";
  tip.style.whiteSpace = "pre";
  tip.style.opacity = "0";
  tip.style.transition = "opacity .12s cubic-bezier(.2,.8,.2,1)";
  tip.style.pointerEvents = "none";
  document.body.appendChild(tip);

  let lastHide = 0;

  return {
    show(text, clientX, clientY) {
      tip.textContent = text;
      tip.style.left = `${clientX + 12}px`;
      tip.style.top = `${clientY + 12}px`;
      tip.style.opacity = "1";
    },
    hide() {
      const now = performance.now();
      if (now - lastHide < 80) return;
      lastHide = now;
      tip.style.opacity = "0";
    },
  };
}

/* ===================== core: draw ===================== */
function drawMonthTrend(container, g) {
  // 古いフォールバック除去
  const oldEmpty = container.querySelector(".empty");
  if (oldEmpty) oldEmpty.remove();

  // Canvas サイズ/DPR
  const DPR = window.devicePixelRatio || 1;
  const contW = Math.max(320, container.clientWidth);
  const contH = Math.max(280, container.clientHeight || 320);

  const sizeChanged = g.lastSize.w !== contW || g.lastSize.h !== contH || g.lastSize.dpr !== DPR;
  if (sizeChanged) {
    g.canvas.width = contW * DPR;
    g.canvas.height = contH * DPR;
    g.canvas.style.width = `${contW}px`;
    g.canvas.style.height = `${contH}px`;
    g.lastSize = { w: contW, h: contH, dpr: DPR };
    g.ctx = g.canvas.getContext("2d");
  }
  const ctx = g.ctx;
  if (!ctx) return;

  // 座標リセット＋DPR適用
  ctx.setTransform(1, 0, 0, 1, 0, 0);
  ctx.scale(DPR, DPR);
  ctx.clearRect(0, 0, contW, contH);

  // style tokens
  const cs = getComputedStyle(document.documentElement);
  const brand500 = cs.getPropertyValue("--brand-500").trim();
  const brand600 = cs.getPropertyValue("--brand-600").trim();
  const brand200 = cs.getPropertyValue("--brand-200").trim();
  const muted = cs.getPropertyValue("--muted").trim();
  const border = cs.getPropertyValue("--border").trim();
  const text = cs.getPropertyValue("--text").trim();
  const surface = cs.getPropertyValue("--surface").trim();

  // 背景
  ctx.fillStyle = surface;
  ctx.fillRect(0, 0, contW, contH);

  // paddings
  const PAD_L = g.isMobile ? 60 : 48;
  const PAD_R = g.isMobile ? 28 : 20;
  const PAD_T = g.isMobile ? 68 : 56;
  const PAD_B = g.isMobile ? 76 : 64;

  const innerW = contW - PAD_L - PAD_R;
  const innerH = contH - PAD_T - PAD_B;

  // 縦軸再設計: zeroYをbottomに置き、上向きスケール（0 at bottom, positive up）
  const zeroY = PAD_T + innerH; // Bottom zero

  // 座標変換
  const xToPx = (xNorm) => PAD_L + (xNorm / 10) * innerW;
  const topMax = g.baseTop * g.yScaleFactor;
  const yToPx = (ms) => zeroY - Math.min(innerH, (ms / topMax) * innerH); // 0 at bottom, max at top

  // 軸
  ctx.lineWidth = 1.5;
  ctx.strokeStyle = text;
  // X軸 (bottom)
  ctx.beginPath();
  ctx.moveTo(PAD_L, zeroY);
  ctx.lineTo(contW - PAD_R, zeroY);
  ctx.stroke();
  // Y軸 (left)
  ctx.beginPath();
  ctx.moveTo(PAD_L, PAD_T);
  ctx.lineTo(PAD_L, zeroY);
  ctx.stroke();

  // グリッド (Y: upper half emphasized, bottom to top)
  ctx.setLineDash([4, 4]);
  ctx.strokeStyle = border;
  ctx.lineWidth = 1;
  [0.25, 0.5, 0.75, 1.0].forEach((r) => {
    const gy = zeroY - r * innerH;
    ctx.beginPath();
    ctx.moveTo(PAD_L, gy);
    ctx.lineTo(contW - PAD_R, gy);
    ctx.stroke();
  });
  ctx.setLineDash([]);

  // Yラベル（topMax 連動、再設計: 0 at bottom, labels dynamic）
  ctx.fillStyle = muted;
  ctx.font = "12px sans-serif";
  ctx.textAlign = "right";
  ctx.textBaseline = "middle";
  [1.0, 0.75, 0.5, 0.25, 0.0].forEach((r) => {
    const label = formatShort(topMax * r);
    const ty = zeroY - r * innerH;
    const clampedY = Math.max(PAD_T + 10, Math.min(zeroY - 10, ty));
    ctx.fillText(label, PAD_L - 8, clampedY);
  });

  // Xラベル（今日まで均等、再設計: bottom label position）
  ctx.fillStyle = muted;
  ctx.textAlign = "center";
  ctx.textBaseline = "top";
  const labelCount = g.isMobile ? 5 : 8;
  const xLabelY = zeroY + 8; // Below zero line
  const today = g.data[g.data.length - 1]?.day || 1;
  for (let i = 0; i <= labelCount; i++) {
    const norm = i / labelCount;
    const day = Math.round(1 + norm * (today - 1));
    const x = PAD_L + norm * innerW;
    ctx.fillText(`日${day}`, x, xLabelY);
  }

  // 平均線（非ゼロ日、再設計: bottom from 0）
  const totalMs = g.data.reduce((s, d) => s + (d.ms > 0 ? d.ms : 0), 0);
  const nonZeroCount = g.data.reduce((s, d) => s + (d.ms > 0 ? 1 : 0), 0);
  const avgMs = nonZeroCount ? totalMs / nonZeroCount : 0;
  if (avgMs > 0) {
    const avgY = yToPx(avgMs);
    ctx.setLineDash([6, 6]);
  ctx.strokeStyle = muted;
  ctx.beginPath();
  ctx.moveTo(PAD_L, avgY);
  ctx.lineTo(contW - PAD_R, avgY);
  ctx.stroke();
  ctx.setLineDash([]);
  ctx.fillStyle = muted;
  ctx.textAlign = "right";
  ctx.textBaseline = "bottom";
  ctx.fillText(
    `平均 ${formatShort(avgMs)}`,
    contW - PAD_R - 4,
    Math.min(zeroY - 4, Math.max(PAD_T + 10, avgY - 4))
  );
  }

  // データ線（再設計: smooth tension for flat data visibility）
  if (g.data.length > 0) {
    ctx.strokeStyle = brand500;
    ctx.lineWidth = 2.5;
    ctx.beginPath();
    let x = xToPx(g.data[0].xNorm);
    let y = yToPx(g.data[0].ms);
    ctx.moveTo(x, y);
    for (let i = 1; i < g.data.length; i++) {
      const d = g.data[i];
      x = xToPx(d.xNorm);
      y = yToPx(d.ms);
      ctx.lineTo(x, y);
    }
    ctx.stroke();
  }

  // ポイント（非ゼロ強調、再設計: larger radius for flat visibility）
  ctx.fillStyle = brand600;
  for (const d of g.data) {
    if (d.ms <= 0) continue;
    const px = xToPx(d.xNorm);
    const py = yToPx(d.ms);
    ctx.beginPath();
    ctx.arc(px, py, 4, 0, Math.PI * 2); // Larger for visibility
    ctx.fill();

    if (d.day === g.today) {
      ctx.strokeStyle = brand200;
      ctx.lineWidth = 2;
      ctx.beginPath();
      ctx.arc(px, py, 7, 0, Math.PI * 2); // Slightly larger highlight
      ctx.stroke();
    }
  }

  // 空フォールバック
  const anyNonZero = g.data.some((d) => d.ms > 0);
  if (!anyNonZero) {
    const fallback = document.createElement("div");
    fallback.className = "empty";
    fallback.textContent = "今月はまだ読書記録がありません";
    Object.assign(fallback.style, {
      position: "absolute",
      inset: "0",
      display: "grid",
      placeItems: "center",
    });
    container.appendChild(fallback);
  }
}

/* ================ interactions: drag ================= */
function enableVerticalDragScaling(container, graph, drawFn) {
  const canvas = graph.canvas;
  let dragging = false;
  let startY = 0;
  let startScale = 1;

  // 200px drag = e^1 ~2.7x change (smooth)
  const PX_TO_EXP = 1 / 200;
  const MIN_SCALE = 0.2;
  const MAX_SCALE = 5;

  canvas.addEventListener("pointerdown", (e) => {
    if (e.pointerType === "mouse" && e.button !== 0) return;
    dragging = true;
    startY = e.clientY;
    startScale = graph.yScaleFactor;
    graph.suppressTooltip = true;
    canvas.setPointerCapture(e.pointerId);
  });

  canvas.addEventListener("pointermove", (e) => {
    if (!dragging) return;
    const dy = e.clientY - startY;
    const next = startScale * Math.exp(dy * PX_TO_EXP);
    graph.yScaleFactor = clamp(next, MIN_SCALE, MAX_SCALE);
    drawFn(container, graph);
  });

  canvas.addEventListener("pointerup", (e) => {
    if (!dragging) return;
    dragging = false;
    graph.suppressTooltip = false;
    canvas.releasePointerCapture(e.pointerId);

    const hit = findNearestPoint(graph, e.clientX, e.clientY);
    if (hit) {
      graph.tooltip.show(`${hit.day}日\n${formatShort(hit.ms)}`, e.clientX, e.clientY);
    } else {
      graph.tooltip.hide();
    }
  });

  canvas.addEventListener("pointercancel", () => {
    dragging = false;
    graph.suppressTooltip = false;
  });
}

/* ================ interactions: hit ================== */
function findNearestPoint(graph, clientX, clientY) {
  const rect = graph.canvas.getBoundingClientRect();
  const mx = clientX - rect.left;
  const my = clientY - rect.top;

  const padL = graph.isMobile ? 60 : 48;
  const innerW = rect.width - padL - (graph.isMobile ? 28 : 20);
  const innerH = rect.height - (graph.isMobile ? 68 : 56) - (graph.isMobile ? 76 : 64);
  const zeroY = rect.height - (graph.isMobile ? 76 : 64); // Re-designed bottom zero

  const topMax = graph.baseTop * graph.yScaleFactor;
  const yToPx = (ms) => zeroY - (Math.min(1, ms / topMax) * innerH);

  const today = graph.data[graph.data.length - 1]?.day || 1;
  const xToPx = (day) => padL + (day / today) * innerW;

  const hitRadius = graph.isMobile ? 18 : 10;
  let nearest = null;
  let minDist = Infinity;

  for (const d of graph.data) {
    const px = xToPx(d.day);
    const py = yToPx(d.ms);
    const dist = Math.hypot(mx - px, my - py);
    if (dist < minDist) {
      minDist = dist;
      nearest = d;
    }
  }
  return minDist <= hitRadius ? nearest : null;
}


  /* =========================================================
     Library
  ========================================================= */
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
    const container = $("#libraryShelf");
    if (!container) return;
    clear(container);

    const grouped = groupByWork(details);
    const q = ($("#filterText")?.value || "").trim().toLowerCase();
    const sortKey = $("#sortLibrary")?.value || "recent";

    let works = Object.values(grouped);
    if (q) works = works.filter(w => (w.workTitle || "").toLowerCase().includes(q));
    works.sort((a, b) => {
      if (sortKey === "total") return b.totalMs - a.totalMs;
      if (sortKey === "title") return (a.workTitle || "").localeCompare(b.workTitle || "");
      return (b.latestTs - a.latestTs);
    });

    const f = frag();
    if (!works.length) {
      const empty = document.createElement("div");
      empty.className = "empty";
      empty.textContent = q ? "検索結果がありません" : "表示できる作品がありません";
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
        const metaFrag = frag();
        [
          { label: "合計", value: fmt(w.totalMs) },
          { label: "最終", value: shortDate(w.latestTs) },
          { label: "件数", value: String(w.episodes.length) }
        ].forEach(mi => {
          const item = document.createElement("span");
          item.className = "meta-item";
          const lab = document.createElement("span"); lab.className = "label"; lab.textContent = mi.label;
          const val = document.createElement("span"); val.className = "value mono"; val.textContent = mi.value;
          item.append(lab, val);
          metaFrag.appendChild(item);
        });
        meta.appendChild(metaFrag);

        const actions = document.createElement("span");
        actions.className = "lib-actions";
        const openBtn = document.createElement("button");
        openBtn.className = "open-work small";
        openBtn.textContent = "作品ページへ";
        openBtn.addEventListener("click", (e) => {
          e.preventDefault(); e.stopPropagation();
          openWorkPage(detailsEl.dataset.workKey);
        });
        actions.append(openBtn);

        const list = document.createElement("div");
        list.className = "episodes";
        w.episodes
          .sort((a, b) => a.ts - b.ts)
          .forEach(ep => {
            const row = document.createElement("div"); row.className = "episode";
            const aEl = document.createElement("a");
            aEl.href = ep.url || "#"; aEl.target = "_blank"; aEl.rel = "noopener";
            aEl.textContent = ep.episodeTitle || "(no title)";
            const time = document.createElement("span"); time.className = "mono muted"; time.textContent = fmt(ep.ms || 0);
            const when = document.createElement("span"); when.className = "mono muted"; when.textContent = shortDate(ep.ts || 0);
            row.append(aEl, time, when);
            list.appendChild(row);
          });

        summary.append(leftTitle, meta, actions);
        detailsEl.append(summary, list);
        f.appendChild(detailsEl);
      });
    }
    container.appendChild(f);
  }

  /* =========================================================
     Work page
  ========================================================= */
  function openWorkPage(workKey) {
    const grouped = groupByWork(lastDetails);
    const w = grouped[workKey];
    if (!w) {
      showToast("作品データが見つかりません", 1600, "error");
      return;
    }
    const titleEl = $("#workPageTitle");
    const metaEl = $("#workMeta");
    if (titleEl) titleEl.textContent = w.workTitle || "(タイトルなし)";
    if (metaEl) metaEl.textContent = `サイト: ${w.site} / 合計: ${fmt(w.totalMs)} / 最終: ${shortDate(w.latestTs)} / 件数: ${w.episodes.length}`;

    // Quick stats
    updateWorkQuickStats(w);

    // Episodes
    const listEl = $("#workEpisodes");
    clear(listEl);
    const ef = frag();
    w.episodes.sort((a, b) => (b.ts || 0) - (a.ts || 0)).forEach(ep => {
      const row = document.createElement("div"); row.className = "episode";
      const aEl = document.createElement("a");
      aEl.href = ep.url || "#"; aEl.target = "_blank"; aEl.rel = "noopener";
      aEl.textContent = ep.episodeTitle || "(no title)";
      const time = document.createElement("span"); time.className = "mono muted"; time.textContent = fmt(ep.ms || 0);
      const when = document.createElement("span"); when.className = "mono muted"; when.textContent = shortDate(ep.ts || 0);
      row.append(aEl, time, when);
      ef.appendChild(row);
    });
    listEl.appendChild(ef);

    // Inner stats
    const totalsBySession = {};
    w.episodes.forEach(ep => {
      const sid = ep.sessionId || "no-session";
      totalsBySession[sid] = (totalsBySession[sid] || 0) + (ep.ms || 0);
    });
    const totalsArr = Object.values(totalsBySession);
    const cards = $("#workInnerStats");
    clear(cards);
    const cf = frag();
    [
      { label: "作品内合計", value: fmt(w.totalMs) },
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
      card.append(title, meta);
      cf.appendChild(card);
    });
    cards.appendChild(cf);

    switchView("work");
  }

  function updateWorkQuickStats(work) {
    const quickA = document.querySelector("#workInnerQuickA .value");
    const quickB = document.querySelector("#workInnerQuickB .value");
    if (!work || !Array.isArray(work.episodes)) return;
    const todayKey = new Date().toISOString().slice(0, 10);
    const todayTotal = work.episodes
      .filter(ep => ep.ts && new Date(ep.ts).toISOString().slice(0, 10) === todayKey)
      .reduce((sum, ep) => sum + (ep.ms || 0), 0);
    if (quickA) quickA.textContent = fmt(todayTotal);
    if (quickB) quickB.textContent = fmt(work.totalMs);
  }

  /* =========================================================
     Settings toggles
  ========================================================= */
  function renderToggles(cfg) {
    const div = $("#siteToggles");
    if (!div) return;
    clear(div);
    const sites = Object.keys(cfg).length ? Object.keys(cfg) : ["kakuyomu.jp", "syosetu.org", "pixiv.net", "syosetu.com"];
    const f = frag();
    sites.forEach(domain => {
      const label = document.createElement("label");
      Object.assign(label.style, { display: "inline-flex", alignItems: "center", gap: "8px" });
      const input = document.createElement("input");
      input.type = "checkbox"; input.checked = !!cfg[domain];
      input.addEventListener("change", (e) => {
        try {
          B?.runtime?.sendMessage?.(
            { type: "set-site-enable", domain, enabled: e.target.checked },
            (resp) => {
              if (B.runtime.lastError) {
                showToast("更新に失敗しました", 1600, "error");
                return;
              }
              showToast(
                resp && resp.ok
                  ? `${domain}: ${e.target.checked ? "有効化" : "無効化"}`
                  : "更新に失敗しました",
                1600,
                resp?.ok ? (e.target.checked ? "success" : "warn") : "error"
              );
            }
          );
        } catch (err) {
          console.error("set-site-enable failed:", err);
          showToast("更新に失敗しました", 1600, "error");
        }
      });
      const span = document.createElement("span");
      span.textContent = `${domain}`;
      label.append(input, span);
      f.appendChild(label);
    });
    div.appendChild(f);
  }

  /* =========================================================
     Composite render
  ========================================================= */
  function renderAll(changed = {}) {
    if (changed.live) renderHomeSummary(lastLive, lastLogs);
    if (changed.details) renderRecentWorks(lastDetails);
    if (changed.logs) renderMonthTrendGraph(lastLogs);
    if (changed.details) renderLibrary(lastDetails);
    if (changed.siteEnable) renderToggles(lastSiteEnable);
  }

  /* =========================================================
     Load & Runtime messaging
  ========================================================= */
  function load() {
    // Export snapshot
    try {
      B?.runtime?.sendMessage?.({ type: "export-store" }, (resp) => {
        if (B?.runtime?.lastError) {
          showToast("データロードに失敗しました", 1600, "error");
          return;
        }
        const snap = (resp && resp.ok && resp.snapshot) ? resp.snapshot : {};
        const changedLogs = JSON.stringify(lastLogs) !== JSON.stringify(snap[KEY_LOG] || {});
        const changedDetails = JSON.stringify(lastDetails) !== JSON.stringify(snap[KEY_DETAILS] || {});
        lastLogs = snap[KEY_LOG] || {};
        lastDetails = snap[KEY_DETAILS] || {};
        renderAll({ logs: changedLogs, details: changedDetails });
      });
    } catch (e) {
      console.error("export-store failed:", e);
      showToast("データロードに失敗しました", 1600, "error");
    }

    // Live stats
    try {
      B?.runtime?.sendMessage?.({ type: "get-stats" }, (live) => {
        if (B?.runtime?.lastError) return;
        const changed = (lastLive.total !== (live?.total ?? lastLive.total)) || (lastLive.daily !== (live?.daily ?? lastLive.daily));
        lastLive = (live && typeof live === "object") ? live : { total: 0, daily: 0 };
        if (changed) renderAll({ live: true });
      });
    } catch (e) {
      console.error("get-stats failed:", e);
    }

    // Site enable
    try {
      B?.runtime?.sendMessage?.({ type: "get-site-enable" }, (cfg) => {
        if (B?.runtime?.lastError) return;
        const changed = JSON.stringify(lastSiteEnable) !== JSON.stringify(cfg || {});
        lastSiteEnable = cfg || {};
        if (changed) renderAll({ siteEnable: true });
      });
    } catch (e) {
      console.error("get-site-enable failed:", e);
    }
  }

  try {
    B?.runtime?.onMessage?.addListener((msg) => {
      if (!msg || !msg.type) return;
      if (msg.type === "live-update" && msg.payload) {
        const changed = (lastLive.total !== (msg.payload.total ?? lastLive.total)) || (lastLive.daily !== (msg.payload.daily ?? lastLive.daily));
        lastLive = {
          total: msg.payload.total ?? lastLive.total,
          daily: msg.payload.daily ?? lastLive.daily,
        };
        if (changed) renderAll({ live: true });
      } else if (msg.type === "export-store") {
        const snap = (msg.snapshot || {});
        const changedLogs = JSON.stringify(lastLogs) !== JSON.stringify(snap[KEY_LOG] || lastLogs);
        const changedDetails = JSON.stringify(lastDetails) !== JSON.stringify(snap[KEY_DETAILS] || lastDetails);
        lastLogs = snap[KEY_LOG] || lastLogs;
        lastDetails = snap[KEY_DETAILS] || lastDetails;
        renderAll({ logs: changedLogs, details: changedDetails });
      } else if (msg.type === "import-store") {
        showToast("データをインポートしました", 1600, "success");
        load();
      }
    });
  } catch (e) {
    console.error("runtime.onMessage failed:", e);
  }

  /* =========================================================
     Actions
  ========================================================= */
  $("#resetToday")?.addEventListener("click", () => {
    if (!confirm("今日のログを消去します。取り消し不可です。よろしいですか？")) return;
    try {
      B?.runtime?.sendMessage?.({ type: "reset-today" }, (resp) => {
        if (B?.runtime?.lastError || !(resp && resp.ok)) showToast("reset-today に失敗しました", 1600, "error");
        else showToast("今日のログをリセットしました", 1600, "warn");
        load();
      });
    } catch (e) {
      console.error("reset-today failed:", e);
      showToast("リセットに失敗しました", 1600, "error");
      load();
    }
  });
  $("#resetAll")?.addEventListener("click", () => {
    if (!confirm("全てのログを消去します。取り消し不可です。よろしいですか？")) return;
    try {
      B?.runtime?.sendMessage?.({ type: "reset-all" }, (resp) => {
        if (B?.runtime?.lastError || !(resp && resp.ok)) showToast("reset-all に失敗しました", 1600, "error");
        else showToast("全ログを削除しました", 1600, "error");
        load();
      });
    } catch (e) {
      console.error("reset-all failed:", e);
      showToast("削除に失敗しました", 1600, "error");
      load();
    }
  });
  $("#backToLibrary")?.addEventListener("click", () => switchView("library"));

  $("#filterText")?.addEventListener("input", () => renderLibrary(lastDetails));
  $("#sortLibrary")?.addEventListener("change", () => renderLibrary(lastDetails));

  /* =========================================================
     Init
  ========================================================= */
  document.addEventListener("DOMContentLoaded", () => {
    bindTheme();
    bindTabs();
    switchView("home");
    load();
    renderMonthTrendGraph(lastLogs);
  });
})();
