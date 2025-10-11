"use strict";
/* popup.js
- ライブ統計取得・表示（累計、今日の確定、直近5件）
- resetToday / resetAll ボタン操作
- live-update メッセージ購読でUI即時更新
- Firefox MV2 前提
*/

(() => {
  const B = (window.browser ?? window.chrome);

  // DOM参照
  const $total = document.getElementById("totalTime");
  const $today = document.getElementById("todayTime");
  const $list = document.getElementById("titleList");
  const $btnResetToday = document.getElementById("resetToday");
  const $btnResetAll = document.getElementById("resetAll");

  // ユーティリティ: ms→H:MM:SS
  function msToHms(ms) {
    const sec = Math.floor(ms / 1000);
    const h = Math.floor(sec / 3600);
    const m = Math.floor((sec % 3600) / 60);
    const s = sec % 60;
    const pad = (n) => String(n).padStart(2, "0");
    return `${h}:${pad(m)}:${pad(s)}`;
  }

  // DOM更新
  function renderStats(payload) {
    if (!payload) return;
    const { total = 0, daily = 0, recent = [] } = payload;

    if ($total) $total.textContent = msToHms(total);
    if ($today) $today.textContent = msToHms(daily);

    if ($list) {
      $list.innerHTML = "";
      if (Array.isArray(recent) && recent.length) {
        for (const item of recent) {
          const li = document.createElement("li");
          const site = item.site || "";
          const work = item.workTitle || "";
          const ep = item.episodeTitle || "";
          const page = item.page ? ` p.${item.page}` : "";
          const time = msToHms(item.ms || 0);
          li.textContent = `[${site}] ${work}${ep ? " / " + ep : ""} (${time}${page})`;
          $list.appendChild(li);
        }
      } else {
        const li = document.createElement("li");
        li.textContent = "履歴がありません";
        $list.appendChild(li);
      }
    }
  }

  // 背景へ要求
  function sendMessageSafe(msg) {
    try {
      const p = B?.runtime?.sendMessage?.(msg);
      if (p && typeof p.then === "function") {
        return p.catch(() => undefined);
      }
    } catch {}
    return Promise.resolve(undefined);
  }

  async function loadStats() {
    try {
      const payload = await sendMessageSafe({ type: "get-stats" });
      if (payload) renderStats(payload);
    } catch {}
  }

  async function resetToday() {
    $btnResetToday.disabled = true;
    try {
      await sendMessageSafe({ type: "reset-today" });
      await loadStats();
    } finally {
      $btnResetToday.disabled = false;
    }
  }

  async function resetAll() {
    const ok = window.confirm("総計をリセットします。よろしいですか？");
    if (!ok) return;
    $btnResetAll.disabled = true;
    try {
      await sendMessageSafe({ type: "reset-all" });
      await loadStats();
    } finally {
      $btnResetAll.disabled = false;
    }
  }

  // ライブ更新購読
  function subscribeLiveUpdates() {
    try {
      B?.runtime?.onMessage?.addListener?.((msg) => {
        if (msg?.type === "live-update") {
          renderStats(msg.payload);
        }
        return false;
      });
    } catch {}
  }

  // 初期化
  function boot() {
    if ($btnResetToday) $btnResetToday.addEventListener("click", resetToday);
    if ($btnResetAll) $btnResetAll.addEventListener("click", resetAll);
    loadStats();
    subscribeLiveUpdates();
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", boot, { once: true });
  } else {
    boot();
  }
})();
