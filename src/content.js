"use strict";
/* content.js
- 500ms heartbeat: visibilityState + lastInteraction
- スクロール/入力アクティビティを500msスロットルで送信
- run_at: document_idleでも即時heartbeatを1回送信（初動取りこぼし軽減）
- hashchange フォールバック
- Android のときだけ beforeunload を使う（PC は pagehide）
- タイトルには関与しない（title は background の tabs.get で一元管理）
*/

(() => {
  const B = (window.browser || window.chrome);
  const HEARTBEAT_INTERVAL_MS = 500;
  const THROTTLE_MS = 500;
  const IS_ANDROID = /Android/i.test(navigator.userAgent);

  let lastActivity = Date.now();
  let hbTimer = null;

  function sendMessageSafe(msg) {
    try {
      const p = B?.runtime?.sendMessage?.(msg);
      if (p && typeof p.then === "function") return p.catch(() => {});
    } catch {}
    return Promise.resolve();
  }

  function onActivity(ev) {
    const now = Date.now();
    if (now - lastActivity > THROTTLE_MS) {
      lastActivity = now;
      sendMessageSafe({ type: "scroll-activity", evType: ev?.type || "unknown", ts: now });
    }
  }

  function getVisibilityState() {
    if (typeof document.visibilityState === "string") return document.visibilityState;
    const hidden = (typeof document.hidden === "boolean" ? document.hidden : (document.mozHidden === true));
    return hidden ? "hidden" : "visible";
  }

  function sendVisibilityHeartbeat() {
    const v = getVisibilityState();
    sendMessageSafe({
      type: "heartbeat",
      visibilityState: v,
      lastInteraction: lastActivity || Date.now()
    });
  }

  function startHeartbeatLoop() {
    stopHeartbeatLoop();
    hbTimer = setInterval(sendVisibilityHeartbeat, HEARTBEAT_INTERVAL_MS);
  }
  function stopHeartbeatLoop() {
    if (hbTimer) { clearInterval(hbTimer); hbTimer = null; }
  }

  ["scroll", "wheel", "touchmove", "keydown", "mousedown", "touchstart"].forEach(ev =>
    window.addEventListener(ev, onActivity, { passive: true })
  );

  document.addEventListener("visibilitychange", sendVisibilityHeartbeat);

  const onHashChange = () => {
    sendMessageSafe({ type: "hash-change", url: location.href });
    sendVisibilityHeartbeat();
  };
  window.addEventListener("hashchange", onHashChange);

  function cleanup() {
    // 可能なら最後の heartbeat を送る（呼ばれないケースもある）
    sendVisibilityHeartbeat();
    stopHeartbeatLoop();
    ["scroll", "wheel", "touchmove", "keydown", "mousedown", "touchstart"].forEach(ev =>
      window.removeEventListener(ev, onActivity)
    );
    document.removeEventListener("visibilitychange", sendVisibilityHeartbeat);
    window.removeEventListener("hashchange", onHashChange);
  }

  // Android のときだけ beforeunload を使い、PC は pagehide を使う
  if (IS_ANDROID) {
    window.addEventListener("beforeunload", cleanup);
  } else {
    window.addEventListener("pagehide", cleanup);
  }

  function boot() {
    // 初動取りこぼし軽減
    sendVisibilityHeartbeat();
    if (document.readyState === "loading") {
      document.addEventListener("DOMContentLoaded", () => {
        requestAnimationFrame(() => {
          sendVisibilityHeartbeat();
          startHeartbeatLoop();
        });
      }, { once: true });
    } else {
      requestAnimationFrame(() => {
        sendVisibilityHeartbeat();
        startHeartbeatLoop();
      });
    }
  }

  boot();
})();
