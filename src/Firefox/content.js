"use strict";
/*
content.js
- 500ms heartbeat: visibilityState + lastInteraction
- 操作検知: 500msスロットル、focus/touchstart/click は1秒に1回フォールバック心拍
- run_at: document_startで早めに注入（manifestで設定）
- hashchange/popstate は background 側 WebNavigation で捕捉済み（ここでは送らない）
- Android のときだけ beforeunload（PC は pagehide）
*/

(() => {
  const B = (typeof browser !== "undefined") ? browser : chrome;
  const HEARTBEAT_INTERVAL_MS = 500;
  const THROTTLE_MS = 500;
  const FALLBACK_INTERVAL_MS = 1000;
  const IS_ANDROID = /Android/i.test(navigator.userAgent);

  let lastActivity = Date.now();
  let lastFallbackSent = 0;
  let hbTimer = null;

  function sendMessageSafe(msg) {
    try {
      const p = B?.runtime?.sendMessage?.(msg);
      if (p && typeof p.then === "function") return p.catch(() => {});
    } catch {}
    return Promise.resolve();
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

  function onActivity(ev) {
    const now = Date.now();
    const type = ev?.type || "unknown";
    const v = getVisibilityState();

    if (now - lastActivity > THROTTLE_MS) {
      lastActivity = now;
      sendMessageSafe({
        type: "scroll-activity",
        evType: type,
        ts: now,
        visibilityState: v
      });
    }

    if ((type === "focus" || type === "click" || type === "touchstart") && (now - lastFallbackSent > FALLBACK_INTERVAL_MS)) {
      lastFallbackSent = now;
      sendVisibilityHeartbeat();
    }
  }

  function startHeartbeatLoop() {
    stopHeartbeatLoop();
    hbTimer = setInterval(sendVisibilityHeartbeat, HEARTBEAT_INTERVAL_MS);
  }
  function stopHeartbeatLoop() {
    if (hbTimer) { clearInterval(hbTimer); hbTimer = null; }
  }

  const activityEvents = [
    "click", "mousedown", "mouseup",
    "mousemove", "mouseenter", "mouseleave",
    "touchstart", "touchmove", "touchend", "touchcancel",
    "pointerdown", "pointermove", "pointerup",
    "scroll", "wheel",
    "keydown", "keyup",
    "input", "focus", "change", "select"
  ];

  // window + document に広く張る（capture=trueで深い階層でも拾う）
  activityEvents.forEach(ev => {
    window.addEventListener(ev, onActivity, { passive: true });
    document.addEventListener(ev, onActivity, { passive: true, capture: true });
  });

  document.addEventListener("visibilitychange", sendVisibilityHeartbeat);

  function cleanup() {
    sendVisibilityHeartbeat();
    stopHeartbeatLoop();
    activityEvents.forEach(ev => {
      window.removeEventListener(ev, onActivity);
      document.removeEventListener(ev, onActivity);
    });
    document.removeEventListener("visibilitychange", sendVisibilityHeartbeat);
  }

  if (IS_ANDROID) {
    window.addEventListener("beforeunload", cleanup);
  } else {
    window.addEventListener("pagehide", cleanup);
  }

  function boot() {
    // 即時ハートビートで background に存在を知らせる
    sendVisibilityHeartbeat();
    setTimeout(sendVisibilityHeartbeat, 100);
    setTimeout(sendVisibilityHeartbeat, 500);

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
