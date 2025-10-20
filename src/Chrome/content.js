"use strict";
/**
 * ブックタイマー Next (Content Script)
 * - 500ms 心拍（visibility + lastInteraction）
 * - 操作検知: 500ms スロットル
 * - MutationObserver 非使用：本文領域のスクロール監視は 1 秒ごとの軽量スキャンで一度だけリスナ付与
 * - run_at: document_start
 */

(() => {
  const HEARTBEAT_INTERVAL_MS = 500;
  const THROTTLE_MS = 500;
  const CONTAINER_SCAN_MS = 1000;

  let lastActivity = Date.now();
  let hbTimer = null;
  function sendMessageSafe(msg) {
    try {
      // callback を渡さないことで Unchecked runtime.lastError を回避
      chrome.runtime.sendMessage(msg);
    } catch {}
  }

  function getVisibilityState() {
    if (typeof document.visibilityState === "string") return document.visibilityState;
    const hidden = typeof document.hidden === "boolean" ? document.hidden : false;
    return hidden ? "hidden" : "visible";
  }

  function sendVisibilityHeartbeat() {
    const v = getVisibilityState();
    sendMessageSafe({ type: "heartbeat", visibilityState: v, lastInteraction: lastActivity || Date.now() });
  }

  // 操作検知（500ms スロットル）
  function onActivity(ev) {
    const now = Date.now();
    if (now - lastActivity < THROTTLE_MS) return;
    lastActivity = now;
    sendMessageSafe({ type: "scroll-activity", evType: ev?.type || "activity", ts: now, visibilityState: getVisibilityState() });
  }


  function startHeartbeatLoop() {
    stopHeartbeatLoop();
    hbTimer = setInterval(sendVisibilityHeartbeat, HEARTBEAT_INTERVAL_MS);
  }
  function stopHeartbeatLoop() {
    if (hbTimer) {
      clearInterval(hbTimer);
      hbTimer = null;
    }
  }


  const activityEvents = [
    "click",
    "mousedown",
    "mouseup",
    "mousemove",
    "mouseenter",
    "mouseleave",
    "touchstart",
    "touchmove",
    "touchend",
    "touchcancel",
    "pointerdown",
    "pointermove",
    "pointerup",
    "wheel",
    "keydown",
    "keyup",
    "input",
    "focus",
    "change",
    "select"
  ];
  activityEvents.forEach((ev) => {
    window.addEventListener(ev, onActivity, { passive: true });
    document.addEventListener(ev, onActivity, { passive: true, capture: true });
  });
  document.addEventListener("visibilitychange", sendVisibilityHeartbeat);

  function cleanup() {
    try {
      sendVisibilityHeartbeat();
      stopHeartbeatLoop();
      activityEvents.forEach((ev) => {
        window.removeEventListener(ev, onActivity);
        document.removeEventListener(ev, onActivity, { capture: true });
      });
      document.removeEventListener("visibilitychange", sendVisibilityHeartbeat);
    } catch {}
  }
  window.addEventListener("pagehide", cleanup);

  function boot() {
    sendVisibilityHeartbeat();
    setTimeout(sendVisibilityHeartbeat, 100);
    setTimeout(sendVisibilityHeartbeat, 500);
    if (document.readyState === "loading") {
      document.addEventListener(
        "DOMContentLoaded",
        () => {
          requestAnimationFrame(() => {
            sendVisibilityHeartbeat();
            startHeartbeatLoop();
          });
        },
        { once: true }
      );
    } else {
      requestAnimationFrame(() => {
        sendVisibilityHeartbeat();
        startHeartbeatLoop();

      });
    }
  }
  boot();
})();