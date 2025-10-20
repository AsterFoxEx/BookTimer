"use strict";
/* content.js
- 500ms heartbeat: visibilityState + lastInteraction
- 操作検知: 500msスロットル、focus/touchstart/click は1秒に1回フォールバック心拍
- run_at: document_startで早めに注入
- hashchange/popstate フォールバック（SPA/fragment）
- Android のときだけ beforeunload（PC は pagehide）
- URL/タイトル送信削除: 絞り込み
*/

(() => {
  const B = browser; // Firefox は browser が標準
  const HEARTBEAT_INTERVAL_MS = 500;
  const THROTTLE_MS = 500;
  const FALLBACK_INTERVAL_MS = 1000;
  const IS_ANDROID = /Android/i.test(navigator.userAgent);

  let lastActivity = Date.now();
  let lastFallbackSent = 0;
  let hbTimer = null;

  function sendMessageSafe(msg) {
    console.log("Content: Sending message:", msg); // デバッグログ
    try {
      const p = B?.runtime?.sendMessage?.(msg);
      if (p && typeof p.then === "function") return p.catch(err => { console.error("Content: Send failed:", err); });
    } catch (err) { console.error("Content: Send error:", err); }
    return Promise.resolve();
  }

  function getVisibilityState() {
    if (typeof document.visibilityState === "string") return document.visibilityState;
    const hidden = (typeof document.hidden === "boolean" ? document.hidden : (document.mozHidden === true));
    return hidden ? "hidden" : "visible";
  }

  function sendVisibilityHeartbeat() {
    const v = getVisibilityState();
    console.log("Content: Sending heartbeat, visibility:", v); // デバッグログ
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
    console.log(`Content: Activity detected: type=${type}, ts=${now}, visibility=${v}`); // デバッグログ
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
    "mousemove", "mouseenter", "mouseleave",  // マウス強化
    "touchstart", "touchmove", "touchend", "touchcancel",
    "pointerdown", "pointermove", "pointerup",
    "scroll", "wheel",
    "keydown", "keyup",
    "input", "focus", "change", "select"  // 追加イベント
  ];

  // イベントリスナー: window + document全体（captureでpixiv divスクロール捕捉）
  activityEvents.forEach(ev => {
    window.addEventListener(ev, onActivity, { passive: true });
    document.addEventListener(ev, onActivity, { passive: true, capture: true });
  });

  document.addEventListener("visibilitychange", sendVisibilityHeartbeat);
  // pixiv特化: 本文divの動的スクロール監視
  const observer = new MutationObserver(() => {
    const novelBody = document.querySelector('[data-episode-body], .novel-text, article, [class*="novel"]');  // pixivセレクタ（調整可）
    if (novelBody && !novelBody.hasAttribute('data-activity-listener')) {
      novelBody.addEventListener("scroll", onActivity, { passive: true });
      novelBody.setAttribute('data-activity-listener', 'true');
      console.log("Content: Attached scroll to novel body");
    }
  });
  observer.observe(document.body, { childList: true, subtree: true });

  function cleanup() {
    sendVisibilityHeartbeat();
    stopHeartbeatLoop();
    activityEvents.forEach(ev => {
      window.removeEventListener(ev, onActivity);
      document.removeEventListener(ev, onActivity);
    });
    document.removeEventListener("visibilitychange", sendVisibilityHeartbeat);
    observer.disconnect();
  }

  if (IS_ANDROID) {
    window.addEventListener("beforeunload", cleanup);
  } else {
    window.addEventListener("pagehide", cleanup);
  }

  function boot() {
    console.log("=== CONTENT SCRIPT LOADED on", location.href, "==="); // 注入確認
    console.log("Content: Booting...");
    sendVisibilityHeartbeat();  // 即時
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