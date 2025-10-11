"use strict";
/*
- 目的
  1) URL変化（tabs.onUpdated / webNavigation.* / hash-change）直後だけ、startDebounce と visStable を除外する
     → stop(NAVIGATION) 直後の再開ブロックを解消し、本文→本文の連続遷移を滑らかにする
  2) URL変化すべての経路で restartPolling(tabId) を必ず呼ぶ
     → タイトル確定の取りこぼしを潰す（sigにURLを含める）
  3) 非同期競合を避けるため、タブ単位の直列キュー withTabQueue を維持
  4) details/pending に url を常に含める
     → 読み開始時の URL を contentUrlAtStart に保持し、commit/pending に記録
*/

const B = (globalThis.browser ?? globalThis.chrome);

/* ===== Utilities ===== */
function isPromise(x) { return !!x && typeof x.then === "function"; }
function hasOwn(o, k) { return Object.prototype.hasOwnProperty.call(o, k); }
function nowMs() { return Date.now(); }
function cryptoId() { return `s_${Date.now().toString(16)}_${Math.random().toString(16).slice(2)}`; }
function dayKey(ts) { const d = new Date(ts); return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}-${String(d.getDate()).padStart(2, "0")}`; }
function cleanWhitespace(s) { return (s || "").replace(/\s+/g, " ").trim(); }

/* ===== Async: per-tab serialized queue ===== */
const tabQueues = new Map(); // tabId -> Promise chain
function withTabQueue(tabId, fn) {
  const prev = tabQueues.get(tabId) || Promise.resolve();
  const next = prev.then(() => Promise.resolve().then(fn)).catch(() => {});
  tabQueues.set(tabId, next);
  return next;
}

/* ===== Logging ===== */
const LOG_LEVEL = 1;
let DEBUG_ONLY_CANDIDATE_LOGS = true;
let DIAG_MODE = false;
const telemetry = []; const TELEMETRY_MAX = 1500;
function tpush(ev) { telemetry.push({ ts: nowMs(), ...ev }); if (telemetry.length > TELEMETRY_MAX) telemetry.splice(0, telemetry.length - TELEMETRY_MAX); }
function log(level, tag, data, stForFilter, force = false) {
  if (level > LOG_LEVEL) return;
  if (!force && stForFilter) {
    const isCand = !!stForFilter.isCandidate;
    const isRead = !!stForFilter.reading;
    const isCore = (tag.startsWith("session.") || tag.startsWith("commit.") || tag.startsWith("reading."));
    if (DEBUG_ONLY_CANDIDATE_LOGS && !DIAG_MODE) {
      if (!isCand) return;
      if (!isRead && !isCore) return;
    }
  }
  try { console.log(`[${new Date().toISOString()}] ${tag}`, data ?? ""); } catch {}
  tpush({ level, tag, data });
}

/* ===== Environment & constants ===== */
const HAS_WINDOWS_API = !!(B?.windows?.onFocusChanged);
const SITE = { KAKUYOMU: "kakuyomu.jp", HAMELN: "syosetu.org", PIXIV: "pixiv.net", NAROU: "syosetu.com" };

/* ===== Storage keys & settings ===== */
const KEY_TOTAL = "rt_total_ms";
const KEY_DAILY = "rt_daily_ms";
const KEY_LOG = "rt_daily_log";
const KEY_DETAILS = "rt_details";
const KEY_SITE_ENABLE = "rt_site_enable";
const KEY_SETTINGS = "rt_settings";
const KEY_VERSION = "rt_version";

const DEFAULT_SETTINGS = {
  version: 13,
  minSessionMs: 5000,
  mergeWindowMs: 30000,
  pendingTimeoutMs: 60000,
  startDebounceMs: 1000,
  stopDebounceMs: 1200,
  startGraceMs: 10000,
  stopDebounceExtraDuringGraceMs: 800,
  idleShortMs: 5000,
  idleLongMs: 10000,
  idleDiscardMs: 20 * 60 * 1000,
  realtimeFlushMs: 5000,
  livePushIntervalMs: 2000,
  livePushMinGapMs: 500,
  visibilityStabilizeMs: 500,
  androidMode: !HAS_WINDOWS_API,
  heartbeatIntervalMs: 500,
  heartbeatMissLimit: 3,
  heartbeatMissWindowMs: 1500,
  watchdogCheckIntervalMs: 1000,
  navDedupWindowMs: 300,
  debugOnlyCandidateLogs: true,
  diagMode: false,
  pollingIntervalMs: 150,
  narouRateWindowMs: 60_000,
  narouRateMaxPerWindow: 20,
  narouCacheTtlMs: 24 * 60 * 60 * 1000,
  hostOverrides: {
    "pixiv.net": { visibilityStabilizeMs: 500, navDedupWindowMs: 300 },
    "syosetu.com": { visibilityStabilizeMs: 500, navDedupWindowMs: 300 },
    "kakuyomu.jp": { visibilityStabilizeMs: 500, navDedupWindowMs: 300 },
    "syosetu.org": { visibilityStabilizeMs: 500, navDedupWindowMs: 300 }
  }
};
let SETTINGS = { ...DEFAULT_SETTINGS };

/* ===== Storage helpers ===== */
function storageGet(keys) {
  return new Promise(res => {
    try {
      const p = B?.storage?.local?.get?.(keys);
      if (isPromise(p)) p.then(v => res(v || {})).catch(() => res({}));
      else B.storage.local.get(keys, o => res(o || {}));
    } catch { res({}); }
  });
}
const writeQ = []; let writeBusy = false;
function mergeObject(target, patch) {
  for (const k of Object.keys(patch)) {
    const v = patch[k];
    if (Array.isArray(v)) target[k] = Array.isArray(target[k]) ? target[k].concat(v) : v.slice();
    else if (typeof v === "object" && v && typeof target[k] === "object" && target[k]) target[k] = { ...target[k], ...v };
    else target[k] = v;
  }
}
function storageSetQueued(obj) { writeQ.push(obj); processWriteQ(); }
async function processWriteQ() {
  if (writeBusy || writeQ.length === 0) return;
  writeBusy = true;
  try {
    const batch = {}; while (writeQ.length) mergeObject(batch, writeQ.shift());
    await new Promise(r => {
      try {
        const p = B?.storage?.local?.set?.(batch);
        if (isPromise(p)) p.then(() => r()).catch(() => r());
        else B.storage.local.set(batch, () => r());
      } catch { r(); }
    });
  } finally {
    writeBusy = false;
    if (writeQ.length) processWriteQ();
  }
}
async function getLocal(key, def) { const o = await storageGet([key]); return hasOwn(o, key) ? o[key] : def; }

/* ===== Settings load/apply ===== */
let storageReady = initStorage();
async function initStorage() {
  const init = {
    [KEY_TOTAL]: 0, [KEY_DAILY]: 0, [KEY_LOG]: {}, [KEY_DETAILS]: {},
    [KEY_SITE_ENABLE]: { [SITE.KAKUYOMU]: true, [SITE.HAMELN]: true, [SITE.PIXIV]: true, [SITE.NAROU]: true },
    [KEY_SETTINGS]: DEFAULT_SETTINGS,
    [KEY_VERSION]: DEFAULT_SETTINGS.version
  };
  const cur = await storageGet(Object.keys(init));
  const put = {}; for (const k of Object.keys(init)) if (cur[k] === undefined) put[k] = init[k];
  if (Object.keys(put).length) storageSetQueued(put);
  await loadSettingsIntoMemory();
}
async function loadSettingsIntoMemory() {
  try {
    const o = await storageGet([KEY_SETTINGS]);
    const cfg = o[KEY_SETTINGS];
    SETTINGS = (cfg && typeof cfg === "object")
      ? (cfg.version && cfg.version >= DEFAULT_SETTINGS.version
          ? { ...DEFAULT_SETTINGS, ...cfg }
          : { ...DEFAULT_SETTINGS, ...cfg, version: DEFAULT_SETTINGS.version })
      : { ...DEFAULT_SETTINGS };
    DEBUG_ONLY_CANDIDATE_LOGS = !!SETTINGS.debugOnlyCandidateLogs;
    DIAG_MODE = !!SETTINGS.diagMode;
    storageSetQueued({ [KEY_SETTINGS]: SETTINGS });
    applySettings();
    console.log("settings.loaded", SETTINGS);
  } catch {
    SETTINGS = { ...DEFAULT_SETTINGS };
    applySettings();
  }
}
function applySettings() { setupAlarms(); setupIntervals(); setupWatchdog(); }

/* ===== Domain & candidate helpers ===== */
function getDomain(url) {
  try {
    const h = new URL(url).hostname;
    if (h.endsWith("kakuyomu.jp")) return SITE.KAKUYOMU;
    if (h.endsWith("syosetu.org")) return SITE.HAMELN;
    if (h.endsWith("pixiv.net")) return SITE.PIXIV;
    if (h.endsWith("syosetu.com")) return SITE.NAROU;
    return null;
  } catch { return null; }
}
function isInternal(url) { return typeof url === "string" && (url.startsWith("chrome-extension://") || url.startsWith("moz-extension://")); }
function isCandidateUrl(url) {
  if (isInternal(url)) return false;
  const domain = getDomain(url); if (!domain) return false;
  try {
    const u = new URL(url);
    if (domain === SITE.KAKUYOMU) return /^\/works\/\d+\/episodes\/\d+\/?$/i.test(u.pathname) || /^\/works\/\d+\/?$/i.test(u.pathname);
    if (domain === SITE.HAMELN) return /^\/novel\/\d+(?:\/\d+\.html|\/?)$/i.test(u.pathname);
    if (domain === SITE.PIXIV) return u.pathname === "/novel/show.php" && u.searchParams.has("id");
    if (domain === SITE.NAROU) return /^\/n[0-9a-z]+\/(?:\d+\/)?$/i.test(u.pathname);
    return false;
  } catch { return false; }
}
function samePathIgnoringHash(aUrl, bUrl) {
  try { const a = new URL(aUrl), b = new URL(bUrl); return a.origin === b.origin && a.pathname === b.pathname && a.search === b.search; }
  catch { return false; }
}
function samePixivWorkIgnoringHash(aUrl, bUrl) {
  try {
    const a = new URL(aUrl), b = new URL(bUrl);
    return getDomain(aUrl) === SITE.PIXIV && getDomain(bUrl) === SITE.PIXIV && a.searchParams.get("id") === b.searchParams.get("id");
  } catch { return false; }
}
function isTransientUrl(url) { try { const u = new URL(url); return u.protocol === "about:" || u.protocol === "data:"; } catch { return false; } }

/* ===== Narou API (rate-limited queue) ===== */
const narouApi = (() => {
  const cache = new Map(); const queue = [];
  let processing = false; let bucket = { ts: 0, count: 0 };
  function refillWindow() {
    const now = nowMs();
    if (now - bucket.ts > SETTINGS.narouRateWindowMs) { bucket.ts = now; bucket.count = 0; }
    return bucket.count < SETTINGS.narouRateMaxPerWindow;
  }
  async function tick() {
    if (processing) return; processing = true;
    try {
      while (queue.length) {
        if (!refillWindow()) { const sleepMs = Math.max(0, SETTINGS.narouRateWindowMs - (nowMs() - bucket.ts)); await new Promise(r => setTimeout(r, sleepMs)); continue; }
        const { ncode, resolve, reject } = queue.shift();
        const cached = cache.get(ncode);
        if (cached && (nowMs() - cached.ts) < SETTINGS.narouCacheTtlMs) { resolve(cached.info); continue; }
        try {
          bucket.count++;
          const url = `https://api.syosetu.com/novelapi/api/?out=json&of=t-w-nt&ncode=${encodeURIComponent(ncode)}`;
          const res = await fetch(url, { method: "GET" });
          const json = await res.json();
          const info = Array.isArray(json) && json.length >= 2 ? json[1] : null;
          cache.set(ncode, { info, ts: nowMs() }); resolve(info);
        } catch (e) { cache.set(ncode, { info: null, ts: nowMs() }); reject(e); }
      }
    } finally { processing = false; }
  }
  function get(ncode) {
    const cached = cache.get(ncode);
    if (cached && (nowMs() - cached.ts) < SETTINGS.narouCacheTtlMs) return Promise.resolve(cached.info);
    return new Promise((resolve, reject) => { queue.push({ ncode, resolve, reject }); tick(); });
  }
  return { get };
})();

/* ===== Meta parsing ===== */
async function parseMeta(url, title) {
  try {
    const u = new URL(url); const host = u.hostname; const t = cleanWhitespace(title || "");
    if (host.endsWith("syosetu.org")) return parseHameln(u, t);
    if (host.endsWith("kakuyomu.jp")) return parseKakuyomu(u, t);
    if (host.endsWith("syosetu.com")) return await parseNarou(u, t);
    if (host.endsWith("pixiv.net")) return parsePixiv(u, t);
    return { isContent: false, cert: "none", site: "unknown", workTitle: "", episodeTitle: "", author: "" };
  } catch { return { isContent: false, cert: "none", site: "unknown", workTitle: "", episodeTitle: "", author: "" }; }
}

/* Hameln */
function parseHameln(u, title) {
  const site = SITE.HAMELN; const path = u.pathname;
  const trimmed = title.replace(/\s*-\s*ハーメルン$/i, "").trim();
  const isSerial = /^\/novel\/\d+\/\d+\.html$/i.test(path);
  const isTop = /^\/novel\/\d+\/?$/i.test(path);
  const parts = trimmed.split(/\s+-\s+/);
  if (isSerial) {
    const work = cleanWhitespace(parts[0] || ""); const ep = cleanWhitespace(parts[1] || ""); const ok = !!(work && ep);
    return { isContent: ok, cert: ok ? "title" : "url", site, workTitle: work, episodeTitle: ep, author: "" };
  }
  if (isTop) {
    if (parts.length >= 2) {
      const work = cleanWhitespace(parts[0] || ""); const ep = cleanWhitespace(parts[1] || "");
      return { isContent: true, cert: "title", site, workTitle: work, episodeTitle: ep, author: "" };
    }
    const work = cleanWhitespace(parts[0] || "");
    return { isContent: false, cert: work ? "title" : "none", site, workTitle: work, episodeTitle: "", author: "" };
  }
  return { isContent: false, cert: "none", site, workTitle: "", episodeTitle: "", author: "" };
}

/* Kakuyomu */
function parseKakuyomu(u, title) {
  const site = SITE.KAKUYOMU; const path = u.pathname;
  const isEpisode = /^\/works\/\d+\/episodes\/\d+\/?$/i.test(path);
  const isTop = /^\/works\/\d+\/?$/i.test(path);
  if (/^https?:\/\//.test(title) || title.startsWith("kakuyomu.jp/")) {
    return { isContent: isEpisode, cert: isEpisode ? "url" : "none", site, workTitle: "", episodeTitle: "", author: "" };
  }
  const t = title.replace(/\s*-\s*カクヨム$/i, "").trim();
  const parts = t.split(/\s+-\s+/);
  if (isEpisode) {
    const subtitle = cleanWhitespace(parts[0] || "");
    const wa = (parts[1] || "").trim();
    const m = wa.match(/^(.*)（(.*)）$/);
    const work = cleanWhitespace(m ? m[1] : wa);
    const author = cleanWhitespace(m ? m[2] : "");
    const ok = !!(subtitle && work);
    return { isContent: ok, cert: ok ? "title" : "url", site, workTitle: ok ? work : "", episodeTitle: ok ? subtitle : "", author: ok ? author : "" };
  }
  if (isTop) {
    const wa = (parts[0] || "").trim();
    const m = wa.match(/^(.*)（(.*)）$/);
    const work = cleanWhitespace(m ? m[1] : wa);
    const author = cleanWhitespace(m ? m[2] : "");
    return { isContent: false, cert: work ? "title" : "none", site, workTitle: work, episodeTitle: "", author };
  }
  return { isContent: false, cert: "none", site, workTitle: "", episodeTitle: "", author: "" };
}

/* Narou */
async function parseNarou(u, title) {
  const site = SITE.NAROU;
  let m = u.pathname.match(/^\/(n[0-9a-z]+)\/(\d+)\/?$/i);
  if (m) {
    const ncode = m[1]; const epNo = m[2];
    let work = cleanWhitespace(title.replace(/\s*-\s*小説家になろう$/i, "").trim());
    let ep = "";
    if (/Android/i.test(navigator.userAgent)) {
      ep = `第${epNo}話`;
    } else {
      const parts = work.split(/\s+-\s+/);
      work = cleanWhitespace(parts[0] || work);
      ep   = cleanWhitespace(parts[1] || "");
    }
    const ok = !!(work && ep);
    return { isContent: ok, cert: ok ? "title" : "url", site, workTitle: work, episodeTitle: ep, author: "", ncode };
  }
  m = u.pathname.match(/^\/(n[0-9a-z]+)\/?$/i);
  if (m) {
    const ncode = m[1]; const t = title.replace(/\s*-\s*小説家になろう$/i, "").trim();
    try {
      const info = await narouApi.get(ncode);
      if (!info) return { isContent: false, cert: "url", site, workTitle: cleanWhitespace(t), episodeTitle: "", author: "", ncode };
      const isShort = Number(info.noveltype) === 2;
      const work = cleanWhitespace(info.title || t);
      const author = cleanWhitespace(info.writer || "");
      return { isContent: !!isShort, cert: "title", site, workTitle: work, episodeTitle: isShort ? "" : "", author, ncode };
    } catch {
      return { isContent: false, cert: "url", site, workTitle: cleanWhitespace(t), episodeTitle: "", author: "", ncode };
    }
  }
  return { isContent: false, cert: "none", site, workTitle: "", episodeTitle: "", author: "" };
}

/* Pixiv */
function parsePixiv(u, title) {
  const site = SITE.PIXIV;
  const isContentPath = /^\/novel\/show\.php$/i.test(u.pathname) && u.searchParams.has("id");
  const pixivId = u.searchParams.get("id") || undefined;
  if (!isContentPath) return { isContent: false, cert: "none", site, workTitle: "", episodeTitle: "", author: "" };
  if (/^https?:\/\//.test(title) || title.startsWith("pixiv.net/") || title.includes("イラストコミュニケーションサービス")) {
    return { isContent: true, cert: "url", site, workTitle: "", episodeTitle: "", author: "", pixivId };
  }
  let t = title.replace(/\s*-\s*pixiv$/i, "").trim();
  t = t.replace(/^(#\S+(?:\s+#\S+)*)\s+/, "").trim();
  if (/のシリーズ \[pixiv\]$/u.test(t)) return { isContent: false, cert: "title", site, workTitle: "", episodeTitle: "", author: "", pixivId };
  if (t.includes("|")) {
    const [ep, rest] = t.split("|").map(s => s.trim());
    const parts = rest.split(/\s*-\s*/);
    const work = cleanWhitespace(parts[0] || ""); const author = cleanWhitespace(parts[1] || "");
    const ok = !!(work && ep);
    return { isContent: ok, cert: ok ? "title" : "url", site, workTitle: ok ? work : "", episodeTitle: ok ? ep : "", author, pixivId };
  }
  const parts = t.split(/\s*-\s*/);
  const work = cleanWhitespace(parts[0] || ""); const author = cleanWhitespace(parts[1] || "");
  const ok = !!work;
  return { isContent: ok, cert: ok ? "title" : "url", site, workTitle: ok ? work : "", episodeTitle: "", author, pixivId };
}

/* ===== Tab state ===== */
const tabState = new Map();
let lastFocusedWindowId = (HAS_WINDOWS_API ? B.windows.WINDOW_ID_NONE : -1);

function makeStateMinimal(tabId, url, title, windowId) {
  const now = nowMs();
  return {
    tabId, windowId: windowId ?? null, url: url || "", title: title || "",
    sessionId: null,
    activeInWindow: SETTINGS.androidMode ? true : false,
    winFocused: SETTINGS.androidMode ? true : (HAS_WINDOWS_API ? false : true),
    inLastFocusedWindow: SETTINGS.androidMode ? true : (HAS_WINDOWS_API ? false : true),
    pageHidden: false,
    _prevEffVisible: undefined,
    lastVisUpdate: now,
    visDebounceTimer: undefined,
    isCandidate: isCandidateUrl(url),
    reading: false,
    lastStart: undefined,
    lastStop: undefined,
    stopTimer: undefined,
    lastInteraction: now,
    lastVisibleTs: now,
    segmentAccumMs: 0,
    lastFlushAt: now,
    pendingSegment: null,
    pendingShort: null,
    meta: { isContent: false, cert: "none", site: "", workTitle: "", episodeTitle: "", author: "", ncode: undefined, pixivId: undefined },
    loadingStatus: undefined,
    justUrlChanged: false, // URL変化直後の特例フラグ
    contentUrlAtStart: "" // 読み開始時のURL（details/pending用）
  };
}
async function ensureStateFromTab(tabId) {
  let st = tabState.get(tabId);
  if (st) return st;
  try {
    const t = await B.tabs.get(tabId);
    st = makeStateMinimal(tabId, t.url || "", t.title || "", t.windowId);
  } catch {
    st = makeStateMinimal(tabId, "", "", null);
  }
  tabState.set(tabId, st);
  return st;
}

/* ===== Domain enable cache ===== */
const siteEnableCache = { value: null, ts: 0 };
async function isDomainEnabled(domain) {
  await storageReady; const now = nowMs();
  if (!siteEnableCache.value || now - siteEnableCache.ts > 5000) {
    const obj = await storageGet([KEY_SITE_ENABLE]);
    siteEnableCache.value = obj[KEY_SITE_ENABLE] || {}; siteEnableCache.ts = now;
  }
  return !!(siteEnableCache.value || {})[domain];
}

/* ===== Effective visibility ===== */
function effectiveVisible(st) {
  if (!st) return false;
  if (HAS_WINDOWS_API && !SETTINGS.androidMode) return !!(st.activeInWindow && st.inLastFocusedWindow && st.winFocused && !st.pageHidden);
  return !st.pageHidden && (st.activeInWindow ?? true);
}

/* ===== Stop scheduling ===== */
function cancelScheduledStop(tabId) {
  const st = tabState.get(tabId);
  if (st?.stopTimer) { clearTimeout(st.stopTimer); st.stopTimer = undefined; tabState.set(tabId, st); }
}
function scheduleStop(tabId, now, reason) {
  const st = tabState.get(tabId);
  if (!st?.reading || st.stopTimer) return;
  const sinceStart = st.lastStart ? (now - st.lastStart) : Infinity;
  const inStartGrace = sinceStart < SETTINGS.startGraceMs;
  const persistMs = inStartGrace ? (SETTINGS.stopDebounceMs + SETTINGS.stopDebounceExtraDuringGraceMs) : SETTINGS.stopDebounceMs;
  const sid = st.sessionId;
  st.stopTimer = setTimeout(() => {
    withTabQueue(tabId, async () => {
      const cur = tabState.get(tabId);
      if (!cur || cur.sessionId !== sid) return;
      cur.stopTimer = undefined;
      const recheckNow = nowMs();
      const stillInvisible = !effectiveVisible(cur);
      const sinceInteraction = recheckNow - (cur.lastInteraction || 0);
      if (reason === "IDLE_LONG" && sinceInteraction >= SETTINGS.idleDiscardMs) {
        if (cur.reading && cur.lastStart) {
          const elapsed = Math.max(0, recheckNow - cur.lastStart);
          cur.reading = false; cur.lastStop = recheckNow;
          cur.lastStart = undefined;
          cur.segmentAccumMs = 0;
          cur.pendingSegment = null; cur.pendingShort = null;
          log(1, "drop.idleDiscard", { tabId, sessionId: cur.sessionId, elapsedMs: elapsed }, cur, true);
          tabState.set(tabId, cur);
        }
        return;
      }
      if (stillInvisible || reason === "IDLE_LONG" || reason === "IDLE_SHORT" || reason === "HEARTBEAT_TIMEOUT") {
        await stopReading(tabId, recheckNow, reason);
        tabState.set(tabId, cur);
      }
    });
  }, persistMs);
  tabState.set(tabId, st);
}

/* ===== Day split ===== */
function splitAcrossDays(startTs, stopTs, totalMs) {
  const startDay = dayKey(startTs), stopDay = dayKey(stopTs);
  if (startDay === stopDay) return [{ day: startDay, ms: totalMs, ts: stopTs }];
  const midnightStop = new Date(stopTs); midnightStop.setHours(0, 0, 0, 0);
  const midTs = midnightStop.getTime();
  const firstPartMs = Math.max(0, midTs - startTs);
  const secondPartMs = Math.max(0, totalMs - firstPartMs);
  return [
    { day: dayKey(startTs), ms: firstPartMs, ts: midTs - 1 },
    { day: dayKey(stopTs), ms: secondPartMs, ts: stopTs }
  ];
}

/* ===== Commit & pendingShort ===== */
function sameContentKey(a, b) {
  return (a.site === b.site) &&
         (a.workTitle === b.workTitle) &&
         (a.episodeTitle === b.episodeTitle) &&
         ((a.ncode ?? null) === (b.ncode ?? null)) &&
         ((a.pixivId ?? null) === (b.pixivId ?? null));
}
async function commitChunk(st, meta, ms, now, startTs) {
  await storageReady;
  if (!meta?.isContent || meta?.cert !== "title") {
    log(1, "commit.skip.nonTitle", { tabId: st.tabId, sessionId: st.sessionId, ms, site: meta?.site }, st, true);
    return;
  }
  const urlForRecord = st.contentUrlAtStart || st.url || "";

  if (ms < SETTINGS.minSessionMs) {
    const prev = st.pendingShort;
    const addition = {
      ms, stop: now,
      site: meta.site, workTitle: meta.workTitle, episodeTitle: meta.episodeTitle, author: meta.author,
      sessionId: st.sessionId,
      ncode: meta.ncode ?? undefined,
      pixivId: meta.pixivId ?? undefined,
      url: urlForRecord
    };
    if (prev && sameContentKey(prev, addition)) {
      prev.ms += addition.ms; prev.stop = addition.stop;
      if (!prev.url) prev.url = addition.url;
      log(1, "pendingShort.add", { tabId: st.tabId, sessionId: st.sessionId, ms: prev.ms }, st);
    } else {
      st.pendingShort = addition;
      log(1, "pendingShort.store", { tabId: st.tabId, sessionId: st.sessionId, ms: addition.ms }, st);
    }
    tabState.set(st.tabId, st);
    return;
  }
  const parts = splitAcrossDays(startTs ?? (now - ms), now, ms);
  const total = await getLocal(KEY_TOTAL, 0);
  const logs = await getLocal(KEY_LOG, {});
  const details = await getLocal(KEY_DETAILS, {});
  const today = dayKey(now);
  let addTotal = 0;
  for (const part of parts) {
    if (part.ms <= 0) continue;
    const list = details[part.day] || [];
    const same = r =>
      r.site === (meta?.site || "") &&
      r.workTitle === (meta?.workTitle || "") &&
      r.episodeTitle === (meta?.episodeTitle || "") &&
      r.sessionId === st.sessionId;
    const ex = list.find(same);
    if (ex) {
      ex.ms += part.ms; ex.ts = part.ts;
      if (meta?.author) ex.author = meta.author;
      if (!ex.url) ex.url = urlForRecord;
    } else {
      list.push({
        site: meta?.site || "", workTitle: meta?.workTitle || "", episodeTitle: meta?.episodeTitle || "",
        author: meta?.author || "", sessionId: st.sessionId, ms: part.ms, ts: part.ts, url: urlForRecord
      });
    }
    details[part.day] = list;
    logs[part.day] = (logs[part.day] || 0) + part.ms;
    addTotal += part.ms;
  }
  storageSetQueued({ [KEY_TOTAL]: total + addTotal, [KEY_DAILY]: (logs[today] || 0), [KEY_LOG]: logs, [KEY_DETAILS]: details });
  log(1, "commit.saved", { tabId: st.tabId, sessionId: st.sessionId, ms: addTotal, site: meta?.site, workTitle: meta?.workTitle, episodeTitle: meta?.episodeTitle }, st, true);
  pushLiveUpdate();
}

/* ===== Periodic commit ===== */
async function commitAll() {
  await storageReady;
  const now = nowMs();
  for (const [tabId, st] of tabState) {
    if (!st.reading) continue;
    if (!st.meta?.isContent || st.meta?.cert !== "title") continue;
    const inflight = st.lastStart ? (now - st.lastStart) : 0;
    const toCommit = st.segmentAccumMs + inflight;
    if (toCommit <= 0) continue;
    await commitChunk(st, st.meta, toCommit, now, now - toCommit);
    st.segmentAccumMs = 0;
    st.lastStart = now;
    st.lastFlushAt = now;
    tabState.set(tabId, st);
  }
}
function maybeCommitPeriodic(tabId, st, now) {
  if (!st.meta?.isContent || st.meta?.cert !== "title") return;
  if ((now - st.lastFlushAt) >= SETTINGS.realtimeFlushMs) {
    const inflight = st.lastStart ? (now - st.lastStart) : 0;
    const toCommit = st.segmentAccumMs + inflight;
    if (toCommit <= 0) { st.lastFlushAt = now; tabState.set(tabId, st); return; }
    commitChunk(st, st.meta, toCommit, now, now - toCommit).then(() => {
      st.segmentAccumMs = 0;
      st.lastStart = now;
      st.lastFlushAt = now;
      tabState.set(tabId, st);
    }).catch(() => {});
  }
}

/* ===== Start/Stop ===== */
async function stopReading(tabId, now, reason = "STOP") {
  const st = tabState.get(tabId); if (!st?.reading || !st?.lastStart) return;
  cancelScheduledStop(tabId);
  const elapsed = Math.max(0, now - st.lastStart);
  const inGrace = elapsed < SETTINGS.startGraceMs;
  if ((reason === "VISIBILITY_FLAP" || reason.startsWith("NOT_VISIBLE") || reason.startsWith("IDLE") || reason === "HEARTBEAT_TIMEOUT") && inGrace) {
    log(1, "stop.skipped.grace", { tabId, sessionId: st.sessionId, elapsed, reason }, st);
    return;
  }
  st.reading = false; st.lastStop = now;
  st.segmentAccumMs = (st.segmentAccumMs || 0) + elapsed;
  st.lastStart = undefined;
  log(1, "reading.stop", { tabId, sessionId: st.sessionId, elapsedMs: elapsed, reason, site: st.meta?.site, workTitle: st.meta?.workTitle, episodeTitle: st.meta?.episodeTitle }, st, true);

  const isHardStop = (reason === "NAVIGATION" || reason === "TAB_REMOVED");
  if (isHardStop) {
    if (st.meta?.isContent && st.meta?.cert === "title") {
      const ms = st.segmentAccumMs;
      if (ms > 0) await commitChunk(st, st.meta, ms, now, now - ms);
    } else {
      log(1, "commit.skip.hard.nonTitle", { tabId, sessionId: st.sessionId, ms: st.segmentAccumMs }, st, true);
    }
    st.segmentAccumMs = 0; st.pendingSegment = null;
  } else {
    const sinceInteraction = now - (st.lastInteraction || 0);
    if (reason === "IDLE_LONG" && sinceInteraction >= SETTINGS.idleDiscardMs) {
      log(1, "pending.skip.idleDiscard", { tabId, sessionId: st.sessionId, ms: st.segmentAccumMs }, st);
      st.segmentAccumMs = 0; st.pendingSegment = null; st.pendingShort = null;
    } else if (st.meta?.isContent && st.meta?.cert === "title") {
      st.pendingSegment = {
        ms: st.segmentAccumMs, stop: now,
        site: st.meta?.site || "", workTitle: st.meta?.workTitle || "", episodeTitle: st.meta?.episodeTitle || "", author: st.meta?.author || "",
        sessionId: st.sessionId, reason, url: st.contentUrlAtStart || st.url || ""
      };
      log(1, "pending.store", { tabId, sessionId: st.sessionId, ms: st.segmentAccumMs, reason }, st);
      st.segmentAccumMs = 0;
    } else {
      log(1, "pending.skip.nonTitle", { tabId, sessionId: st.sessionId, ms: st.segmentAccumMs, reason }, st);
      st.segmentAccumMs = 0;
    }
  }
  tabState.set(tabId, st);
}

function canRead(st, now, domainEnabled, effVisible) {
  if (!st || !domainEnabled || !st.isCandidate) return false;
  if (!st.meta?.isContent || st.meta?.cert !== "title") return false;
  const idleTooLong = st.lastInteraction && (now - st.lastInteraction) >= SETTINGS.idleLongMs;
  if (idleTooLong) return false;
  return effVisible;
}

async function startOrContinue(tabId, now) {
  const st = tabState.get(tabId); if (!st) return;
  const domain = getDomain(st.url);
  const domainEnabled = domain ? await isDomainEnabled(domain) : false;
  const effVisible = effectiveVisible(st);
  const visStable = st.lastVisUpdate ? (now - st.lastVisUpdate) >= SETTINGS.visibilityStabilizeMs : true;

  if (!domainEnabled || !st.isCandidate || !st.meta?.isContent || st.meta?.cert !== "title") {
    if (st.reading) {
      const reason = !domainEnabled ? "DOMAIN_DISABLED" : (!st.isCandidate ? "NON_CANDIDATE" : (!st.meta?.isContent ? "NON_CONTENT" : "NON_TITLE"));
      scheduleStop(tabId, now, reason);
    }
    if (!st.meta?.isContent || st.meta?.cert !== "title") {
      if (st.pendingSegment) {
        log(1, "pending.drop.nonTitle", { tabId, sessionId: st.sessionId, ms: st.pendingSegment.ms }, st);
        st.pendingSegment = null; tabState.set(tabId, st);
      }
    }
    return;
  }

  const allowed = canRead(st, now, domainEnabled, effVisible);
  if (allowed) {
    if (!st.reading) {
      // URL変化直後だけ、debounce と visStable をスキップ
      const skipDebounce = !!st.justUrlChanged;
      const skipVisStable = !!st.justUrlChanged;

      if (!skipDebounce && st.lastStop && (now - st.lastStop) < SETTINGS.startDebounceMs) return;
      if (!skipVisStable && !visStable) return;

      if (!st.sessionId) {
        st.sessionId = cryptoId();
        log(1, "session.new", { tabId: st.tabId, sessionId: st.sessionId, reason: "content.confirmed" }, st, true);
      }

      // pendingShort 合算
      if (st.pendingShort) {
        const p = st.pendingShort;
        const sameWork = p.workTitle === (st.meta?.workTitle || "");
        const sameEp = p.episodeTitle === (st.meta?.episodeTitle || "");
        const within = (now - p.stop) <= SETTINGS.mergeWindowMs;
        if (sameWork && sameEp && within) { st.segmentAccumMs += p.ms; log(1, "pendingShort.merge", { tabId, sessionId: st.sessionId, ms: p.ms }, st); }
        else { log(1, "pendingShort.drop", { tabId, sessionId: st.sessionId, ms: p.ms }, st); }
        st.pendingShort = null;
      }

      // pendingSegment 合算
      if (st.pendingSegment) {
        const p = st.pendingSegment;
        const sameWork = p.workTitle === (st.meta?.workTitle || "");
        const sameEp = p.episodeTitle === (st.meta?.episodeTitle || "");
        const within = (now - p.stop) <= SETTINGS.mergeWindowMs;
        if (sameWork && sameEp && within) { st.segmentAccumMs += p.ms; log(1, "pending.merge", { tabId, sessionId: st.sessionId, ms: p.ms }, st); }
        else { log(1, "pending.drop.nomatch", { tabId, sessionId: st.sessionId, ms: p.ms }, st); }
        st.pendingSegment = null;
      }

      // 読み開始時点のURLを保持（詳細保存用）
      st.contentUrlAtStart = st.url || "";

      st.reading = true; st.lastStart = now; st.lastFlushAt = now; st.lastVisibleTs = now;
      st.justUrlChanged = false; // 成功したら特例フラグをクリア
      tabState.set(tabId, st);
      log(1, "reading.start", { tabId, sessionId: st.sessionId, reason: skipDebounce || skipVisStable ? "URL_CHANGE_START" : "START_VISIBLE", site: st.meta?.site, workTitle: st.meta?.workTitle, episodeTitle: st.meta?.episodeTitle }, st, true);
    } else {
      cancelScheduledStop(tabId);
      st.lastVisibleTs = now; tabState.set(tabId, st);
      maybeCommitPeriodic(tabId, st, now);
    }
  } else {
    const shouldStopForVisibility = !effVisible;
    const startingPhase = st.reading && st.lastStart && (now - st.lastStart) < SETTINGS.visibilityStabilizeMs;
    if (startingPhase && shouldStopForVisibility) { scheduleStop(tabId, now, "VISIBILITY_FLAP"); return; }
    const sinceInteraction = now - (st.lastInteraction || 0);
    if (st.reading) {
      if (shouldStopForVisibility) { scheduleStop(tabId, now, "NOT_VISIBLE"); return; }
      if (sinceInteraction >= SETTINGS.idleShortMs && sinceInteraction < SETTINGS.idleLongMs) { scheduleStop(tabId, now, "IDLE_SHORT"); return; }
      if (sinceInteraction >= SETTINGS.idleLongMs) { scheduleStop(tabId, now, "IDLE_LONG"); }
    }
  }
}

/* ===== Candidate polling (URL-aware sig) ===== */
const candidatePolls = new Map(); // tabId -> { timer, lastSig }

function stopPolling(tabId) {
  const s = candidatePolls.get(tabId);
  if (s?.timer) clearInterval(s.timer);
  candidatePolls.delete(tabId);
}

function restartPolling(tabId) {
  stopPolling(tabId);
  const state = { lastSig: "", timer: null };
  state.timer = setInterval(() => {
    withTabQueue(tabId, async () => {
      const st = tabState.get(tabId); if (!st) return stopPolling(tabId);
      try {
        const tab = await B.tabs.get(tabId);
        const title = tab.title || "";
        const meta = await parseMeta(st.url, title);
        const sig = `${st.url}|${meta.site}|${meta.isContent}|${meta.cert}|${meta.workTitle}|${meta.episodeTitle}`;
        if (sig === state.lastSig) return;
        state.lastSig = sig;
        st.title = title; st.meta = meta; st.isCandidate = isCandidateUrl(st.url);
        tabState.set(tabId, st);
        log(1, "meta.poll.update", { tabId, cert: meta.cert, isContent: meta.isContent, work: meta.workTitle, ep: meta.episodeTitle }, st);
        if (!st.isCandidate) { stopPolling(tabId); return; }
        if (meta.isContent && meta.cert === "title") startOrContinue(tabId, nowMs());
      } catch { stopPolling(tabId); }
    });
  }, SETTINGS.pollingIntervalMs);
  candidatePolls.set(tabId, state);
}
function startPolling(tabId) { if (candidatePolls.has(tabId)) restartPolling(tabId); else restartPolling(tabId); }

/* ===== Evaluators ===== */
async function evaluateReadingState(tabId, now) {
  const st = tabState.get(tabId); if (!st) return;
  if (st.pendingSegment && (now - st.pendingSegment.stop) > SETTINGS.pendingTimeoutMs) { log(1, "pending.timeout", { tabId, sessionId: st.sessionId, ms: st.pendingSegment.ms }, st); st.pendingSegment = null; tabState.set(tabId, st); }
  if (st.pendingShort && (now - st.pendingShort.stop) > SETTINGS.pendingTimeoutMs) { log(1, "pendingShort.timeout", { tabId, sessionId: st.sessionId, ms: st.pendingShort.ms }, st); st.pendingShort = null; tabState.set(tabId, st); }
  const effVisible = effectiveVisible(st);
  if (st._prevEffVisible !== effVisible) {
    if (st.visDebounceTimer) clearTimeout(st.visDebounceTimer);
    st.visDebounceTimer = setTimeout(() => {
      withTabQueue(tabId, async () => {
        const t = nowMs();
        const cur = tabState.get(tabId); if (!cur) return;
        const curEff = effectiveVisible(cur);
        cur.lastVisUpdate = t; cur._prevEffVisible = curEff; cur.visDebounceTimer = undefined;
        log(1, curEff ? "vis.true" : "vis.false", { tabId, activeInWindow: cur.activeInWindow, winFocused: cur.winFocused, pageHidden: cur.pageHidden }, cur);
        tabState.set(tabId, cur);
        await startOrContinue(tabId, t);
      });
    }, SETTINGS.visibilityStabilizeMs);
    tabState.set(tabId, st);
    return;
  } else {
    st.lastVisUpdate = now; tabState.set(tabId, st);
  }
  await startOrContinue(tabId, now);
}

/* ===== Live updates ===== */
let _lastLiveSig = null, _lastLiveAt = 0, liveUpdateInterval = null, _commitInterval = null;
async function getLiveFinal() {
  await storageReady;
  const total = await getLocal(KEY_TOTAL, 0);
  const details = await getLocal(KEY_DETAILS, {});
  const logs = await getLocal(KEY_LOG, {});
  const today = dayKey(nowMs());
  const daily = logs[today] || 0;
  const recent = (details[today] || []).slice().sort((a, b) => b.ts - a.ts).slice(0, 5);
  const inflightMs = Array.from(tabState.values())
    .filter(st => st.reading && st.meta?.isContent && st.meta?.cert === "title")
    .reduce((sum, st) => sum + ((nowMs() - (st.lastStart || nowMs())) + (st.segmentAccumMs || 0)), 0);
  return { total, daily, inflightMs, recent };
}
async function runtimeSendMessageSafe(msg) {
  try { const p = B.runtime?.sendMessage?.(msg); if (isPromise(p)) return p.catch(() => {}); } catch {}
  return Promise.resolve();
}
function setupIntervals() {
  try {
    if (_commitInterval) clearInterval(_commitInterval);
    _commitInterval = setInterval(() => { commitAll().catch(() => {}); }, SETTINGS.realtimeFlushMs);
    if (liveUpdateInterval) clearInterval(liveUpdateInterval);
    liveUpdateInterval = setInterval(() => { pushLiveUpdate().catch(() => {}); }, SETTINGS.livePushIntervalMs);
  } catch {}
}
async function pushLiveUpdate() {
  const now = nowMs(); if (now - _lastLiveAt < SETTINGS.livePushMinGapMs) return;
  const payload = await getLiveFinal();
  const recentTopTs = payload.recent && payload.recent[0] ? payload.recent[0].ts : 0;
  const sig = JSON.stringify({ total: payload.total, daily: payload.daily, inflightMs: payload.inflightMs, recentTopTs });
  if (sig === _lastLiveSig) return;
  _lastLiveSig = sig; _lastLiveAt = now;
  await runtimeSendMessageSafe({ type: "live-update", payload });
}

/* ===== Active recompute ===== */
async function recomputeActiveForWindow(winId, activeTabId) {
  const focused = HAS_WINDOWS_API ? (winId !== B.windows.WINDOW_ID_NONE) : true;
  for (const [tid, st] of tabState) {
    if (HAS_WINDOWS_API) {
      st.activeInWindow = (st.windowId === winId) && (tid === activeTabId);
      st.winFocused = focused;
      st.inLastFocusedWindow = focused && (st.windowId === winId);
    } else {
      st.activeInWindow = (tid === activeTabId);
      st.winFocused = true;
      st.inLastFocusedWindow = true;
    }
    tabState.set(tid, st);
  }
}

/* ===== Windows focus ===== */
if (HAS_WINDOWS_API && !SETTINGS.androidMode) {
  B.windows.onFocusChanged.addListener((winId) => {
    lastFocusedWindowId = winId;
    const focused = winId !== B.windows.WINDOW_ID_NONE;
    try {
      const p = B.tabs.query({ active: true, windowId: winId }); const use = isPromise(p) ? p : Promise.resolve([]);
      use.then((tabs) => {
        const activeTabId = tabs[0]?.id;
        withTabQueue(activeTabId ?? -1, async () => {
          await recomputeActiveForWindow(winId, activeTabId);
          if (activeTabId) {
            const s = tabState.get(activeTabId);
            if (s) {
              s.winFocused = focused; s.pageHidden = false; s.lastVisUpdate = nowMs(); tabState.set(activeTabId, s);
              await startOrContinue(activeTabId, nowMs());
            }
          }
          setTimeout(() => {
            for (const [tid] of tabState) {
              if (tid === activeTabId) continue;
              withTabQueue(tid, () => evaluateReadingState(tid, nowMs()));
            }
          }, 0);
        });
      });
    } catch {}
  });
} else {
  console.log("windows.api.absent or androidMode=true");
}

/* ===== Dedup ===== */
const lastNavHandled = new Map(); // tabId -> { url, ts, src }
function recentlyHandled(tabId, url, withinMs) {
  const w = withinMs ?? SETTINGS.navDedupWindowMs;
  const last = lastNavHandled.get(tabId);
  return last && last.url === url && (nowMs() - last.ts) < w;
}
function markHandled(tabId, url, src) { lastNavHandled.set(tabId, { url, ts: nowMs(), src }); }

/* ===== Tabs activation ===== */
B.tabs.onActivated.addListener(({ tabId, windowId }) => {
  withTabQueue(tabId, async () => {
    const focused = HAS_WINDOWS_API ? (windowId !== B.windows.WINDOW_ID_NONE) : true;
    lastFocusedWindowId = windowId;
    await recomputeActiveForWindow(windowId, tabId);
    const st = await ensureStateFromTab(tabId);
    st.activeInWindow = true;
    st.winFocused = HAS_WINDOWS_API ? focused : true;
    st.inLastFocusedWindow = HAS_WINDOWS_API ? (focused && (st.windowId === windowId)) : true;
    st.pageHidden = false;
    st.lastInteraction = nowMs();
    st.lastVisibleTs = nowMs();
    st.lastVisUpdate = nowMs();
    st.isCandidate = isCandidateUrl(st.url);
    tabState.set(tabId, st);
    if (st.isCandidate) startPolling(tabId); else stopPolling(tabId);
    await startOrContinue(tabId, nowMs());
  });
});

/* ===== Tabs created ===== */
if (B?.tabs?.onCreated) {
  B.tabs.onCreated.addListener((tab) => {
    withTabQueue(tab.id, async () => {
      const st = makeStateMinimal(tab.id, tab.url || "", tab.title || "", tab.windowId);
      st.isCandidate = isCandidateUrl(st.url);
      tabState.set(tab.id, st);
      log(1, "tab.created", { tabId: tab.id, url: st.url }, st);
      if (st.isCandidate) startPolling(tab.id);
    });
  });
}

/* ===== Tabs.onUpdated ===== */
B.tabs.onUpdated.addListener((tabId, changeInfo, tab) => {
  withTabQueue(tabId, async () => {
    if (!tab) return;
    const now = nowMs();
    const newUrl = ("url" in changeInfo) ? changeInfo.url : (tab.url || "");
    const st = await ensureStateFromTab(tabId);

    const urlChanged = ("url" in changeInfo) && newUrl && newUrl !== st.url;
    if (!urlChanged) {
      if ("title" in changeInfo) { st.title = changeInfo.title || st.title; tabState.set(tabId, st); }
      await startOrContinue(tabId, now);
      return;
    }

    if (recentlyHandled(tabId, newUrl)) { log(1, "nav.dedup.tabs", { tabId, newUrl }, st); return; }

    if (st.reading) { cancelScheduledStop(tabId); await stopReading(tabId, now, "NAVIGATION"); }

    st.url = newUrl; st.isCandidate = isCandidateUrl(newUrl);
    st.sessionId = null;
    st.contentUrlAtStart = ""; // 次回読み開始で再設定
    st.lastInteraction = now; st.lastVisibleTs = now;
    st.lastVisUpdate = now;
    st.justUrlChanged = true; // URL変化直後の特例フラグをON
    tabState.set(tabId, st); markHandled(tabId, newUrl, "tabs");

    try { const immediateMeta = await parseMeta(newUrl, ""); st.meta = immediateMeta; tabState.set(tabId, st); } catch {}

    if (st.isCandidate) { restartPolling(tabId); await startOrContinue(tabId, now); } else { stopPolling(tabId); }
  });
});

/* ===== SPA history ===== */
if (B.webNavigation?.onHistoryStateUpdated) {
  B.webNavigation.onHistoryStateUpdated.addListener((details) => {
    withTabQueue(details.tabId, async () => {
      if (details.frameId !== 0) return;
      const tabId = details.tabId;
      const st = tabState.get(tabId); if (!st) return;
      const newUrl = details.url;
      const now = nowMs();

      if (!newUrl || newUrl === st.url) return;
      if (recentlyHandled(tabId, newUrl)) { log(1, "nav.dedup.history", { tabId, newUrl }, st); return; }

      if (st.reading) { cancelScheduledStop(tabId); await stopReading(tabId, now, "NAVIGATION"); }
      st.url = newUrl; st.isCandidate = isCandidateUrl(newUrl); st.sessionId = null;
      st.contentUrlAtStart = ""; // 次回読み開始で再設定
      st.lastInteraction = now; st.lastVisibleTs = now; st.lastVisUpdate = now;
      st.justUrlChanged = true; // 特例フラグON
      tabState.set(tabId, st); markHandled(tabId, newUrl, "history");

      try { const immediateMeta = await parseMeta(newUrl, ""); st.meta = immediateMeta; tabState.set(tabId, st); } catch {}

      if (st.isCandidate) { restartPolling(tabId); await startOrContinue(tabId, now); } else { stopPolling(tabId); }
    });
  });
}

/* ===== SPA fragment (hash) ===== */
if (B.webNavigation?.onReferenceFragmentUpdated) {
  B.webNavigation.onReferenceFragmentUpdated.addListener((details) => {
    withTabQueue(details.tabId, async () => {
      if (details.frameId !== 0) return;
      const tabId = details.tabId;
      const st = tabState.get(tabId); if (!st) return;
      const prevUrl = st.url; const newUrl = details.url;
      const now = nowMs();

      if (!newUrl || newUrl === prevUrl) return;
      if (recentlyHandled(tabId, newUrl)) { log(1, "nav.dedup.hash", { tabId, newUrl }, st); return; }

      if (samePixivWorkIgnoringHash(prevUrl, newUrl) || samePathIgnoringHash(prevUrl, newUrl)) {
        st.url = newUrl; st.lastInteraction = now; st.lastVisibleTs = now; st.lastVisUpdate = now;
        st.isCandidate = isCandidateUrl(st.url);
        st.justUrlChanged = true; // ハッシュでも本文継続なら特例許容
        tabState.set(tabId, st);
        try { const meta = await parseMeta(newUrl, ""); st.meta = meta; tabState.set(tabId, st); } catch {}
        if (st.isCandidate) { restartPolling(tabId); await startOrContinue(tabId, now); } else { stopPolling(tabId); }
        markHandled(tabId, newUrl, "hash");
        return;
      }

      if (st.reading) { cancelScheduledStop(tabId); await stopReading(tabId, now, "NAVIGATION"); }
      st.isCandidate = isCandidateUrl(newUrl);
      st.url = newUrl; st.sessionId = null;
      st.contentUrlAtStart = ""; // 次回読み開始で再設定
      st.lastInteraction = now; st.lastVisibleTs = now; st.lastVisUpdate = now;
      st.justUrlChanged = true; // 特例フラグON
      tabState.set(tabId, st); markHandled(tabId, newUrl, "hash");

      try { const meta = await parseMeta(newUrl, ""); st.meta = meta; tabState.set(tabId, st); } catch {}
      if (st.isCandidate) { restartPolling(tabId); await startOrContinue(tabId, now); } else { stopPolling(tabId); }
    });
  });
}

/* ===== Tab removal ===== */
B.tabs.onRemoved.addListener((tabId) => {
  withTabQueue(tabId, async () => {
    const st = tabState.get(tabId);
    stopPolling(tabId);
    if (st) {
      cancelScheduledStop(tabId);
      if (st.reading) { await stopReading(tabId, nowMs(), "TAB_REMOVED"); }
      if (st.pendingSegment) {
        const p = st.pendingSegment; const now = nowMs();
        if (st.meta?.isContent && st.meta?.cert === "title") {
          await commitChunk(st, st.meta, p.ms, now, now - p.ms);
          log(1, "pending.commit.onRemove", { tabId, sessionId: st.sessionId, ms: p.ms }, st);
        } else {
          log(1, "pending.drop.onRemove", { tabId, sessionId: st.sessionId, ms: p.ms }, st);
        }
        st.pendingSegment = null;
      }
      if (st.pendingShort) { log(1, "pendingShort.drop.onRemove", { tabId, sessionId: st.sessionId, ms: st.pendingShort.ms }, st); st.pendingShort = null; }
    }
    tabState.delete(tabId);
    heartbeatState.delete(tabId);
  });
});

/* ===== Messaging ===== */
B.runtime.onMessage.addListener((msg, sender, sendResponse) => {
  const tabId = sender?.tab?.id;

  if (msg?.type === "heartbeat" && tabId) {
    withTabQueue(tabId, async () => {
      const now = nowMs(); registerHeartbeat(tabId, now);
      const st = await ensureStateFromTab(tabId);
      st.lastInteraction = msg.lastInteraction || st.lastInteraction || now;
      st.pageHidden = (msg.visibilityState === "hidden");
      if (!st.pageHidden) st.lastVisibleTs = now;

      if (SETTINGS.androidMode) {
        const hidden = (msg.visibilityState === "hidden");
        st.activeInWindow = !hidden; st.winFocused = !hidden; st.inLastFocusedWindow = !hidden;
        if (now - st.lastFlushAt >= SETTINGS.realtimeFlushMs) { commitAll().catch(() => {}); }
      }

      st.isCandidate = isCandidateUrl(st.url);
      tabState.set(tabId, st);

      if (st.isCandidate) {
        try { const meta = await parseMeta(st.url, ""); st.meta = meta; tabState.set(tabId, st); } catch {}
        restartPolling(tabId);
        await startOrContinue(tabId, now);
      } else { stopPolling(tabId); }

      await evaluateReadingState(tabId, now);
      log(1, "heartbeat", { tabId, pageHidden: st.pageHidden, lastInteraction: st.lastInteraction }, st);
    });
    return false;
  }

  if (msg?.type === "scroll-activity" && tabId) {
    withTabQueue(tabId, async () => {
      const st = tabState.get(tabId);
      if (st) {
        st.lastInteraction = msg.ts || nowMs();
        tabState.set(tabId, st);
        await evaluateReadingState(tabId, nowMs());
        log(1, "activity.scroll", { tabId, lastInteraction: st.lastInteraction, evType: msg.evType }, st);
      }
    });
    return false;
  }

  if (msg?.type === "hash-change" && tabId) {
    withTabQueue(tabId, async () => {
      const st = tabState.get(tabId); if (!st) return;
      const now = nowMs(); const newUrl = msg.url || st.url; const prevUrl = st.url;

      if (samePixivWorkIgnoringHash(prevUrl, newUrl) || samePathIgnoringHash(prevUrl, newUrl)) {
        st.url = newUrl; st.isCandidate = isCandidateUrl(st.url);
        st.lastInteraction = now; st.lastVisibleTs = now; st.lastVisUpdate = now;
        st.justUrlChanged = true;
        tabState.set(tabId, st);
        try { const meta = await parseMeta(newUrl, ""); st.meta = meta; tabState.set(tabId, st); } catch {}
        if (st.isCandidate) { restartPolling(tabId); await startOrContinue(tabId, now); } else { stopPolling(tabId); }
        log(1, "hash.update", { tabId, url: newUrl }, st);
      } else {
        if (st.reading) { cancelScheduledStop(tabId); await stopReading(tabId, now, "NAVIGATION"); }
        st.url = newUrl; st.isCandidate = isCandidateUrl(st.url); st.sessionId = null;
        st.contentUrlAtStart = ""; // 次回読み開始で再設定
        st.lastInteraction = now; st.lastVisibleTs = now; st.lastVisUpdate = now;
        st.justUrlChanged = true;
        tabState.set(tabId, st);
        try { const meta = await parseMeta(newUrl, ""); st.meta = meta; tabState.set(tabId, st); } catch {}
        if (st.isCandidate) { restartPolling(tabId); await startOrContinue(tabId, now); } else { stopPolling(tabId); }
        log(1, "hash.update.hard", { tabId, url: newUrl }, st);
      }
    });
    return false;
  }

  if (msg?.type === "get-stats") { getLiveFinal().then(payload => { try { sendResponse(payload); } catch {} }); return true; }
  if (msg?.type === "get-telemetry") { try { sendResponse({ telemetry }); } catch {} return true; }

  if (msg?.type === "set-settings") {
    try {
      const next = { ...SETTINGS, ...msg.settings, version: DEFAULT_SETTINGS.version };
      SETTINGS = next; DEBUG_ONLY_CANDIDATE_LOGS = !!SETTINGS.debugOnlyCandidateLogs; DIAG_MODE = !!SETTINGS.diagMode;
      storageSetQueued({ [KEY_SETTINGS]: next }); applySettings();
      try { sendResponse({ ok: true, settings: next }); } catch {}
      console.log("settings.updated", next);
    } catch (e) { try { sendResponse({ ok: false, error: String(e) }); } catch {} }
    return true;
  }

  if (msg?.type === "export-store") {
    storageGet([KEY_TOTAL, KEY_DAILY, KEY_LOG, KEY_DETAILS, KEY_SETTINGS, KEY_SITE_ENABLE])
      .then(snap => { try { sendResponse({ ok: true, snapshot: snap }); } catch {} });
    return true;
  }

  if (msg?.type === "import-store") {
    try {
      const snap = msg.snapshot || {};
      const allowedKeys = new Set([KEY_TOTAL, KEY_DAILY, KEY_LOG, KEY_DETAILS, KEY_SETTINGS, KEY_SITE_ENABLE]);
      const safe = {};
      for (const k of Object.keys(snap)) {
        if (!allowedKeys.has(k)) continue;
        const v = snap[k];
        if (k === KEY_TOTAL || k === KEY_DAILY) { if (typeof v !== "number" || !isFinite(v) || v < 0) continue; }
        safe[k] = v;
      }
      if (safe[KEY_SETTINGS]?.version && safe[KEY_SETTINGS].version < DEFAULT_SETTINGS.version) {
        safe[KEY_SETTINGS] = { ...DEFAULT_SETTINGS, ...safe[KEY_SETTINGS], version: DEFAULT_SETTINGS.version };
      }
      storageSetQueued(safe);
      try { sendResponse({ ok: true }); } catch {}
      pushLiveUpdate();
    } catch (e) { try { sendResponse({ ok: false, error: String(e) }); } catch {} }
    return true;
  }

  if (msg?.type === "get-site-enable") { storageGet([KEY_SITE_ENABLE]).then(v => { try { sendResponse(v[KEY_SITE_ENABLE] || {}); } catch {} }); return true; }

  if (msg?.type === "set-site-enable") {
    storageGet([KEY_SITE_ENABLE]).then(cur => {
      const cfg = { ...(cur[KEY_SITE_ENABLE] || {}) };
      cfg[msg.domain] = !!msg.enabled;
      storageSetQueued({ [KEY_SITE_ENABLE]: cfg });
      siteEnableCache.value = cfg; siteEnableCache.ts = nowMs();
      try { sendResponse({ ok: true }); } catch {}
      pushLiveUpdate();
    });
    return true;
  }

  if (msg?.type === "get-diagnosis") {
    try {
      const issues = diagnoseLogs(telemetry.slice(-400));
      sendResponse({ ok: true, diagnosis: issues });
    } catch (e) {
      sendResponse({ ok: false, error: String(e) });
    }
    return true;
  }

  if (msg?.type === "reset-all") {
    try {
      const init = { [KEY_TOTAL]: 0, [KEY_DAILY]: 0, [KEY_LOG]: {}, [KEY_DETAILS]: {} };
      storageSetQueued(init); pushLiveUpdate(); sendResponse({ ok: true }); console.log("reset.all");
    } catch (e) { sendResponse({ ok: false, error: String(e) }); }
    return true;
  }

  if (msg?.type === "reset-today") {
    (async () => {
      try {
        const today = dayKey(nowMs());
        const logs = await getLocal(KEY_LOG, {});
        const details = await getLocal(KEY_DETAILS, {});
        const total = await getLocal(KEY_TOTAL, 0);
        const todayMs = logs[today] || 0;
        delete logs[today]; delete details[today];
        const nextTotal = Math.max(0, total - todayMs);
        storageSetQueued({ [KEY_LOG]: logs, [KEY_DETAILS]: details, [KEY_TOTAL]: nextTotal, [KEY_DAILY]: 0 });
        pushLiveUpdate(); sendResponse({ ok: true }); console.log("reset.today");
      } catch (e) { sendResponse({ ok: false, error: String(e) }); }
    })();
    return true;
  }

  if (msg?.type === "debug.dumpState") {
    try {
      const tabs = Array.from(tabState.values());
      const diagnosis = diagnoseLogs(telemetry.slice(-400));
      sendResponse({ tabs, diagnosis });
    } catch {}
    return true;
  }

  return false;
});

/* ===== Bootstrap (with retry) ===== */
async function bootstrapExistingTabsOnce() {
  try {
    const p = B.tabs.query({}); const tabs = await (isPromise(p) ? p : Promise.resolve([]));
    for (const t of tabs) {
      const st = makeStateMinimal(t.id, t.url || "", t.title || "", t.windowId);
      if (SETTINGS.androidMode) { st.activeInWindow = true; st.winFocused = true; st.inLastFocusedWindow = true; }
      st.isCandidate = isCandidateUrl(st.url);
      st.sessionId = null;
      tabState.set(t.id, st);
      if (st.isCandidate) {
        try { const meta = await parseMeta(st.url, t.title || ""); st.meta = meta; tabState.set(t.id, st); } catch {}
        restartPolling(t.id);
      }
    }
    console.log("bootstrap.initTabs", { count: tabs.length });
    return tabs.length;
  } catch (e) { console.log("bootstrap.error", { e: String(e) }); return 0; }
}
async function bootstrapExistingTabs() {
  let count = await bootstrapExistingTabsOnce();
  if (count === 0) {
    for (let i = 0; i < 5 && count === 0; i++) { await new Promise(r => setTimeout(r, 500)); count = await bootstrapExistingTabsOnce(); }
  }
}
if (B?.runtime?.onStartup) B.runtime.onStartup.addListener(() => { bootstrapExistingTabs(); });
if (B?.runtime?.onInstalled) B.runtime.onInstalled.addListener(() => { bootstrapExistingTabs(); });
bootstrapExistingTabs();

/* ===== Alarms + flush ===== */
function setupAlarms() {
  try {
    if (B?.alarms?.create) {
      B.alarms.clear("rt-commit");
      const periodMin = 0.25; // 15秒
      B.alarms.create("rt-commit", { periodInMinutes: periodMin });
      console.log("alarms.setup", { periodMin });
    }
  } catch (e) { console.log("alarms.setup.error", { e: String(e) }); }
}
if (B?.alarms?.onAlarm) B.alarms.onAlarm.addListener((alarm) => { if (alarm?.name === "rt-commit") { commitAll().catch(() => {}); } });
if (B?.runtime?.onSuspend) B.runtime.onSuspend.addListener(() => { commitAll().catch(() => {}); });
console.log("background.js loaded", new Date().toISOString());

/* ===== Heartbeat watchdog ===== */
const heartbeatState = new Map();
function registerHeartbeat(tabId, now) {
  let hb = heartbeatState.get(tabId);
  if (!hb) hb = { lastBeat: now, missCount: 0 };
  hb.lastBeat = now; hb.missCount = 0; heartbeatState.set(tabId, hb);
}
let watchdogInterval = null;
function setupWatchdog() {
  try {
    if (watchdogInterval) clearInterval(watchdogInterval);
    watchdogInterval = setInterval(() => {
      const now = nowMs();
      for (const [tabId, hb] of heartbeatState) {
        const st = tabState.get(tabId);
        if (!st) { heartbeatState.delete(tabId); continue; }
        const sinceBeat = now - hb.lastBeat;
        if (sinceBeat > SETTINGS.heartbeatMissWindowMs) {
          hb.missCount++;
          if (hb.missCount >= SETTINGS.heartbeatMissLimit) {
            if (st.reading) {
              st.pageHidden = true; tabState.set(tabId, st);
              scheduleStop(tabId, now, "HEARTBEAT_TIMEOUT");
              log(1, "heartbeat.timeout.stop", { tabId, sessionId: st.sessionId, missCount: hb.missCount, sinceBeat }, st);
            }
          }
        } else { hb.missCount = 0; }
        heartbeatState.set(tabId, hb);
      }
    }, SETTINGS.watchdogCheckIntervalMs);
  } catch {}
}

/* ===== Self-diagnosis ===== */
function diagnoseLogs(logs) {
  const issues = [];
  const hasTag = (prefix) => logs.some(l => l.tag?.startsWith?.(prefix));
  const countTag = (prefix) => logs.filter(l => l.tag?.startsWith?.(prefix)).length;
  const pollUpdates = countTag("meta.poll.update");
  if (pollUpdates === 0) issues.push("候補内ポーリング未作動: URL候補判定/権限を確認");
  if (!hasTag("heartbeat")) issues.push("heartbeat未受信: content.js注入/対象URL確認");
  if (hasTag("heartbeat") && !hasTag("reading.start")) issues.push("読み開始未成立: cert='title'未確定/可視性/サイト有効の確認");
  const commitSaved = countTag("commit.saved");
  const pendingShortOps = countTag("pendingShort.add") + countTag("pendingShort.store") + countTag("pendingShort.merge");
  if (commitSaved === 0 && pendingShortOps > 0) issues.push("短断片は pendingShort に蓄積・合算されています（flush境界のノイズ回避）");
  if (issues.length === 0) issues.push("特に異常は検出されませんでした");
  return issues;
}