"use strict";

const B = (typeof browser !== "undefined") ? browser : chrome;

/* ===== Utils ===== */
function nowMs() { return Date.now(); }
function cleanWhitespace(s) { return String(s || "").replace(/\s+/g, " ").trim(); }
function dayKey(ts) { const d = new Date(ts); return `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,"0")}-${String(d.getDate()).padStart(2,"0")}`; }
function cryptoId() { return `s_${Date.now().toString(16)}_${Math.random().toString(16).slice(2)}`; }
function isPromiseLike(x) { return !!x && typeof x.then === "function"; }
function isInternal(url) { return typeof url === "string" && (url.startsWith("chrome-extension://") || url.startsWith("moz-extension://") || url.startsWith("about:") || url.startsWith("chrome:")); }
function toEpochMs(val, fallbackNow) { const now = fallbackNow ?? nowMs(); const v = Number(val); if (!Number.isFinite(v)) return now; if (v > 1e12 && v < 4102444800000) return v; return now; }
function normalizeUrlForCompare(url) {
  try {
    const u = new URL(url);
    u.hash = "";
    const params = [...u.searchParams.entries()].sort((a,b)=>a[0].localeCompare(b[0]));
    u.search = params.map(([k,v])=>`${encodeURIComponent(k)}=${encodeURIComponent(v)}`).join("&");
    if (u.pathname.length > 1 && u.pathname.endsWith("/")) u.pathname = u.pathname.slice(0, -1);
    return u.origin + u.pathname + (u.search ? `?${u.search}` : "");
  } catch { return String(url || ""); }
}
function isSamePageIgnoringFragment(prevUrl, newUrl) {
  try {
    if (!prevUrl || !newUrl) return false;
    const a = new URL(prevUrl), b = new URL(newUrl);
    return a.origin === b.origin && a.pathname === b.pathname && a.search === b.search;
  } catch { return false; }
}

/* ===== Logging ===== */
const telemetry = [];
function log(tag, data) {
  try {
    if (!SETTINGS?.logTags || SETTINGS.logTags[tag] !== true) return;
    const sig = JSON.stringify({ tag, data });
    const t = nowMs();
    const gap = t - (log.__lastSigTs || 0);
    if (log.__lastSig === sig && gap < SETTINGS.logDuplicateSuppressMs) return;
    log.__lastSig = sig; log.__lastSigTs = t;
    console.log(`[${new Date().toISOString()}] ${tag}`, data ?? "");
    telemetry.push({ ts: t, tag, data });
  } catch {}
}

/* ===== Settings & storage ===== */
const SITE = { KAKUYOMU: "kakuyomu.jp", HAMELN: "syosetu.org", PIXIV: "pixiv.net", NAROU: "syosetu.com" };
const KEY_TOTAL = "rt_total_ms";
const KEY_DAILY = "rt_daily_ms";
const KEY_LOG = "rt_daily_log";
const KEY_DETAILS = "rt_details";
const KEY_SITE_ENABLE = "rt_site_enable";
const KEY_SETTINGS = "rt_settings";
const KEY_VERSION = "rt_version";

const DEFAULT_SETTINGS = {
  version: 62,
  minSessionMs: 4000,
  realtimeFlushMs: 15000,
  livePushIntervalMs: 1500,
  livePushMinGapMs: 500,
  titleStableMs: 500,
  visibilityStabilizeMs: 0,
  idleHoldMs: 20000,
  idleDiscardMs: 20 * 60 * 1000,
  pendingAbsorbWindowMs: 5000,
  pendingShortTimeoutMs: 15000,
  pendingSegmentTimeoutMs: 60000,
  heartbeatIntervalMs: 500,
  watchdogSinceBeatLimitMs: 3000,
  watchdogConsecutiveMissThreshold: 3,
  watchdogCheckIntervalMs: 800,
  watchdogStartGraceMs: 7000,
  watchdogSuppressRecentInteractionMs: 4000,
  globalScanIntervalMs: 900,
  focusPollIntervalMs: 180,
  startGraceMs: 500,
  recentInteractionSkipStartGraceMs: 2000,
  idleResumeGraceMs: 2000,
  narouRateWindowMs: 60000,
  narouRateMaxPerWindow: 20,
  narouCacheTtlMs: 24 * 60 * 60 * 1000,
  narouAwaitCertMaxWaitMs: 15000,
  narouAwaitCertRetryIntervalMs: 1200,
  androidMode: true,
  promoteDebounceMs: 600,
  logDuplicateSuppressMs: 250,
  commitOnCloseBelowMin: true,
  logTags: {
    "session.bootstrap": true,
    "session.provisional": true,
    "session.promote": true,
    "reading.start": true,
    "reading.pause": true,
    "reading.stop": true,
    "commit.saved": true,
    "pending.store": true,
    "pending.absorb": true,
    "pending.drop": true,
    "awaitCert.promote": true,
    "awaitCert.expire": true,
    "meta.apply": true,
    "meta.apply.stale": true, // enable to observe stale decisions
    "meta.parse.error": true,
    "title.stable": true,
    "title.promote.block": true,
    "interaction": true,
    "visible.transition": true,
    "tab.closed": true,
    "tab.closed.poll": true
  }
};
let SETTINGS = { ...DEFAULT_SETTINGS };

function storageGet(keys) {
  return new Promise(res => {
    try {
      const p = B?.storage?.local?.get?.(keys);
      if (isPromiseLike(p)) p.then(v => res(v || {})).catch(() => res({}));
      else B.storage.local.get(keys, o => res(o || {}));
    } catch { res({}); }
  });
}
const writeQ = []; let writeBusy = false;
function mergeObject(target, patch) { for (const k of Object.keys(patch)) target[k] = patch[k]; }
function storageSetQueued(obj) { writeQ.push(obj); processWriteQ(); }
async function processWriteQ() {
  if (writeBusy || writeQ.length === 0) return;
  writeBusy = true;
  try {
    const batch = {}; while (writeQ.length) mergeObject(batch, writeQ.shift());
    await new Promise(r => {
      try {
        const p = B?.storage?.local?.set?.(batch);
        if (isPromiseLike(p)) p.then(() => r()).catch(() => r());
        else B.storage.local.set(batch, () => r());
      } catch { r(); }
    });
  } finally { writeBusy = false; if (writeQ.length) processWriteQ(); }
}
async function getLocal(key, def) { const o = await storageGet([key]); return Object.prototype.hasOwnProperty.call(o, key) ? o[key] : def; }

let storageReady = initStorage();
async function initStorage() {
  const init = {
    [KEY_TOTAL]: 0, [KEY_DAILY]: 0, [KEY_LOG]: {}, [KEY_DETAILS]: {},
    [KEY_SITE_ENABLE]: { [SITE.KAKUYOMU]: true, [SITE.HAMELN]: true, [SITE.PIXIV]: true, [SITE.NAROU]: true },
    [KEY_SETTINGS]: DEFAULT_SETTINGS, [KEY_VERSION]: DEFAULT_SETTINGS.version
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
      ? { ...DEFAULT_SETTINGS, ...cfg, version: DEFAULT_SETTINGS.version }
      : { ...DEFAULT_SETTINGS };
    storageSetQueued({ [KEY_SETTINGS]: SETTINGS });
    applySettings();
  } catch {
    SETTINGS = { ...DEFAULT_SETTINGS };
    applySettings();
  }
}
function applySettings() {
  try { if (_commitInterval) clearInterval(_commitInterval); } catch {}
  try { if (liveUpdateInterval) clearInterval(liveUpdateInterval); } catch {}
  try { if (watchdogInterval) clearInterval(watchdogInterval); } catch {}
  try { if (globalScanTimer) clearInterval(globalScanTimer); } catch {}
  setupAlarms(); setupIntervals(); setupWatchdog(); setupGlobalScanner();
}

/* ===== Domain helpers ===== */
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
function isCandidateUrl(url) {
  if (isInternal(url)) return false;
  const dom = getDomain(url); if (!dom) return false;
  try {
    const u = new URL(url);
    if (dom === SITE.KAKUYOMU) return /^\/works\/\d+\/episodes\/\d+\/?$/i.test(u.pathname) || /^\/works\/\d+\/?$/i.test(u.pathname);
    if (dom === SITE.HAMELN) return /^\/novel\/\d+(?:\/\d+\.html|\/?)$/i.test(u.pathname);
    if (dom === SITE.PIXIV) return u.pathname === "/novel/show.php" && u.searchParams.has("id");
    if (dom === SITE.NAROU) return /^\/(n[0-9a-z]+)\/(?:\d+\/)?$/i.test(u.pathname);
    return false;
  } catch { return false; }
}

/* ===== Tab state ===== */
const tabState = new Map();
function makeStateMinimal(tabId, url, title, windowId) {
  const now = nowMs();
  return {
    tabId, windowId: windowId ?? null,
    urlObserved: url || "", urlConfirmed: "", title: title || "",
    isCandidate: isCandidateUrl(url || ""),
    meta: { isContent: false, cert: "none", site: "", workTitle: "", episodeTitle: "", author: "", ncode: undefined, pixivId: undefined },
    // reading session
    reading: false,
    sessionId: null,
    sessionStartTs: undefined,
    activeStartTs: undefined,
    accumMs: 0,
    committedMs: 0,
    lastFlushAt: undefined,
    contentUrlAtStart: "",
    lastStableTitle: "",
    // visibility & interactions
    pageHidden: false,
    becameVisibleAt: now,
    lastVisibilityMsgAt: now,
    lastInteraction: now,
    // pending, await-cert
    pending: null,
    awaitCert: null,
    // timers
    stopTimer: undefined,
    idleTimer: undefined,
    // gates/grace
    _graceUntil: now + SETTINGS.startGraceMs,
    _idleResumeGraceUntil: 0,
    _stoppedForResumeGate: false,
    // title debounce
    _promoteTitleDebounce: new Map(),
    _lastMetaSig: ""
  };
}
async function ensureStateFromTab(tabId) {
  let st = tabState.get(tabId);
  if (st) return st;
  try { const t = await B.tabs.get(tabId); st = makeStateMinimal(tabId, t.url || "", t.title || "", t.windowId); }
  catch { st = makeStateMinimal(tabId, "", "", null); }
  tabState.set(tabId, st);
  return st;
}

/* ===== Title stability ===== */
const titleStableMap = new Map(); // tabId -> { last, firstTs, stable }
function titleStableMsFor(site) {
  return site === SITE.PIXIV ? 200 : SETTINGS.titleStableMs;
}
function updateTitleStability(tabId, newTitle, now, site) {
  const t = cleanWhitespace(newTitle || "");
  let s = titleStableMap.get(tabId) || { last: "", firstTs: now, stable: false };

  if (!t) {
    s = { last: "", firstTs: now, stable: false };
    titleStableMap.set(tabId, s);
    return "";
  }
  if (s.last !== t) s = { last: t, firstTs: now, stable: false };

  const needMs = titleStableMsFor(site);
  const span = now - s.firstTs;
  const isStableNow = span >= needMs;

  if (isStableNow && !s.stable) {
    s.stable = true;
    const st = tabState.get(tabId);
    if (!st || st.lastStableTitle !== t) {
      log("title.stable", { tabId, title: t, spanMs: span });
      titleStableMap.set(tabId, s);
      return t;
    }
  }

  titleStableMap.set(tabId, s);
  const st = tabState.get(tabId);
  if (isStableNow && st && st.lastStableTitle !== t) {
    return t;
  }
  return "";
}

/* ===== Site enable cache ===== */
const siteEnableCache = { value: null, ts: 0 };
async function isDomainEnabled(domain) {
  await storageReady; const n = nowMs();
  if (!siteEnableCache.value || n - siteEnableCache.ts > 5000) {
    const obj = await storageGet([KEY_SITE_ENABLE]);
    siteEnableCache.value = obj[KEY_SITE_ENABLE] || {}; siteEnableCache.ts = n;
  }
  return !!(siteEnableCache.value || {})[domain];
}

/* ===== Pending windows ===== */
function schedulePendingDrop(tabId) {
  const st = tabState.get(tabId); if (!st?.pending) return;
  // actual drop happens via evaluateVisibility/global scan
}
function tryAbsorbPendingOnResume(st, now) {
  if (!st?.pending) return false;

  const base = Number(st.pending.stop ?? st.pending.queuedAt ?? now);
  const age = now - base;

  const limit = (st.pending.kind === "segment")
    ? Math.max(SETTINGS.pendingAbsorbWindowMs, 15000)
    : SETTINGS.pendingAbsorbWindowMs;

  if (!Number.isFinite(base) || st.pending.ms <= 0 || age > limit) {
    log("pending.drop", {
      tabId: st.tabId,
      kind: st.pending.kind,
      ms: st.pending.ms,
      ageMs: age,
      reason: "resume_discard"
    });
    st.pending = null;
    return false;
  }

  st.accumMs = (st.accumMs || 0) + st.pending.ms;
  log("pending.absorb", {
    tabId: st.tabId,
    kind: st.pending.kind,
    ms: st.pending.ms,
    ageMs: age
  });
  st.pending = null;
  return true;
}

/* ===== Meta parse ===== */
async function parseMeta(url, title) {
  const dom = getDomain(url); if (!dom) return { isContent: false, cert: "none", site: "", workTitle: "", episodeTitle: "", author: "" };
  const u = new URL(url);
  if (dom === SITE.PIXIV) return parsePixiv(u, title);
  if (dom === SITE.NAROU) return parseNarou(u, title);
  if (dom === SITE.KAKUYOMU) return parseKakuyomu(u, title);
  if (dom === SITE.HAMELN) return parseHameln(u, title);
  return { isContent: false, cert: "none", site: dom, workTitle: "", episodeTitle: "", author: "" };
}
function metaSig(m) {
  if (!m) return "";
  const pick = {
    site: m.site || "", isContent: !!m.isContent, cert: m.cert || "",
    workTitle: m.workTitle || "", episodeTitle: m.episodeTitle || "", author: m.author || "",
    ncode: m.ncode || "", pixivId: m.pixivId || ""
  };
  try { return JSON.stringify(pick); } catch { return `${pick.site}|${pick.cert}|${pick.workTitle}|${pick.episodeTitle}|${pick.author}|${pick.ncode}|${pick.pixivId}`; }
}
async function safeParseAndApply(tabId, url, title, now) {
  const st = tabState.get(tabId); if (!st) return;
  const curUrl = st.urlObserved; // prefer observed URL for SPA consistency
  const curTitle = st.title;

  try {
    const meta = await parseMeta(url, title);

    // stale check: only require URL equality; title may differ while stabilizing
    if (url !== curUrl) {
      if (SETTINGS.logTags["meta.apply.stale"]) log("meta.apply.stale",{ tabId, url, title });
      return;
    }

    const sig = metaSig(meta);
    const changed = (sig !== st._lastMetaSig);
    st.meta = meta;
    if (changed && meta.cert !== "none") log("meta.apply",{ tabId, cert: meta.cert, site: meta.site });
    st._lastMetaSig = sig;
    tabState.set(tabId, st);
  } catch (e) { log("meta.parse.error",{ tabId, url, error: String(e) }); }
}
function metaMatchesUrl(st) {
  try {
    // prefer latest observed URL to avoid mismatch during SPA transitions
    const url = st.urlObserved || st.urlConfirmed || "";
    const dom = getDomain(url); const meta = st.meta || {};
    if (!dom || meta.site !== dom) return false;
    if (dom === SITE.PIXIV) {
      const u = new URL(url);
      const idInUrl = u.searchParams.get("id") || null;
      const idInMeta = meta.pixivId ?? null;
      return !!(idInUrl && idInMeta && idInUrl === idInMeta);
    }
    if (dom === SITE.NAROU) {
      const m = new URL(url).pathname.match(/^\/(n[0-9a-z]+)\/?/i);
      const ncodeInUrl = m ? m[1] : null;
      const ncodeInMeta = meta.ncode ?? null;
      return !!(ncodeInUrl && ncodeInMeta && ncodeInUrl.toLowerCase() === ncodeInMeta.toLowerCase());
    }
    return true;
  } catch { return false; }
}

/* ===== Pixiv parsing ===== */
function stripPixivLeadingTags(rawTitle) {
  let s = String(rawTitle || "").trim();
  s = s.replace(/^(\s*#\S+(?:\([^)]+\))?\s*)+/i, "").trim();
  return s;
}
function stripPixivLeadingTags(rawTitle) {
  // 先頭のハッシュタグ群を除去。ただし #<数字> は話数の可能性があるため残す
  // 例: "#ブルアカ #ヤンデレ もしも..." は除去、"#1 俺は..." は残す
  let s = String(rawTitle || "").trim();
  s = s.replace(/^(\s*#(?!\d+\b)\S+(?:\([^)]+\))?\s*)+/u, "").trim();
  return s;
}
function parsePixiv(u, rawTitle) {
  const site = SITE.PIXIV;

  // 1) URL gate: 小説本文ページのみ対象
  const pathname = String(u.pathname || "");
  const search = u.searchParams || new URLSearchParams();
  const isNovelContent = /^\/novel\/show\.php$/i.test(pathname) && search.has("id");
  const pixivId = search.get("id") || undefined;

  if (!isNovelContent) {
    // 明確に本文ページではない
    return { isContent: false, cert: "none", site, workTitle: "", episodeTitle: "", author: "", pixivId: undefined };
  }

  // 2) タイトル前処理 & プレースホルダ拒否
  const raw = String(rawTitle || "").trim();
  if (!raw || /^\[pixiv\]/i.test(raw) || /ローディング中/i.test(raw)) {
    return { isContent: true, cert: "url", site, workTitle: "", episodeTitle: "", author: "", pixivId };
  }

  // 3) シリーズ形式（タグ除去前）
  let m = raw.match(/^#(\d+)\s+(.+?)\s*\|\s*(.+?)(?:\s*-\s*.+)?$/u);
  if (m) {
    const epNumber = m[1];
    const epSubtitle = m[2].replace(/\u3000/g, " ").replace(/\s+/g, " ").trim();
    const workTitle = m[3].replace(/\u3000/g, " ").replace(/\s+/g, " ").trim();
    const episodeTitle = `#${epNumber} ${epSubtitle}`;
    return { isContent: true, cert: "title", site, workTitle, episodeTitle, author: "", pixivId };
  }

  // 4) 単発処理：タグ除去 → サフィックス除去 → 抽出
  let s = raw.replace(/^(\s*#\S+(?:\([^)]+\))?\s*)+/u, "").trim();

  // 「- pixiv」や「- 作者の小説 - pixiv」を後方から落とす
  s = s.replace(/\s*-\s*pixiv\s*$/iu, "")
       .replace(/\s*-\s*[^-]*?の小説\s*-\s*pixiv\s*$/iu, "")
       .trim();

  // 「副題 | 作品」（話数なし前提）
  m = s.match(/^(?!#\d+\s+)(.+?)\s*\|\s*(.+?)(?:\s*-\s*.+)?$/u);
  if (m) {
    const episodeTitle = m[1].replace(/\u3000/g, " ").replace(/\s+/g, " ").trim();
    const workTitle = m[2].replace(/\u3000/g, " ").replace(/\s+/g, " ").trim();
    return { isContent: true, cert: "title", site, workTitle, episodeTitle, author: "", pixivId };
  }

  // 「タイトル - 作者の小説 - pixiv」など
  m = s.match(/^(.+?)\s*-\s*.+$/u);
  if (m) {
    const title = m[1].replace(/\u3000/g, " ").replace(/\s+/g, " ").trim();
    return { isContent: true, cert: "title", site, workTitle: title, episodeTitle: title, author: "", pixivId };
  }

  // 5) フォールバック（本文だが構造不明）
  const title = s.replace(/\u3000/g, " ").replace(/\s+/g, " ").trim();
  if (title) {
    return { isContent: true, cert: "title", site, workTitle: title, episodeTitle: title, author: "", pixivId };
  }
  return { isContent: true, cert: "url", site, workTitle: "", episodeTitle: "", author: "", pixivId };
}


/* Hameln */
function parseHameln(u, title, doc) {
  const site = SITE.HAMELN;
  const path = u.pathname;

  const isSerial = /^\/novel\/\d+\/\d+\.html$/i.test(path);
  const isTopOrShort = /^\/novel\/\d+\/?$/i.test(path);

  // 成人確認ページなどの除外
  const isGenericTitle = /^ハーメルン\s*-\s*SS･小説投稿サイト-?$/i.test(String(title || "").trim());
  const hasContent = doc && !!doc.querySelector("#novel_contents, #novel_honbun");

  if (isGenericTitle || !hasContent) {
    return { isContent: false, cert: "none", site, workTitle: "", episodeTitle: "", author: "" };
  }

  // ここから先は従来通り
  const trimmed = String(title || "").replace(/\s*-\s*ハーメルン$/i, "").trim();
  const parts = trimmed.split(/\s+-\s+/).map(s => cleanWhitespace(s));

  if (isSerial) {
    const work = cleanWhitespace(parts[0] || "");
    const ep = cleanWhitespace(parts[1] || "");
    const ok = !!(work && ep);
    return { isContent: ok, cert: ok ? "title" : "url", site, workTitle: work, episodeTitle: ep, author: "" };
  }

  if (isTopOrShort) {
    const work = cleanWhitespace(parts[0] || "");
    const ep = cleanWhitespace(parts[1] || "");
    if (parts.length >= 2) {
      return { isContent: true, cert: "title", site, workTitle: work, episodeTitle: ep, author: "" };
    } else {
      return { isContent: false, cert: work ? "title" : "none", site, workTitle: work, episodeTitle: "", author: "" };
    }
  }

  return { isContent: false, cert: "none", site, workTitle: "", episodeTitle: "", author: "" };
}


/* Kakuyomu */
function parseKakuyomu(u, title) {
  const site = SITE.KAKUYOMU; const path = u.pathname;
  const isEpisode = /^\/works\/\d+\/episodes\/\d+\/?$/i.test(path);
  const isTop = /^\/works\/\d+\/?$/i.test(path);
  if (/^https?:\/\//.test(title) || String(title).startsWith("kakuyomu.jp/")) return { isContent: isEpisode, cert: isEpisode ? "url" : "none", site, workTitle: "", episodeTitle: "", author: "" };
  const t = String(title || "").replace(/\s*-\s*カクヨム$/i, "").trim();
  const parts = String(t || "").split(/\s+-\s+/).map(s => cleanWhitespace(s));
  if (isEpisode) {
    const subtitle = cleanWhitespace(parts[0] || "");
    const wa = (parts[1] || "").trim();
    const m = String(wa || "").trim().match(/^(.*)（(.*)）$/);
    const work = cleanWhitespace(m ? m[1] : wa);
    const author = cleanWhitespace(m ? m[2] : "");
    const ok = !!(subtitle && work);
    return { isContent: ok, cert: ok ? "title" : "url", site, workTitle: ok ? work : "", episodeTitle: ok ? subtitle : "", author: ok ? author : "" };
  }
  if (isTop) {
    const wa2 = (parts[0] || "").trim();
    const m2 = String(wa2 || "").trim().match(/^(.*)（(.*)）$/);
    const work2 = cleanWhitespace(m2 ? m2[1] : wa2);
    const author2 = cleanWhitespace(m2 ? m2[2] : "");
    return { isContent: false, cert: work2 ? "title" : "none", site, workTitle: work2, episodeTitle: "", author: author2 };
  }
  return { isContent: false, cert: "none", site, workTitle: "", episodeTitle: "", author: "" };
}

/* Narou */
const narouApi = (() => {
  const cache = new Map(); const queue = [];
  let processing = false; let bucket = { ts: 0, count: 0 };
  function refillWindow() { const n = nowMs(); if (n - bucket.ts > SETTINGS.narouRateWindowMs) { bucket.ts = n; bucket.count = 0; } return bucket.count < SETTINGS.narouRateMaxPerWindow; }
  async function tick() {
    if (processing) return; processing = true;
    try {
      while (queue.length) {
        if (!refillWindow()) { const sleepMs = Math.max(0, SETTINGS.narouRateWindowMs - (nowMs() - bucket.ts)); await new Promise(r => setTimeout(r, sleepMs)); continue; }
        const { ncode, resolve } = queue.shift();
        const cached = cache.get(ncode);
        if (cached && (nowMs() - cached.ts) < SETTINGS.narouCacheTtlMs) { resolve(cached.info); continue; }
        try {
          bucket.count++;
          const url = `https://api.syosetu.com/novelapi/api/?out=json&of=t-w-nt&ncode=${encodeURIComponent(ncode)}`;
          const res = await fetch(url, { method: "GET" });
          const json = await res.json();
          const info = Array.isArray(json) && json.length >= 2 ? json[1] : null;
          cache.set(ncode, { info, ts: nowMs() }); resolve(info)
        } catch { cache.set(ncode, { info: null, ts: nowMs() }); resolve(null); }
      }
    } finally { processing = false; }
  }
  function get(ncode) {
    const cached = cache.get(ncode);
    if (cached && (nowMs() - cached.ts) < SETTINGS.narouCacheTtlMs) { return Promise.resolve(cached.info); }
    return new Promise((resolve) => { queue.push({ ncode, resolve }); tick(); });
  }
  return { get };
})();
async function parseNarou(u, title) {
  const site = SITE.NAROU;
  let m = u.pathname.match(/^\/(n[0-9a-z]+)\/(\d+)\/?$/i);
  if (m) {
    const ncode = m[1];
    const epNo = m[2];
    const raw = String(title || "").replace(/\s*-\s*小説家になろう$/i, "").trim();
    const parts = raw.split(/\s*-\s+/).map(s => cleanWhitespace(s)).filter(Boolean);
    const cand = parts.filter(p => !/^第\d+話/.test(p));
    const work = cleanWhitespace((cand.length ? cand[cand.length - 1] : parts[0]) || "");
    const ep = `第${epNo}話`;
    const ok = !!(work && ep);
    return { isContent: ok, cert: ok ? "title" : "url", site, workTitle: work, episodeTitle: ep, author: "", ncode };
  }
  m = u.pathname.match(/^\/(n[0-9a-z]+)\/?$/i);
  if (m) {
    const ncode = m[1];
    const t = String(title || "").replace(/\s*-\s*小説家になろう$/i, "").trim();
    try {
      const info = await narouApi.get(ncode);
      const isShort = !!(info && Number(info.noveltype) === 2);
      const work = cleanWhitespace((info && info.title) || t);
      const author = cleanWhitespace((info && info.writer) || "");
      return { isContent: isShort, cert: isShort ? "title" : "url", site, workTitle: work, episodeTitle: "", author, ncode };
    } catch {
      return { isContent: false, cert: "url", site, workTitle: cleanWhitespace(t), episodeTitle: "", author: "", ncode };
    }
  }
  return { isContent: false, cert: "none", site, workTitle: "", episodeTitle: "", author: "" };
}

/* ===== Session timing helpers ===== */
function inflightMs(st, now) {
  return st.reading && st.activeStartTs ? Math.max(0, now - st.activeStartTs) : 0;
}
function uncommittedMs(st, now) {
  return Math.max(0, (st.accumMs + inflightMs(st, now)) - (st.committedMs || 0));
}
function pauseReading(tabId, now, reason = "PAUSE") {
  const st = tabState.get(tabId); if (!st?.reading) return;

  clearIdleTimer(tabId);

  const add = inflightMs(st, now);
  st.reading = false;
  st.activeStartTs = undefined;

  st._stoppedForResumeGate = (reason === "IDLE_HOLD");

  if (st.meta?.isContent && st.meta?.cert === "title" && add > 0) {
    const minSeg = SETTINGS.pendingSegmentMinMs || 250;
    if (add >= minSeg) {
      st.pending = {
        kind: "segment",
        ms: add,
        stop: reason === "IDLE_HOLD"
          ? (st.lastInteraction || now) + SETTINGS.idleHoldMs
          : now,
        site: st.meta.site,
        workTitle: st.meta.workTitle,
        episodeTitle: st.meta.episodeTitle,
        author: st.meta.author || "",
        sessionId: st.sessionId,
        ncode: st.meta.ncode,
        pixivId: st.meta.pixivId,
        url: st.contentUrlAtStart || st.urlConfirmed || st.urlObserved || ""
      };
      schedulePendingDrop(tabId);
      log("pending.store", { tabId: st.tabId, sessionId: st.sessionId, ms: add, reason });
    }
  }

  tabState.set(tabId, st);
  log("reading.pause", { tabId, sessionId: st.sessionId, addMs: add, reason });
}

function clearIdleTimer(tabId) { const st = tabState.get(tabId); if (!st) return; if (st.idleTimer) { clearTimeout(st.idleTimer); st.idleTimer = undefined; tabState.set(tabId, st); } }
function resetIdleTimer(tabId) {
  const st = tabState.get(tabId); if (!st) return;
  clearIdleTimer(tabId);
  st.idleTimer = setTimeout(() => {
    withTabQueue(tabId, async () => {
      const cur = tabState.get(tabId); if (!cur) return;
      const now = nowMs();
      if (cur.reading) pauseReading(cur.tabId, now, "IDLE_HOLD");
    });
  }, SETTINGS.idleHoldMs);
  tabState.set(tabId, st);
}

/* ===== Commit helpers ===== */
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
async function commitDelta(st, deltaMs, now, reason = "FLUSH") {
  if (deltaMs <= 0) return;
  const meta = st.meta || {};
  if (!meta?.isContent || meta?.cert !== "title") return;

  await storageReady;
  const parts = splitAcrossDays(now - deltaMs, now, deltaMs);
  const total = await getLocal(KEY_TOTAL, 0);
  const logs = await getLocal(KEY_LOG, {});
  const details = await getLocal(KEY_DETAILS, {});
  const today = dayKey(now);
  let addTotal = 0;

  for (const part of parts) {
    if (part.ms <= 0) continue;
    const list = details[part.day] || [];
    const urlForRecord = st.contentUrlAtStart || st.urlConfirmed || st.urlObserved || "";
    const same = r =>
      r.site === meta.site &&
      r.workTitle === meta.workTitle &&
      r.episodeTitle === meta.episodeTitle &&
      r.url === urlForRecord &&
      r.pixivId === meta.pixivId &&
      r.ncode === meta.ncode &&
      r.sessionId === st.sessionId;
    const ex = list.find(same);
    if (ex) {
      ex.ms += part.ms; ex.ts = part.ts;
      if (!ex.url) ex.url = urlForRecord;
    } else {
      list.push({
        site: meta.site, workTitle: meta.workTitle, episodeTitle: meta.episodeTitle,
        author: meta.author || "", sessionId: st.sessionId, ms: part.ms, ts: part.ts,
        url: urlForRecord, pixivId: meta.pixivId, ncode: meta.ncode
      });
    }
    details[part.day] = list;
    logs[part.day] = (logs[part.day] || 0) + part.ms;
    addTotal += part.ms;
  }
  storageSetQueued({ [KEY_TOTAL]: total + addTotal, [KEY_DAILY]: (logs[today] || 0), [KEY_LOG]: logs, [KEY_DETAILS]: details });
  log("commit.saved",{ tabId: st.tabId, sessionId: st.sessionId, ms: addTotal, site: meta.site, reason });
  pushLiveUpdate();
}

/* ===== Reading lifecycle ===== */
async function canStart(st, now) {
  if (!st) return false;

  const domain = getDomain(st.urlObserved || st.urlConfirmed);
  const stableOk = !!st.lastStableTitle;
  const visOk = !st.pageHidden && (
    st.becameVisibleAt
      ? (now - st.becameVisibleAt >= SETTINGS.visibilityStabilizeMs)
      : true
  );

  const enabled = domain ? await isDomainEnabled(domain) : false;

  const inStartGrace = st._graceUntil && now < st._graceUntil;
  const recentGap = now - (st.lastInteraction || 0);
  const canSkipGrace = recentGap <= SETTINGS.recentInteractionSkipStartGraceMs;
  const graceOk = !inStartGrace || canSkipGrace;

  const resumeGateOk = !st._stoppedForResumeGate;

  return !!(
    enabled &&
    st.isCandidate &&
    st.meta?.isContent &&
    st.meta?.cert === "title" &&
    visOk &&
    stableOk &&
    graceOk &&
    resumeGateOk
  );
}
function canContinue(st, now) {
  if (!st.reading) return false;
  const visOk = !st.pageHidden;
  const recentGap = now - (st.lastInteraction || 0);
  return visOk && recentGap < SETTINGS.idleHoldMs;
}

async function startOrResumeReading(tabId, now) {
  const st = tabState.get(tabId); if (!st || st.reading) return;
  if (!(await canStart(st, now))) return;

  const isResume = !!st.sessionId;

  st.sessionId = st.sessionId || cryptoId();
  st.sessionStartTs = st.sessionStartTs || now;
  st.activeStartTs = now;
  st.reading = true;
  st.contentUrlAtStart = st.contentUrlAtStart || (st.urlObserved || st.urlConfirmed || "");
  st.committedMs = st.committedMs || 0;

  tryAbsorbPendingOnResume(st, now);

  st.lastFlushAt = now;
  st._idleResumeGraceUntil = now + SETTINGS.idleResumeGraceMs;

  st._stoppedForResumeGate = false;
  st.lastInteraction = now;

  tabState.set(tabId, st);
  resetIdleTimer(tabId);

  log("session.promote", {
    tabId, sessionId: st.sessionId, title: st.lastStableTitle, site: st.meta.site,
    resume: isResume === true
  });
  log("reading.start", { tabId, sessionId: st.sessionId, resume: isResume === true });
}

async function stopReading(tabId, now, reason = "STOP") {
  const st = tabState.get(tabId); if (!st?.sessionId) return;

  if (st.reading) {
    const add = inflightMs(st, now);
    st.accumMs += add;
    st.reading = false;
    st.activeStartTs = undefined;
  }

  const delta = uncommittedMs(st, now);
  if (st.meta?.isContent && st.meta?.cert === "title" && delta > 0) {
    const forceCommit = (reason === "TAB_REMOVED" && SETTINGS.commitOnCloseBelowMin === true);
    if (delta >= SETTINGS.minSessionMs || forceCommit) {
      await commitDelta(st, delta, now, reason);
      st.committedMs += delta;
    } else {
      st.pending = {
        kind: "short", ms: delta, stop: now, expiresAt: now + SETTINGS.pendingShortTimeoutMs,
        site: st.meta.site, workTitle: st.meta.workTitle, episodeTitle: st.meta.episodeTitle, author: st.meta.author,
        sessionId: st.sessionId, ncode: st.meta.ncode, pixivId: st.meta.pixivId,
        url: st.contentUrlAtStart || st.urlConfirmed || st.urlObserved || ""
      };
      schedulePendingDrop(tabId);
      log("pending.store",{ tabId: st.tabId, sessionId: st.sessionId, ms: delta, reason: "SHORT_COMMIT", stopReason: reason });
    }
  }

  // セッション完全終了
  st.accumMs = 0; st.committedMs = 0; st.pending = null; st.sessionId = null; st.sessionStartTs = undefined;
  st.contentUrlAtStart = ""; st.lastStableTitle = "";
  st._promoteTitleDebounce = new Map();
  tabState.set(tabId, st);

  log("reading.stop",{ tabId, elapsedMs: delta, reason, site: st.meta?.site });
}

/* ===== Promotion ===== */
function promoteStableTitle(tabId, stableTitle, now) {
  const st = tabState.get(tabId); if (!st) return;
  const titleClean = cleanWhitespace(stableTitle || "");

  const prev = st.contentUrlAtStart || st.urlConfirmed || st.urlObserved || "";
  const cur = st.urlObserved || "";
  const prevKey = normalizeUrlForCompare(prev);
  const curKey  = normalizeUrlForCompare(cur);

  if (prev && cur && prev !== cur && prevKey === curKey) {
    st.urlConfirmed = st.urlConfirmed || st.urlObserved || "";
    st.contentUrlAtStart = st.contentUrlAtStart || st.urlConfirmed;
  }

  // metaとURLの整合性（observed優先で評価）
  if (!metaMatchesUrl(st)) {
    try {
      const u = new URL(st.urlObserved || "");
      const urlId = u.searchParams.get("id");
      const metaId = st.meta?.pixivId;
      log("title.promote.block", { tabId, reason: "metaUrlMismatch", urlObserved: st.urlObserved, urlConfirmed: st.urlConfirmed, urlId, metaId });
    } catch {
      log("title.promote.block", { tabId, reason: "metaUrlMismatch" });
    }
    return;
  }

  if (st.reading && st.lastStableTitle === titleClean && prevKey === curKey) return;

  const debounceMs = st.meta?.site === SITE.PIXIV ? 200 : SETTINGS.promoteDebounceMs;
  const lastAttempt = st._promoteTitleDebounce.get(titleClean) || 0;
  if (now - lastAttempt < debounceMs) {
    log("title.promote.block", { tabId, reason: "debounce", msSince: now - lastAttempt });
    return;
  }
  st._promoteTitleDebounce.set(titleClean, now);

  // タイトルとURL確定（SPA整合のため観測URLを確定へ反映）
  st.lastStableTitle = titleClean;
  st.urlConfirmed = st.urlObserved || st.urlConfirmed;
  tabState.set(tabId, st);

  startOrResumeReading(tabId, now);
}

/* ===== Unified change handler ===== */
async function onUrlOrTitleChange(tabId, newUrl, newTitle) {
  await withTabQueue(tabId, async () => {
    const now = nowMs();
    let st = tabState.get(tabId);
    if (!st) {
      let windowId = undefined; try { const t = await B.tabs.get(tabId); windowId = t?.windowId; } catch {}
      st = makeStateMinimal(tabId, newUrl || "", newTitle || "", windowId);
      tabState.set(tabId, st);
      log("session.bootstrap",{ tabId, url: st.urlObserved, title: st.title });
    }

    const prevUrl = st.urlObserved || "";
    const urlChanged = prevUrl !== (newUrl || prevUrl);
    const titleChanged = st.title !== (newTitle ?? st.title);

    if (urlChanged) {
      const fragmentOnlyEq = isSamePageIgnoringFragment(prevUrl, newUrl);
      if (!fragmentOnlyEq) {
        await stopReading(tabId, now, "NAVIGATION");
        titleStableMap.set(tabId, { last: "", firstTs: now, stable: false });
        st.lastStableTitle = "";
        st._promoteTitleDebounce = new Map();
        st.accumMs = 0; st.committedMs = 0; st.sessionId = null; st.sessionStartTs = undefined;
        st.contentUrlAtStart = "";
        st.pending = null; st.awaitCert = null;
      }
      st.urlObserved = newUrl;
    }
    if (titleChanged) st.title = newTitle || "";
    st.isCandidate = isCandidateUrl(st.urlObserved);
    tabState.set(tabId, st);

    await safeParseAndApply(tabId, st.urlObserved, st.title || "", now);

    const stable = updateTitleStability(tabId, st.title || "", now, st.meta?.site);
    if (stable && st.meta?.cert === "title") promoteStableTitle(tabId, stable, now);

    await evaluateVisibility(tabId, now);
  });
}

/* ===== Visibility evaluation ===== */
async function evaluateVisibility(tabId, now) {
  const st = tabState.get(tabId); if (!st) return;

  if (st.pending) {
    const timeoutMs = st.pending.expiresAt
      ? Math.max(0, st.pending.expiresAt - now)
      : (st.pending.kind === "short"
          ? SETTINGS.pendingShortTimeoutMs
          : st.pending.kind === "segment"
            ? SETTINGS.pendingSegmentTimeoutMs
            : SETTINGS.narouAwaitCertMaxWaitMs);
    const base = st.pending.stop ?? st.pending.queuedAt ?? now;
    if ((now - base) > (timeoutMs || 0)) {
      log(st.pending.kind === "await_cert" ? "awaitCert.expire" : "pending.drop", { tabId: st.tabId, ageMs: (now - base) });
      st.pending = null; tabState.set(tabId, st);
    }
  }

  if (st.reading) {
    if (!canContinue(st, now)) {
      const reason = st.pageHidden ? "NOT_VISIBLE" : "IDLE_HOLD";
      pauseReading(tabId, now, reason);
      return;
    }
  } else {
    if (await canStart(st, now)) {
      await startOrResumeReading(tabId, now);
    }
  }
}

/* ===== Polling core ===== */
let globalScanTimer = null;
const focusedPollTimers = new Map();

async function pollTab(tabId, urlNow, titleNow) {
  const st = await ensureStateFromTab(tabId);
  if (urlNow !== (st.urlObserved || "") || titleNow !== (st.title || "")) {
    await onUrlOrTitleChange(tabId, urlNow, titleNow);
  } else {
    const now = nowMs();
    const stable = updateTitleStability(tabId, st.title || "", now, st.meta?.site);
    if (stable && st.meta?.cert === "title") promoteStableTitle(tabId, stable, now);
    await evaluateVisibility(tabId, now);
  }
}
function startFocusedPoll(tabId) {
  if (focusedPollTimers.has(tabId)) return;
  const timer = setInterval(async () => {
    try {
      const t = await B.tabs.get(tabId);
      if (!t) { stopFocusedPoll(tabId); return; }
      await pollTab(tabId, t.url || "", t.title || "");
    } catch {}
  }, SETTINGS.focusPollIntervalMs);
  focusedPollTimers.set(tabId, timer);
}
function stopFocusedPoll(tabId) { const timer = focusedPollTimers.get(tabId); if (timer) clearInterval(timer); focusedPollTimers.delete(tabId); }
function stopAllFocusedPolls() { for (const [id, timer] of focusedPollTimers) clearInterval(timer); focusedPollTimers.clear(); }
function setupGlobalScanner() {
  try {
    if (globalScanTimer) clearInterval(globalScanTimer);
    globalScanTimer = setInterval(async () => {
      try {
        const tabs = await B.tabs.query({});
        const seenIds = new Set();
        let hasCandidate = false;
        for (const t of tabs) {
          const tabId = t.id; if (!tabId) continue;
          seenIds.add(tabId);
          let st = tabState.get(tabId);
          if (!st) {
            st = makeStateMinimal(tabId, t.url || "", t.title || "", t.windowId);
            tabState.set(tabId, st);
          }
          const urlNow = t.url || "";
          const titleNow = t.title || "";
          await pollTab(tabId, urlNow, titleNow);
          if (isCandidateUrl(urlNow)) { hasCandidate = true; startFocusedPoll(tabId); }
          else { stopFocusedPoll(tabId); }
        }
        for (const [tid] of tabState) {
          if (!seenIds.has(tid)) cleanupTabState(tid, "poll.diff");
        }
        if (!hasCandidate) stopAllFocusedPolls();
      } catch {}
    }, SETTINGS.globalScanIntervalMs);
  } catch {}
}

/* ===== Intervals & live updates ===== */
let _commitInterval = null, liveUpdateInterval = null, _lastLiveSig = null, _lastLiveAt = 0;
function setupIntervals() {
  try {
    if (_commitInterval) clearInterval(_commitInterval);
    _commitInterval = setInterval(() => { commitAll().catch(()=>{}); }, SETTINGS.realtimeFlushMs);
    if (liveUpdateInterval) clearInterval(liveUpdateInterval);
    liveUpdateInterval = setInterval(() => { pushLiveUpdate().catch(()=>{}); }, SETTINGS.livePushIntervalMs);
  } catch {}
}
async function getLiveFinal() {
  await storageReady;
  const total = await getLocal(KEY_TOTAL, 0);
  const details = await getLocal(KEY_DETAILS, {});
  const logs = await getLocal(KEY_LOG, {});
  const today = dayKey(nowMs());
  const daily = logs[today] || 0;
  const recent = (details[today] || []).slice().sort((a, b) => b.ts - a.ts).slice(0, 30);
  const now = nowMs();
  const inflightSum = Array.from(tabState.values())
    .filter(st => st.meta?.isContent && st.meta?.cert === "title")
    .reduce((sum, st) => sum + uncommittedMs(st, now), 0);
  return { total, daily, inflightMs: inflightSum, recent };
}
async function runtimeSendMessageSafe(msg) { try { const p = B.runtime?.sendMessage?.(msg); if (isPromiseLike(p)) return p.catch(() => {}); } catch {} return Promise.resolve(); }
async function pushLiveUpdate() {
  const now = nowMs(); if (now - _lastLiveAt < SETTINGS.livePushMinGapMs) return;
  const payload = await getLiveFinal();
  const recentTopTs = payload.recent && payload.recent[0] ? payload.recent[0].ts : 0;
  const recentCount = (payload.recent || []).length;
  const recentSum = (payload.recent || []).reduce((s,x)=>s+x.ms,0);
  const sig = JSON.stringify({ total: payload.total, daily: payload.daily, inflightMs: payload.inflightMs, recentTopTs, recentCount, recentSum });
  if (sig === _lastLiveSig) return;
  _lastLiveSig = sig; _lastLiveAt = now;
  await runtimeSendMessageSafe({ type: "live-update", payload });
}

/* ===== Commit loop & alarms ===== */
async function commitAll() {
  try {
    const now = nowMs();
    for (const [tabId, st] of tabState) {
      if (!st.sessionId) continue;
      const delta = uncommittedMs(st, now);
      if (delta >= SETTINGS.minSessionMs && st.meta?.isContent && st.meta?.cert === "title") {
        await commitDelta(st, delta, now, "AUTO_FLUSH");
        st.committedMs += delta;
        st.lastFlushAt = now; tabState.set(tabId, st);
      }
    }
  } catch {}
}
function setupAlarms() {
  try {
    if (B?.alarms?.create) { B.alarms.clear("rt-commit"); B.alarms.create("rt-commit", { periodInMinutes: 0.25 }); }
  } catch {}
}
if (B?.alarms?.onAlarm) B.alarms.onAlarm.addListener((alarm) => { if (alarm?.name === "rt-commit") { commitAll().catch(()=>{}); } });

/* ===== Focus/activation ===== */
B.tabs.onActivated.addListener(({ tabId }) => {
  withTabQueue(tabId, async () => {
    const st = await ensureStateFromTab(tabId);
    const now = nowMs();
    const wasHidden = st.pageHidden;
    st.pageHidden = false;
    if (wasHidden) {
      st.becameVisibleAt = now;
      log("visible.transition", { tabId, atMs: now });
    }
    st.lastInteraction = now;
    tabState.set(tabId, st);
    resetIdleTimer(tabId);
    await evaluateVisibility(tabId, now);
  });
});

/* ===== Events as triggers ===== */
if (B?.tabs?.onUpdated) {
  B.tabs.onUpdated.addListener((tabId, changeInfo, tab) => {
    const urlNow = (typeof changeInfo.url === "string" && changeInfo.url) ? changeInfo.url : (tab?.url || "");
    const titleNow = (typeof changeInfo.title === "string") ? changeInfo.title : (tab?.title || "");
    if (isInternal(urlNow)) return;
    pollTab(tabId, urlNow, titleNow).catch(()=>{});
  });
}

/* ===== WebNavigation ===== */
(function setupWebNavigation() {
  const H = B.webNavigation; if (!H) return;
  const wrap = (ev) => H[ev]?.addListener?.((details) => {
    if (details.frameId !== 0) return;
    const tabId = details.tabId; const newUrl = details.url; if (!newUrl) return;
    const st = tabState.get(tabId);
    const titleNow = st?.title || "";
    pollTab(tabId, newUrl, titleNow).catch(()=>{});
  });
  ["onHistoryStateUpdated","onReferenceFragmentUpdated","onCommitted","onCompleted"].forEach(wrap);
})();

/* ===== Tab closed handling ===== */
function cleanupTabState(tabId, source = "unknown") {
  const st = tabState.get(tabId);
  if (st?.sessionId) {
    const now = nowMs();
    stopReading(tabId, now, "TAB_REMOVED").catch(()=>{});
  }
  clearIdleTimer(tabId);
  stopFocusedPoll(tabId);
  titleStableMap.delete(tabId);
  tabQueues.delete(tabId);
  tabState.delete(tabId);
  log(source === "poll.diff" ? "tab.closed.poll" : "tab.closed", { tabId, source });
}
if (B?.tabs?.onRemoved) {
  B.tabs.onRemoved.addListener((tabId) => {
    cleanupTabState(tabId, "onRemoved");
  });
}

/* ===== runtime.onMessage ===== */
async function resolveTabId(msg, sender) {
  if (typeof msg.tabId === "number") return msg.tabId;
  if (sender?.tab?.id) return sender.tab.id;
  return null;
}
B.runtime.onMessage.addListener((msg, sender, sendResponse) => {
  try { if (!msg || typeof msg !== "object") return false; } catch { return false; }
  const now = nowMs();
  const type = String(msg.type || "");

  switch (type) {
    case "heartbeat": {
      (async () => {
        const tabId = await resolveTabId(msg, sender);
        if (!tabId) return;
        withTabQueue(tabId, async () => {
          const st = await ensureStateFromTab(tabId);
          const nowLocal = nowMs();
          const wasHidden = st.pageHidden;
          st.pageHidden = (msg.visibilityState === "hidden");
          st.lastVisibilityMsgAt = nowLocal;

          if (wasHidden !== st.pageHidden) {
            if (!st.pageHidden) {
              st.becameVisibleAt = nowLocal;
              log("visible.transition", { tabId, atMs: nowLocal });
            } else {
              pauseReading(tabId, nowLocal, "NOT_VISIBLE");
            }
          }

          if (typeof msg.lastInteraction === "number") {
            const li = toEpochMs(msg.lastInteraction, nowLocal);
            if (li > (st.lastInteraction || 0)) {
              st.lastInteraction = li;
              if (st._stoppedForResumeGate) st._stoppedForResumeGate = false;
              log("interaction", { tabId, evType: "heartbeat-update", ts: li, site: st.meta?.site, reading: st.reading });
              if (st.reading) resetIdleTimer(tabId);
            }
          }

          st.isCandidate = isCandidateUrl(st.urlObserved);
          tabState.set(tabId, st);

          await pollTab(tabId, st.urlObserved, st.title || "");
        });
      })();
      return false;
    }

    case "scroll-activity": {
      (async () => {
        const tabId = await resolveTabId(msg, sender);
        if (!tabId) return;
        withTabQueue(tabId, async () => {
          const st = await ensureStateFromTab(tabId);
          const nowLocal = nowMs();

          let ts = nowLocal;
          if (typeof msg.ts === "number") ts = toEpochMs(msg.ts, nowLocal);
          st.lastInteraction = ts;

          if (st._stoppedForResumeGate) st._stoppedForResumeGate = false;

          tabState.set(tabId, st);
          if (st.reading) resetIdleTimer(tabId);

          log("interaction", { tabId, evType: msg.evType || "scroll", ts, site: st.meta?.site, reading: st.reading });

          await pollTab(tabId, st.urlObserved, st.title || "");
        });
      })();
      return true;
    }

    case "spa-url-change": { return false; }

    case "get-stats": { getLiveFinal().then(payload => { try { sendResponse(payload); } catch {} }); return true; }
    case "get-telemetry": { try { sendResponse({ telemetry }); } catch {} return true; }

    case "set-log-tags": {
      try {
        const next = { ...SETTINGS.logTags, ...(msg.tags || {}) };
        SETTINGS.logTags = next;
        storageSetQueued({ [KEY_SETTINGS]: { ...SETTINGS } });
        sendResponse({ ok: true, logTags: next });
      } catch (e) { try { sendResponse({ ok: false, error: String(e) }); } catch {} }
      return true;
    }
    case "get-log-tags": { try { sendResponse({ ok: true, logTags: SETTINGS.logTags }); } catch {} return true; }

    case "set-settings": {
      try {
        const next = { ...SETTINGS, ...msg.settings, version: DEFAULT_SETTINGS.version };
        SETTINGS = next;
        storageSetQueued({ [KEY_SETTINGS]: next }); applySettings();
        try { sendResponse({ ok: true, settings: next }); } catch {}
      } catch (e) { try { sendResponse({ ok: false, error: String(e) }); } catch {} }
      return true;
    }
    case "export-store": { storageGet([KEY_TOTAL, KEY_DAILY, KEY_LOG, KEY_DETAILS, KEY_SETTINGS, KEY_SITE_ENABLE]).then(snap => { try { sendResponse({ ok: true, snapshot: snap }); } catch {} }); return true; }
    case "import-store": {
      try {
        const snap = msg.snapshot || {};
        const allowed = new Set([KEY_TOTAL, KEY_DAILY, KEY_LOG, KEY_DETAILS, KEY_SETTINGS, KEY_SITE_ENABLE]);
        const safe = {};
        for (const k of Object.keys(snap)) { if (!allowed.has(k)) continue; safe[k] = snap[k]; }
        if (safe[KEY_SETTINGS]?.version && safe[KEY_SETTINGS].version < DEFAULT_SETTINGS.version) {
          safe[KEY_SETTINGS] = { ...DEFAULT_SETTINGS, ...safe[KEY_SETTINGS], version: DEFAULT_SETTINGS.version };
        }
        storageSetQueued(safe);
        try { sendResponse({ ok: true }); } catch {}
        pushLiveUpdate();
      } catch (e) { try { sendResponse({ ok: false, error: String(e) }); } catch {} }
      return true;
    }
    case "get-site-enable": { storageGet([KEY_SITE_ENABLE]).then(v => { try { sendResponse(v[KEY_SITE_ENABLE] || {}); } catch {} }); return true; }
    case "set-site-enable": {
      storageGet([KEY_SITE_ENABLE]).then(cur => {
        const cfg = { ...(cur[KEY_SITE_ENABLE] || {}) };
        cfg[msg.domain] = !!msg.enabled;
        storageSetQueued({ [KEY_SITE_ENABLE]: cfg });
        const ts = nowMs();
        siteEnableCache.value = cfg; siteEnableCache.ts = ts;
        try { sendResponse({ ok: true }); } catch {}
        pushLiveUpdate();
      });
      return true;
    }
    case "reset-all": {
      try {
        const init = { [KEY_TOTAL]: 0, [KEY_DAILY]: 0, [KEY_LOG]: {}, [KEY_DETAILS]: {} };
        storageSetQueued(init); pushLiveUpdate(); sendResponse({ ok: true });
      } catch (e) { sendResponse({ ok: false, error: String(e) }); }
      return true;
    }
    case "reset-today": {
      (async () => {
        try {
          const today = dayKey(now);
          const logs = await getLocal(KEY_LOG, {});
          const details = await getLocal(KEY_DETAILS, {});
          const total = await getLocal(KEY_TOTAL, 0);
          const todayMs = logs[today] || 0;
          delete logs[today]; delete details[today];
          const nextTotal = Math.max(0, total - todayMs);
          storageSetQueued({ [KEY_LOG]: logs, [KEY_DETAILS]: details, [KEY_TOTAL]: nextTotal, [KEY_DAILY]: 0 });
          pushLiveUpdate(); sendResponse({ ok: true });
        } catch (e) { sendResponse({ ok: false, error: String(e) }); }
      })();
      return true;
    }
    case "debug.dumpState": { try { const tabs = Array.from(tabState.values()); sendResponse({ tabs }); } catch {} return true; }
    default: { return false; }
  }
});

/* ===== Bootstrap existing tabs ===== */
async function bootstrapExistingTabsOnce() {
  try {
    const tabs = await B.tabs.query({});
    for (const t of tabs) {
      let latest; try { latest = await B.tabs.get(t.id); } catch { latest = t; }
      const st = makeStateMinimal(t.id, latest.url || "", latest.title || "", latest.windowId ?? t.windowId);
      st.isCandidate = isCandidateUrl(st.urlObserved);
      st.sessionId = null;
      st.urlObserved = latest.url || "";
      tabState.set(t.id, st);
      try { await safeParseAndApply(t.id, st.urlObserved, latest.title || "", nowMs()); } catch {}
      await pollTab(t.id, st.urlObserved, latest.title || "");
    }
    return tabs.length;
  } catch { return 0; }
}
async function bootstrapExistingTabs() {
  let count = await bootstrapExistingTabsOnce();
  if (count === 0) setTimeout(() => { bootstrapExistingTabsOnce(); }, 1500);
}
if (B?.runtime?.onStartup) B.runtime.onStartup.addListener(() => { bootstrapExistingTabs(); });
if (B?.runtime?.onInstalled) B.runtime.onInstalled.addListener(() => { bootstrapExistingTabs(); });

/* ===== Watchdog (missed heartbeat) ===== */
let watchdogInterval = null;
function setupWatchdog() {
  try {
    if (watchdogInterval) clearInterval(watchdogInterval);
    watchdogInterval = setInterval(() => {
      const now = nowMs();
      for (const [tabId, st] of tabState) {
        if (!st.sessionId) continue;
        const recentInteractionGap = now - (st.lastInteraction || 0);
        if (st.reading) {
          if (st.pageHidden || recentInteractionGap > SETTINGS.idleHoldMs) {
            pauseReading(tabId, now, st.pageHidden ? "NOT_VISIBLE" : "IDLE_HOLD");
          }
        }
      }
    }, SETTINGS.watchdogCheckIntervalMs);
  } catch {}
}

/* ===== Console banner ===== */
console.log("background.js loaded (revised)", new Date().toISOString());

/* ===== Tab queue ===== */
const tabQueues = new Map();
function withTabQueue(tabId, fn) {
  if (typeof tabId !== "number" || tabId <= 0) return Promise.resolve();
  const prev = tabQueues.get(tabId) || Promise.resolve();
  const next = prev.then(() => Promise.resolve().then(fn)).catch(()=>{});
  tabQueues.set(tabId, next);
  return next;
}
