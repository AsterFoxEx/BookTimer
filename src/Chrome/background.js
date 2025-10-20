"use strict";

/**
 * ブックタイマー Next (MV3 Service Worker)
 * - 原則ポーリング（2段階: グローバル2s/フォーカス200ms）
 * - 心拍(content)で可視/操作時刻を更新（SWの生存性確保）
 * - タイトル安定化は候補URLのみ/重複抑止
 * - pending.short/segment の保存と再吸収の正当化（消失バグ修正）
 * - sendMessage は callback 無しで Unchecked runtime.lastError を回避
 * - MutationObserver は一切使用しない（content 側も同様）
 */

const B = chrome;

/* ====== Utils ====== */
const nowMs = () => Date.now();
const pad2 = (n) => String(n).padStart(2, "0");
const dayKey = (ts) => {
  const d = new Date(ts);
  return `${d.getFullYear()}-${pad2(d.getMonth() + 1)}-${pad2(d.getDate())}`;
};
const clean = (s) => String(s ?? "").replace(/\s+/g, " ").trim();
const isPromiseLike = (x) => !!x && typeof x.then === "function";
const isInternal = (url) =>
  typeof url === "string" &&
  (url.startsWith("chrome-extension://") ||
    url.startsWith("about:") ||
    url.startsWith("chrome:"));
const toEpochMs = (val, fallbackNow) => {
  const now = fallbackNow ?? nowMs();
  const v = Number(val);
  if (!Number.isFinite(v)) return now;
  if (v > 1e12 && v < 4102444800000) return v;
  return now;
};
function normalizeUrl(url) {
  try {
    const u = new URL(url);
    u.hash = "";
    const params = [...u.searchParams.entries()].sort((a, b) => a[0].localeCompare(b[0]));
    u.search = params.map(([k, v]) => `${encodeURIComponent(k)}=${encodeURIComponent(v)}`).join("&");
    const p = u.pathname;
    if (p.length > 1 && p.endsWith("/")) u.pathname = p.slice(0, -1);
    return u.origin + u.pathname + (u.search ? `?${u.search}` : "");
  } catch {
    return String(url || "");
  }
}
function samePageIgnoringFragment(a, b) {
  try {
    const A = new URL(a), B = new URL(b);
    return A.origin === B.origin && A.pathname === B.pathname && A.search === B.search;
  } catch {
    return false;
  }
}

/* ====== Logging ====== */
const telemetry = [];
const LOGT = {
  "session.bootstrap": true,
  "session.promote": true,
  "reading.start": true,
  "reading.pause": true,
  "reading.stop": true,
  "commit.saved": true,
  "pending.store": true,
  "pending.absorb": true,
  "pending.drop": true,
  "meta.apply": true,
  "meta.apply.stale": false,
  "meta.parse.error": true,
  "title.stable": true,
  "title.promote.block": true,
  "interaction": true,
  "visible.transition": true,
  "tab.closed": true,
  "tab.closed.poll": true
};
let LOG_DUP_SUPPRESS_MS = 250;

function log(tag, data) {
  try {
    if (!LOGT[tag]) return;
    const sig = JSON.stringify({ tag, data });
    const t = nowMs();
    const gap = t - (log.__lastSigTs || 0);
    if (log.__lastSig === sig && gap < LOG_DUP_SUPPRESS_MS) return;
    log.__lastSig = sig;
    log.__lastSigTs = t;
    console.log(`[${new Date().toISOString()}] ${tag}`, data ?? "");
    telemetry.push({ ts: t, tag, data });
    if (telemetry.length > 1000) telemetry.shift();
  } catch {}
}

/* ====== Domains & candidates ====== */
const SITE = { KAKUYOMU: "kakuyomu.jp", HAMELN: "syosetu.org", PIXIV: "pixiv.net", NAROU: "syosetu.com" };

function getDomain(url) {
  try {
    const h = new URL(url).hostname;
    if (h.endsWith("kakuyomu.jp")) return SITE.KAKUYOMU;
    if (h.endsWith("syosetu.org")) return SITE.HAMELN;
    if (h.endsWith("pixiv.net")) return SITE.PIXIV;
    if (h.endsWith("syosetu.com") || h.endsWith("ncode.syosetu.com")) return SITE.NAROU;
    return null;
  } catch {
    return null;
  }
}

function isCandidateUrl(url) {
  if (!url || isInternal(url)) return false;
  const dom = getDomain(url);
  if (!dom) return false;
  try {
    const u = new URL(url);
    const p = u.pathname;
    if (dom === SITE.KAKUYOMU) {
      return /^\/works\/\d+\/episodes\/\d+\/?$/i.test(p) || /^\/works\/\d+\/?$/i.test(p);
    }
    if (dom === SITE.HAMELN) {
      return /^\/novel\/\d+(?:\/\d+\.html|\/?)$/i.test(p);
    }
    if (dom === SITE.PIXIV) {
      return /^\/novel\/show\.php$/i.test(p) && u.searchParams.has("id");
    }
    if (dom === SITE.NAROU) {
      return /^\/(n[0-9a-z]+)\/(?:\d+\/)?$/i.test(p);
    }
    return false;
  } catch {
    return false;
  }
}

/* ====== Settings & storage ====== */
const KEY = {
  TOTAL: "rt_total_ms",
  DAILY: "rt_daily_ms",
  LOG: "rt_daily_log",
  DETAILS: "rt_details",
  SITE_ENABLE: "rt_site_enable",
  SETTINGS: "rt_settings",
  VERSION: "rt_version"
};

const DEFAULT = {
  version: 100,
  minSessionMs: 4000,
  realtimeFlushMs: 5000,
  livePushIntervalMs: 1500,
  livePushMinGapMs: 500,
  titleStableDefaultMs: 500,
  titleStablePixivMs: 200,
  visibilityStabilizeMs: 150,
  idleHoldMs: 20000,
  pendingAbsorbWindowMs: 15000,
  pendingShortTimeoutMs: 15000,
  pendingSegmentTimeoutMs: 60000,
  heartbeatIntervalMs: 500,
  watchdogCheckIntervalMs: 800,
  globalScanIntervalMs: 2000,
  focusedPollIntervalMs: 200,
  startGraceMs: 500,
  recentInteractionSkipStartGraceMs: 2000,
  idleResumeGraceMs: 2000,
  narouRateWindowMs: 60000,
  narouRateMaxPerWindow: 20,
  narouCacheTtlMs: 24 * 60 * 60 * 1000,
  narouAwaitCertMaxWaitMs: 15000,
  narouAwaitCertRetryIntervalMs: 1200,
  promoteDebounceMs: 600,
  commitOnCloseBelowMin: true,
  logDuplicateSuppressMs: 250
};
let SETTINGS = { ...DEFAULT };

function storageGet(keys) {
  return new Promise((res) => {
    try {
      chrome.storage.local.get(keys, (o) => res(o || {}));
    } catch {
      res({});
    }
  });
}
const writeQ = [];
let writeBusy = false;
function mergeObject(target, patch) {
  for (const k of Object.keys(patch)) target[k] = patch[k];
}
function storageSetQueued(obj) {
  writeQ.push(obj);
  processWriteQ();
}
async function processWriteQ() {
  if (writeBusy || writeQ.length === 0) return;
  writeBusy = true;
  try {
    const batch = {};
    while (writeQ.length) mergeObject(batch, writeQ.shift());
    await new Promise((r) => {
      try {
        chrome.storage.local.set(batch, () => r());
      } catch {
        r();
      }
    });
  } finally {
    writeBusy = false;
    if (writeQ.length) processWriteQ();
  }
}
async function getLocal(key, defVal) {
  const o = await storageGet([key]);
  return Object.prototype.hasOwnProperty.call(o, key) ? o[key] : defVal;
}

let storageReady = initStorage();
async function initStorage() {
  const init = {
    [KEY.TOTAL]: 0,
    [KEY.DAILY]: 0,
    [KEY.LOG]: {},
    [KEY.DETAILS]: {},
    [KEY.SITE_ENABLE]: {
      [SITE.KAKUYOMU]: true,
      [SITE.HAMELN]: true,
      [SITE.PIXIV]: true,
      [SITE.NAROU]: true
    },
    [KEY.SETTINGS]: DEFAULT,
    [KEY.VERSION]: DEFAULT.version
  };
  const cur = await storageGet(Object.keys(init));
  const put = {};
  for (const k of Object.keys(init)) {
    if (cur[k] === undefined) put[k] = init[k];
  }
  if (Object.keys(put).length) storageSetQueued(put);
  await loadSettingsIntoMemory();
}
async function loadSettingsIntoMemory() {
  try {
    const o = await storageGet([KEY.SETTINGS]);
    const cfg = o[KEY.SETTINGS];
    SETTINGS =
      cfg && typeof cfg === "object"
        ? { ...DEFAULT, ...cfg, version: DEFAULT.version }
        : { ...DEFAULT };
    LOG_DUP_SUPPRESS_MS = SETTINGS.logDuplicateSuppressMs;
    storageSetQueued({ [KEY.SETTINGS]: SETTINGS });
    applySettings();
  } catch {
    SETTINGS = { ...DEFAULT };
    applySettings();
  }
}
function applySettings() {
  try {
    if (_commitInterval) clearInterval(_commitInterval);
  } catch {}
  try {
    if (liveUpdateInterval) clearInterval(liveUpdateInterval);
  } catch {}
  try {
    if (watchdogInterval) clearInterval(watchdogInterval);
  } catch {}
  try {
    if (globalScanTimer) clearInterval(globalScanTimer);
  } catch {}
  setupAlarms();
  setupIntervals();
  setupWatchdog();
  setupGlobalScanner();
}

/* ====== Site enable cache ====== */
const siteEnableCache = { value: null, ts: 0 };
async function isDomainEnabled(domain) {
  await storageReady;
  const n = nowMs();
  if (!siteEnableCache.value || n - siteEnableCache.ts > 5000) {
    const obj = await storageGet([KEY.SITE_ENABLE]);
    siteEnableCache.value = obj[KEY.SITE_ENABLE] || {};
    siteEnableCache.ts = n;
  }
  return !!(siteEnableCache.value || {})[domain];
}

/* ====== Title stability (gated by candidate) ====== */
const titleStableMap = new Map(); // tabId -> { last, firstTs, stable, lastSig }
function titleStableMsFor(site) {
  return site === SITE.PIXIV ? SETTINGS.titleStablePixivMs : SETTINGS.titleStableDefaultMs;
}
function updateTitleStability(tabId, newTitle, now, site, isCandidate) {
  if (!isCandidate || !site) return ""; // 候補URL/対象サイトのみ追跡
  const t = clean(newTitle || "");
  let s = titleStableMap.get(tabId) || { last: "", firstTs: now, stable: false, lastSig: "" };
  if (!t) {
    titleStableMap.set(tabId, { last: "", firstTs: now, stable: false, lastSig: "" });
    return "";
  }
  if (s.last !== t) s = { last: t, firstTs: now, stable: false, lastSig: s.lastSig };
  const needMs = titleStableMsFor(site);
  const span = now - s.firstTs;
  const isStableNow = span >= needMs;
  const st = tabState.get(tabId);

  if (isStableNow && !s.stable) {
    s.stable = true;
    if (st && st.lastStableSig !== makeTitleSig(st.urlObserved, t)) {
      log("title.stable", { tabId, title: t, spanMs: span });
      titleStableMap.set(tabId, s);
      return t;
    }
  }
  titleStableMap.set(tabId, s);
  if (isStableNow && st && st.lastStableSig !== makeTitleSig(st.urlObserved, t)) return t;
  return "";
}
function makeTitleSig(url, title) {
  return `${normalizeUrl(url || "")}::${clean(title || "")}`;
}

/* ====== Tab state ====== */
const tabState = new Map();
function makeStateMinimal(tabId, url, title, windowId) {
  const now = nowMs();
  return {
    tabId,
    windowId: windowId ?? null,
    urlObserved: url || "",
    urlConfirmed: "",
    title: title || "",
    isCandidate: isCandidateUrl(url || ""),
    meta: {
      isContent: false,
      cert: "none",
      site: "",
      workTitle: "",
      episodeTitle: "",
      author: "",
      ncode: undefined,
      pixivId: undefined
    },
    reading: false,
    sessionId: null,
    sessionStartTs: undefined,
    activeStartTs: undefined,
    accumMs: 0,
    committedMs: 0,
    lastFlushAt: undefined,
    contentUrlAtStart: "",
    lastStableTitle: "",
    lastStableSig: "",
    pageHidden: false,
    becameVisibleAt: now,
    lastVisibilityMsgAt: now,
    lastInteraction: now,
    pending: null,
    idleTimer: undefined,
    _graceUntil: now + SETTINGS.startGraceMs,
    _idleResumeGraceUntil: 0,
    _stoppedForResumeGate: false,
    _promoteTitleDebounceAt: 0,
    _lastMetaSig: ""
  };
}
async function ensureStateFromTab(tabId) {
  let st = tabState.get(tabId);
  if (st) return st;
  try {
    const t = await chrome.tabs.get(tabId);
    st = makeStateMinimal(tabId, t.url || "", t.title || "", t.windowId);
  } catch {
    st = makeStateMinimal(tabId, "", "", null);
  }
  tabState.set(tabId, st);
  return st;
}

/* ====== Meta parsing ====== */
function metaSig(m) {
  if (!m) return "";
  const pick = {
    site: m.site || "",
    isContent: !!m.isContent,
    cert: m.cert || "",
    workTitle: m.workTitle || "",
    episodeTitle: m.episodeTitle || "",
    author: m.author || "",
    ncode: m.ncode || "",
    pixivId: m.pixivId || ""
  };
  try {
    return JSON.stringify(pick);
  } catch {
    return `${pick.site}|${pick.cert}|${pick.workTitle}|${pick.episodeTitle}|${pick.author}|${pick.ncode}|${pick.pixivId}`;
  }
}
async function parseMeta(url, title) {
  const dom = getDomain(url);
  if (!dom) return { isContent: false, cert: "none", site: "", workTitle: "", episodeTitle: "", author: "" };
  const u = new URL(url);
  if (dom === SITE.PIXIV) return parsePixiv(u, title);
  if (dom === SITE.NAROU) return parseNarou(u, title);
  if (dom === SITE.KAKUYOMU) return parseKakuyomu(u, title);
  if (dom === SITE.HAMELN) return parseHameln(u, title);
  return { isContent: false, cert: "none", site: dom, workTitle: "", episodeTitle: "", author: "" };
}

/* ---- Pixiv ---- */
function stripPixivLeadingTags(rawTitle) {
  let s = String(rawTitle || "").trim();
  s = s.replace(/^(\s*#(?!\d+\b)\S+(?:\([^)]+\))?\s*)+/u, "").trim();
  return s;
}
function parsePixiv(u, rawTitle) {
  const site = SITE.PIXIV;
  const isNovel = /^\/novel\/show\.php$/i.test(u.pathname) && u.searchParams.has("id");
  const pixivId = u.searchParams.get("id") || undefined;
  if (!isNovel) return { isContent: false, cert: "none", site, workTitle: "", episodeTitle: "", author: "", pixivId: undefined };

  const raw = String(rawTitle || "").trim();
  if (!raw || /^\[pixiv\]/i.test(raw) || /ローディング中/i.test(raw)) {
    return { isContent: true, cert: "url", site, workTitle: "", episodeTitle: "", author: "", pixivId };
  }
  let m = raw.match(/^#(\d+)\s+(.+?)\s*\|\s*(.+?)(?:\s*-\s*.+)?$/u);
  if (m) {
    const epNumber = m[1];
    const epSubtitle = m[2].replace(/\u3000/g, " ").replace(/\s+/g, " ").trim();
    const workTitle = m[3].replace(/\u3000/g, " ").replace(/\s+/g, " ").trim();
    return { isContent: true, cert: "title", site, workTitle, episodeTitle: `#${epNumber} ${epSubtitle}`, author: "", pixivId };
  }
  let s = stripPixivLeadingTags(raw)
    .replace(/\s*-\s*pixiv\s*$/iu, "")
    .replace(/\s*-\s*[^-]*?の小説\s*-\s*pixiv\s*$/iu, "")
    .trim();
  m = s.match(/^(?!#\d+\s+)(.+?)\s*\|\s*(.+?)(?:\s*-\s*.+)?$/u);
  if (m) {
    const episodeTitle = m[1].replace(/\u3000/g, " ").replace(/\s+/g, " ").trim();
    const workTitle = m[2].replace(/\u3000/g, " ").replace(/\s+/g, " ").trim();
    return { isContent: true, cert: "title", site, workTitle, episodeTitle, author: "", pixivId };
  }
  m = s.match(/^(.+?)\s*-\s*.+$/u);
  if (m) {
    const title = m[1].replace(/\u3000/g, " ").replace(/\s+/g, " ").trim();
    return { isContent: true, cert: "title", site, workTitle: title, episodeTitle: title, author: "", pixivId };
  }
  const title = s.replace(/\u3000/g, " ").replace(/\s+/g, " ").trim();
  if (title) return { isContent: true, cert: "title", site, workTitle: title, episodeTitle: title, author: "", pixivId };
  return { isContent: true, cert: "url", site, workTitle: "", episodeTitle: "", author: "", pixivId };
}

/* ---- Hameln ---- */
function parseHameln(u, title) {
  const site = SITE.HAMELN;
  const path = u.pathname;
  const isSerial = /^\/novel\/\d+\/\d+\.html$/i.test(path);
  const isTopOrShort = /^\/novel\/\d+\/?$/i.test(path);
  const isGeneric = /^ハーメルン\s*-\s*SS･小説投稿サイト-?$/i.test(String(title || "").trim());
  if (isGeneric) return { isContent: false, cert: "none", site, workTitle: "", episodeTitle: "", author: "" };

  const trimmed = String(title || "").replace(/\s*-\s*ハーメルン$/i, "").trim();
  const parts = trimmed.split(/\s+-\s+/).map((s) => clean(s));

  if (isSerial) {
    const work = clean(parts[0] || "");
    const ep = clean(parts[1] || "");
    const ok = !!(work && ep);
    return { isContent: ok, cert: ok ? "title" : "url", site, workTitle: work, episodeTitle: ep, author: "" };
  }
  if (isTopOrShort) {
    const work = clean(parts[0] || "");
    const ep = clean(parts[1] || "");
    if (parts.length >= 2) return { isContent: true, cert: "title", site, workTitle: work, episodeTitle: ep, author: "" };
    return { isContent: false, cert: work ? "title" : "none", site, workTitle: work, episodeTitle: "", author: "" };
  }
  return { isContent: false, cert: "none", site, workTitle: "", episodeTitle: "", author: "" };
}

/* ---- Kakuyomu ---- */
function parseKakuyomu(u, title) {
  const site = SITE.KAKUYOMU;
  const path = u.pathname;
  const isEpisode = /^\/works\/\d+\/episodes\/\d+\/?$/i.test(path);
  const isTop = /^\/works\/\d+\/?$/i.test(path);
  if (/^https?:\/\//.test(title) || String(title).startsWith("kakuyomu.jp/")) {
    return { isContent: isEpisode, cert: isEpisode ? "url" : "none", site, workTitle: "", episodeTitle: "", author: "" };
  }
  const t = String(title || "").replace(/\s*-\s*カクヨム$/i, "").trim();
  const parts = String(t || "").split(/\s+-\s+/).map((s) => clean(s));

  if (isEpisode) {
    const subtitle = clean(parts[0] || "");
    const wa = (parts[1] || "").trim();
    const m = String(wa || "").trim().match(/^(.*)（(.*)）$/);
    const work = clean(m ? m[1] : wa);
    const author = clean(m ? m[2] : "");
    const ok = !!(subtitle && work);
    return { isContent: ok, cert: ok ? "title" : "url", site, workTitle: ok ? work : "", episodeTitle: ok ? subtitle : "", author: ok ? author : "" };
  }
  if (isTop) {
    const wa2 = (parts[0] || "").trim();
    const m2 = String(wa2 || "").trim().match(/^(.*)（(.*)）$/);
    const work2 = clean(m2 ? m2[1] : wa2);
    const author2 = clean(m2 ? m2[2] : "");
    return { isContent: false, cert: work2 ? "title" : "none", site, workTitle: work2, episodeTitle: "", author: author2 };
  }
  return { isContent: false, cert: "none", site, workTitle: "", episodeTitle: "", author: "" };
}

/* ---- Narou ---- */
const narouApi = (() => {
  const cache = new Map();
  const queue = [];
  let processing = false;
  let bucket = { ts: 0, count: 0 };

  function refillWindow() {
    const n = nowMs();
    if (n - bucket.ts > SETTINGS.narouRateWindowMs) {
      bucket.ts = n;
      bucket.count = 0;
    }
    return bucket.count < SETTINGS.narouRateMaxPerWindow;
  }
  async function tick() {
    if (processing) return;
    processing = true;
    try {
      while (queue.length) {
        if (!refillWindow()) {
          const sleepMs = Math.max(0, SETTINGS.narouRateWindowMs - (nowMs() - bucket.ts));
          await new Promise((r) => setTimeout(r, sleepMs));
          continue;
        }
        const { ncode, resolve } = queue.shift();
        const cached = cache.get(ncode);
        if (cached && nowMs() - cached.ts < SETTINGS.narouCacheTtlMs) {
          resolve(cached.info);
          continue;
        }
        try {
          bucket.count++;
          const url = `https://api.syosetu.com/novelapi/api/?out=json&of=t-w-nt&ncode=${encodeURIComponent(ncode)}`;
          const res = await fetch(url, { method: "GET" });
          const json = await res.json();
          const info = Array.isArray(json) && json.length >= 2 ? json[1] : null;
          cache.set(ncode, { info, ts: nowMs() });
          resolve(info);
        } catch {
          cache.set(ncode, { info: null, ts: nowMs() });
          resolve(null);
        }
      }
    } finally {
      processing = false;
    }
  }
  function get(ncode) {
    const cached = cache.get(ncode);
    if (cached && nowMs() - cached.ts < SETTINGS.narouCacheTtlMs) return Promise.resolve(cached.info);
    return new Promise((resolve) => {
      queue.push({ ncode, resolve });
      tick();
    });
  }
  return { get };
})();

async function parseNarou(u, title) {
  const site = SITE.NAROU;
  const m = u.pathname.match(/^\/(n[0-9a-z]+)(?:\/(\d+)\/?)?/i);
  if (!m) {
    return { isContent: false, cert: "none", site, workTitle: "", episodeTitle: "", author: "", ncode: undefined };
  }
  const ncode = m[1].toLowerCase();
  const epNo = m[2] ? Number(m[2]) : null;

  try {
    const info = await narouApi.get(ncode); // API呼び出し
    const workTitle = clean(info.title);
    const author = clean(info.writer);
    const isShort = Number(info.noveltype) === 2;

    let episodeTitle = "";
    if (epNo) {
      // 連載話 → URLの番号を必ず利用
      episodeTitle = `第${epNo}話`;
    } else if (isShort) {
      // 短編 → 作品名をそのまま episodeTitle に
      episodeTitle = workTitle;
    }

    return {
      isContent: true,
      cert: "title",
      site,
      workTitle,
      episodeTitle,
      author,
      ncode
    };
  } catch {
    // API失敗時はフォールバック
    return {
      isContent: false,
      cert: "url",
      site,
      workTitle: clean(title || ""),
      episodeTitle: epNo ? `第${epNo}話` : "",
      author: "",
      ncode
    };
  }
}


/* ====== Meta helpers ====== */
async function safeParseAndApply(tabId, url, title, now) {
  const st = tabState.get(tabId);
  if (!st) return;
  const curUrl = st.urlObserved;
  try {
    const meta = await parseMeta(url, title);
    if (url !== curUrl) {
      if (LOGT["meta.apply.stale"]) log("meta.apply.stale", { tabId, url, title });
      return;
    }
    const sig = metaSig(meta);
    const changed = sig !== st._lastMetaSig;
    st.meta = meta;
    if (changed && meta.cert !== "none") log("meta.apply", { tabId, cert: meta.cert, site: meta.site });
    st._lastMetaSig = sig;
    tabState.set(tabId, st);
  } catch (e) {
    log("meta.parse.error", { tabId, url, error: String(e) });
  }
}
function metaMatchesUrl(st) {
  try {
    const url = st.urlObserved || st.urlConfirmed || "";
    const dom = getDomain(url);
    const meta = st.meta || {};
    if (!dom || meta.site !== dom) return false;
    if (dom === SITE.PIXIV) {
      const u = new URL(url);
      const idInUrl = u.searchParams.get("id") || null;
      const idInMeta = meta.pixivId ?? null;
      return !!(idInUrl && idInMeta && idInUrl === idInMeta);
    }
    if (dom === SITE.NAROU) {
      const m = new URL(url).pathname.match(/^\/(n[0-9a-z]+)\/?/i);
      const ncodeInUrl = m ? m[1].toLowerCase() : null;
      const ncodeInMeta = (meta.ncode || "").toLowerCase() || null;
      return !!(ncodeInUrl && ncodeInMeta && ncodeInUrl === ncodeInMeta);
    }
    return true;
  } catch {
    return false;
  }
}

/* ====== Session timing helpers ====== */
function inflightMs(st, now) {
  return st.reading && st.activeStartTs ? Math.max(0, now - st.activeStartTs) : 0;
}
function uncommittedMs(st, now) {
  return Math.max(0, st.accumMs + inflightMs(st, now) - (st.committedMs || 0));
}
function clearIdleTimer(tabId) {
  const st = tabState.get(tabId);
  if (!st) return;
  if (st.idleTimer) {
    clearTimeout(st.idleTimer);
    st.idleTimer = undefined;
    tabState.set(tabId, st);
  }
}
function resetIdleTimer(tabId) {
  const st = tabState.get(tabId);
  if (!st) return;
  clearIdleTimer(tabId);
  st.idleTimer = setTimeout(() => {
    withTabQueue(tabId, async () => {
      const cur = tabState.get(tabId);
      if (!cur) return;
      const now = nowMs();
      if (cur.reading) pauseReading(cur.tabId, now, "IDLE_HOLD");
    });
  }, SETTINGS.idleHoldMs);
  tabState.set(tabId, st);
}
function tryAbsorbPendingOnResume(st, now) {
  if (!st?.pending) return false;
  const base = Number(st.pending.stop ?? st.pending.queuedAt ?? now);
  const age = now - base;
  const limit = st.pending.kind === "segment" ? SETTINGS.pendingAbsorbWindowMs : SETTINGS.pendingAbsorbWindowMs;
  if (!Number.isFinite(base) || st.pending.ms <= 0 || age > limit) {
    log("pending.drop", { tabId: st.tabId, kind: st.pending.kind, ms: st.pending.ms, ageMs: age, reason: "resume_discard" });
    st.pending = null;
    return false;
  }
  st.accumMs = (st.accumMs || 0) + st.pending.ms;
  log("pending.absorb", { tabId: st.tabId, kind: st.pending.kind, ms: st.pending.ms, ageMs: age });
  st.pending = null;
  return true;
}
function pauseReading(tabId, now, reason = "PAUSE") {
  const st = tabState.get(tabId);
  if (!st?.reading) return;
  clearIdleTimer(tabId);
  const add = inflightMs(st, now);
  st.reading = false;
  st.activeStartTs = undefined;
  st._stoppedForResumeGate = reason === "IDLE_HOLD";
  // segment pending（安定したタイトルのときのみ）
  if (st.meta?.isContent && st.meta?.cert === "title" && add > 0) {
    st.pending = {
      kind: "segment",
      ms: add,
      stop: reason === "IDLE_HOLD" ? (st.lastInteraction || now) + SETTINGS.idleHoldMs : now,
      site: st.meta.site,
      workTitle: st.meta.workTitle,
      episodeTitle: st.meta.episodeTitle,
      author: st.meta.author || "",
      sessionId: st.sessionId,
      ncode: st.meta.ncode,
      pixivId: st.meta.pixivId,
      url: st.contentUrlAtStart || st.urlConfirmed || st.urlObserved || ""
    };
    log("pending.store", { tabId: st.tabId, sessionId: st.sessionId, ms: add, reason });
  }
  tabState.set(tabId, st);
  log("reading.pause", { tabId, sessionId: st.sessionId, addMs: add, reason });
}

/* ====== Commit helpers ====== */
function splitAcrossDays(startTs, stopTs, totalMs) {
  const startDay = dayKey(startTs);
  const stopDay = dayKey(stopTs);
  if (startDay === stopDay) return [{ day: startDay, ms: totalMs, ts: stopTs }];
  const midnightStop = new Date(stopTs);
  midnightStop.setHours(0, 0, 0, 0);
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
  const total = await getLocal(KEY.TOTAL, 0);
  const logs = await getLocal(KEY.LOG, {});
  const details = await getLocal(KEY.DETAILS, {});
  const today = dayKey(now);
  let addTotal = 0;

  for (const part of parts) {
    if (part.ms <= 0) continue;
    const list = details[part.day] || [];
    const urlForRecord = st.contentUrlAtStart || st.urlConfirmed || st.urlObserved || "";
    const same = (r) =>
      r.site === meta.site &&
      r.workTitle === meta.workTitle &&
      r.episodeTitle === meta.episodeTitle &&
      r.url === urlForRecord &&
      r.pixivId === meta.pixivId &&
      r.ncode === meta.ncode &&
      r.sessionId === st.sessionId;
    const ex = list.find(same);
    if (ex) {
      ex.ms += part.ms;
      ex.ts = part.ts;
      if (!ex.url) ex.url = urlForRecord;
    } else {
      list.push({
        site: meta.site,
        workTitle: meta.workTitle,
        episodeTitle: meta.episodeTitle,
        author: meta.author || "",
        sessionId: st.sessionId,
        ms: part.ms,
        ts: part.ts,
        url: urlForRecord,
        pixivId: meta.pixivId,
        ncode: meta.ncode
      });
    }
    details[part.day] = list;
    logs[part.day] = (logs[part.day] || 0) + part.ms;
    addTotal += part.ms;
  }
  storageSetQueued({
    [KEY.TOTAL]: total + addTotal,
    [KEY.DAILY]: logs[today] || 0,
    [KEY.LOG]: logs,
    [KEY.DETAILS]: details
  });
  log("commit.saved", { tabId: st.tabId, sessionId: st.sessionId, ms: addTotal, site: meta.site, reason });
  pushLiveUpdate();
}

/* ====== Reading lifecycle ====== */
async function canStart(st, now) {
  if (!st) return false;
  const domain = getDomain(st.urlObserved || st.urlConfirmed);
  const enabled = domain ? await isDomainEnabled(domain) : false;
  const stableOk = !!st.lastStableTitle && st.lastStableSig === makeTitleSig(st.urlObserved, st.lastStableTitle);
  const visOk = !st.pageHidden && (st.becameVisibleAt ? now - st.becameVisibleAt >= SETTINGS.visibilityStabilizeMs : true);
  const inStartGrace = st._graceUntil && now < st._graceUntil;
  const recentGap = now - (st.lastInteraction || 0);
  const canSkipGrace = recentGap <= SETTINGS.recentInteractionSkipStartGraceMs;
  const graceOk = !inStartGrace || canSkipGrace;
  const resumeGateOk = !st._stoppedForResumeGate;

  return !!(enabled && st.isCandidate && st.meta?.isContent && st.meta?.cert === "title" && visOk && stableOk && graceOk && resumeGateOk);
}
function canContinue(st, now) {
  if (!st.reading) return false;
  const visOk = !st.pageHidden;
  const recentGap = now - (st.lastInteraction || 0);
  return visOk && recentGap < SETTINGS.idleHoldMs;
}
async function startOrResumeReading(tabId, now) {
  const st = tabState.get(tabId);
  if (!st || st.reading) return;
  if (!(await canStart(st, now))) return;
  const isResume = !!st.sessionId;
  st.sessionId = st.sessionId || `s_${Date.now().toString(16)}_${Math.random().toString(16).slice(2)}`;
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
  log("session.promote", { tabId, sessionId: st.sessionId, title: st.lastStableTitle, site: st.meta.site, resume: isResume === true });
  log("reading.start", { tabId, sessionId: st.sessionId, resume: isResume === true });
}
async function stopReading(tabId, now, reason = "STOP") {
  const st = tabState.get(tabId);
  if (!st?.sessionId) return;
  if (st.reading) {
    const add = inflightMs(st, now);
    st.accumMs += add;
    st.reading = false;
    st.activeStartTs = undefined;
  }
  const delta = uncommittedMs(st, now);
  if (st.meta?.isContent && st.meta?.cert === "title" && delta > 0) {
    const forceCommit = reason === "TAB_REMOVED" && SETTINGS.commitOnCloseBelowMin === true;
    if (delta >= SETTINGS.minSessionMs || forceCommit) {
      await commitDelta(st, delta, now, reason);
      st.committedMs += delta;
    } else {
      // ここで pending.short を設定し、末尾で消さない（バグ修正）
      st.pending = {
        kind: "short",
        ms: delta,
        stop: now,
        expiresAt: now + SETTINGS.pendingShortTimeoutMs,
        site: st.meta.site,
        workTitle: st.meta.workTitle,
        episodeTitle: st.meta.episodeTitle,
        author: st.meta.author,
        sessionId: st.sessionId,
        ncode: st.meta.ncode,
        pixivId: st.meta.pixivId,
        url: st.contentUrlAtStart || st.urlConfirmed || st.urlObserved || ""
      };
      log("pending.store", { tabId: st.tabId, sessionId: st.sessionId, ms: delta, reason: "SHORT_COMMIT", stopReason: reason });
    }
  }
  // セッション固有のフィールドだけを初期化。pending は残す（重要）
  st.accumMs = 0;
  st.committedMs = 0;
  st.sessionId = null;
  st.sessionStartTs = undefined;
  st.contentUrlAtStart = "";
  st.lastStableTitle = "";
  st.lastStableSig = "";
  tabState.set(tabId, st);
  log("reading.stop", { tabId, elapsedMs: delta, reason, site: st.meta?.site });
}

/* ====== Promotion ====== */
function promoteStableTitle(tabId, stableTitle, now) {
  const st = tabState.get(tabId);
  if (!st) return;
  const titleClean = clean(stableTitle || "");
  const prev = st.contentUrlAtStart || st.urlConfirmed || st.urlObserved || "";
  const cur = st.urlObserved || "";
  const prevKey = normalizeUrl(prev);
  const curKey = normalizeUrl(cur);

  if (prev && cur && prev !== cur && prevKey === curKey) {
    st.urlConfirmed = st.urlConfirmed || st.urlObserved || "";
    st.contentUrlAtStart = st.contentUrlAtStart || st.urlConfirmed;
  }

  if (!metaMatchesUrl(st)) {
    log("title.promote.block", { tabId, reason: "metaUrlMismatch", urlObserved: st.urlObserved, urlConfirmed: st.urlConfirmed });
    return;
  }

  // デバウンス（pixiv は短め）
  const debounceMs = st.meta?.site === SITE.PIXIV ? 200 : SETTINGS.promoteDebounceMs;
  if (now - st._promoteTitleDebounceAt < debounceMs) {
    log("title.promote.block", { tabId, reason: "debounce", msSince: now - st._promoteTitleDebounceAt });
    return;
  }
  st._promoteTitleDebounceAt = now;

  const sig = makeTitleSig(st.urlObserved, titleClean);
  if (st.reading && st.lastStableSig === sig) return;

  st.lastStableTitle = titleClean;
  st.lastStableSig = sig;
  st.urlConfirmed = st.urlObserved || st.urlConfirmed;
  tabState.set(tabId, st);
  startOrResumeReading(tabId, now);
}

/* ====== Unified change handler ====== */
async function onUrlOrTitleChange(tabId, newUrl, newTitle) {
  await withTabQueue(tabId, async () => {
    const now = nowMs();
    let st = tabState.get(tabId);
    if (!st) {
      let windowId = undefined;
      try {
        const t = await chrome.tabs.get(tabId);
        windowId = t?.windowId;
      } catch {}
      st = makeStateMinimal(tabId, newUrl || "", newTitle || "", windowId);
      tabState.set(tabId, st);
      log("session.bootstrap", { tabId, url: st.urlObserved, title: st.title });
    }
    const prevUrl = st.urlObserved || "";
    const urlChanged = prevUrl !== (newUrl || prevUrl);
    const titleChanged = st.title !== (newTitle ?? st.title);

    if (urlChanged) {
      const fragmentOnlyEq = samePageIgnoringFragment(prevUrl, newUrl);
      if (!fragmentOnlyEq) {
        await stopReading(tabId, now, "NAVIGATION");
        titleStableMap.set(tabId, { last: "", firstTs: now, stable: false, lastSig: "" });
        st.lastStableTitle = "";
        st.lastStableSig = "";
        st.accumMs = 0;
        st.committedMs = 0;
        st.sessionId = null;
        st.sessionStartTs = undefined;
        st.contentUrlAtStart = "";
        // pending は残す（短時間復帰の吸収に使う）
      }
      st.urlObserved = newUrl;
    }
    if (titleChanged) st.title = newTitle || "";
    st.isCandidate = isCandidateUrl(st.urlObserved);
    tabState.set(tabId, st);

    await safeParseAndApply(tabId, st.urlObserved, st.title || "", now);
    const stable = updateTitleStability(tabId, st.title || "", now, st.meta?.site, st.isCandidate);
    if (stable && st.meta?.cert === "title") promoteStableTitle(tabId, stable, now);
    await evaluateVisibility(tabId, now);
  });
}

/* ====== Visibility evaluation ====== */
async function evaluateVisibility(tabId, now) {
  const st = tabState.get(tabId);
  if (!st) return;

  // pending expiration
  if (st.pending) {
    const timeoutMs =
      st.pending.kind === "short"
        ? SETTINGS.pendingShortTimeoutMs
        : st.pending.kind === "segment"
          ? SETTINGS.pendingSegmentTimeoutMs
          : SETTINGS.narouAwaitCertMaxWaitMs;
    const base = st.pending.stop ?? st.pending.queuedAt ?? now;
    if (now - base > timeoutMs) {
      log(st.pending.kind === "await_cert" ? "awaitCert.expire" : "pending.drop", {
        tabId: st.tabId,
        ageMs: now - base
      });
      st.pending = null;
      tabState.set(tabId, st);
    }
  }

  if (st.reading) {
    if (!canContinue(st, now)) {
      const reason = st.pageHidden ? "NOT_VISIBLE" : "IDLE_HOLD";
      pauseReading(tabId, now, reason);
      return;
    }
  } else {
    if (await canStart(st, now)) await startOrResumeReading(tabId, now);
  }
}

/* ====== Polling core ====== */
let globalScanTimer = null;
let focusedPollTimer = null;
let focusedTabId = null;

async function pollTab(tabId, urlNow, titleNow) {
  const st = await ensureStateFromTab(tabId);
  if (urlNow !== (st.urlObserved || "") || titleNow !== (st.title || "")) {
    await onUrlOrTitleChange(tabId, urlNow, titleNow);
  } else {
    const now = nowMs();
    const stable = updateTitleStability(tabId, st.title || "", now, st.meta?.site, st.isCandidate);
    if (stable && st.meta?.cert === "title") promoteStableTitle(tabId, stable, now);
    await evaluateVisibility(tabId, now);
  }
}
async function pulseScanOnce() {
  try {
    const tabs = await chrome.tabs.query({});
    const seen = new Set();
    let hasCandidate = false;
    for (const t of tabs) {
      const tabId = t.id;
      if (!tabId) continue;
      seen.add(tabId);
      let st = tabState.get(tabId);
      if (!st) {
        st = makeStateMinimal(tabId, t.url || "", t.title || "", t.windowId);
        tabState.set(tabId, st);
      }
      const urlNow = t.url || "";
      const titleNow = t.title || "";
      await pollTab(tabId, urlNow, titleNow);
      if (isCandidateUrl(urlNow)) hasCandidate = true;
    }
    // 既存にないタブはクリーンアップ
    for (const [tid] of tabState) {
      if (!seen.has(tid)) cleanupTabState(tid, "poll.diff");
    }
    // フォーカスタブ更新
    try {
      const w = await chrome.windows.getLastFocused({ populate: true });
      const activeTab = (w?.tabs || []).find((tt) => tt.active);
      const nextFocusedId = activeTab?.id || null;
      if (focusedTabId !== nextFocusedId) {
        focusedTabId = nextFocusedId;
        setupFocusedPoll();
      }
    } catch {}
    if (!hasCandidate && focusedTabId == null) stopFocusedPoll();
  } catch {}
}
function setupGlobalScanner() {
  try {
    if (globalScanTimer) clearInterval(globalScanTimer);
    globalScanTimer = setInterval(() => {
      pulseScanOnce().catch(() => {});
    }, SETTINGS.globalScanIntervalMs);
  } catch {}
}
function setupFocusedPoll() {
  try {
    if (focusedPollTimer) clearInterval(focusedPollTimer);
    if (focusedTabId == null) return;
    focusedPollTimer = setInterval(async () => {
      try {
        const t = await chrome.tabs.get(focusedTabId);
        if (!t) return;
        await pollTab(focusedTabId, t.url || "", t.title || "");
      } catch {}
    }, SETTINGS.focusedPollIntervalMs);
  } catch {}
}
function stopFocusedPoll() {
  if (focusedPollTimer) clearInterval(focusedPollTimer);
  focusedPollTimer = null;
}

/* ====== Intervals & live updates ====== */
let _commitInterval = null,
  liveUpdateInterval = null,
  _lastLiveSig = null,
  _lastLiveAt = 0;

function setupIntervals() {
  try {
    if (_commitInterval) clearInterval(_commitInterval);
    _commitInterval = setInterval(() => {
      commitAll().catch(() => {});
    }, SETTINGS.realtimeFlushMs);
    if (liveUpdateInterval) clearInterval(liveUpdateInterval);
    liveUpdateInterval = setInterval(() => {
      pushLiveUpdate().catch(() => {});
    }, SETTINGS.livePushIntervalMs);
  } catch {}
}
async function getLiveFinal() {
  await storageReady;
  const total = await getLocal(KEY.TOTAL, 0);
  const details = await getLocal(KEY.DETAILS, {});
  const logs = await getLocal(KEY.LOG, {});
  const today = dayKey(nowMs());
  const daily = logs[today] || 0;
  const recent = (details[today] || []).slice().sort((a, b) => b.ts - a.ts).slice(0, 30);
  const now = nowMs();
  const inflightSum = Array.from(tabState.values())
    .filter((st) => st.meta?.isContent && st.meta?.cert === "title")
    .reduce((sum, st) => sum + uncommittedMs(st, now), 0);
  return { total, daily, inflightMs: inflightSum, recent };
}
async function pushLiveUpdate() {
  const now = nowMs();
  if (now - _lastLiveAt < SETTINGS.livePushMinGapMs) return;
  const payload = await getLiveFinal();
  const recentTopTs = payload.recent && payload.recent[0] ? payload.recent[0].ts : 0;
  const recentCount = (payload.recent || []).length;
  const recentSum = (payload.recent || []).reduce((s, x) => s + x.ms, 0);
  const sig = JSON.stringify({
    total: payload.total,
    daily: payload.daily,
    inflightMs: payload.inflightMs,
    recentTopTs,
    recentCount,
    recentSum
  });
  if (sig === _lastLiveSig) return;
  _lastLiveSig = sig;
  _lastLiveAt = now;
  try {
    chrome.runtime.sendMessage({ type: "live-update", payload });
  } catch {}
}

/* ====== Commit loop & alarms ====== */
async function commitAll() {
  try {
    const now = nowMs();
    for (const [, st] of tabState) {
      if (!st.sessionId) continue;
      const delta = uncommittedMs(st, now);
      if (delta >= SETTINGS.minSessionMs && st.meta?.isContent && st.meta?.cert === "title") {
        await commitDelta(st, delta, now, "AUTO_FLUSH");
        st.committedMs += delta;
        st.lastFlushAt = now;
        tabState.set(st.tabId, st);
      }
    }
  } catch {}
}
function setupAlarms() {
  try {
    chrome.alarms.clear("rt-commit");
    chrome.alarms.clear("rt-scan");
    chrome.alarms.create("rt-commit", { periodInMinutes: 1 });
    chrome.alarms.create("rt-scan", { periodInMinutes: 1 });
  } catch {}
}
chrome.alarms.onAlarm.addListener((alarm) => {
  if (alarm?.name === "rt-commit") {
    commitAll().catch(() => {});
  }
  if (alarm?.name === "rt-scan") {
    pulseScanOnce().catch(() => {});
  }
});

/* ====== Focus/activation ====== */
chrome.tabs.onActivated.addListener(({ tabId }) => {
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

/* ====== Events as triggers（補助） ====== */
chrome.tabs.onUpdated.addListener((tabId, changeInfo, tab) => {
  const urlNow = typeof changeInfo.url === "string" && changeInfo.url ? changeInfo.url : tab?.url || "";
  const titleNow = typeof changeInfo.title === "string" ? changeInfo.title : tab?.title || "";
  if (isInternal(urlNow)) return;
  pollTab(tabId, urlNow, titleNow).catch(() => {});
});

/* ====== WebNavigation（補助） ====== */
(function setupWebNavigation() {
  const H = chrome.webNavigation;
  if (!H) return;
  const wrap = (ev) =>
    H[ev]?.addListener?.((details) => {
      if (details.frameId !== 0) return;
      const tabId = details.tabId;
      const newUrl = details.url;
      if (!newUrl) return;
      const st = tabState.get(tabId);
      const titleNow = st?.title || "";
      pollTab(tabId, newUrl, titleNow).catch(() => {});
    });
  ["onHistoryStateUpdated", "onReferenceFragmentUpdated", "onCommitted", "onCompleted"].forEach(wrap);
})();

/* ====== Tab closed handling ====== */
function cleanupTabState(tabId, source = "unknown") {
  const st = tabState.get(tabId);
  if (st?.sessionId) {
    const now = nowMs();
    stopReading(tabId, now, "TAB_REMOVED").catch(() => {});
  }
  clearIdleTimer(tabId);
  titleStableMap.delete(tabId);
  tabQueues.delete(tabId);
  tabState.delete(tabId);
  log(source === "poll.diff" ? "tab.closed.poll" : "tab.closed", { tabId, source });
}
chrome.tabs.onRemoved.addListener((tabId) => {
  cleanupTabState(tabId, "onRemoved");
});

/* ====== runtime.onMessage ====== */
async function resolveTabId(msg, sender) {
  if (typeof msg.tabId === "number") return msg.tabId;
  if (sender?.tab?.id) return sender.tab.id;
  return null;
}
chrome.runtime.onMessage.addListener((msg, sender, sendResponse) => {
  try {
    if (!msg || typeof msg !== "object") return false;
  } catch {
    return false;
  }
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
          st.pageHidden = msg.visibilityState === "hidden";
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
              log("interaction", {
                tabId,
                evType: "heartbeat-update",
                ts: li,
                site: st.meta?.site,
                reading: st.reading
              });
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
      return false;
    }
    case "get-stats": {
      getLiveFinal()
        .then((payload) => {
          try {
            sendResponse(payload);
          } catch {}
        })
        .catch(() => {
          try {
            sendResponse({ total: 0, daily: 0, inflightMs: 0, recent: [] });
          } catch {}
        });
      return true;
    }
    case "get-telemetry": {
      try {
        sendResponse({ telemetry });
      } catch {}
      return true;
    }
    case "set-log-tags": {
      try {
        const next = { ...LOGT, ...(msg.tags || {}) };
        for (const k of Object.keys(LOGT)) LOGT[k] = !!next[k];
        storageSetQueued({ [KEY.SETTINGS]: { ...SETTINGS } });
        sendResponse({ ok: true, logTags: LOGT });
      } catch (e) {
        try {
          sendResponse({ ok: false, error: String(e) });
        } catch {}
      }
      return true;
    }
    case "get-log-tags": {
      try {
        sendResponse({ ok: true, logTags: LOGT });
      } catch {}
      return true;
    }
    case "set-settings": {
      try {
        const next = { ...SETTINGS, ...(msg.settings || {}), version: DEFAULT.version };
        SETTINGS = next;
        LOG_DUP_SUPPRESS_MS = SETTINGS.logDuplicateSuppressMs;
        storageSetQueued({ [KEY.SETTINGS]: next });
        applySettings();
        try {
          sendResponse({ ok: true, settings: next });
        } catch {}
      } catch (e) {
        try {
          sendResponse({ ok: false, error: String(e) });
        } catch {}
      }
      return true;
    }
    case "export-store": {
      storageGet([KEY.TOTAL, KEY.DAILY, KEY.LOG, KEY.DETAILS, KEY.SETTINGS, KEY.SITE_ENABLE]).then((snap) => {
        try {
          sendResponse({ ok: true, snapshot: snap });
        } catch {}
      });
      return true;
    }
    case "import-store": {
      try {
        const snap = msg.snapshot || {};
        const allowed = new Set([KEY.TOTAL, KEY.DAILY, KEY.LOG, KEY.DETAILS, KEY.SETTINGS, KEY.SITE_ENABLE]);
        const safe = {};
        for (const k of Object.keys(snap)) {
          if (!allowed.has(k)) continue;
          safe[k] = snap[k];
        }
        if (safe[KEY.SETTINGS]?.version && safe[KEY.SETTINGS].version < DEFAULT.version) {
          safe[KEY.SETTINGS] = { ...DEFAULT, ...safe[KEY.SETTINGS], version: DEFAULT.version };
        }
        storageSetQueued(safe);
        try {
          sendResponse({ ok: true });
        } catch {}
        pushLiveUpdate();
      } catch (e) {
        try {
          sendResponse({ ok: false, error: String(e) });
        } catch {}
      }
      return true;
    }
    case "get-site-enable": {
      storageGet([KEY.SITE_ENABLE]).then((v) => {
        try {
          sendResponse(v[KEY.SITE_ENABLE] || {});
        } catch {}
      });
      return true;
    }
    case "set-site-enable": {
      storageGet([KEY.SITE_ENABLE]).then((cur) => {
        const cfg = { ...(cur[KEY.SITE_ENABLE] || {}) };
        cfg[msg.domain] = !!msg.enabled;
        storageSetQueued({ [KEY.SITE_ENABLE]: cfg });
        const ts = nowMs();
        siteEnableCache.value = cfg;
        siteEnableCache.ts = ts;
        try {
          sendResponse({ ok: true });
        } catch {}
        pushLiveUpdate();
      });
      return true;
    }
    case "reset-all": {
      try {
        const init = { [KEY.TOTAL]: 0, [KEY.DAILY]: 0, [KEY.LOG]: {}, [KEY.DETAILS]: {} };
        storageSetQueued(init);
        pushLiveUpdate();
        sendResponse({ ok: true });
      } catch (e) {
        sendResponse({ ok: false, error: String(e) });
      }
      return true;
    }
    case "reset-today": {
      (async () => {
        try {
          const now = nowMs();
          const today = dayKey(now);
          const logs = await getLocal(KEY.LOG, {});
          const details = await getLocal(KEY.DETAILS, {});
          const total = await getLocal(KEY.TOTAL, 0);
          const todayMs = logs[today] || 0;
          delete logs[today];
          delete details[today];
          const nextTotal = Math.max(0, total - todayMs);
          storageSetQueued({
            [KEY.LOG]: logs,
            [KEY.DETAILS]: details,
            [KEY.TOTAL]: nextTotal,
            [KEY.DAILY]: 0
          });
          pushLiveUpdate();
          sendResponse({ ok: true });
        } catch (e) {
          sendResponse({ ok: false, error: String(e) });
        }
      })();
      return true;
    }
    case "debug.dumpState": {
      try {
        const tabs = Array.from(tabState.values());
        sendResponse({ tabs });
      } catch {}
      return true;
    }
    default:
      return false;
  }
});

/* ====== Bootstrap existing tabs ====== */
async function bootstrapExistingTabsOnce() {
  try {
    const tabs = await chrome.tabs.query({});
    for (const t of tabs) {
      let latest;
      try {
        latest = await chrome.tabs.get(t.id);
      } catch {
        latest = t;
      }
      const st = makeStateMinimal(t.id, latest.url || "", latest.title || "", latest.windowId ?? t.windowId);
      st.isCandidate = isCandidateUrl(st.urlObserved);
      st.sessionId = null;
      st.urlObserved = latest.url || "";
      tabState.set(t.id, st);
      try {
        await safeParseAndApply(t.id, st.urlObserved, latest.title || "", nowMs());
      } catch {}
      await pollTab(t.id, st.urlObserved, latest.title || "");
    }
    return tabs.length;
  } catch {
    return 0;
  }
}
async function bootstrapExistingTabs() {
  let count = await bootstrapExistingTabsOnce();
  if (count === 0) setTimeout(() => bootstrapExistingTabsOnce(), 1500);
}
chrome.runtime.onStartup.addListener(() => {
  bootstrapExistingTabs();
});
chrome.runtime.onInstalled.addListener(() => {
  bootstrapExistingTabs();
});

/* ====== Watchdog ====== */
let watchdogInterval = null;
function setupWatchdog() {
  try {
    if (watchdogInterval) clearInterval(watchdogInterval);
    watchdogInterval = setInterval(() => {
      const now = nowMs();
      for (const [tabId, st] of tabState) {
        if (!st.sessionId) continue;
        const recentGap = now - (st.lastInteraction || 0);
        if (st.reading) {
          if (st.pageHidden || recentGap > SETTINGS.idleHoldMs) {
            pauseReading(tabId, now, st.pageHidden ? "NOT_VISIBLE" : "IDLE_HOLD");
          }
        }
      }
    }, SETTINGS.watchdogCheckIntervalMs);
  } catch {}
}

/* ====== Tab queue ====== */
const tabQueues = new Map();
function withTabQueue(tabId, fn) {
  if (typeof tabId !== "number" || tabId <= 0) return Promise.resolve();
  const prev = tabQueues.get(tabId) || Promise.resolve();
  const next = prev.then(() => Promise.resolve().then(fn)).catch(() => {});
  tabQueues.set(tabId, next);
  return next;
}

console.log("background.js (Chrome MV3 service worker) loaded", new Date().toISOString());