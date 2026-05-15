import { createClient } from "@supabase/supabase-js";
import { createHash, randomBytes } from "crypto";
import { createTransport } from "nodemailer";

const SUPABASE_URL = process.env.SUPABASE_URL!;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY!;
const D1_WORKER_URL = process.env.D1_WORKER_URL || "https://s.panlay.com";
const D1_API_KEY = process.env.CF_WORKER_API_KEY!;

if (!SUPABASE_URL || !SUPABASE_SERVICE_KEY) {
  console.error("Missing SUPABASE_URL or SUPABASE_SERVICE_KEY");
  process.exit(1);
}
if (!D1_API_KEY) {
  console.error("Missing CF_WORKER_API_KEY");
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

const MAX_LINKS_PER_RUN = 10000;
const CONCURRENCY = 20;
const LINK_CHECK_TIMEOUT = 10000;
const BATCH_UPDATE_SIZE = 500; // 每批更新 500 条
const D1_TIMEOUT = 30000; // D1 请求超时 30 秒
const XUNLEI_CONCURRENCY = 5;
const XUNLEI_USES_PER_TOKEN = 30;

function escapeHtml(str: string): string {
  return str.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
}

async function fetchWithTimeout(url: string, options: RequestInit, timeout: number = LINK_CHECK_TIMEOUT): Promise<Response> {
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), timeout);
  try {
    const response = await fetch(url, { ...options, signal: controller.signal });
    return response;
  } finally {
    clearTimeout(timeoutId);
  }
}

const TABLES = [
  { name: "short_links", urlField: "original_url", statusField: "status", checkedField: "last_checked", rpcName: "batch_update_short_link_status" },
  { name: "resources", urlField: "url", statusField: "status", checkedField: "last_checked_at", rpcName: "batch_update_resource_status" },
];

// D1 helper functions (resources 表已迁移到 D1)
async function d1Query<T = Record<string, unknown>>(sql: string, params: unknown[] = []): Promise<{ rows: T[] }> {
  const res = await fetchWithTimeout(`${D1_WORKER_URL}/api/d1`, {
    method: "POST",
    headers: { "Content-Type": "application/json", Authorization: `Bearer ${D1_API_KEY}` },
    body: JSON.stringify({ sql, params }),
  }, D1_TIMEOUT);
  if (!res.ok) throw new Error(`D1 query failed: ${res.status} ${await res.text()}`);
  return res.json();
}

async function d1Batch(items: Array<{ sql: string; params?: unknown[] }>): Promise<void> {
  const res = await fetchWithTimeout(`${D1_WORKER_URL}/api/d1`, {
    method: "POST",
    headers: { "Content-Type": "application/json", Authorization: `Bearer ${D1_API_KEY}` },
    body: JSON.stringify({ batch: items }),
  }, D1_TIMEOUT);
  if (!res.ok) throw new Error(`D1 batch failed: ${res.status} ${await res.text()}`);
}

async function checkLinkStatus(url: string): Promise<{ valid: boolean | null; reason?: string; title?: string; partialViolation?: boolean }> {
  try {
    if (url.includes("quark.cn")) return await checkQuarkLink(url);
    if (url.includes("drive.uc.cn") || url.includes("fast.uc.cn")) return await checkUcLink(url);
    if (url.includes("pan.baidu.com") || url.includes("yun.baidu.com")) return await checkBaiduLink(url);
    if (url.includes("pan.xunlei.com")) return await checkXunleiLink(url);
    return { valid: null, reason: "不支持的网盘类型" };
  } catch (error) {
    if (error instanceof Error && error.name === "AbortError") return { valid: null, reason: "检测超时" };
    console.error("Check error:", error);
    return { valid: null, reason: "检查出错" };
  }
}

// UC：与夸克同构；detail 用 share/sharepage/detail + _fetch_share=1，返回 share.partial_violation
async function checkUcLink(url: string): Promise<{ valid: boolean | null; reason?: string; title?: string; partialViolation?: boolean }> {
  try {
    const match = url.match(/\/s\/([a-zA-Z0-9]+)/);
    if (!match) return { valid: false, reason: "链接格式无效" };
    const pwd_id = match[1];
    const urlObj = new URL(url);
    const passcode = urlObj.searchParams.get("pwd") || urlObj.searchParams.get("password") || "";

    const COMMON = "entry=ft&fr=pc&pr=UCBrowser";
    const UC_HEADERS = {
      "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
      "Referer": "https://drive.uc.cn/",
      "Origin": "https://drive.uc.cn",
    };

    const tokenUrl = `https://pc-api.uc.cn/1/clouddrive/share/sharepage/token?${COMMON}&__dt=2000&__t=${Date.now()}`;
    const tokenRes = await fetchWithTimeout(tokenUrl, {
      method: "POST",
      headers: { ...UC_HEADERS, "Content-Type": "application/json" },
      body: JSON.stringify({ pwd_id, passcode }),
    });
    const tokenData = await tokenRes.json();

    if (tokenData.code === 41003) return { valid: false, reason: "分享已过期" };
    if (tokenData.code === 41006) return { valid: false, reason: "分享已取消" };
    if (tokenData.code !== 0) return { valid: false, reason: tokenData.message || "链接无效" };

    const stoken = tokenData.data?.stoken;
    if (stoken) {
      const detailUrl = `https://pc-api.uc.cn/1/clouddrive/share/sharepage/detail?${COMMON}&pwd_id=${pwd_id}&stoken=${encodeURIComponent(stoken)}&pdir_fid=0&force=0&_page=1&_size=50&_fetch_banner=1&_fetch_share=1&_fetch_total=1&_sort=file_type:asc,updated_at:desc`;
      const detailRes = await fetchWithTimeout(detailUrl, { headers: UC_HEADERS });
      const detailData = await detailRes.json();

      if (detailData.code === 0) {
        const list = detailData.data?.list || [];
        if (list.length === 0) return { valid: false, reason: "文件已被删除" };
        const share = detailData.data?.share;
        const partialViolation = !!share?.partial_violation;
        const title = share?.title || tokenData.data?.title || list[0]?.file_name || undefined;
        return { valid: true, title, partialViolation };
      }
    }

    return { valid: true, title: tokenData.data?.title || undefined };
  } catch (error) {
    if (error instanceof Error && error.name === "AbortError") return { valid: null, reason: "检测超时" };
    console.error("UC check error:", error);
    return { valid: null, reason: "检查出错" };
  }
}

async function checkQuarkLink(url: string): Promise<{ valid: boolean | null; reason?: string; title?: string }> {
  try {
    const match = url.match(/\/s\/([a-zA-Z0-9]+)/);
    if (!match) return { valid: false, reason: "链接格式无效" };
    const pwd_id = match[1];
    const urlObj = new URL(url);
    const passcode = urlObj.searchParams.get("pwd") || "";

    const tokenUrl = `https://drive-h.quark.cn/1/clouddrive/share/sharepage/token?pr=ucpro&fr=pc&uc_param_str=&__dt=2000&__t=${Date.now()}`;
    const tokenRes = await fetchWithTimeout(tokenUrl, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ pwd_id, passcode }),
    });
    const tokenData = await tokenRes.json();

    if (tokenData.code === 41003) return { valid: false, reason: "分享已过期" };
    if (tokenData.code === 41006) return { valid: false, reason: "分享已取消" };
    if (tokenData.code !== 0) return { valid: false, reason: tokenData.message || "链接无效" };

    // 标题优先从 token API 的 data.title 取
    const shareTitle = tokenData.data?.title || undefined;

    const stoken = tokenData.data?.stoken;
    if (stoken) {
      const detailUrl = `https://drive-h.quark.cn/1/clouddrive/share/sharepage/detail?pr=ucpro&fr=pc&pwd_id=${pwd_id}&stoken=${encodeURIComponent(stoken)}&pdir_fid=0&force=0&_page=1&_size=50`;
      const detailRes = await fetchWithTimeout(detailUrl, {});
      const detailData = await detailRes.json();
      if (detailData.code === 0) {
        if ((detailData.metadata?._total || 0) === 0) return { valid: false, reason: "文件已被删除" };
        const title = shareTitle || detailData.data?.share?.title || detailData.data?.list?.[0]?.file_name || undefined;
        return { valid: true, title };
      }
    }

    return { valid: true, title: shareTitle };
  } catch (error) {
    if (error instanceof Error && error.name === "AbortError") return { valid: null, reason: "检测超时" };
    console.error("Quark check error:", error);
    return { valid: null, reason: "检查出错" };
  }
}

async function checkBaiduLink(url: string): Promise<{ valid: boolean | null; reason?: string; title?: string }> {
  try {
    const urlObj = new URL(url);
    const password = urlObj.searchParams.get("pwd") || "";

    // 第一步：访问分享链接，获取重定向
    const step1Res = await fetchWithTimeout(url, {
      method: "GET",
      headers: {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36",
      },
      redirect: "manual",
    });

    const location = step1Res.headers.get("location") || "";
    if (step1Res.status === 200 && !location) return { valid: false, reason: "分享已过期" };
    if (location.includes("error")) return { valid: false, reason: "链接已失效" };

    // 提取 surl
    const surlMatch = location.match(/surl=([^&]+)/);
    if (!surlMatch) return { valid: true };
    const surl = surlMatch[1];

    // 收集 cookies
    const cookies = step1Res.headers.getSetCookie?.() || [];
    const cookieStr = cookies.map((c: string) => c.split(";")[0]).join("; ");

    // 第二步：验证密码，获取 BDCLND
    const verifyUrl = `https://pan.baidu.com/share/verify?surl=${surl}&t=${Date.now()}&channel=chunlei&web=1&app_id=250528&clienttype=0`;
    const step2Res = await fetchWithTimeout(verifyUrl, {
      method: "POST",
      headers: {
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Origin": "https://pan.baidu.com",
        "Referer": `https://pan.baidu.com/share/init?surl=${surl}`,
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36",
        "Cookie": cookieStr,
      },
      body: `pwd=${password}&vcode=&vcode_str=`,
    });

    const step2Json = await step2Res.json();
    if (step2Json.errno !== 0) return { valid: true };

    // 提取 BDCLND
    const newCookies = step2Res.headers.getSetCookie?.() || [];
    let bdclnd = "";
    for (const cookie of newCookies) {
      if (cookie.startsWith("BDCLND=")) {
        bdclnd = cookie.split("=")[1].split(";")[0];
        break;
      }
    }

    // 第三步：获取文件列表（包含标题）
    const allCookies = [...cookies, ...newCookies];
    if (bdclnd) allCookies.push(`BDCLND=${bdclnd}`);
    const allCookieStr = allCookies.map((c: string) => c.split(";")[0]).join("; ");

    const listUrl = `https://pan.baidu.com/share/list?web=5&app_id=250528&shorturl=${surl}&root=1&page=1&num=20&order=time&desc=1&showempty=0&channel=chunlei&clienttype=0`;
    const step3Res = await fetchWithTimeout(listUrl, {
      method: "GET",
      headers: {
        "Referer": `https://pan.baidu.com/share/init?surl=${surl}`,
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36",
        "Cookie": allCookieStr,
      },
    });

    const step3Json = await step3Res.json();
    if (step3Json.errno === 0) {
      let title = step3Json.title || "";
      if (title.startsWith("/")) title = title.slice(1);
      if (!title && step3Json.list?.length > 0) title = step3Json.list[0].server_filename || "";
      return { valid: true, title: title || undefined };
    }

    return { valid: true };
  } catch (error) {
    if (error instanceof Error && error.name === "AbortError") return { valid: null, reason: "检测超时" };
    console.error("Baidu check failed, fallback:", error);
    try {
      const res = await fetchWithTimeout(url, {
        headers: { "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 13_2_3 like Mac OS X) AppleWebKit/605.1.15" },
        redirect: "manual",
      });
      const location = res.headers.get("location") || "";
      if (location.includes("error")) return { valid: false, reason: "链接已失效" };
      return { valid: true };
    } catch {
      return { valid: null, reason: "检查出错" };
    }
  }
}

// ─── 迅雷链接检测（匿名方式，不需要登录账号） ──────────────
// 常量同步自 frontend/src/lib/pan/xunlei-api.ts，更新时需同步

const XUNLEI_CLIENT_ID = "Xqp0kJBXWhwaTpB6";
const XUNLEI_CLIENT_VERSION = "1.92.36";
const XUNLEI_PACKAGE_NAME = "pan.xunlei.com";
const XUNLEI_CAPTCHA_SALTS = [
  "QIBABOlpDvA2v0fKnj7XghyKAJcQg1iSnQXYF984",
  "J1YUyz+VhaK8a2XLOw4",
  "1Ijz+KAMo2EaEFBxuaWzWXDFc6elpw",
  "gxvxtLin/vSdF4KpWDrsTG",
  "aE5Pc6U3mwmysNxbUE",
  "oEfQaxODSGv760DVHq1fHmamLPMmm9HXb/m",
  "4neeDjxdgJiTcTen1E",
  "SKyJtnf/5ECeLG1zzhL",
  "J5NrxPsM7bSmLQ",
  "hPueqITSMJhvb3JlMNK6CwKC",
  "hosGF+0Xhhr",
];

function md5(str: string): string {
  return createHash("md5").update(str).digest("hex");
}

let xunleiCaptchaCache = { token: "", deviceId: "", expiry: 0, uses: 0 };
let xunleiConsecutiveFailures = 0;
let xunleiTokenRefreshInProgress: Promise<{ token: string; deviceId: string }> | null = null;
const XUNLEI_MAX_CONSECUTIVE_FAILURES = 5;

async function generateXunleiCaptchaToken(): Promise<{ token: string; deviceId: string }> {
  const deviceId = randomBytes(32).toString("hex").substring(0, 32);
  const timestamp = String(Date.now());
  let str = XUNLEI_CLIENT_ID + XUNLEI_CLIENT_VERSION + XUNLEI_PACKAGE_NAME + deviceId + timestamp;
  for (const salt of XUNLEI_CAPTCHA_SALTS) str = md5(str + salt);

  const res = await fetchWithTimeout("https://xluser-ssl.xunlei.com/v1/shield/captcha/init", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      client_id: XUNLEI_CLIENT_ID,
      action: "get:/drive/v1/files",
      device_id: deviceId,
      meta: { package_name: XUNLEI_PACKAGE_NAME, client_version: XUNLEI_CLIENT_VERSION, captcha_sign: "1." + str, timestamp, user_id: "0" },
    }),
  });
  const data = await res.json() as Record<string, unknown>;
  if (!data.captcha_token) throw new Error("captcha init 失败: " + ((data.error_description as string) || (data.error as string) || "unknown"));

  xunleiCaptchaCache = { token: data.captcha_token as string, deviceId, expiry: Date.now() + 240000, uses: 1 };
  return { token: xunleiCaptchaCache.token, deviceId };
}

async function getXunleiCaptchaToken(): Promise<{ token: string; deviceId: string }> {
  if (xunleiCaptchaCache.token && Date.now() < xunleiCaptchaCache.expiry && xunleiCaptchaCache.uses < XUNLEI_USES_PER_TOKEN) {
    xunleiCaptchaCache.uses++;
    return { token: xunleiCaptchaCache.token, deviceId: xunleiCaptchaCache.deviceId };
  }

  // Mutex: 防止并发请求同时刷新 token
  if (xunleiTokenRefreshInProgress) return xunleiTokenRefreshInProgress;
  xunleiTokenRefreshInProgress = generateXunleiCaptchaToken();
  try {
    return await xunleiTokenRefreshInProgress;
  } finally {
    xunleiTokenRefreshInProgress = null;
  }
}

async function checkXunleiLink(url: string): Promise<{ valid: boolean | null; reason?: string; title?: string }> {
  // 连续失败过多，跳过剩余迅雷链接
  if (xunleiConsecutiveFailures >= XUNLEI_MAX_CONSECUTIVE_FAILURES) {
    return { valid: null, reason: "迅雷连续失败过多，已跳过" };
  }

  try {
    const match = url.match(/pan\.xunlei\.com\/s\/([a-zA-Z0-9_-]+)/);
    if (!match) return { valid: false, reason: "链接格式无效" };
    const shareId = match[1];

    let passCode = "";
    try { passCode = new URL(url).searchParams.get("pwd") || ""; } catch {}

    const { token, deviceId } = await getXunleiCaptchaToken();

    const apiUrl = `https://api-pan.xunlei.com/drive/v1/share?share_id=${shareId}&pass_code=${encodeURIComponent(passCode)}&limit=1&pass_code_token=&page_token=&thumbnail_size=SIZE_SMALL`;
    const res = await fetchWithTimeout(apiUrl, {
      headers: {
        "x-captcha-token": token,
        "x-client-id": XUNLEI_CLIENT_ID,
        "x-device-id": deviceId,
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Origin": "https://pan.xunlei.com",
        "Referer": "https://pan.xunlei.com/",
      },
    });

    const data = await res.json() as Record<string, unknown>;

    // captcha_invalid → 强制刷新 token，返回未知
    if (data.error === "captcha_invalid") {
      xunleiCaptchaCache.expiry = 0; // 强制下次刷新
      xunleiConsecutiveFailures++;
      return { valid: null, reason: "captcha_invalid" };
    }

    // 成功响应，重置连续失败计数
    xunleiConsecutiveFailures = 0;

    if (data.share_status === "OK" || data.share_status === "PASS_CODE_EMPTY") {
      const files = data.files as Array<{ name?: string }> | undefined;
      const title = files?.[0]?.name || undefined;
      return { valid: true, title };
    }

    return { valid: false, reason: (data.share_status as string) || (data.error_description as string) || "分享无效" };
  } catch (error) {
    if (error instanceof Error && error.name === "AbortError") return { valid: null, reason: "检测超时" };
    xunleiConsecutiveFailures++;
    console.error("Xunlei check error:", error);
    return { valid: null, reason: "检查出错" };
  }
}

// 待批量更新的缓冲区，按表名分组
const pendingUpdates: Map<string, { ids: string[]; statuses: string[]; rpcName: string }> = new Map();

async function flushUpdates(tableName?: string) {
  const tables = tableName ? [tableName] : [...pendingUpdates.keys()];
  for (const name of tables) {
    const pending = pendingUpdates.get(name);
    if (!pending || pending.ids.length === 0) continue;

    if (name === "resources") {
      // resources 表在 D1
      try {
        const now = new Date().toISOString();
        const items = pending.ids.map((id, i) => ({
          sql: "UPDATE resources SET status = ?, last_checked_at = ? WHERE id = ?",
          params: [pending.statuses[i], now, id],
        }));
        await d1Batch(items);
        console.log(`  💾 批量更新 ${name} (D1): ${pending.ids.length} 条`);
      } catch (e) {
        console.error(`批量更新 ${name} (D1) 失败 (${pending.ids.length} 条):`, e);
      }
    } else {
      // short_links 等表仍在 Supabase
      const { error } = await supabase.rpc(pending.rpcName, {
        p_ids: pending.ids,
        p_statuses: pending.statuses,
        p_checked_at: new Date().toISOString(),
      });

      if (error) {
        console.error(`批量更新 ${name} 失败 (${pending.ids.length} 条):`, error);
      } else {
        console.log(`  💾 批量更新 ${name}: ${pending.ids.length} 条`);
      }
    }

    pending.ids = [];
    pending.statuses = [];
  }
}

async function queueUpdate(link: { id: string; table: string }, newStatus: string) {
  let pending = pendingUpdates.get(link.table);
  if (!pending) {
    const tableConfig = TABLES.find(t => t.name === link.table)!;
    pending = { ids: [], statuses: [], rpcName: tableConfig.rpcName };
    pendingUpdates.set(link.table, pending);
  }

  pending.ids.push(link.id);
  pending.statuses.push(newStatus);

  // 达到批量大小，立即刷新
  if (pending.ids.length >= BATCH_UPDATE_SIZE) {
    await flushUpdates(link.table);
  }
}

// 待批量更新标题的缓冲区
const pendingTitles: { ids: string[]; titles: string[] } = { ids: [], titles: [] };

async function flushTitles() {
  if (pendingTitles.ids.length === 0) return;

  try {
    const items = pendingTitles.ids.map((id, i) => ({
      sql: "UPDATE resources SET title = ? WHERE id = ?",
      params: [pendingTitles.titles[i], id],
    }));
    await d1Batch(items);
    console.log(`  📝 批量更新标题 (D1): ${pendingTitles.ids.length} 条`);
  } catch (e) {
    console.error(`批量更新标题 (D1) 失败 (${pendingTitles.ids.length} 条):`, e);
  }

  pendingTitles.ids = [];
  pendingTitles.titles = [];
}

async function queueTitleUpdate(id: string, title: string) {
  pendingTitles.ids.push(id);
  pendingTitles.titles.push(title);

  if (pendingTitles.ids.length >= BATCH_UPDATE_SIZE) {
    await flushTitles();
  }
}

async function markLinksNotified(links: { linkId: string; table: string }[]) {
  const now = new Date().toISOString();
  const d1Links: typeof links = [];
  const supaLinks: typeof links = [];
  for (const l of links) {
    if (l.table === "resources") d1Links.push(l);
    else if (l.table === "short_links") supaLinks.push(l);
  }

  if (d1Links.length > 0) {
    try {
      await d1Batch(d1Links.map(l => ({
        sql: "UPDATE resources SET notified_at = ? WHERE id = ?",
        params: [now, l.linkId],
      })));
      console.log(`  📌 标记 D1 notified_at: ${d1Links.length} 条`);
    } catch (e) {
      console.error("标记 D1 notified_at 失败:", e);
    }
  }

  if (supaLinks.length > 0) {
    try {
      const ids = supaLinks.map(l => l.linkId);
      const { error } = await supabase.from("short_links").update({ notified_at: now }).in("id", ids);
      if (error) {
        console.error("标记 short_links notified_at 失败:", error);
      } else {
        console.log(`  📌 标记 short_links notified_at: ${supaLinks.length} 条`);
      }
    } catch (e) {
      console.error("标记 short_links notified_at 失败:", e);
    }
  }
}

const DEFAULT_TITLES = ["百度网盘资源", "夸克网盘资源", "迅雷网盘资源"];
let titleUpdated = 0;

interface LinkToCheck {
  id: string;
  url: string;
  table: string;
  statusField: string;
  checkedField: string;
  lastChecked: string | null;
  currentStatus: string | null;
  currentTitle: string | null;
  userId: string | null;
  notifiedAt: string | null;
  /** 仅 short_links 有此列；resources 表恒 false */
  currentPartial: boolean;
}

// 新失效链接按用户分组
const newlyExpiredByUser = new Map<string, { url: string; title: string | null; linkId: string; table: string }[]>();

// 新触发「部分文件被过滤」的链接按用户分组（false → true 翻转时入队）
const newlyPartialByUser = new Map<string, { url: string; title: string | null }[]>();
// 需要批量写回 partial_violation 列的 short_links（既包含 false→true 也包含 true→false）
const pendingPartialUpdates: { id: string; partial: boolean }[] = [];

async function checkBatch(links: LinkToCheck[], startIndex: number, totalCount: number) {
  const results = await Promise.all(
    links.map(async (link, i) => {
      const progress = `[${startIndex + i + 1}/${totalCount}]`;
      try {
        const result = await checkLinkStatus(link.url);
        const newStatus = result.valid === true ? "valid" : result.valid === false ? "expired" : null;

        if (!newStatus) {
          // 检测超时/出错，保持原状态不变，但更新 last_checked 防止同一条链接反复排在队列最前
          console.log(`${progress} ? [${link.table}] ${link.url.slice(0, 50)}... - ${link.currentStatus} (${result.reason || "检测失败"})`);
          await queueUpdate(link, link.currentStatus || "unchecked");
          return { status: link.currentStatus, changed: false, error: false };
        }

        if (newStatus !== link.currentStatus) {
          const icon = newStatus === "valid" ? "✓" : "✗";
          console.log(`${progress} ${icon} [${link.table}] ${link.url.slice(0, 50)}... - ${link.currentStatus} → ${newStatus} (${result.reason || ""})`);

          // 记录新失效链接（用于邮件通知），已通知过的不再通知
          if (newStatus === "expired" && link.userId && !link.notifiedAt) {
            const list = newlyExpiredByUser.get(link.userId) || [];
            list.push({ url: link.url, title: link.currentTitle, linkId: link.id, table: link.table });
            newlyExpiredByUser.set(link.userId, list);
          }

          // 同步状态到 monitored_links（如果该用户有监控同一 URL）
          if (link.userId && link.table === "short_links") {
            await supabase
              .from("monitored_links")
              .update({ status: newStatus, last_checked: new Date().toISOString() })
              .eq("user_id", link.userId)
              .eq("url", link.url);
          }
        } else {
          console.log(`${progress} = [${link.table}] ${link.url.slice(0, 50)}... - ${newStatus} (unchanged)`);
        }

        // 部分文件被过滤状态翻转（仅 short_links 有此列；false→true 发邮件，true→false 静默改回）
        const newPartial = !!result.partialViolation;
        if (link.table === "short_links" && newPartial !== link.currentPartial) {
          pendingPartialUpdates.push({ id: link.id, partial: newPartial });
          if (newPartial && link.userId) {
            console.log(`${progress} ⚠ [${link.table}] ${link.url.slice(0, 50)}... - 触发「部分文件被过滤」`);
            const list = newlyPartialByUser.get(link.userId) || [];
            list.push({ url: link.url, title: link.currentTitle });
            newlyPartialByUser.set(link.userId, list);
          }
          // 同步到 monitored_links 同一 URL
          if (link.userId) {
            await supabase
              .from("monitored_links")
              .update({ partial_violation: newPartial })
              .eq("user_id", link.userId)
              .eq("url", link.url);
          }
        }

        // 无论状态是否变化都更新 last_checked
        await queueUpdate(link, newStatus);

        // 默认标题 + 抓到了真实标题 → 更新标题
        if (result.title && link.currentTitle && DEFAULT_TITLES.includes(link.currentTitle) && link.table === "resources") {
          await queueTitleUpdate(link.id, result.title.slice(0, 500));
          console.log(`${progress} 📝 标题更新: "${link.currentTitle}" → "${result.title.slice(0, 50)}"`);
          titleUpdated++;
        }

        return { status: newStatus, changed: newStatus !== link.currentStatus, error: false };
      } catch (e) {
        console.log(`${progress} ! [${link.table}] ${link.url.slice(0, 50)}... - 检测出错`);
        return { status: "error", changed: false, error: true };
      }
    })
  );
  return results;
}

async function main() {
  console.log("========================================");
  console.log("开始检测链接状态 (优化版: 跳过未变化 + 批量更新)");
  console.log("时间:", new Date().toISOString());
  console.log("并发数:", CONCURRENCY);
  console.log("批量更新大小:", BATCH_UPDATE_SIZE);
  console.log("========================================\n");

  console.log("迅雷检测: 匿名模式（无需登录）\n");

  const allLinks: LinkToCheck[] = [];

  for (const table of TABLES) {
    if (table.name === "resources") {
      // resources 表从 D1 读取
      console.log(`${table.name} 表: 开始分页读取链接...`);

      const PAGE_SIZE = 1000;
      // 按状态分开查 + 游标分页，利用 (status, last_checked_at, id) 索引，任意页都只扫描 PAGE_SIZE 行
      const ACTIVE_STATUSES = ["unchecked", "valid"];

      for (const st of ACTIVE_STATUSES) {
        if (allLinks.length >= MAX_LINKS_PER_RUN) break;
        let cursorTime: string | null = null;
        let cursorId: string | null = null;
        let nullCursorId: string | null = null; // NULL 记录的游标（按 id 分页）
        let nullPhase = true; // 是否还在查 NULL 记录

        while (allLinks.length < MAX_LINKS_PER_RUN) {
          let sql: string;
          let params: unknown[];

          if (nullPhase) {
            // 查 last_checked_at 为 NULL 的（未检测过的优先），用 id 做游标
            if (nullCursorId) {
              sql = "SELECT id, user_id, title, url, status, last_checked_at, notified_at FROM resources WHERE status = ? AND last_checked_at IS NULL AND id > ? ORDER BY id ASC LIMIT ?";
              params = [st, nullCursorId, PAGE_SIZE];
            } else {
              sql = "SELECT id, user_id, title, url, status, last_checked_at, notified_at FROM resources WHERE status = ? AND last_checked_at IS NULL ORDER BY id ASC LIMIT ?";
              params = [st, PAGE_SIZE];
            }
          } else if (cursorTime === null) {
            // NULL 阶段结束，开始查有时间戳的，从最早的开始
            sql = "SELECT id, user_id, title, url, status, last_checked_at, notified_at FROM resources WHERE status = ? AND last_checked_at IS NOT NULL ORDER BY last_checked_at ASC, id ASC LIMIT ?";
            params = [st, PAGE_SIZE];
          } else {
            // 游标分页：用 last_checked_at > ? 简单游标（OR 条件会破坏索引效率）
            sql = "SELECT id, user_id, title, url, status, last_checked_at, notified_at FROM resources WHERE status = ? AND last_checked_at > ? ORDER BY last_checked_at ASC, id ASC LIMIT ?";
            params = [st, cursorTime, PAGE_SIZE];
          }

          const { rows } = await d1Query<{ id: string; user_id: string | null; title: string | null; url: string; status: string; last_checked_at: string | null; notified_at: string | null }>(sql, params);

          if (!rows || rows.length === 0) {
            if (nullPhase) { nullPhase = false; continue; }
            break;
          }

          for (const row of rows) {
            if (allLinks.length >= MAX_LINKS_PER_RUN) break;
            allLinks.push({
              id: row.id,
              url: row.url,
              table: table.name,
              statusField: table.statusField,
              checkedField: table.checkedField,
              lastChecked: row.last_checked_at,
              currentStatus: row.status,
              currentTitle: row.title,
              currentPartial: false,
              userId: row.user_id || null,
              notifiedAt: row.notified_at || null,
            });
          }

          const lastRow = rows[rows.length - 1];
          if (nullPhase) {
            nullCursorId = lastRow.id;
            if (rows.length < PAGE_SIZE) { nullPhase = false; continue; }
          } else {
            cursorTime = lastRow.last_checked_at;
            cursorId = lastRow.id;
            if (rows.length < PAGE_SIZE) break;
          }
        }
      }
    } else {
      // short_links 等表从 Supabase 读取
      const { count: totalCount } = await supabase.from(table.name).select("*", { count: "exact", head: true });
      const { count: expiredCount } = await supabase.from(table.name).select("*", { count: "exact", head: true }).eq(table.statusField, "expired");
      console.log(`${table.name} 表总数: ${totalCount || 0}，已失效: ${expiredCount || 0}，待检测: ${(totalCount || 0) - (expiredCount || 0)}`);

      const PAGE_SIZE = 1000;
      let page = 0;
      let hasMore = true;

      while (hasMore && allLinks.length < MAX_LINKS_PER_RUN) {
        const { data, error } = await supabase
          .from(table.name)
          .select(`id, user_id, title, ${table.urlField}, ${table.statusField}, ${table.checkedField}, notified_at, partial_violation`)
          .neq(table.statusField, "expired")
          .order(table.checkedField, { ascending: true, nullsFirst: true })
          .range(page * PAGE_SIZE, (page + 1) * PAGE_SIZE - 1);

        if (error) { console.error(`获取 ${table.name} 失败:`, error); break; }
        if (!data || data.length === 0) { hasMore = false; break; }

        for (const row of data) {
          if (allLinks.length >= MAX_LINKS_PER_RUN) break;
          allLinks.push({
            id: row.id,
            url: row[table.urlField],
            table: table.name,
            statusField: table.statusField,
            checkedField: table.checkedField,
            lastChecked: row[table.checkedField],
            currentStatus: row[table.statusField],
            currentTitle: row.title,
            userId: row.user_id || null,
            notifiedAt: row.notified_at || null,
            currentPartial: !!row.partial_violation,
          });
        }

        hasMore = data.length === PAGE_SIZE;
        page++;
      }
    }
  }

  allLinks.sort((a, b) => {
    if (!a.lastChecked && !b.lastChecked) return 0;
    if (!a.lastChecked) return -1;
    if (!b.lastChecked) return 1;
    return new Date(a.lastChecked).getTime() - new Date(b.lastChecked).getTime();
  });

  // 分离迅雷链接和其他链接
  const otherLinks = allLinks.filter(l => !l.url.includes("xunlei")).slice(0, MAX_LINKS_PER_RUN);
  const xunleiLinks = allLinks.filter(l => l.url.includes("xunlei")).slice(0, MAX_LINKS_PER_RUN);
  const toCheck = [...otherLinks, ...xunleiLinks];

  console.log(`\n本次检测: ${toCheck.length} 条 (百度/夸克: ${otherLinks.length}, 迅雷: ${xunleiLinks.length})`);
  if (otherLinks.length > 0) console.log(`  百度/夸克: 并发 ${CONCURRENCY}`);
  if (xunleiLinks.length > 0) console.log(`  迅雷: 并发 ${XUNLEI_CONCURRENCY}`);
  console.log("");

  if (toCheck.length === 0) { console.log("没有需要检测的链接"); return; }

  titleUpdated = 0;
  const stats = { valid: 0, expired: 0, unknown: 0, errors: 0, changed: 0, skipped: 0 };

  function tallyResults(results: Awaited<ReturnType<typeof checkBatch>>) {
    for (const r of results) {
      if (r.error) stats.errors++;
      else if (r.status === "valid") stats.valid++;
      else if (r.status === "expired") stats.expired++;
      else stats.unknown++;
      if (r.changed) stats.changed++;
      else if (!r.error) stats.skipped++;
    }
  }

  let processedSoFar = 0;

  // 第一轮：百度 + 夸克（并发 20）
  for (let i = 0; i < otherLinks.length; i += CONCURRENCY) {
    const batch = otherLinks.slice(i, i + CONCURRENCY);
    tallyResults(await checkBatch(batch, processedSoFar + i, toCheck.length));
    if (i + CONCURRENCY < otherLinks.length) await new Promise((r) => setTimeout(r, 500));
  }
  processedSoFar += otherLinks.length;

  await flushUpdates();
  await flushTitles();

  // 第二轮：迅雷（并发 5）
  if (xunleiLinks.length > 0) {
    console.log(`\n─── 迅雷链接检测 (${xunleiLinks.length} 条, 并发 ${XUNLEI_CONCURRENCY}) ───\n`);
  }
  for (let i = 0; i < xunleiLinks.length; i += XUNLEI_CONCURRENCY) {
    const batch = xunleiLinks.slice(i, i + XUNLEI_CONCURRENCY);
    tallyResults(await checkBatch(batch, processedSoFar + i, toCheck.length));
    if (i + XUNLEI_CONCURRENCY < xunleiLinks.length) await new Promise((r) => setTimeout(r, 500));
  }

  // 刷新剩余的待更新数据
  await flushUpdates();
  await flushTitles();

  // 写回 partial_violation 列翻转
  if (pendingPartialUpdates.length > 0) {
    const trueIds = pendingPartialUpdates.filter(p => p.partial).map(p => p.id);
    const falseIds = pendingPartialUpdates.filter(p => !p.partial).map(p => p.id);
    if (trueIds.length > 0) {
      const { error } = await supabase.from("short_links").update({ partial_violation: true }).in("id", trueIds);
      if (error) console.error("批量更新 partial_violation=true 失败:", error);
      else console.log(`  ⚠ 标记 partial_violation=true: ${trueIds.length} 条`);
    }
    if (falseIds.length > 0) {
      const { error } = await supabase.from("short_links").update({ partial_violation: false }).in("id", falseIds);
      if (error) console.error("批量更新 partial_violation=false 失败:", error);
      else console.log(`  ✓ 标记 partial_violation=false: ${falseIds.length} 条`);
    }
  }

  // ─── 邮件通知 ────────────────────────────────────────
  let emailsSent = 0;
  if ((newlyExpiredByUser.size > 0 || newlyPartialByUser.size > 0) && (process.env.SMTP_PASSWORD || process.env.SMTP_163_PASSWORD)) {
    console.log(`\n📧 发送通知邮件（失效=${newlyExpiredByUser.size} 用户 / 部分过滤=${newlyPartialByUser.size} 用户）...`);
    const userIds = [...new Set([...newlyExpiredByUser.keys(), ...newlyPartialByUser.keys()])];
    const { data: users } = await supabase
      .from("users")
      .select("id, notification_email")
      .in("id", userIds)
      .not("notification_email", "is", null);

    if (users && users.length > 0) {
      const QQ_DAILY_LIMIT = 99;

      // 读取今天已发送数量
      const today = new Date().toISOString().slice(0, 10);
      const { data: todayCounts } = await supabase
        .from("email_daily_counts")
        .select("provider, count")
        .eq("date", today);
      let qq1SentCount = todayCounts?.find((c: { provider: string }) => c.provider === "qq")?.count || 0;
      let qq2SentCount = todayCounts?.find((c: { provider: string }) => c.provider === "qq2")?.count || 0;
      console.log(`  今日已发送: QQ1=${qq1SentCount}, QQ2=${qq2SentCount}, 163=${todayCounts?.find((c: { provider: string }) => c.provider === "163")?.count || 0}`);

      const qq1Transporter = process.env.SMTP_PASSWORD ? createTransport({
        host: "smtp.qq.com", port: 465, secure: true,
        auth: { user: "panyouzhushou@foxmail.com", pass: process.env.SMTP_PASSWORD },
      }) : null;

      const qq2Transporter = process.env.SMTP_QQ2_PASSWORD ? createTransport({
        host: "smtp.qq.com", port: 465, secure: true,
        auth: { user: "panyouzhushou2@foxmail.com", pass: process.env.SMTP_QQ2_PASSWORD },
      }) : null;

      const neteaseUser = process.env.SMTP_163_USER || "ipwenan@163.com";
      const neteaseTransporter = process.env.SMTP_163_PASSWORD ? createTransport({
        host: "smtp.163.com", port: 465, secure: true,
        auth: { user: neteaseUser, pass: process.env.SMTP_163_PASSWORD },
      }) : null;

      // 按优先级选择通道：QQ1 → QQ2 → 163
      function pickChannel(): { transporter: typeof qq1Transporter; from: string; provider: string; providerKey: string } | null {
        if (qq1Transporter && qq1SentCount < QQ_DAILY_LIMIT) {
          return { transporter: qq1Transporter, from: '"盘友助手" <panyouzhushou@foxmail.com>', provider: "QQ1", providerKey: "qq" };
        }
        if (qq2Transporter && qq2SentCount < QQ_DAILY_LIMIT) {
          return { transporter: qq2Transporter, from: '"盘友助手" <panyouzhushou2@foxmail.com>', provider: "QQ2", providerKey: "qq2" };
        }
        if (neteaseTransporter) {
          return { transporter: neteaseTransporter, from: `"盘友助手" <${neteaseUser}>`, provider: "163", providerKey: "163" };
        }
        return null;
      }

      const notifiedLinks: { linkId: string; table: string }[] = [];

      for (const u of users) {
        if (!u.notification_email) continue;
        const expiredLinks = newlyExpiredByUser.get(u.id);
        if (!expiredLinks || expiredLinks.length === 0) continue;

        const linkRows = expiredLinks
          .map((l) => `<tr><td style="padding:6px 12px;border:1px solid #eee">${escapeHtml(l.title || "未命名")}</td><td style="padding:6px 12px;border:1px solid #eee"><a href="${escapeHtml(l.url)}">${escapeHtml(l.url)}</a></td></tr>`)
          .join("");

        const emailHtml = `
              <div style="font-family:sans-serif;max-width:600px;margin:0 auto">
                <h2 style="color:#333">链接失效通知</h2>
                <p style="color:#666">检测到以下 ${expiredLinks.length} 个链接已失效，请及时处理：</p>
                <table style="border-collapse:collapse;width:100%;margin:16px 0">
                  <thead><tr style="background:#f8f8f8"><th style="padding:8px 12px;border:1px solid #eee;text-align:left">资源名称</th><th style="padding:8px 12px;border:1px solid #eee;text-align:left">链接</th></tr></thead>
                  <tbody>${linkRows}</tbody>
                </table>
                <p style="color:#999;font-size:12px">此邮件由盘友助手自动发送，如不想收到通知请在链接检测页面关闭邮箱通知。</p>
              </div>`;

        const channel = pickChannel();
        if (!channel || !channel.transporter) {
          console.error(`  ❌ 无可用邮箱通道，跳过 ${u.notification_email}`);
          continue;
        }

        let sent = false;
        try {
          await channel.transporter.sendMail({
            from: channel.from,
            to: u.notification_email,
            subject: `链接失效通知 - ${expiredLinks.length} 个链接已失效`,
            html: emailHtml,
          });
          emailsSent++;
          if (channel.providerKey === "qq") qq1SentCount++;
          else if (channel.providerKey === "qq2") qq2SentCount++;
          await supabase.rpc("increment_email_count", { p_provider: channel.providerKey });
          sent = true;
          console.log(`  ✉️ [${channel.provider}] 已通知 ${u.notification_email}（${expiredLinks.length} 个失效链接）`);
        } catch (e) {
          console.error(`  ❌ [${channel.provider}] 发送失败 ${u.notification_email}:`, e);
          // 当前通道失败，标记满额后重试
          if (channel.providerKey === "qq") qq1SentCount = QQ_DAILY_LIMIT;
          else if (channel.providerKey === "qq2") qq2SentCount = QQ_DAILY_LIMIT;
          const fallback = pickChannel();
          if (fallback && fallback.transporter) {
            try {
              await fallback.transporter.sendMail({
                from: fallback.from,
                to: u.notification_email,
                subject: `链接失效通知 - ${expiredLinks.length} 个链接已失效`,
                html: emailHtml,
              });
              emailsSent++;
              if (fallback.providerKey === "qq") qq1SentCount++;
              else if (fallback.providerKey === "qq2") qq2SentCount++;
              await supabase.rpc("increment_email_count", { p_provider: fallback.providerKey });
              sent = true;
              console.log(`  ✉️ [${fallback.provider}备用] 已通知 ${u.notification_email}（${expiredLinks.length} 个失效链接）`);
            } catch (e2) {
              console.error(`  ❌ [${fallback.provider}备用] 也失败 ${u.notification_email}:`, e2);
            }
          }
        }

        if (sent) {
          notifiedLinks.push(...expiredLinks);
        }
      }

      // 「部分文件被过滤」通知（false → true 翻转才会进入此队列）
      for (const u of users) {
        if (!u.notification_email) continue;
        const partialLinks = newlyPartialByUser.get(u.id);
        if (!partialLinks || partialLinks.length === 0) continue;

        const partialRows = partialLinks
          .map((l) => `<tr><td style="padding:6px 12px;border:1px solid #eee">${escapeHtml(l.title || "未命名")}</td><td style="padding:6px 12px;border:1px solid #eee"><a href="${escapeHtml(l.url)}">${escapeHtml(l.url)}</a></td></tr>`)
          .join("");

        const partialHtml = `
              <div style="font-family:sans-serif;max-width:600px;margin:0 auto">
                <h2 style="color:#333">部分文件被过滤通知</h2>
                <p style="color:#666">检测到以下 ${partialLinks.length} 个链接里部分文件已被官方过滤，访问者看到的文件会比作者上传的少，建议尽快更换：</p>
                <table style="border-collapse:collapse;width:100%;margin:16px 0">
                  <thead><tr style="background:#f8f8f8"><th style="padding:8px 12px;border:1px solid #eee;text-align:left">资源名称</th><th style="padding:8px 12px;border:1px solid #eee;text-align:left">链接</th></tr></thead>
                  <tbody>${partialRows}</tbody>
                </table>
                <p style="color:#999;font-size:12px">此邮件由盘友助手自动发送，如不想收到通知请在链接检测页面关闭邮箱通知。</p>
              </div>`;

        const channel = pickChannel();
        if (!channel || !channel.transporter) {
          console.error(`  ❌ 无可用邮箱通道，跳过 ${u.notification_email}（部分过滤通知）`);
          continue;
        }

        try {
          await channel.transporter.sendMail({
            from: channel.from,
            to: u.notification_email,
            subject: `部分文件被过滤通知 - ${partialLinks.length} 个链接`,
            html: partialHtml,
          });
          emailsSent++;
          if (channel.providerKey === "qq") qq1SentCount++;
          else if (channel.providerKey === "qq2") qq2SentCount++;
          await supabase.rpc("increment_email_count", { p_provider: channel.providerKey });
          console.log(`  ✉️ [${channel.provider}] 已通知 ${u.notification_email}（${partialLinks.length} 个部分过滤链接）`);
        } catch (e) {
          console.error(`  ❌ [${channel.provider}] 部分过滤通知发送失败 ${u.notification_email}:`, e);
        }
      }

      // 统一标记已通知，减少网络请求次数
      if (notifiedLinks.length > 0) {
        await markLinksNotified(notifiedLinks);
      }
    }
  } else if (newlyExpiredByUser.size > 0 || newlyPartialByUser.size > 0) {
    console.log(`\n⚠️ 失效=${newlyExpiredByUser.size} 用户 / 部分过滤=${newlyPartialByUser.size} 用户，但未配置邮箱通道，跳过邮件通知`);
  }

  // ─── QQ 额度告警 ────────────────────────────────────────
  // 每个 QQ 邮箱用完 99 封时，各发一封告警（用第 100 封）
  {
    const today = new Date().toISOString().slice(0, 10);
    const { data: latestCounts } = await supabase
      .from("email_daily_counts")
      .select("provider, count")
      .eq("date", today);
    const latestQQ1 = latestCounts?.find((c: { provider: string }) => c.provider === "qq")?.count || 0;
    const latestQQ2 = latestCounts?.find((c: { provider: string }) => c.provider === "qq2")?.count || 0;
    const latest163 = latestCounts?.find((c: { provider: string }) => c.provider === "163")?.count || 0;

    const alertChecks = [
      { key: "qq1_alert", label: "QQ邮箱1", email: "panyouzhushou@foxmail.com", count: latestQQ1, envKey: "SMTP_PASSWORD" as const },
      { key: "qq2_alert", label: "QQ邮箱2", email: "panyouzhushou2@foxmail.com", count: latestQQ2, envKey: "SMTP_QQ2_PASSWORD" as const },
    ];

    for (const ac of alertChecks) {
      if (ac.count < 99) continue;
      const { data: sentRow } = await supabase
        .from("email_daily_counts")
        .select("count")
        .eq("date", today)
        .eq("provider", ac.key)
        .maybeSingle();
      if ((sentRow?.count || 0) > 0) continue;

      console.log(`\n⚠️ ${ac.label} 额度已用完，发送告警邮件...`);
      const pass = process.env[ac.envKey];
      if (!pass) continue;
      const alertTransporter = createTransport({
        host: "smtp.qq.com", port: 465, secure: true,
        auth: { user: ac.email, pass },
      });

      try {
        await alertTransporter.sendMail({
          from: `"盘友助手告警" <${ac.email}>`,
          to: "775754012@qq.com",
          subject: `⚠️ 邮件额度告警 - ${ac.label}额度已用完 (${today})`,
          html: `
            <div style="font-family:sans-serif;max-width:480px;margin:0 auto;padding:24px">
              <h2 style="color:#dc2626">邮件额度告警</h2>
              <p><b>${ac.label} (${ac.email})</b> 今日额度已用完（${ac.count}/99）</p>
              <p>当前各通道状态：</p>
              <ul>
                <li>QQ邮箱1: <b>${latestQQ1}/99</b></li>
                <li>QQ邮箱2: <b>${latestQQ2}/99</b></li>
                <li>163邮箱: <b>${latest163}/50</b>（剩余 ${Math.max(0, 50 - latest163)} 封）</li>
              </ul>
              <p>请关注是否需要增加邮箱通道。</p>
              <p style="color:#999;font-size:12px;margin-top:20px">此邮件由盘友助手系统自动发送</p>
            </div>`,
        });
        await supabase.rpc("increment_email_count", { p_provider: ac.key });
        console.log("  ✉️ 告警邮件已发送到 775754012@qq.com");
      } catch (e) {
        console.error("  ❌ 告警邮件发送失败:", e);
      }
    }
  }

  // ─── D1 额度告警（读 >=4M 或 写 >=90K 时通知） ────────────────────
  const cfToken = process.env.CF_API_TOKEN;
  const cfAccountId = process.env.CF_ACCOUNT_ID;
  if (cfToken && cfAccountId) {
    try {
      const today = new Date().toISOString().split("T")[0];
      const d1DatabaseId = "7ec59300-c62f-4a12-a62d-e9d6f3dd5c28";
      const res = await fetch("https://api.cloudflare.com/client/v4/graphql", {
        method: "POST",
        headers: { Authorization: `Bearer ${cfToken}`, "Content-Type": "application/json" },
        body: JSON.stringify({
          query: `query ($accountTag: String!, $date: Date!, $dbId: String!) {
            viewer { accounts(filter: { accountTag: $accountTag }) {
              d1AnalyticsAdaptiveGroups(filter: { date_geq: $date, date_leq: $date, databaseId: $dbId }, limit: 10) {
                sum { readQueries writeQueries }
              }
            }}
          }`,
          variables: { accountTag: cfAccountId, date: today, dbId: d1DatabaseId },
        }),
      });
      if (res.ok) {
        const data = await res.json();
        const groups = data?.data?.viewer?.accounts?.[0]?.d1AnalyticsAdaptiveGroups || [];
        let reads = 0, writes = 0;
        for (const g of groups) { reads += g.sum?.readQueries || 0; writes += g.sum?.writeQueries || 0; }
        console.log(`\nD1 今日额度: 读 ${reads.toLocaleString()}/5M, 写 ${writes.toLocaleString()}/100K`);

        const readAlert = reads >= 4000000;
        const writeAlert = writes >= 90000;
        if ((readAlert || writeAlert) && process.env.SMTP_PASSWORD) {
          const alertType = readAlert && writeAlert ? "读+写" : readAlert ? "读" : "写";
          const alertTransporter = createTransport({
            host: "smtp.qq.com", port: 465, secure: true,
            auth: { user: "panyouzhushou@foxmail.com", pass: process.env.SMTP_PASSWORD },
          });
          await alertTransporter.sendMail({
            from: '"盘友助手告警" <panyouzhushou@foxmail.com>',
            to: "775754012@qq.com",
            subject: `⚠️ D1 ${alertType}额度告警 (${today})`,
            html: `<div style="font-family:sans-serif;max-width:480px;margin:0 auto;padding:24px">
              <h2 style="color:#dc2626">D1 额度告警</h2>
              <p>今日 D1 读操作：<b>${reads.toLocaleString()}</b> / 5,000,000${readAlert ? " ⚠️" : ""}</p>
              <p>今日 D1 写操作：<b>${writes.toLocaleString()}</b> / 100,000${writeAlert ? " ⚠️" : ""}</p>
              <p>请检查是否有异常查询，必要时暂停链接检测 Cron。</p>
              <p style="color:#999;font-size:12px;margin-top:20px">此邮件由盘友助手系统自动发送</p>
            </div>`,
          });
          console.log(`  ⚠️ D1 ${alertType}额度告警邮件已发送`);
        }
      }
    } catch (e) {
      console.error("D1 额度检查失败:", e);
    }
  }

  console.log("\n========================================");
  console.log("检测完成");
  console.log(`本次检测: ${toCheck.length} 条`);
  console.log(`有效: ${stats.valid} | 失效: ${stats.expired} | 未知: ${stats.unknown} | 错误: ${stats.errors}`);
  console.log(`状态变化: ${stats.changed} | 跳过更新: ${stats.skipped}`);
  console.log(`标题更新: ${titleUpdated} 条默认标题被替换为真实标题`);
  console.log(`邮件通知: ${emailsSent} 封`);
  console.log(`API 调用节省: ${stats.skipped} 次单独更新 → ${Math.ceil(stats.changed / BATCH_UPDATE_SIZE)} 次批量更新`);
  console.log("========================================");
}

main().catch(console.error);
