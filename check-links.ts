import { createClient } from "@supabase/supabase-js";
import { createHash, createCipheriv, createDecipheriv, randomBytes } from "crypto";
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

// D1 helper functions (resources 表已迁移到 D1)
async function d1Query<T = Record<string, unknown>>(sql: string, params: unknown[] = []): Promise<{ rows: T[] }> {
  const res = await fetch(`${D1_WORKER_URL}/api/d1`, {
    method: "POST",
    headers: { "Content-Type": "application/json", Authorization: `Bearer ${D1_API_KEY}` },
    body: JSON.stringify({ sql, params }),
  });
  if (!res.ok) throw new Error(`D1 query failed: ${res.status} ${await res.text()}`);
  return res.json();
}

async function d1Batch(items: Array<{ sql: string; params?: unknown[] }>): Promise<void> {
  const res = await fetch(`${D1_WORKER_URL}/api/d1`, {
    method: "POST",
    headers: { "Content-Type": "application/json", Authorization: `Bearer ${D1_API_KEY}` },
    body: JSON.stringify({ batch: items }),
  });
  if (!res.ok) throw new Error(`D1 batch failed: ${res.status} ${await res.text()}`);
}

const MAX_LINKS_PER_RUN = 100000;
const CONCURRENCY = 20;
const LINK_CHECK_TIMEOUT = 10000;
const BATCH_UPDATE_SIZE = 500; // 每批更新 500 条

function escapeHtml(str: string): string {
  return str.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
}

const TABLES = [
  { name: "short_links", urlField: "original_url", statusField: "status", checkedField: "last_checked", rpcName: "batch_update_short_link_status" },
  { name: "resources", urlField: "url", statusField: "status", checkedField: "last_checked_at", rpcName: "batch_update_resource_status" },
];

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

async function checkLinkStatus(url: string): Promise<{ valid: boolean | null; reason?: string; title?: string }> {
  try {
    if (url.includes("quark.cn")) return await checkQuarkLink(url);
    if (url.includes("pan.baidu.com") || url.includes("yun.baidu.com")) return await checkBaiduLink(url);
    if (url.includes("pan.xunlei.com")) return { valid: null, reason: "迅雷暂不支持检测" };
    return { valid: null, reason: "不支持的网盘类型" };
  } catch (error) {
    if (error instanceof Error && error.name === "AbortError") return { valid: null, reason: "检测超时" };
    console.error("Check error:", error);
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

// ─── 迅雷链接检测 ────────────────────────────────────────

const WORKER_URL = "https://s.panlay.com";
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

// 凭证加解密（AES-256-GCM）
const ENC_ALGO = "aes-256-gcm";
const ENC_IV_LEN = 12;
const ENC_TAG_LEN = 16;

function getEncKey(): Buffer | null {
  const key = process.env.PAN_ENCRYPTION_KEY;
  if (!key || key.length !== 64) return null;
  return Buffer.from(key, "hex");
}

function decryptCreds(encoded: string): string {
  const key = getEncKey();
  if (!key) throw new Error("PAN_ENCRYPTION_KEY not set");
  const buf = Buffer.from(encoded, "base64");
  const iv = buf.subarray(0, ENC_IV_LEN);
  const tag = buf.subarray(ENC_IV_LEN, ENC_IV_LEN + ENC_TAG_LEN);
  const ciphertext = buf.subarray(ENC_IV_LEN + ENC_TAG_LEN);
  const decipher = createDecipheriv(ENC_ALGO, key, iv);
  decipher.setAuthTag(tag);
  return decipher.update(ciphertext).toString("utf8") + decipher.final("utf8");
}

function encryptCreds(plaintext: string): string {
  const key = getEncKey();
  if (!key) throw new Error("PAN_ENCRYPTION_KEY not set");
  const iv = randomBytes(ENC_IV_LEN);
  const cipher = createCipheriv(ENC_ALGO, key, iv);
  const encrypted = Buffer.concat([cipher.update(plaintext, "utf8"), cipher.final()]);
  const tag = cipher.getAuthTag();
  return Buffer.concat([iv, tag, encrypted]).toString("base64");
}

// 迅雷认证状态
interface XunleiAuthState {
  accessToken: string;
  captchaToken: string;
  deviceId: string;
  refreshToken: string;
  dbId: string;
  userId: string;
  tokenExpiry: number;
  captchaExpiry: number;
}

let xunleiAuth: XunleiAuthState | null = null;

function md5(str: string): string {
  return createHash("md5").update(str).digest("hex");
}

function computeCaptchaSign(fullDeviceId: string, timestamp: string): string {
  const shortId = fullDeviceId.replace(/^[a-z]+\d*\./, "").slice(0, 32);
  let str = XUNLEI_CLIENT_ID + XUNLEI_CLIENT_VERSION + XUNLEI_PACKAGE_NAME + shortId + timestamp;
  for (const salt of XUNLEI_CAPTCHA_SALTS) {
    str = md5(str + salt);
  }
  return "1." + str;
}

async function refreshXunleiAccessToken(refreshToken: string) {
  const res = await fetch(`${WORKER_URL}/xunlei/auth/token`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ client_id: XUNLEI_CLIENT_ID, grant_type: "refresh_token", refresh_token: refreshToken }),
  });
  const data = await res.json() as Record<string, unknown>;
  if (!data.access_token) throw new Error(`迅雷 Token 刷新失败: ${data.error_description || "unknown"}`);
  return {
    accessToken: data.access_token as string,
    refreshToken: (data.refresh_token as string) || refreshToken,
    expiresIn: (data.expires_in as number) || 43200,
    userId: (data.user_id as string) || (data.sub as string) || "",
  };
}

async function refreshXunleiCaptchaToken(deviceId: string, userId: string): Promise<string> {
  const timestamp = String(Date.now());
  const captchaSign = computeCaptchaSign(deviceId, timestamp);
  const res = await fetch(`${WORKER_URL}/xunlei/captcha`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      client_id: XUNLEI_CLIENT_ID,
      action: "get:/drive/v1/files",
      device_id: deviceId,
      meta: { package_name: XUNLEI_PACKAGE_NAME, client_version: XUNLEI_CLIENT_VERSION, captcha_sign: captchaSign, timestamp, user_id: userId || "0" },
    }),
  });
  const data = await res.json() as Record<string, unknown>;
  if (!data.captcha_token) { console.warn("[xunlei] captcha 获取失败:", data); return ""; }
  return data.captcha_token as string;
}

async function ensureXunleiAuth(): Promise<void> {
  if (!xunleiAuth) return;
  const now = Date.now();

  // 刷新 access_token（提前 5 分钟）
  if (!xunleiAuth.accessToken || now > xunleiAuth.tokenExpiry - 300000) {
    const result = await refreshXunleiAccessToken(xunleiAuth.refreshToken);
    xunleiAuth.accessToken = result.accessToken;
    xunleiAuth.tokenExpiry = now + result.expiresIn * 1000;
    xunleiAuth.userId = result.userId;

    // 持久化轮换后的 refresh_token
    if (result.refreshToken !== xunleiAuth.refreshToken) {
      xunleiAuth.refreshToken = result.refreshToken;
      const newCreds = encryptCreds(JSON.stringify({ refreshToken: result.refreshToken, deviceId: xunleiAuth.deviceId }));
      await supabase.from("pan_accounts").update({ credentials: newCreds }).eq("id", xunleiAuth.dbId);
      console.log("  🔄 迅雷 refresh_token 已轮换并持久化");
    }
  }

  // 刷新 captcha_token（提前 30 秒）
  if (!xunleiAuth.captchaToken || now > xunleiAuth.captchaExpiry - 30000) {
    xunleiAuth.captchaToken = await refreshXunleiCaptchaToken(xunleiAuth.deviceId, xunleiAuth.userId);
    xunleiAuth.captchaExpiry = now + 270000;
  }
}

async function initXunleiAccount(): Promise<boolean> {
  if (!getEncKey()) {
    console.log("⚠️ PAN_ENCRYPTION_KEY 未配置，跳过迅雷链接检测");
    return false;
  }

  const accountId = process.env.XUNLEI_CHECKER_ACCOUNT_ID || "825d60c9-6618-4448-b2fd-7d558d0dfd6c";
  const { data, error } = await supabase.from("pan_accounts").select("id, account_id, credentials").eq("id", accountId).single();
  if (error || !data) {
    console.log("⚠️ 迅雷检测账号不存在，跳过迅雷链接检测");
    return false;
  }

  try {
    const creds = JSON.parse(decryptCreds(data.credentials)) as { refreshToken: string; deviceId: string; accessToken?: string; tokenExpiry?: number };
    xunleiAuth = {
      accessToken: creds.accessToken || "",
      captchaToken: "",
      deviceId: creds.deviceId,
      refreshToken: creds.refreshToken,
      dbId: data.id,
      userId: "",
      tokenExpiry: creds.tokenExpiry || 0,
      captchaExpiry: 0,
    };

    await ensureXunleiAuth();
    console.log("✅ 迅雷检测账号已就绪");
    return true;
  } catch (e) {
    console.error("⚠️ 迅雷账号初始化失败:", e);
    xunleiAuth = null;
    return false;
  }
}

async function checkXunleiLink(url: string): Promise<{ valid: boolean | null; reason?: string; title?: string }> {
  if (!xunleiAuth) return { valid: null, reason: "迅雷账号未配置" };

  try {
    await ensureXunleiAuth();

    const match = url.match(/pan\.xunlei\.com\/s\/([a-zA-Z0-9_-]+)/);
    if (!match) return { valid: false, reason: "链接格式无效" };
    const shareId = match[1];

    let passCode = "";
    try { passCode = new URL(url).searchParams.get("pwd") || ""; } catch {}

    const params = new URLSearchParams({ share_id: shareId, pass_code: passCode, limit: "100" });
    const res = await fetchWithTimeout(`${WORKER_URL}/xunlei/share/info?${params}`, {
      headers: {
        "X-Xunlei-Token": xunleiAuth.accessToken,
        "X-Xunlei-CaptchaToken": xunleiAuth.captchaToken,
        "X-Xunlei-DeviceId": xunleiAuth.deviceId,
      },
    });

    // Token 失效 → 放弃后续迅雷检测
    if (res.status === 401) {
      console.warn("⚠️ 迅雷 Token 失效，后续迅雷链接将跳过");
      xunleiAuth = null;
      return { valid: null, reason: "迅雷 Token 失效" };
    }

    const data = await res.json() as Record<string, unknown>;

    if (data.share_status === "OK") {
      return { valid: true, title: (data.title as string) || undefined };
    }
    if (data.share_status === "PASS_CODE_EMPTY") {
      return { valid: true, title: (data.title as string) || undefined };
    }

    return { valid: false, reason: (data.share_status as string) || (data.error_description as string) || "分享无效" };
  } catch (error) {
    if (error instanceof Error && error.name === "AbortError") return { valid: null, reason: "检测超时" };
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
}

// 新失效链接按用户分组
const newlyExpiredByUser = new Map<string, { url: string; title: string | null }[]>();

async function checkBatch(links: LinkToCheck[], startIndex: number, totalCount: number) {
  const results = await Promise.all(
    links.map(async (link, i) => {
      const progress = `[${startIndex + i + 1}/${totalCount}]`;
      try {
        const result = await checkLinkStatus(link.url);
        const newStatus = result.valid === true ? "valid" : result.valid === false ? "expired" : "unchecked";

        // 只在状态变化时才更新
        if (newStatus !== link.currentStatus) {
          await queueUpdate(link, newStatus);
          const icon = newStatus === "valid" ? "✓" : newStatus === "expired" ? "✗" : "?";
          console.log(`${progress} ${icon} [${link.table}] ${link.url.slice(0, 50)}... - ${link.currentStatus} → ${newStatus} (${result.reason || ""})`);

          // 记录新失效链接（用于邮件通知）
          if (newStatus === "expired" && link.userId) {
            const list = newlyExpiredByUser.get(link.userId) || [];
            list.push({ url: link.url, title: link.currentTitle });
            newlyExpiredByUser.set(link.userId, list);
          }

          // 同步状态到 monitored_links（如果该用户有监控同一 URL）
          if ((newStatus === "valid" || newStatus === "expired") && link.userId && link.table === "short_links") {
            await supabase
              .from("monitored_links")
              .update({ status: newStatus, last_checked: new Date().toISOString() })
              .eq("user_id", link.userId)
              .eq("url", link.url);
          }
        } else {
          console.log(`${progress} = [${link.table}] ${link.url.slice(0, 50)}... - ${newStatus} (unchanged)`);
        }

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

  // 初始化迅雷检测账号
  const xunleiReady = await initXunleiAccount();
  console.log(`迅雷检测: ${xunleiReady ? "已启用" : "已跳过"}\n`);

  const allLinks: LinkToCheck[] = [];

  for (const table of TABLES) {
    if (table.name === "resources") {
      // resources 表从 D1 读取
      const { rows: [{ c: totalCount }] } = await d1Query<{ c: number }>("SELECT COUNT(*) as c FROM resources");
      const { rows: [{ c: expiredCount }] } = await d1Query<{ c: number }>("SELECT COUNT(*) as c FROM resources WHERE status = 'expired'");
      console.log(`${table.name} 表总数: ${totalCount || 0}，已失效: ${expiredCount || 0}，待检测: ${(totalCount || 0) - (expiredCount || 0)}`);

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
              sql = "SELECT id, user_id, title, url, status, last_checked_at FROM resources WHERE status = ? AND last_checked_at IS NULL AND id > ? ORDER BY id ASC LIMIT ?";
              params = [st, nullCursorId, PAGE_SIZE];
            } else {
              sql = "SELECT id, user_id, title, url, status, last_checked_at FROM resources WHERE status = ? AND last_checked_at IS NULL ORDER BY id ASC LIMIT ?";
              params = [st, PAGE_SIZE];
            }
          } else if (cursorTime === null) {
            // NULL 阶段结束，开始查有时间戳的，从最早的开始
            sql = "SELECT id, user_id, title, url, status, last_checked_at FROM resources WHERE status = ? AND last_checked_at IS NOT NULL ORDER BY last_checked_at ASC, id ASC LIMIT ?";
            params = [st, PAGE_SIZE];
          } else {
            // 游标分页：用 (last_checked_at, id) 双游标
            sql = "SELECT id, user_id, title, url, status, last_checked_at FROM resources WHERE status = ? AND (last_checked_at > ? OR (last_checked_at = ? AND id > ?)) ORDER BY last_checked_at ASC, id ASC LIMIT ?";
            params = [st, cursorTime, cursorTime, cursorId, PAGE_SIZE];
          }

          const { rows } = await d1Query<{ id: string; user_id: string | null; title: string | null; url: string; status: string; last_checked_at: string | null }>(sql, params);

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
              userId: row.user_id || null,
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
          .select(`id, user_id, title, ${table.urlField}, ${table.statusField}, ${table.checkedField}`)
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

  const toCheck = allLinks.slice(0, MAX_LINKS_PER_RUN);
  console.log(`\n本次检测: ${toCheck.length} 条`);
  console.log(`预计耗时: ${Math.ceil((toCheck.length / CONCURRENCY) * 2 / 60)} 分钟\n`);

  if (toCheck.length === 0) { console.log("没有需要检测的链接"); return; }

  titleUpdated = 0;
  let valid = 0, expired = 0, unknown = 0, errors = 0, changed = 0, skipped = 0;

  for (let i = 0; i < toCheck.length; i += CONCURRENCY) {
    const batch = toCheck.slice(i, i + CONCURRENCY);
    const results = await checkBatch(batch, i, toCheck.length);
    for (const r of results) {
      if (r.error) errors++;
      else if (r.status === "valid") valid++;
      else if (r.status === "expired") expired++;
      else unknown++;
      if (r.changed) changed++;
      else if (!r.error) skipped++;
    }
    if (i + CONCURRENCY < toCheck.length) await new Promise((r) => setTimeout(r, 500));
  }

  // 刷新剩余的待更新数据
  await flushUpdates();
  await flushTitles();

  // ─── 邮件通知 ────────────────────────────────────────
  let emailsSent = 0;
  if (newlyExpiredByUser.size > 0 && (process.env.SMTP_PASSWORD || process.env.SMTP_163_PASSWORD)) {
    console.log(`\n📧 发送失效通知邮件...`);
    const userIds = [...newlyExpiredByUser.keys()];
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
              console.log(`  ✉️ [${fallback.provider}备用] 已通知 ${u.notification_email}（${expiredLinks.length} 个失效链接）`);
            } catch (e2) {
              console.error(`  ❌ [${fallback.provider}备用] 也失败 ${u.notification_email}:`, e2);
            }
          }
        }
      }
    }
  } else if (newlyExpiredByUser.size > 0) {
    console.log(`\n⚠️ 有 ${newlyExpiredByUser.size} 个用户的链接失效，但未配置邮箱通道，跳过邮件通知`);
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
  console.log(`有效: ${valid} | 失效: ${expired} | 未知: ${unknown} | 错误: ${errors}`);
  console.log(`状态变化: ${changed} | 跳过更新: ${skipped}`);
  console.log(`标题更新: ${titleUpdated} 条默认标题被替换为真实标题`);
  console.log(`邮件通知: ${emailsSent} 封`);
  console.log(`API 调用节省: ${skipped} 次单独更新 → ${Math.ceil(changed / BATCH_UPDATE_SIZE)} 次批量更新`);
  console.log("========================================");
}

main().catch(console.error);
