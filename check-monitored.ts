/**
 * 监控链接检测脚本（轻量版）
 * 只检测 monitored_links 表中的链接，失效后发邮件通知
 * 每小时半点由 GitHub Actions 触发
 */
import { createClient } from "@supabase/supabase-js";
import { createHash, randomBytes } from "crypto";
import { createTransport } from "nodemailer";

const SUPABASE_URL = process.env.SUPABASE_URL!;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY!;

if (!SUPABASE_URL || !SUPABASE_SERVICE_KEY) {
  console.error("Missing SUPABASE_URL or SUPABASE_SERVICE_KEY");
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

const CONCURRENCY = 20;
const XUNLEI_CONCURRENCY = 5;
const LINK_CHECK_TIMEOUT = 10000;

function escapeHtml(str: string): string {
  return str.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
}

async function fetchWithTimeout(url: string, options: RequestInit, timeout: number = LINK_CHECK_TIMEOUT): Promise<Response> {
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), timeout);
  try {
    return await fetch(url, { ...options, signal: controller.signal });
  } finally {
    clearTimeout(timeoutId);
  }
}

async function checkQuarkLink(url: string): Promise<{ valid: boolean | null; reason?: string }> {
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
    return { valid: true };
  } catch (error) {
    if (error instanceof Error && error.name === "AbortError") return { valid: null, reason: "检测超时" };
    return { valid: null, reason: "检查出错" };
  }
}

async function checkBaiduLink(url: string): Promise<{ valid: boolean | null; reason?: string }> {
  try {
    const res = await fetchWithTimeout(url, {
      method: "GET",
      headers: {
        "Accept": "text/html,application/xhtml+xml",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
      },
      redirect: "manual",
    });

    const location = res.headers.get("location") || "";
    if (res.status === 200 && !location) return { valid: false, reason: "分享已过期" };
    if (location.includes("error")) return { valid: false, reason: "链接已失效" };
    return { valid: true };
  } catch (error) {
    if (error instanceof Error && error.name === "AbortError") return { valid: null, reason: "检测超时" };
    return { valid: null, reason: "检查出错" };
  }
}

// ─── 迅雷匿名检测 ──────────────────────────────────────────
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

const XUNLEI_USES_PER_TOKEN = 30;
let xunleiCaptchaCache = { token: "", deviceId: "", expiry: 0, uses: 0 };
let xunleiTokenRefreshInProgress: Promise<{ token: string; deviceId: string }> | null = null;
let xunleiConsecutiveFailures = 0;
const XUNLEI_MAX_CONSECUTIVE_FAILURES = 5;

async function generateXunleiCaptchaToken(): Promise<{ token: string; deviceId: string }> {
  const deviceId = randomBytes(32).toString("hex").substring(0, 32);
  const timestamp = String(Date.now());
  let str = XUNLEI_CLIENT_ID + XUNLEI_CLIENT_VERSION + XUNLEI_PACKAGE_NAME + deviceId + timestamp;
  for (const salt of XUNLEI_CAPTCHA_SALTS) str = createHash("md5").update(str + salt).digest("hex");

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
  if (xunleiTokenRefreshInProgress) return xunleiTokenRefreshInProgress;
  xunleiTokenRefreshInProgress = generateXunleiCaptchaToken();
  try { return await xunleiTokenRefreshInProgress; }
  finally { xunleiTokenRefreshInProgress = null; }
}

async function checkXunleiLink(url: string): Promise<{ valid: boolean | null; reason?: string }> {
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
        "x-captcha-token": token, "x-client-id": XUNLEI_CLIENT_ID, "x-device-id": deviceId,
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Origin": "https://pan.xunlei.com", "Referer": "https://pan.xunlei.com/",
      },
    });
    const data = await res.json() as Record<string, unknown>;

    if (data.error === "captcha_invalid") {
      xunleiCaptchaCache.expiry = 0;
      xunleiConsecutiveFailures++;
      return { valid: null, reason: "captcha_invalid" };
    }

    xunleiConsecutiveFailures = 0;

    if (data.share_status === "OK" || data.share_status === "PASS_CODE_EMPTY") return { valid: true };
    return { valid: false, reason: (data.share_status as string) || (data.error_description as string) || "分享无效" };
  } catch (error) {
    if (error instanceof Error && error.name === "AbortError") return { valid: null, reason: "检测超时" };
    xunleiConsecutiveFailures++;
    return { valid: null, reason: "检查出错" };
  }
}

async function checkLinkStatus(url: string): Promise<{ valid: boolean | null; reason?: string; partialViolation?: boolean }> {
  try {
    if (url.includes("quark.cn")) return await checkQuarkLink(url);
    if (url.includes("drive.uc.cn") || url.includes("fast.uc.cn")) return await checkUcLink(url);
    if (url.includes("pan.baidu.com") || url.includes("yun.baidu.com")) return await checkBaiduLink(url);
    if (url.includes("pan.xunlei.com")) return await checkXunleiLink(url);
    return { valid: null, reason: "不支持的网盘类型" };
  } catch {
    return { valid: null, reason: "检查出错" };
  }
}

// UC：与夸克同构；detail 用 share/sharepage/detail + _fetch_share=1，返回 share.partial_violation
async function checkUcLink(url: string): Promise<{ valid: boolean | null; reason?: string; partialViolation?: boolean }> {
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
    const tokenData = await tokenRes.json() as { code: number; message?: string; data?: { stoken?: string } };

    if (tokenData.code === 41003) return { valid: false, reason: "分享已过期" };
    if (tokenData.code === 41006) return { valid: false, reason: "分享已取消" };
    if (tokenData.code !== 0) return { valid: false, reason: tokenData.message || "链接无效" };

    const stoken = tokenData.data?.stoken;
    if (stoken) {
      const detailUrl = `https://pc-api.uc.cn/1/clouddrive/share/sharepage/detail?${COMMON}&pwd_id=${pwd_id}&stoken=${encodeURIComponent(stoken)}&pdir_fid=0&force=0&_page=1&_size=50&_fetch_banner=1&_fetch_share=1&_fetch_total=1&_sort=file_type:asc,updated_at:desc`;
      const detailRes = await fetchWithTimeout(detailUrl, { headers: UC_HEADERS });
      const detailData = await detailRes.json() as { code: number; data?: { list?: unknown[]; share?: { partial_violation?: boolean } } };
      if (detailData.code === 0) {
        const list = detailData.data?.list || [];
        if (list.length === 0) return { valid: false, reason: "文件已被删除" };
        return { valid: true, partialViolation: !!detailData.data?.share?.partial_violation };
      }
    }
    return { valid: true };
  } catch (error) {
    if (error instanceof Error && error.name === "AbortError") return { valid: null, reason: "检测超时" };
    return { valid: null, reason: "检查出错" };
  }
}

async function main() {
  console.log("========================================");
  console.log("监控链接检测（半点触发）");
  console.log("时间:", new Date().toISOString());
  console.log("========================================\n");

  // 获取所有监控链接
  const { data: links, error } = await supabase
    .from("monitored_links")
    .select("id, url, title, platform, status, user_id, partial_violation")
    .order("last_checked", { ascending: true, nullsFirst: true });

  if (error) {
    console.error("获取监控链接失败:", error);
    return;
  }

  if (!links || links.length === 0) {
    console.log("没有需要检测的监控链接");
    return;
  }

  const otherLinks = links.filter(l => !l.url.includes("xunlei"));
  const xunleiLinks = links.filter(l => l.url.includes("xunlei"));
  console.log(`待检测: ${links.length} 条 (百度/夸克: ${otherLinks.length}, 迅雷: ${xunleiLinks.length})\n`);

  const newlyExpiredByUser = new Map<string, { url: string; title: string | null }[]>();
  // 「部分文件被过滤」false → true 翻转
  const newlyPartialByUser = new Map<string, { url: string; title: string | null }[]>();
  let valid = 0, expired = 0, unknown = 0, changed = 0;

  async function processLink(link: typeof links[0]) {
    const result = await checkLinkStatus(link.url);
    const newStatus = result.valid === true ? "valid" : result.valid === false ? "expired" : "unchecked";
    const newPartial = !!result.partialViolation;
    const oldPartial = !!link.partial_violation;

    if (newStatus === "valid") valid++;
    else if (newStatus === "expired") expired++;
    else unknown++;

    const now = new Date().toISOString();
    const partialChanged = newPartial !== oldPartial;
    const updates: Record<string, unknown> = { status: newStatus, last_checked: now };
    if (partialChanged) updates.partial_violation = newPartial;
    await supabase
      .from("monitored_links")
      .update(updates)
      .eq("id", link.id);

    // 同步到 short_links：status 仅 valid/expired 时同步（unchecked 不可信）；partial 翻转无论何时都同步
    if (newStatus === "valid" || newStatus === "expired") {
      const shortUpdates: Record<string, unknown> = { status: newStatus, last_checked: now };
      if (partialChanged) shortUpdates.partial_violation = newPartial;
      await supabase
        .from("short_links")
        .update(shortUpdates)
        .eq("user_id", link.user_id)
        .eq("original_url", link.url);
    } else if (partialChanged) {
      await supabase
        .from("short_links")
        .update({ partial_violation: newPartial })
        .eq("user_id", link.user_id)
        .eq("original_url", link.url);
    }

    if (newStatus !== link.status) {
      changed++;
      const icon = newStatus === "valid" ? "✓" : newStatus === "expired" ? "✗" : "?";
      console.log(`${icon} ${link.url.slice(0, 60)}... ${link.status} → ${newStatus}`);

      if (newStatus === "expired" && link.status !== "expired") {
        const list = newlyExpiredByUser.get(link.user_id) || [];
        list.push({ url: link.url, title: link.title });
        newlyExpiredByUser.set(link.user_id, list);
      }
    }

    // 部分文件被过滤翻转：false → true 入邮件队列；true → false 静默
    if (newPartial && !oldPartial) {
      console.log(`⚠ ${link.url.slice(0, 60)}... 触发「部分文件被过滤」`);
      const list = newlyPartialByUser.get(link.user_id) || [];
      list.push({ url: link.url, title: link.title });
      newlyPartialByUser.set(link.user_id, list);
    }
  }

  // 第一轮：百度 + 夸克（并发 10）
  for (let i = 0; i < otherLinks.length; i += CONCURRENCY) {
    const batch = otherLinks.slice(i, i + CONCURRENCY);
    await Promise.all(batch.map(processLink));
    if (i + CONCURRENCY < otherLinks.length) await new Promise((r) => setTimeout(r, 500));
  }

  // 第二轮：迅雷（并发 5）
  if (xunleiLinks.length > 0) {
    console.log(`\n─── 迅雷链接 (${xunleiLinks.length} 条, 并发 ${XUNLEI_CONCURRENCY}) ───\n`);
  }
  for (let i = 0; i < xunleiLinks.length; i += XUNLEI_CONCURRENCY) {
    const batch = xunleiLinks.slice(i, i + XUNLEI_CONCURRENCY);
    await Promise.all(batch.map(processLink));
    if (i + XUNLEI_CONCURRENCY < xunleiLinks.length) await new Promise((r) => setTimeout(r, 500));
  }

  let emailsSent = 0;
  if ((newlyExpiredByUser.size > 0 || newlyPartialByUser.size > 0) && (process.env.SMTP_PASSWORD || process.env.SMTP_163_PASSWORD)) {
    console.log(`\n📧 发送通知（失效=${newlyExpiredByUser.size} 用户 / 部分过滤=${newlyPartialByUser.size} 用户）...`);
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

      for (const u of users) {
        if (!u.notification_email) continue;
        const expiredLinks = newlyExpiredByUser.get(u.id);
        if (!expiredLinks || expiredLinks.length === 0) {
          // 没有失效链接，跳到下面 partial 循环
          continue;
        }

        const linkRows = expiredLinks
          .map((l) => `<tr><td style="padding:6px 12px;border:1px solid #eee">${escapeHtml(l.title || "未命名")}</td><td style="padding:6px 12px;border:1px solid #eee"><a href="${escapeHtml(l.url)}">${escapeHtml(l.url)}</a></td></tr>`)
          .join("");

        const emailHtml = `
              <div style="font-family:sans-serif;max-width:600px;margin:0 auto">
                <h2 style="color:#333">链接失效通知</h2>
                <p style="color:#666">检测到以下 ${expiredLinks.length} 个监控链接已失效，请及时处理：</p>
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
          // 当前通道失败，标记满额后重试（pickChannel 会选下一个）
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

      // 「部分文件被过滤」邮件（false → true 翻转触发）
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
                <p style="color:#666">检测到以下 ${partialLinks.length} 个监控链接里部分文件已被官方过滤，访问者看到的文件会比作者上传的少，建议尽快更换：</p>
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
    }
  }

  // ─── QQ 额度告警 ────────────────────────────────────────
  // 每个 QQ 邮箱用完 99 封时，各发一封告警（用第 100 封）
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

  console.log("\n========================================");
  console.log("检测完成");
  console.log(`总计: ${links.length} | 有效: ${valid} | 失效: ${expired} | 未知: ${unknown}`);
  console.log(`状态变化: ${changed} | 邮件通知: ${emailsSent} 封`);
  console.log("========================================");
}

main().catch(console.error);
