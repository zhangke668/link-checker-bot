/**
 * 监控链接检测脚本（轻量版）
 * 只检测 monitored_links 表中的链接，失效后发邮件通知
 * 每小时半点由 GitHub Actions 触发
 */
import { createClient } from "@supabase/supabase-js";
import { createTransport } from "nodemailer";

const SUPABASE_URL = process.env.SUPABASE_URL!;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY!;

if (!SUPABASE_URL || !SUPABASE_SERVICE_KEY) {
  console.error("Missing SUPABASE_URL or SUPABASE_SERVICE_KEY");
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

const CONCURRENCY = 10;
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

async function checkLinkStatus(url: string): Promise<{ valid: boolean | null; reason?: string }> {
  try {
    if (url.includes("quark.cn")) return await checkQuarkLink(url);
    if (url.includes("pan.baidu.com") || url.includes("yun.baidu.com")) return await checkBaiduLink(url);
    if (url.includes("pan.xunlei.com")) return { valid: null, reason: "迅雷暂不支持检测" };
    return { valid: null, reason: "不支持的网盘类型" };
  } catch {
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
    .select("id, url, title, platform, status, user_id")
    .order("last_checked", { ascending: true, nullsFirst: true });

  if (error) {
    console.error("获取监控链接失败:", error);
    return;
  }

  if (!links || links.length === 0) {
    console.log("没有需要检测的监控链接");
    return;
  }

  console.log(`待检测: ${links.length} 条\n`);

  const newlyExpiredByUser = new Map<string, { url: string; title: string | null }[]>();
  let valid = 0, expired = 0, unknown = 0, changed = 0;

  for (let i = 0; i < links.length; i += CONCURRENCY) {
    const batch = links.slice(i, i + CONCURRENCY);
    await Promise.all(batch.map(async (link) => {
      const result = await checkLinkStatus(link.url);
      const newStatus = result.valid === true ? "valid" : result.valid === false ? "expired" : "unchecked";

      if (newStatus === "valid") valid++;
      else if (newStatus === "expired") expired++;
      else unknown++;

      const now = new Date().toISOString();
      await supabase
        .from("monitored_links")
        .update({ status: newStatus, last_checked: now })
        .eq("id", link.id);

      // 同步状态到 short_links（同一用户、同一 URL）
      if (newStatus === "valid" || newStatus === "expired") {
        await supabase
          .from("short_links")
          .update({ status: newStatus, last_checked: now })
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
    }));

    if (i + CONCURRENCY < links.length) await new Promise((r) => setTimeout(r, 500));
  }

  let emailsSent = 0;
  if (newlyExpiredByUser.size > 0 && (process.env.SMTP_PASSWORD || process.env.SMTP_163_PASSWORD)) {
    console.log(`\n📧 发送失效通知...`);
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
      let qqSentCount = todayCounts?.find((c: { provider: string }) => c.provider === "qq")?.count || 0;
      console.log(`  今日已发送: QQ=${qqSentCount}, 163=${todayCounts?.find((c: { provider: string }) => c.provider === "163")?.count || 0}`);

      const qqTransporter = process.env.SMTP_PASSWORD ? createTransport({
        host: "smtp.qq.com", port: 465, secure: true,
        auth: { user: "panyouzhushou@foxmail.com", pass: process.env.SMTP_PASSWORD },
      }) : null;

      const neteaseUser = process.env.SMTP_163_USER || "ipwenan@163.com";
      const neteaseTransporter = process.env.SMTP_163_PASSWORD ? createTransport({
        host: "smtp.163.com", port: 465, secure: true,
        auth: { user: neteaseUser, pass: process.env.SMTP_163_PASSWORD },
      }) : null;

      for (const u of users) {
        if (!u.notification_email) continue;
        const expiredLinks = newlyExpiredByUser.get(u.id);
        if (!expiredLinks || expiredLinks.length === 0) continue;

        const linkRows = expiredLinks
          .map((l) => `<tr><td style="padding:6px 12px;border:1px solid #eee">${escapeHtml(l.title || "未命名")}</td><td style="padding:6px 12px;border:1px solid #eee"><a href="${escapeHtml(l.url)}">${escapeHtml(l.url)}</a></td></tr>`)
          .join("");

        const useNetease = qqSentCount >= QQ_DAILY_LIMIT || !qqTransporter;
        const transporter = useNetease ? neteaseTransporter : qqTransporter;
        const from = useNetease ? `"盘友助手" <${neteaseUser}>` : '"盘友助手" <panyouzhushou@foxmail.com>';
        const provider = useNetease ? "163" : "QQ";

        if (!transporter) {
          console.error(`  ❌ 无可用邮箱通道，跳过 ${u.notification_email}`);
          continue;
        }

        try {
          await transporter.sendMail({
            from,
            to: u.notification_email,
            subject: `链接失效通知 - ${expiredLinks.length} 个链接已失效`,
            html: `
              <div style="font-family:sans-serif;max-width:600px;margin:0 auto">
                <h2 style="color:#333">链接失效通知</h2>
                <p style="color:#666">检测到以下 ${expiredLinks.length} 个监控链接已失效，请及时处理：</p>
                <table style="border-collapse:collapse;width:100%;margin:16px 0">
                  <thead><tr style="background:#f8f8f8"><th style="padding:8px 12px;border:1px solid #eee;text-align:left">资源名称</th><th style="padding:8px 12px;border:1px solid #eee;text-align:left">链接</th></tr></thead>
                  <tbody>${linkRows}</tbody>
                </table>
                <p style="color:#999;font-size:12px">此邮件由盘友助手自动发送，如不想收到通知请在链接检测页面关闭邮箱通知。</p>
              </div>
            `,
          });
          emailsSent++;
          if (!useNetease) qqSentCount++;
          await supabase.rpc("increment_email_count", { p_provider: useNetease ? "163" : "qq" });
          console.log(`  ✉️ [${provider}] 已通知 ${u.notification_email}（${expiredLinks.length} 个失效链接）`);
        } catch (e) {
          console.error(`  ❌ [${provider}] 发送失败 ${u.notification_email}:`, e);
          // QQ 发送失败，尝试 163 备用
          if (!useNetease && neteaseTransporter) {
            try {
              await neteaseTransporter.sendMail({
                from: `"盘友助手" <${neteaseUser}>`,
                to: u.notification_email,
                subject: `链接失效通知 - ${expiredLinks.length} 个链接已失效`,
                html: `
                  <div style="font-family:sans-serif;max-width:600px;margin:0 auto">
                    <h2 style="color:#333">链接失效通知</h2>
                    <p style="color:#666">检测到以下 ${expiredLinks.length} 个监控链接已失效，请及时处理：</p>
                    <table style="border-collapse:collapse;width:100%;margin:16px 0">
                      <thead><tr style="background:#f8f8f8"><th style="padding:8px 12px;border:1px solid #eee;text-align:left">资源名称</th><th style="padding:8px 12px;border:1px solid #eee;text-align:left">链接</th></tr></thead>
                      <tbody>${linkRows}</tbody>
                    </table>
                    <p style="color:#999;font-size:12px">此邮件由盘友助手自动发送，如不想收到通知请在链接检测页面关闭邮箱通知。</p>
                  </div>
                `,
              });
              emailsSent++;
              await supabase.rpc("increment_email_count", { p_provider: "163" });
              console.log(`  ✉️ [163备用] 已通知 ${u.notification_email}（${expiredLinks.length} 个失效链接）`);
            } catch (e2) {
              console.error(`  ❌ [163备用] 也失败 ${u.notification_email}:`, e2);
            }
          }
        }
      }
    }
  }

  console.log("\n========================================");
  console.log("检测完成");
  console.log(`总计: ${links.length} | 有效: ${valid} | 失效: ${expired} | 未知: ${unknown}`);
  console.log(`状态变化: ${changed} | 邮件通知: ${emailsSent} 封`);
  console.log("========================================");
}

main().catch(console.error);
