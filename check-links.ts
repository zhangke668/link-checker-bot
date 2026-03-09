import { createClient } from "@supabase/supabase-js";

const SUPABASE_URL = process.env.SUPABASE_URL!;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY!;

if (!SUPABASE_URL || !SUPABASE_SERVICE_KEY) {
  console.error("Missing SUPABASE_URL or SUPABASE_SERVICE_KEY");
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

const MAX_LINKS_PER_RUN = 15000;
const CONCURRENCY = 20;
const LINK_CHECK_TIMEOUT = 10000;
const BATCH_UPDATE_SIZE = 500; // 每批更新 500 条

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

// 待批量更新的缓冲区，按表名分组
const pendingUpdates: Map<string, { ids: string[]; statuses: string[]; rpcName: string }> = new Map();

async function flushUpdates(tableName?: string) {
  const tables = tableName ? [tableName] : [...pendingUpdates.keys()];
  for (const name of tables) {
    const pending = pendingUpdates.get(name);
    if (!pending || pending.ids.length === 0) continue;

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

  const { error } = await supabase.rpc("batch_update_resource_titles", {
    p_ids: pendingTitles.ids,
    p_titles: pendingTitles.titles,
  });

  if (error) {
    console.error(`批量更新标题失败 (${pendingTitles.ids.length} 条):`, error);
  } else {
    console.log(`  📝 批量更新标题: ${pendingTitles.ids.length} 条`);
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

interface LinkToCheck {
  id: string;
  url: string;
  table: string;
  statusField: string;
  checkedField: string;
  lastChecked: string | null;
  currentStatus: string | null;
  currentTitle: string | null;
}

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

  const allLinks: LinkToCheck[] = [];

  for (const table of TABLES) {
    const { count: totalCount } = await supabase.from(table.name).select("*", { count: "exact", head: true });
    const { count: expiredCount } = await supabase.from(table.name).select("*", { count: "exact", head: true }).eq(table.statusField, "expired");
    console.log(`${table.name} 表总数: ${totalCount || 0}，已失效: ${expiredCount || 0}，待检测: ${(totalCount || 0) - (expiredCount || 0)}`);

    // 分页获取数据（Supabase 单次最多 1000 条），跳过已失效链接
    const PAGE_SIZE = 1000;
    let page = 0;
    let hasMore = true;

    while (hasMore && allLinks.length < MAX_LINKS_PER_RUN) {
      const { data, error } = await supabase
        .from(table.name)
        .select(`id, title, ${table.urlField}, ${table.statusField}, ${table.checkedField}`)
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
        });
      }

      hasMore = data.length === PAGE_SIZE;
      page++;
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

  let valid = 0, expired = 0, unknown = 0, errors = 0, changed = 0, skipped = 0, titleUpdated = 0;

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

  console.log("\n========================================");
  console.log("检测完成");
  console.log(`本次检测: ${toCheck.length} 条`);
  console.log(`有效: ${valid} | 失效: ${expired} | 未知: ${unknown} | 错误: ${errors}`);
  console.log(`状态变化: ${changed} | 跳过更新: ${skipped}`);
  console.log(`标题更新: ${titleUpdated} 条默认标题被替换为真实标题`);
  console.log(`API 调用节省: ${skipped} 次单独更新 → ${Math.ceil(changed / BATCH_UPDATE_SIZE)} 次批量更新`);
  console.log("========================================");
}

main().catch(console.error);
