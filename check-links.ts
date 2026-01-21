import { createClient } from "@supabase/supabase-js";

const SUPABASE_URL = process.env.SUPABASE_URL!;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY!;

if (!SUPABASE_URL || !SUPABASE_SERVICE_KEY) {
  console.error("Missing SUPABASE_URL or SUPABASE_SERVICE_KEY");
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

const MAX_LINKS_PER_RUN = 5000;
const CONCURRENCY = 20;
const LINK_CHECK_TIMEOUT = 10000;

const TABLES = [
  { name: "short_links", urlField: "original_url", statusField: "status", checkedField: "last_checked" },
  { name: "resources", urlField: "url", statusField: "status", checkedField: "last_checked" },
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

async function checkLinkStatus(url: string): Promise<{ valid: boolean | null; reason?: string }> {
  try {
    if (url.includes("quark.cn")) {
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
      const stoken = tokenData.data?.stoken;
      if (stoken) {
        const detailUrl = `https://drive-h.quark.cn/1/clouddrive/share/sharepage/detail?pr=ucpro&fr=pc&pwd_id=${pwd_id}&stoken=${encodeURIComponent(stoken)}&pdir_fid=0&force=0&_page=1&_size=50`;
        const detailRes = await fetchWithTimeout(detailUrl, {});
        const detailData = await detailRes.json();
        if (detailData.code === 0 && (detailData.metadata?._total || 0) === 0) {
          return { valid: false, reason: "文件已被删除" };
        }
      }
      return { valid: true };
    }
    if (url.includes("pan.baidu.com") || url.includes("yun.baidu.com")) {
      const res = await fetchWithTimeout(url, {
        headers: { "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 13_2_3 like Mac OS X) AppleWebKit/605.1.15" },
        redirect: "manual",
      });
      const location = res.headers.get("location") || "";
      if (res.status === 200 && !location) return { valid: false, reason: "分享已过期" };
      if (location.includes("error")) return { valid: false, reason: "链接已失效" };
      return { valid: true };
    }
    if (url.includes("pan.xunlei.com")) return { valid: null, reason: "迅雷暂不支持检测" };
    return { valid: null, reason: "不支持的网盘类型" };
  } catch (error) {
    if (error instanceof Error && error.name === "AbortError") return { valid: null, reason: "检测超时" };
    console.error("Check error:", error);
    return { valid: null, reason: "检查出错" };
  }
}

async function checkBatch(links: Array<{ id: string; url: string; table: string; statusField: string; checkedField: string }>, startIndex: number, totalCount: number) {
  const results = await Promise.all(
    links.map(async (link, i) => {
      const progress = `[${startIndex + i + 1}/${totalCount}]`;
      try {
        const result = await checkLinkStatus(link.url);
        const status = result.valid === true ? "valid" : result.valid === false ? "expired" : "unchecked";
        await supabase.from(link.table).update({ [link.statusField]: status, [link.checkedField]: new Date().toISOString() }).eq("id", link.id);
        const icon = status === "valid" ? "✓" : status === "expired" ? "✗" : "?";
        console.log(`${progress} ${icon} [${link.table}] ${link.url.slice(0, 50)}... - ${result.reason || status}`);
        return { status, error: false };
      } catch (e) {
        console.log(`${progress} ! [${link.table}] ${link.url.slice(0, 50)}... - 检测出错`);
        return { status: "error", error: true };
      }
    })
  );
  return results;
}

async function main() {
  console.log("========================================");
  console.log("开始检测链接状态");
  console.log("时间:", new Date().toISOString());
  console.log("并发数:", CONCURRENCY);
  console.log("========================================\n");

  const allLinks: Array<{ id: string; url: string; table: string; statusField: string; checkedField: string; lastChecked: string | null }> = [];

  for (const table of TABLES) {
    const { count } = await supabase.from(table.name).select("*", { count: "exact", head: true });
    console.log(`${table.name} 表总数: ${count || 0}`);
    const { data, error } = await supabase.from(table.name).select(`id, ${table.urlField}, ${table.checkedField}`).order(table.checkedField, { ascending: true, nullsFirst: true }).limit(MAX_LINKS_PER_RUN);
    if (error) { console.error(`获取 ${table.name} 失败:`, error); continue; }
    if (data) {
      for (const row of data) {
        allLinks.push({ id: row.id, url: row[table.urlField], table: table.name, statusField: table.statusField, checkedField: table.checkedField, lastChecked: row[table.checkedField] });
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

  let valid = 0, expired = 0, unknown = 0, errors = 0;

  for (let i = 0; i < toCheck.length; i += CONCURRENCY) {
    const batch = toCheck.slice(i, i + CONCURRENCY);
    const results = await checkBatch(batch, i, toCheck.length);
    for (const r of results) {
      if (r.error) errors++;
      else if (r.status === "valid") valid++;
      else if (r.status === "expired") expired++;
      else unknown++;
    }
    if (i + CONCURRENCY < toCheck.length) await new Promise((r) => setTimeout(r, 500));
  }

  console.log("\n========================================");
  console.log("检测完成");
  console.log(`本次检测: ${toCheck.length} 条`);
  console.log(`有效: ${valid} | 失效: ${expired} | 未知: ${unknown} | 错误: ${errors}`);
  console.log("========================================");
}

main().catch(console.error);
