/**
 * 自动检测网盘链接状态
 * 支持：夸克、百度网盘
 * 通过 GitHub Actions 每日定时运行
 */

import { createClient } from "@supabase/supabase-js";

const SUPABASE_URL = process.env.SUPABASE_URL!;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY!;

if (!SUPABASE_URL || !SUPABASE_SERVICE_KEY) {
  console.error("Missing SUPABASE_URL or SUPABASE_SERVICE_KEY");
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY);

/**
 * 检查网盘链接是否有效
 */
async function checkLinkStatus(url: string): Promise<{ valid: boolean | null; reason?: string }> {
  try {
    // 夸克网盘
    if (url.includes("quark.cn")) {
      const match = url.match(/\/s\/([a-zA-Z0-9]+)/);
      if (match) {
        const pwd_id = match[1];
        const tokenUrl = `https://drive-h.quark.cn/1/clouddrive/share/sharepage/token?pr=ucpro&fr=pc&uc_param_str=&__dt=2000&__t=${Date.now()}`;
        const tokenRes = await fetch(tokenUrl, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ pwd_id, passcode: "" }),
        });
        const tokenData = await tokenRes.json();

        if (tokenData.code === 41003) return { valid: false, reason: "分享已过期" };
        if (tokenData.code === 41006) return { valid: false, reason: "分享已取消" };
        if (tokenData.code !== 0) return { valid: false, reason: tokenData.message || "链接无效" };

        const stoken = tokenData.data?.stoken;
        if (stoken) {
          const detailUrl = `https://drive-h.quark.cn/1/clouddrive/share/sharepage/detail?pr=ucpro&fr=pc&pwd_id=${pwd_id}&stoken=${encodeURIComponent(stoken)}&pdir_fid=0&force=0&_page=1&_size=50`;
          const detailRes = await fetch(detailUrl);
          const detailData = await detailRes.json();
          if (detailData.code === 0 && (detailData.metadata?._total || 0) === 0) {
            return { valid: false, reason: "文件已被删除" };
          }
        }
        return { valid: true };
      }
    }

    // 百度网盘 - 使用重定向检测（更省流量）
    if (url.includes("pan.baidu.com") || url.includes("yun.baidu.com")) {
      const res = await fetch(url, {
        headers: {
          "User-Agent": "Mozilla/5.0 (iPhone; CPU iPhone OS 13_2_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0.3 Mobile/15E148 Safari/604.1"
        },
        redirect: "manual",  // 不自动跟随重定向
      });
      const location = res.headers.get("location") || "";
      if (location.includes("error")) {
        return { valid: false, reason: "链接已失效" };
      }
      return { valid: true };
    }

    // 迅雷 - 暂不支持
    if (url.includes("pan.xunlei.com")) {
      return { valid: null, reason: "迅雷暂不支持检测" };
    }

    return { valid: null, reason: "不支持的网盘类型" };
  } catch (error) {
    console.error("Check error:", error);
    return { valid: null, reason: "检查出错" };
  }
}

// 配置
const MAX_LINKS_PER_RUN = 15000;  // 每次最多检测数量（约2.5小时）
const DELAY_MS = 500;  // 每个链接间隔

async function main() {
  console.log("========================================");
  console.log("开始检测链接状态");
  console.log("时间:", new Date().toISOString());
  console.log("========================================\n");

  // 先获取总数
  const { count: totalCount } = await supabase
    .from("short_links")
    .select("*", { count: "exact", head: true });

  console.log(`数据库总链接数: ${totalCount}`);
  console.log(`本次最多检测: ${MAX_LINKS_PER_RUN}`);
  console.log(`预计耗时: ${Math.ceil((Math.min(totalCount || 0, MAX_LINKS_PER_RUN) * DELAY_MS) / 1000 / 60)} 分钟\n`);

  // 按 last_checked 排序，最久没检测的优先
  const { data: links, error } = await supabase
    .from("short_links")
    .select("id, original_url, title, status, last_checked")
    .order("last_checked", { ascending: true, nullsFirst: true })
    .limit(MAX_LINKS_PER_RUN);

  if (error) {
    console.error("获取链接失败:", error);
    process.exit(1);
  }

  if (!links || links.length === 0) {
    console.log("没有需要检测的链接");
    return;
  }

  console.log(`共 ${links.length} 个链接需要检测\n`);

  let valid = 0, expired = 0, unknown = 0, errors = 0;

  for (let i = 0; i < links.length; i++) {
    const link = links[i];
    const progress = `[${i + 1}/${links.length}]`;

    try {
      const result = await checkLinkStatus(link.original_url);
      const status = result.valid === true ? "valid" : result.valid === false ? "expired" : "unknown";

      await supabase
        .from("short_links")
        .update({ status, last_checked: new Date().toISOString() })
        .eq("id", link.id);

      if (status === "valid") valid++;
      else if (status === "expired") expired++;
      else unknown++;

      const icon = status === "valid" ? "✓" : status === "expired" ? "✗" : "?";
      console.log(`${progress} ${icon} ${link.title || link.original_url.slice(0, 50)}... - ${result.reason || status}`);
    } catch {
      errors++;
      console.log(`${progress} ! ${link.title || "未命名"} - 检测出错`);
    }

    await new Promise((r) => setTimeout(r, DELAY_MS));
  }

  console.log("\n========================================");
  console.log("检测完成");
  console.log(`本次检测: ${links.length} 条`);
  console.log(`有效: ${valid} | 失效: ${expired} | 未知: ${unknown} | 错误: ${errors}`);
  if (totalCount && totalCount > links.length) {
    console.log(`剩余未检测: ${totalCount - links.length} 条（下次运行继续）`);
  }
  console.log("========================================");
}

main().catch(console.error);
