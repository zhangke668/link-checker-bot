# 网盘链接检测

自动检测网盘链接有效性，支持夸克、百度网盘。

## 功能

- 每日自动检测所有链接状态
- 支持手动触发检测
- 检测结果自动更新到数据库

## 配置

在 GitHub Secrets 中添加：

- `SUPABASE_URL` - Supabase 项目 URL
- `SUPABASE_SERVICE_KEY` - Supabase service_role 密钥
