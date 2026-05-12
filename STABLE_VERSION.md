# GeoKit Lab Stable Version

## Stable Tag

- Tag: v1-stable-quota-system
- Commit: 7cf258a29993dd80131fafec7cd9cc64ea7a71ee
- Date: 2026-05-12

## 当前稳定功能

- 坐标/KML识别与导出。
- 矿地快判上传、判定与结果展示。
- 普通用户、VIP月度版用户、付费加次用户的次数体系。
- 每日免费次数自动恢复。
- 额度拦截与真实失败分开统计。
- 前台低打扰次数展示与“我的次数”弹窗。
- 后台总览、失败记录、额度拦截记录与基础运营查看。

## 已完成模块

- 前台页面：index.html。
- 后台页面：admin.html。
- 后端服务：server.js。
- 价格与套餐配置：pricing-config.js。
- Supabase 数据存储。
- Render 部署。

## 当前正式域名

- https://geokitlab.com

## 当前数据库表

- users
- usage_logs
- judge_cases
- system_config

## 当前次数规则

- 普通用户：
  - 矿地快判每天免费 3 次。
  - 坐标/KML每天免费 3 次。
- 免费次数每天自动恢复为 3 次，不累计。
- VIP月度版不是无限使用：
  - 矿地快判 50 次/月。
  - 坐标/KML 50 次/月。
- 付费加次不会每日清空。
- 扣减顺序：
  - 先扣每日免费次数。
  - 免费次数用完后，再扣 VIP月度版/付费次数。
- VIP月度版用户仍然拥有每日免费次数。
- 每日自动恢复触发点：
  - 用户访问网站。
  - 查询次数。
  - 使用功能。
- 自动恢复依据：
  - 检查 last_free_reset_date 是否为今天。
  - 如果不是今天，恢复免费次数。

## 当前案例库状态

- judge_cases 表继续保留并作为矿地快判案例库数据来源。
- 本稳定版本未清空、迁移或重构案例库。
- 本稳定版本不改变 user_code、渠道统计、usage_logs 与 system_config 的既有职责。

## 当前后台状态

- 后台保留运营总览与用户/记录查看能力。
- 今日失败只统计真正 failed。
- 额度用完拦截记录为 quota_blocked，不计入失败。
- 后台可区分 success、quota_blocked、failed。
- 最近失败记录不展示 quota_exhausted / limit_exceeded 额度拦截。

