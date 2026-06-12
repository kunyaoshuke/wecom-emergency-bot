# 企业微信紧急响应机器人

## 功能
- 监听紧急响应群中的消息
- 识别 🚨 紧急通知，自动@对应开发人员
- 30分钟未响应 → 自动二次@提醒 + 记录超时
- 24小时未响应 → 自动上报到老板
- 所有记录存入数据库，每月自动生成报表

## 技术栈
Python + Flask + SQLite + 企业微信API
部署到 Railway（免费）

## 环境变量（在 Railway 中配置）
- WECOM_CORP_ID：企业ID
- WECOM_CORP_SECRET：应用Secret
- WECOM_AGENT_ID：应用AgentId
- WECOM_TOKEN：回调Token
- WECOM_ENCODING_AES_KEY：回调EncodingAESKey
- BOSS_USER_ID：老板的企业微信UserId（用于超时上报）
- EMERGENCY_GROUP_ID：紧急响应群ID（可选）
