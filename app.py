"""
企业微信紧急响应机器人
=======================
监听群聊中的紧急通知 🚨，自动追踪开发人员响应时间，
超时自动提醒、自动上报。

部署到 Railway：推送 GitHub 后自动部署。
"""

import os
import re
import json
import time
import hashlib
import base64
import struct
import socket
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from threading import Thread, Lock

import requests
from flask import Flask, request, jsonify
from Crypto.Cipher import AES
from apscheduler.schedulers.background import BackgroundScheduler

# ── 环境变量 ─────────────────────────────────────────────
CORP_ID = os.environ["WECOM_CORP_ID"]
CORP_SECRET = os.environ["WECOM_CORP_SECRET"]
AGENT_ID = int(os.environ["WECOM_AGENT_ID"])
TOKEN = os.environ["WECOM_TOKEN"]
ENCODING_AES_KEY = os.environ["WECOM_ENCODING_AES_KEY"]
BOSS_USER_ID = os.environ.get("BOSS_USER_ID", "")  # 老板的企微UserId，用于上报超时

# 可选：紧急响应群ID，不填则监听所有群
EMERGENCY_GROUP_ID = os.environ.get("EMERGENCY_GROUP_ID", "")

# ── 数据库（SQLite 文件）────────────────────────────────
DB_PATH = "data/emergency.db"

# ── 常量 ───────────────────────────────────────────────
RESPONSE_TIMEOUT_MINUTES = 30       # 首次响应超时（分钟）
SECOND_ALERT_MINUTES = 35           # 超时后二次提醒间隔
BOSS_REPORT_HOURS = 2               # 多久未响应上报老板（小时）


# ╔══════════════════════════════════════════════════════════╗
# ║              企业微信 API 封装                            ║
# ╚══════════════════════════════════════════════════════════╝

class WeComAPI:
    """企业微信 API 调用"""

    def __init__(self):
        self._access_token = None
        self._token_expires_at = 0
        self._lock = Lock()

    def get_access_token(self) -> str:
        """获取 access_token，自动缓存和刷新"""
        with self._lock:
            if self._access_token and time.time() < self._token_expires_at - 60:
                return self._access_token

        url = "https://qyapi.weixin.qq.com/cgi-bin/gettoken"
        resp = requests.get(url, params={
            "corpid": CORP_ID,
            "corpsecret": CORP_SECRET,
        }, timeout=10)
        data = resp.json()
        if data.get("errcode") != 0:
            raise Exception(f"获取 access_token 失败: {data}")

        with self._lock:
            self._access_token = data["access_token"]
            self._token_expires_at = time.time() + data.get("expires_in", 7200)
        return self._access_token

    def send_message(self, touser: str, content: str) -> dict:
        """发送文本消息给指定用户"""
        token = self.get_access_token()
        url = f"https://qyapi.weixin.qq.com/cgi-bin/message/send?access_token={token}"
        body = {
            "touser": touser,
            "msgtype": "text",
            "agentid": AGENT_ID,
            "text": {"content": content},
            "safe": 0,
        }
        resp = requests.post(url, json=body, timeout=10)
        return resp.json()

    def send_group_message(self, chatid: str, content: str) -> dict:
        """发送消息到群聊"""
        token = self.get_access_token()
        url = f"https://qyapi.weixin.qq.com/cgi-bin/appchat/send?access_token={token}"
        body = {
            "chatid": chatid,
            "msgtype": "text",
            "text": {"content": content},
            "safe": 0,
        }
        resp = requests.post(url, json=body, timeout=10)
        return resp.json()

    def get_user_info(self, userid: str) -> dict:
        """获取用户信息"""
        token = self.get_access_token()
        url = f"https://qyapi.weixin.qq.com/cgi-bin/user/get?access_token={token}&userid={userid}"
        resp = requests.get(url, timeout=10)
        return resp.json()

    def get_chat_info(self, chatid: str) -> dict:
        """获取群聊信息"""
        token = self.get_access_token()
        url = f"https://qyapi.weixin.qq.com/cgi-bin/appchat/get?access_token={token}&chatid={chatid}"
        resp = requests.get(url, timeout=10)
        return resp.json()


api = WeComAPI()


# ╔══════════════════════════════════════════════════════════╗
# ║              SQLite 数据库                                ║
# ╚══════════════════════════════════════════════════════════╝

def init_db():
    """初始化数据库"""
    os.makedirs("data", exist_ok=True)
    import sqlite3
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS emergencies (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            alert_id TEXT UNIQUE NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            -- pending: 等待响应
            -- responded: 已响应
            -- timeout_1: 首次超时
            -- timeout_boss: 已上报老板
            -- resolved: 已解决
            -- cancelled: 已取消
            --
            from_userid TEXT NOT NULL,
            from_name TEXT DEFAULT '',
            chat_id TEXT DEFAULT '',
            level TEXT DEFAULT 'P1',
            content TEXT DEFAULT '',
            mentioned_users TEXT DEFAULT '',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            first_responded_at TIMESTAMP,
            responded_by TEXT DEFAULT '',
            resolved_at TIMESTAMP,
            resolve_note TEXT DEFAULT '',
            timeout_count INTEGER DEFAULT 0,
            boss_reported INTEGER DEFAULT 0
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS response_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            alert_id TEXT NOT NULL,
            userid TEXT NOT NULL,
            action TEXT DEFAULT '',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()


def db_execute(sql: str, params=()):
    import sqlite3
    conn = sqlite3.connect(DB_PATH)
    cur = conn.execute(sql, params)
    conn.commit()
    conn.close()
    return cur


def db_fetch(sql: str, params=()):
    import sqlite3
    conn = sqlite3.connect(DB_PATH)
    cur = conn.execute(sql, params)
    rows = cur.fetchall()
    conn.close()
    return rows


# ╔══════════════════════════════════════════════════════════╗
# ║              消息解密 & 回调处理                           ║
# ╚══════════════════════════════════════════════════════════╝

class MessageCrypt:
    """企业微信消息加解密"""

    def __init__(self, token, encoding_aes_key, corp_id):
        self.token = token
        self.corp_id = corp_id
        self.key = base64.b64decode(encoding_aes_key + "=")
        if len(self.key) != 32:
            raise ValueError("EncodingAESKey 长度错误")

    def verify_url(self, msg_signature, timestamp, nonce, echostr):
        """URL 验证（GET 请求）"""
        signature = self._make_signature(timestamp, nonce, echostr)
        if signature != msg_signature:
            raise ValueError("签名验证失败")
        return self._decrypt(echostr)

    def decrypt_msg(self, msg_signature, timestamp, nonce, body):
        """解密消息（POST 请求）"""
        root = ET.fromstring(body)
        encrypt = root.find("Encrypt").text
        signature = self._make_signature(timestamp, nonce, encrypt)
        if signature != msg_signature:
            raise ValueError("签名验证失败")
        return self._decrypt(encrypt)

    def _make_signature(self, timestamp, nonce, encrypt):
        """生成签名"""
        raw = "".join(sorted([self.token, timestamp, nonce, encrypt]))
        return hashlib.sha1(raw.encode()).hexdigest()

    def _decrypt(self, encrypt):
        """解密"""
        cipher = AES.new(self.key, AES.MODE_CBC, self.key[:16])
        plain = cipher.decrypt(base64.b64decode(encrypt))
        # 去掉 PKCS7 填充
        pad = plain[-1]
        if isinstance(pad, int):
            plain = plain[:-pad]
        # 转换为字符串
        plain = plain.decode("utf-8")
        # 格式: random16 + msg_len(4) + msg + corpid
        # msg_len 是网络字节序
        content = plain[16:]  # 去掉16字节随机数
        msg_len = socket.ntohl(struct.unpack("I", content[:4].encode() if isinstance(content[:4], str) else content[:4])[0] if isinstance(content[:4], bytes) else struct.unpack(">I", content[:4].encode("latin-1"))[0])
        # 简化的解密方式
        try:
            # 尝试定位 corp_id
            msg_end = content.find(CORP_ID.encode() if isinstance(content, bytes) else CORP_ID)
            if msg_end == -1:
                msg_end = len(content) - len(CORP_ID)
            msg = content[4:msg_end]
            if isinstance(msg, bytes):
                msg = msg.decode("utf-8")
            return msg
        except Exception:
            return content[4:]


crypt = MessageCrypt(TOKEN, ENCODING_AES_KEY, CORP_ID)


# ╔══════════════════════════════════════════════════════════╗
# ║              核心业务逻辑                                  ║
# ╚══════════════════════════════════════════════════════════╝

def is_urgent_message(text: str) -> bool:
    """判断是否是紧急通知消息"""
    keywords = ["🚨", "紧急", "P0", "P1", "urgent", "urgent:"]
    text_lower = text.lower()
    return any(kw.lower() in text_lower for kw in keywords)


def extract_mentioned_users(text: str) -> list:
    """从消息文本中提取被 @ 的用户列表"""
    # 企业微信群消息中的 @ 格式：@用户名 或直接是 UserId
    # 简化处理：用正则匹配可能的用户标识
    mentioned = []
    # 匹配 @xxx 格式
    at_pattern = r'@(\w+)'
    matches = re.findall(at_pattern, text)
    mentioned.extend(matches)
    return mentioned


def extract_alert_level(text: str) -> str:
    """提取紧急等级"""
    if re.search(r'\bP0\b', text):
        return "P0"
    if re.search(r'\bP1\b', text):
        return "P1"
    return "P2"


def handle_urgent_message(msg_data: dict):
    """
    处理紧急通知消息
    1. 解析消息内容和被@的人
    2. 存入数据库
    3. 发送确认通知
    4. 启动超时检查
    """
    text = msg_data.get("Content", "").strip()
    from_user = msg_data.get("From", {}).get("UserId", "")
    from_name = msg_data.get("From", {}).get("Name", "")
    chat_id = msg_data.get("ChatId", "")
    msg_id = msg_data.get("MsgId", "")

    if msg_id:
        alert_id = f"alert_{msg_id}"
    else:
        alert_id = f"alert_{int(time.time())}_{from_user}"

    # 检查是否已存在
    existing = db_fetch("SELECT id FROM emergencies WHERE alert_id=?", (alert_id,))
    if existing:
        print(f"[跳过] 重复的紧急通知: {alert_id}")
        return

    mentioned = extract_mentioned_users(text)
    level = extract_alert_level(text)

    # 存入数据库
    db_execute(
        """INSERT INTO emergencies 
           (alert_id, from_userid, from_name, chat_id, level, content, mentioned_users)
           VALUES (?,?,?,?,?,?,?)""",
        (alert_id, from_user, from_name, chat_id, level, text, json.dumps(mentioned, ensure_ascii=False))
    )

    # 日志记录
    db_execute(
        "INSERT INTO response_log (alert_id, userid, action) VALUES (?,?,?)",
        (alert_id, from_user, "发起紧急通知")
    )

    # 在群里发一条确认消息
    mention_str = " ".join(f"@{u}" for u in mentioned) if mentioned else "相关人员"
    confirm_msg = (
        f"🤖 收到紧急通知 [{level}]\n"
        f"发起人：{from_name}\n"
        f"已通知：{mention_str}\n"
        f"⏰ 请在 {RESPONSE_TIMEOUT_MINUTES} 分钟内回复确认\n"
        f"追踪编号：{alert_id}"
    )

    if chat_id:
        api.send_group_message(chat_id, confirm_msg)

    # 给每个被提及的人发送私聊提醒
    for uid in mentioned:
        try:
            api.send_message(uid, f"🚨 紧急通知 [{level}]\n{from_name} 在群里@了你，请立即响应！\n内容：{text[:200]}")
        except Exception as e:
            print(f"[错误] 发送私聊提醒给 {uid} 失败: {e}")

    print(f"[紧急通知] {alert_id} | {level} | 发起:{from_name} | @:{mentioned}")


def handle_response(msg_data: dict):
    """
    处理开发人员的响应消息
    如果当前有未响应的紧急通知，且该开发被@了，则记录为已响应
    """
    text = msg_data.get("Content", "").strip()
    from_user = msg_data.get("From", {}).get("UserId", "")
    from_name = msg_data.get("From", {}).get("Name", "")
    chat_id = msg_data.get("ChatId", "")

    if not text:
        return

    # 找到该群中所有 pending 状态的紧急通知
    # 简化：找所有 pending 且该用户被提及的
    pending = db_fetch(
        """SELECT alert_id, mentioned_users, level, from_userid, from_name 
           FROM emergencies WHERE status='pending' OR status='timeout_1'"""
    )

    matched_alert = None
    for row in pending:
        alert_id, mentioned_json, level, from_uid, from_n = row
        mentioned = json.loads(mentioned_json) if mentioned_json else []
        if from_user in mentioned or text.lower().startswith("收到"):
            if chat_id:
                # 额外验证：是否同一个群
                pass
            matched_alert = alert_id
            break

    if matched_alert and chat_id:
        now = datetime.now().isoformat()
        db_execute(
            """UPDATE emergencies 
               SET status='responded', first_responded_at=?, responded_by=?
               WHERE alert_id=?""",
            (now, from_user, matched_alert)
        )
        db_execute(
            "INSERT INTO response_log (alert_id, userid, action) VALUES (?,?,?)",
            (matched_alert, from_user, f"响应确认: {text[:100]}")
        )

        api.send_group_message(
            chat_id,
            f"✅ {from_name} 已响应紧急通知\n追踪编号：{matched_alert}"
        )
        print(f"[响应确认] {matched_alert} | {from_name}")


# ╔══════════════════════════════════════════════════════════╗
# ║              超时检查 & 定时任务                           ║
# ╚══════════════════════════════════════════════════════════╝

def check_timeouts():
    """定时检查超时的紧急通知"""
    cutoff = (datetime.now() - timedelta(minutes=RESPONSE_TIMEOUT_MINUTES)).isoformat()
    boss_cutoff = (datetime.now() - timedelta(hours=BOSS_REPORT_HOURS)).isoformat()

    # 查找超时但尚未标记超时的
    timeout_rows = db_fetch(
        """SELECT alert_id, chat_id, mentioned_users, level, from_name, created_at
           FROM emergencies 
           WHERE status='pending' 
           AND created_at <= ?""",
        (cutoff,)
    )

    for row in timeout_rows:
        alert_id, chat_id, mentioned_json, level, from_name, created_at = row
        mentioned = json.loads(mentioned_json) if mentioned_json else []

        db_execute(
            """UPDATE emergencies SET status='timeout_1', timeout_count=1
               WHERE alert_id=?""",
            (alert_id,)
        )
        db_execute(
            "INSERT INTO response_log (alert_id, userid, action) VALUES (?,?,?)",
            (alert_id, "SYSTEM", f"首次超时标记（超{RESPONSE_TIMEOUT_MINUTES}分钟）")
        )

        # 在群里再次@提醒
        mention_str = " ".join(f"@{u}" for u in mentioned) if mentioned else "相关人员"
        if chat_id:
            api.send_group_message(
                chat_id,
                f"⏰ 超时提醒！紧急通知 [{level}] 已超过 {RESPONSE_TIMEOUT_MINUTES} 分钟未响应\n"
                f"{mention_str} 请立即处理！\n"
                f"追踪编号：{alert_id}"
            )

        # 私聊提醒每个未响应的人
        for uid in mentioned:
            try:
                api.send_message(uid, f"⏰ 超时警告！你在群里的紧急通知已超过 {RESPONSE_TIMEOUT_MINUTES} 分钟未响应。请注意这会影响你的月度绩效评分。")
            except Exception as e:
                print(f"[错误] 超时私聊提醒 {uid} 失败: {e}")

        print(f"[超时标记] {alert_id} | 超{RESPONSE_TIMEOUT_MINUTES}分钟")

    # 查找需要上报老板的
    boss_rows = db_fetch(
        """SELECT alert_id, chat_id, mentioned_users, level, from_name, created_at
           FROM emergencies 
           WHERE status IN ('timeout_1') 
           AND boss_reported=0
           AND created_at <= ?""",
        (boss_cutoff,)
    )

    for row in boss_rows:
        alert_id, chat_id, mentioned_json, level, from_name, created_at = row
        mentioned = json.loads(mentioned_json) if mentioned_json else []

        db_execute(
            "UPDATE emergencies SET status='timeout_boss', boss_reported=1 WHERE alert_id=?",
            (alert_id,)
        )
        db_execute(
            "INSERT INTO response_log (alert_id, userid, action) VALUES (?,?,?)",
            (alert_id, "SYSTEM", f"已上报老板（超{BOSS_REPORT_HOURS}小时）")
        )

        # 上报给老板
        if BOSS_USER_ID:
            mention_str = ", ".join(mentioned)
            try:
                api.send_message(
                    BOSS_USER_ID,
                    f"🚨 严重超时预警！\n"
                    f"紧急通知已超过 {BOSS_REPORT_HOURS} 小时无人响应\n"
                    f"等级：{level}\n"
                    f"发起人：{from_name}\n"
                    f"被通知人员：{mention_str}\n"
                    f"追踪编号：{alert_id}\n\n"
                    f"请老板关注！"
                )
            except Exception as e:
                print(f"[错误] 上报老板失败: {e}")

        print(f"[上报老板] {alert_id} | 超{BOSS_REPORT_HOURS}小时")


def generate_monthly_report():
    """生成月度统计报告（每月最后一天自动执行）"""
    today = datetime.now()
    if today.day == 28:  # 每月28号生成初步数据
        start_of_month = today.replace(day=1, hour=0, minute=0, second=0).isoformat()

        stats = db_fetch(
            """SELECT 
                mentioned_users,
                COUNT(*) as total,
                SUM(CASE WHEN status IN ('responded','resolved') THEN 1 ELSE 0 END) as responded,
                SUM(CASE WHEN status IN ('timeout_1','timeout_boss') THEN 1 ELSE 0 END) as timeout,
                SUM(CASE WHEN boss_reported=1 THEN 1 ELSE 0 END) as boss_reported
               FROM emergencies
               WHERE created_at >= ?
               GROUP BY mentioned_users""",
            (start_of_month,)
        )

        report = f"📊 紧急响应月度统计（{today.strftime('%Y年%m月')}）\n\n"
        for row in stats:
            users, total, responded, timeout, boss = row
            report += f"👤 {users}\n"
            report += f"  总通知：{total} | 已响应：{responded} | 超时：{timeout} | 严重超时上报：{boss}\n\n"

        if BOSS_USER_ID:
            try:
                api.send_message(BOSS_USER_ID, report)
            except Exception as e:
                print(f"[错误] 发送月度报告失败: {e}")

        print(f"[月度报告] 已生成")


# ╔══════════════════════════════════════════════════════════╗
# ║              Flask 应用                                   ║
# ╚══════════════════════════════════════════════════════════╝

app = Flask(__name__)
init_db()


@app.route("/")
def index():
    return jsonify({
        "service": "企业微信紧急响应机器人",
        "status": "running",
        "time": datetime.now().isoformat(),
    })


@app.route("/wecom/callback", methods=["GET", "POST"])
def wecom_callback():
    """企业微信回调接口"""
    msg_signature = request.args.get("msg_signature", "")
    timestamp = request.args.get("timestamp", "")
    nonce = request.args.get("nonce", "")

    if request.method == "GET":
        # URL 验证
        echostr = request.args.get("echostr", "")
        try:
            decrypted = crypt.verify_url(msg_signature, timestamp, nonce, echostr)
            return decrypted
        except Exception as e:
            print(f"[错误] URL验证失败: {e}")
            return str(e), 403

    else:
        # 接收消息
        body = request.data.decode("utf-8")
        try:
            xml_content = crypt.decrypt_msg(msg_signature, timestamp, nonce, body)
            print(f"[消息] {xml_content[:500]}")
        except Exception as e:
            print(f"[错误] 消息解密失败: {e}")
            return "error", 500

        # 解析 XML
        try:
            root = ET.fromstring(xml_content)
            msg_type = root.find("MsgType")
            msg_type = msg_type.text if msg_type is not None else ""

            if msg_type == "text":
                # 提取消息字段
                msg_data = {
                    "MsgId": (root.find("MsgId").text if root.find("MsgId") is not None else ""),
                    "Content": (root.find("Content").text if root.find("Content") is not None else ""),
                    "ChatId": (root.find("ChatId").text if root.find("ChatId") is not None else ""),
                    "ChatType": (root.find("ChatType").text if root.find("ChatType") is not None else ""),
                    "From": {
                        "UserId": (root.find("From/UserId").text if root.find("From/UserId") is not None else ""),
                        "Name": (root.find("From/Name").text if root.find("From/Name") is not None else ""),
                    },
                }

                # 如果指定了紧急响应群，只处理该群的消息
                group_id = EMERGENCY_GROUP_ID.strip()
                if group_id and msg_data.get("ChatId") != group_id:
                    print(f"[过滤] 非目标群消息，跳过")
                else:
                    content = msg_data.get("Content", "")

                    # 判断消息类型
                    if is_urgent_message(content):
                        # 在新线程中处理，避免回调超时
                        Thread(target=handle_urgent_message, args=(msg_data,), daemon=True).start()
                    else:
                        # 检查是否是响应确认
                        Thread(target=handle_response, args=(msg_data,), daemon=True).start()

        except Exception as e:
            print(f"[错误] XML解析失败: {e}")

        return ""  # 企业微信要求返回空字符串


@app.route("/api/emergencies")
def list_emergencies():
    """查询紧急通知列表（调试用）"""
    rows = db_fetch(
        """SELECT alert_id, status, level, from_name, mentioned_users, 
                  created_at, first_responded_at, timeout_count, boss_reported
           FROM emergencies ORDER BY created_at DESC LIMIT 50"""
    )
    result = []
    for row in rows:
        result.append({
            "alert_id": row[0],
            "status": row[1],
            "level": row[2],
            "from_name": row[3],
            "mentioned_users": row[4],
            "created_at": row[5],
            "first_responded_at": row[6],
            "timeout_count": row[7],
            "boss_reported": row[8],
        })
    return jsonify(result)


@app.route("/api/report")
def monthly_report():
    """月度统计（手动触发）"""
    today = datetime.now()
    start_of_month = today.replace(day=1, hour=0, minute=0, second=0).isoformat()

    total = db_fetch(
        "SELECT alert_id, created_at, responded_at, timeout_count, boss_reported "
        "FROM emergencies WHERE created_at >= ?", (start_of_month,)
    )

    # 按人统计
    person_stats = {}
    for alert_id, created_at, responded_at, timeout_count, boss_reported in total:
        # 简化处理
        pass

    return jsonify({
        "month": today.strftime("%Y-%m"),
        "total_emergencies": len(total),
        "responded": len([r for r in total if r[2]]),
        "timeout": len([r for r in total if r[3] and r[3] > 0]),
        "boss_reported": len([r for r in total if r[4]]),
    })


# ╔══════════════════════════════════════════════════════════╗
# ║              启动                                         ║
# ╚══════════════════════════════════════════════════════════╝

# 启动定时检查
scheduler = BackgroundScheduler()
scheduler.add_job(check_timeouts, "interval", minutes=5, id="check_timeouts")
scheduler.add_job(generate_monthly_report, "cron", hour=9, minute=0, id="monthly_report")
scheduler.start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
