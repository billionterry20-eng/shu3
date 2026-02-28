import os
import json
import sqlite3
import threading
import time
from datetime import datetime
from typing import Optional, Dict, Any
from zoneinfo import ZoneInfo

import requests
from flask import Flask, g, redirect, render_template, request, url_for, flash
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.executors.pool import ThreadPoolExecutor


APP_TZ = ZoneInfo("Asia/Shanghai")  # 北京时间
DEFAULT_API_URL = os.getenv("STEP_API_URL", "http://8.140.250.130/king/api/step")
DEFAULT_REFERER = os.getenv("STEP_REFERER", "http://8.140.250.130/bushu/")
DEFAULT_ORIGIN = os.getenv("STEP_ORIGIN", "http://8.140.250.130")
DEFAULT_AUTH = os.getenv("STEP_AUTH_TOKEN", "5aa77abb20f11a5e7f2440747a655a55")
DEFAULT_PHONE = os.getenv("DEFAULT_PHONE", "Tbh2356@163.com")
DEFAULT_PWD = os.getenv("DEFAULT_PWD", "112233qq")
DEFAULT_STEPS = int(os.getenv("DEFAULT_STEPS", "89888"))
DEFAULT_TIME = os.getenv("DEFAULT_SUBMIT_TIME", "00:05")
DB_PATH = os.getenv("DB_PATH", os.path.join(os.path.dirname(__file__), "data.db"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "20"))

app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", "change-me-in-render")

os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
_db_init_lock = threading.Lock()

scheduler = BackgroundScheduler(
    timezone=APP_TZ,
    executors={"default": ThreadPoolExecutor(max_workers=20)},
    job_defaults={
        "coalesce": True,
        "max_instances": 1,
        "misfire_grace_time": 300,
    },
)


def now_bj() -> datetime:
    return datetime.now(APP_TZ)


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA busy_timeout=5000;")
    return conn


def init_db() -> None:
    with _db_init_lock:
        conn = get_conn()
        try:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS accounts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    phone TEXT NOT NULL,
                    pwd TEXT NOT NULL,
                    step_num INTEGER NOT NULL DEFAULT 89888,
                    submit_time TEXT NOT NULL DEFAULT '00:05',
                    auth_token TEXT NOT NULL,
                    api_url TEXT NOT NULL,
                    enabled INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS submit_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    account_id INTEGER,
                    account_name TEXT,
                    phone TEXT,
                    step_num INTEGER,
                    run_type TEXT NOT NULL,
                    status TEXT NOT NULL,
                    http_status INTEGER,
                    response_code INTEGER,
                    response_msg TEXT,
                    response_text TEXT,
                    error_text TEXT,
                    submitted_at_bj TEXT NOT NULL,
                    submitted_date_bj TEXT NOT NULL,
                    duration_ms INTEGER,
                    FOREIGN KEY(account_id) REFERENCES accounts(id)
                );
                """
            )
            conn.commit()
        finally:
            conn.close()


def seed_default_account_if_empty() -> None:
    conn = get_conn()
    try:
        cnt = conn.execute("SELECT COUNT(*) AS c FROM accounts").fetchone()["c"]
        if cnt == 0:
            ts = now_bj().isoformat(timespec="seconds")
            conn.execute(
                """
                INSERT INTO accounts (name, phone, pwd, step_num, submit_time, auth_token, api_url, enabled, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
                """,
                (
                    "默认账号",
                    DEFAULT_PHONE,
                    DEFAULT_PWD,
                    DEFAULT_STEPS,
                    DEFAULT_TIME,
                    DEFAULT_AUTH,
                    DEFAULT_API_URL,
                    ts,
                    ts,
                ),
            )
            conn.commit()
    finally:
        conn.close()


def mask_password(pwd: str) -> str:
    if not pwd:
        return ""
    if len(pwd) <= 2:
        return "*" * len(pwd)
    return pwd[0] + "*" * (len(pwd) - 2) + pwd[-1]


def fetch_account(account_id: int) -> Optional[sqlite3.Row]:
    conn = get_conn()
    try:
        return conn.execute("SELECT * FROM accounts WHERE id=?", (account_id,)).fetchone()
    finally:
        conn.close()


def list_accounts():
    conn = get_conn()
    try:
        rows = conn.execute("SELECT * FROM accounts ORDER BY id ASC").fetchall()
        return rows
    finally:
        conn.close()


def list_logs(limit: int = 100):
    conn = get_conn()
    try:
        rows = conn.execute(
            "SELECT * FROM submit_logs ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
        return rows
    finally:
        conn.close()


def parse_time_hhmm(hhmm: str):
    hhmm = (hhmm or "").strip()
    parts = hhmm.split(":")
    if len(parts) != 2:
        raise ValueError("时间格式必须为 HH:MM")
    h = int(parts[0])
    m = int(parts[1])
    if not (0 <= h <= 23 and 0 <= m <= 59):
        raise ValueError("时间超出范围")
    return h, m


def build_request_headers(account: sqlite3.Row) -> Dict[str, str]:
    # time 头按毫秒时间戳动态生成
    t_ms = str(int(time.time() * 1000))
    origin = DEFAULT_ORIGIN
    referer = DEFAULT_REFERER
    try:
        if account and account["api_url"] and account["api_url"].startswith("http"):
            # 如接口域名变了，可通过环境变量覆盖 Origin/Referer
            pass
    except Exception:
        pass

    return {
        "Accept": "*/*",
        "Authorization": account["auth_token"] or DEFAULT_AUTH,
        "X-Requested-With": "XMLHttpRequest",
        "time": t_ms,
        "Accept-Language": "zh-TW,zh-Hant;q=0.9",
        "Accept-Encoding": "gzip, deflate",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Origin": origin,
        "User-Agent": (
            "Mozilla/5.0 (iPhone; CPU iPhone OS 17_6_1 like Mac OS X) "
            "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.6 Mobile/15E148 Safari/604.1"
        ),
        "Referer": referer,
        "Connection": "keep-alive",
    }


def submit_step_for_account(account_id: int, run_type: str = "auto") -> Dict[str, Any]:
    account = fetch_account(account_id)
    if not account:
        raise ValueError(f"账号不存在: {account_id}")

    started = time.time()
    url = account["api_url"] or DEFAULT_API_URL
    data = {
        "phone": account["phone"],
        "pwd": account["pwd"],
        "num": str(account["step_num"]),
    }
    headers = build_request_headers(account)

    result: Dict[str, Any] = {
        "ok": False,
        "http_status": None,
        "response_code": None,
        "response_msg": None,
        "response_text": None,
        "error_text": None,
    }

    try:
        resp = requests.post(url, headers=headers, data=data, timeout=REQUEST_TIMEOUT)
        result["http_status"] = resp.status_code
        text = resp.text.strip()
        result["response_text"] = text[:5000]

        try:
            payload = resp.json()
            result["response_code"] = payload.get("code")
            result["response_msg"] = payload.get("msg")
            result["ok"] = (resp.status_code == 200 and payload.get("code") == 200)
        except Exception:
            # 有些接口返回非标准 JSON 时至少按 HTTP 200 判断
            result["ok"] = (resp.status_code == 200 and "success" in text.lower())
    except Exception as e:
        result["error_text"] = str(e)
        result["ok"] = False

    duration_ms = int((time.time() - started) * 1000)
    write_submit_log(account, run_type, duration_ms, result)
    return result


def write_submit_log(account: sqlite3.Row, run_type: str, duration_ms: int, result: Dict[str, Any]) -> None:
    ts_bj = now_bj()
    conn = get_conn()
    try:
        conn.execute(
            """
            INSERT INTO submit_logs (
                account_id, account_name, phone, step_num, run_type, status,
                http_status, response_code, response_msg, response_text, error_text,
                submitted_at_bj, submitted_date_bj, duration_ms
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                account["id"],
                account["name"],
                account["phone"],
                account["step_num"],
                run_type,
                "success" if result.get("ok") else "failed",
                result.get("http_status"),
                result.get("response_code"),
                result.get("response_msg"),
                result.get("response_text"),
                result.get("error_text"),
                ts_bj.isoformat(timespec="seconds"),
                ts_bj.strftime("%Y-%m-%d"),
                duration_ms,
            ),
        )
        conn.commit()
    finally:
        conn.close()


def _job_wrapper(account_id: int):
    try:
        submit_step_for_account(account_id, run_type="auto")
    except Exception as e:
        # 尽量把异常写入日志（账号可能已被删除）
        acct = fetch_account(account_id)
        if acct:
            write_submit_log(
                acct,
                run_type="auto",
                duration_ms=0,
                result={
                    "ok": False,
                    "http_status": None,
                    "response_code": None,
                    "response_msg": None,
                    "response_text": None,
                    "error_text": f"Scheduler job exception: {e}",
                },
            )


def job_id_for_account(account_id: int) -> str:
    return f"account_submit_{account_id}"


def schedule_account_job(account: sqlite3.Row) -> None:
    jid = job_id_for_account(account["id"])

    # 删除旧任务再按最新配置重建
    try:
        scheduler.remove_job(jid)
    except Exception:
        pass

    if int(account["enabled"]) != 1:
        return

    h, m = parse_time_hhmm(account["submit_time"])
    trigger = CronTrigger(hour=h, minute=m, timezone=APP_TZ)
    scheduler.add_job(
        func=_job_wrapper,
        trigger=trigger,
        id=jid,
        args=[account["id"]],
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=300,
    )


def reload_all_jobs() -> None:
    if not scheduler.running:
        return
    for job in list(scheduler.get_jobs()):
        try:
            scheduler.remove_job(job.id)
        except Exception:
            pass
    for acct in list_accounts():
        schedule_account_job(acct)


def init_scheduler_once() -> None:
    if not scheduler.running:
        scheduler.start()
    reload_all_jobs()


@app.before_request
def _attach_now():
    g.now_bj = now_bj()


@app.template_filter("pwdmask")
def _pwdmask_filter(v):
    return mask_password(v or "")


@app.route("/")
def index():
    accounts = list_accounts()
    logs = list_logs(limit=50)
    jobs = {job.id: job for job in scheduler.get_jobs()} if scheduler.running else {}
    return render_template(
        "index.html",
        accounts=accounts,
        logs=logs,
        jobs=jobs,
        now_bj=now_bj(),
        scheduler_running=scheduler.running,
    )


@app.route("/health")
def health():
    return {
        "ok": True,
        "time_bj": now_bj().isoformat(timespec="seconds"),
        "scheduler_running": scheduler.running,
        "job_count": len(scheduler.get_jobs()) if scheduler.running else 0,
    }


@app.route("/account/add", methods=["POST"])
def account_add():
    try:
        name = request.form.get("name", "").strip() or "未命名账号"
        phone = request.form.get("phone", "").strip()
        pwd = request.form.get("pwd", "").strip()
        step_num = int(request.form.get("step_num", DEFAULT_STEPS))
        submit_time = request.form.get("submit_time", DEFAULT_TIME).strip()
        enabled = 1 if request.form.get("enabled") == "on" else 0
        auth_token = request.form.get("auth_token", "").strip() or DEFAULT_AUTH
        api_url = request.form.get("api_url", "").strip() or DEFAULT_API_URL

        if not phone or not pwd:
            raise ValueError("账号和密码不能为空")
        parse_time_hhmm(submit_time)
        if step_num <= 0:
            raise ValueError("步数必须大于 0")

        ts = now_bj().isoformat(timespec="seconds")
        conn = get_conn()
        try:
            cur = conn.execute(
                """
                INSERT INTO accounts (name, phone, pwd, step_num, submit_time, auth_token, api_url, enabled, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (name, phone, pwd, step_num, submit_time, auth_token, api_url, enabled, ts, ts),
            )
            conn.commit()
            account_id = cur.lastrowid
        finally:
            conn.close()

        acct = fetch_account(account_id)
        if acct:
            schedule_account_job(acct)
        flash("账号已新增", "success")
    except Exception as e:
        flash(f"新增失败：{e}", "danger")
    return redirect(url_for("index"))


@app.route("/account/<int:account_id>/update", methods=["POST"])
def account_update(account_id: int):
    try:
        old = fetch_account(account_id)
        if not old:
            raise ValueError("账号不存在")

        name = request.form.get("name", old["name"]).strip() or old["name"]
        phone = request.form.get("phone", old["phone"]).strip() or old["phone"]
        pwd = request.form.get("pwd", "").strip() or old["pwd"]
        step_num = int(request.form.get("step_num", old["step_num"]))
        submit_time = request.form.get("submit_time", old["submit_time"]).strip()
        enabled = 1 if request.form.get("enabled") == "on" else 0
        auth_token = request.form.get("auth_token", old["auth_token"]).strip() or old["auth_token"]
        api_url = request.form.get("api_url", old["api_url"]).strip() or old["api_url"]

        parse_time_hhmm(submit_time)
        if step_num <= 0:
            raise ValueError("步数必须大于 0")

        ts = now_bj().isoformat(timespec="seconds")
        conn = get_conn()
        try:
            conn.execute(
                """
                UPDATE accounts
                SET name=?, phone=?, pwd=?, step_num=?, submit_time=?, auth_token=?, api_url=?, enabled=?, updated_at=?
                WHERE id=?
                """,
                (name, phone, pwd, step_num, submit_time, auth_token, api_url, enabled, ts, account_id),
            )
            conn.commit()
        finally:
            conn.close()

        acct = fetch_account(account_id)
        if acct:
            schedule_account_job(acct)
        flash("账号已更新（对应定时任务已重载）", "success")
    except Exception as e:
        flash(f"更新失败：{e}", "danger")
    return redirect(url_for("index"))


@app.route("/account/<int:account_id>/delete", methods=["POST"])
def account_delete(account_id: int):
    try:
        conn = get_conn()
        try:
            conn.execute("DELETE FROM accounts WHERE id=?", (account_id,))
            conn.commit()
        finally:
            conn.close()
        try:
            scheduler.remove_job(job_id_for_account(account_id))
        except Exception:
            pass
        flash("账号已删除", "success")
    except Exception as e:
        flash(f"删除失败：{e}", "danger")
    return redirect(url_for("index"))


@app.route("/account/<int:account_id>/submit", methods=["POST"])
def account_submit(account_id: int):
    try:
        result = submit_step_for_account(account_id, run_type="manual")
        if result.get("ok"):
            flash(
                f"手动提交成功：HTTP {result.get('http_status')} / code {result.get('response_code')} / msg {result.get('response_msg')}",
                "success",
            )
        else:
            flash(
                f"手动提交失败：HTTP {result.get('http_status')} / code {result.get('response_code')} / msg {result.get('response_msg')} / err {result.get('error_text')}",
                "danger",
            )
    except Exception as e:
        flash(f"提交异常：{e}", "danger")
    return redirect(url_for("index"))


@app.route("/account/<int:account_id>/test", methods=["POST"])
def account_test(account_id: int):
    # 与手动提交行为一致，但 run_type 单独记录，便于排查
    try:
        result = submit_step_for_account(account_id, run_type="test")
        if result.get("ok"):
            flash(
                f"测试成功：HTTP {result.get('http_status')} / code {result.get('response_code')} / msg {result.get('response_msg')}",
                "success",
            )
        else:
            flash(
                f"测试失败：HTTP {result.get('http_status')} / code {result.get('response_code')} / msg {result.get('response_msg')} / err {result.get('error_text')}",
                "danger",
            )
    except Exception as e:
        flash(f"测试异常：{e}", "danger")
    return redirect(url_for("index"))


@app.route("/logs/clear", methods=["POST"])
def clear_logs():
    try:
        conn = get_conn()
        try:
            conn.execute("DELETE FROM submit_logs")
            conn.commit()
        finally:
            conn.close()
        flash("日志已清空", "success")
    except Exception as e:
        flash(f"清空日志失败：{e}", "danger")
    return redirect(url_for("index"))


@app.route("/jobs/reload", methods=["POST"])
def jobs_reload():
    try:
        reload_all_jobs()
        flash("所有定时任务已重载", "success")
    except Exception as e:
        flash(f"重载失败：{e}", "danger")
    return redirect(url_for("index"))


def create_app() -> Flask:
    init_db()
    seed_default_account_if_empty()
    init_scheduler_once()
    return app


# Gunicorn 启动时会导入 app 对象，因此在导入阶段完成初始化
create_app()


if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    # debug=True 会触发双进程重载，可能导致定时任务重复；本项目默认关闭 debug
    app.run(host="0.0.0.0", port=port, debug=False)
