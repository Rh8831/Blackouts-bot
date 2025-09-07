#!/usr/bin/env python3
# Telegram Bot: Planned Blackouts (Tehran time) — Single-Message UI
# - Live "now" via BlackoutsReport (with tiny cache), planned API fallback
# - Alerts (1h/10m/00:01)
# - Delete bill (removes alerts/logs/caches)
# - ✅ Single-message navigation: always edit the same message
# - ✅ Bot Menu Button + only /start command
# - ✅ Reminders strictly for matching outage_date
# - ✅ Main menu ONLY on Home; elsewhere a single Back button

import os, re, sqlite3, datetime, requests, logging, asyncio
from urllib.parse import urlparse
from pathlib import Path
from typing import List, Dict, Optional, Tuple

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

from dotenv import load_dotenv
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, MenuButtonCommands
from telegram.ext import (
    ApplicationBuilder, CommandHandler, CallbackQueryHandler,
    MessageHandler, ContextTypes, filters
)

# =========================
# Config
# =========================
APP_DIR = Path(__file__).parent
DB_PATH = APP_DIR / "db.sqlite"

API_URL = "https://uiapi2.saapa.ir/api/ebills/PlannedBlackoutsReport"
API_URL_CURRENT = "https://uiapi2.saapa.ir/api/ebills/BlackoutsReport"
PROXY_FILE = APP_DIR / "proxy.txt"
FAR_FUTURE_DATE = "1499/12/29"
TZ_TEHRAN = ZoneInfo("Asia/Tehran")

REFRESH_SECONDS = 3600
CACHE_TTL_SECONDS = REFRESH_SECONDS
NOW_CACHE_TTL_SECONDS = 45
ALERT_WINDOW_MINUTES = 2
SENT_ALERTS_RETENTION_DAYS = 30

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# env
load_dotenv()
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
API_JWT = os.getenv("API_JWT")
if not TELEGRAM_TOKEN:
    raise SystemExit("❌ TELEGRAM_TOKEN is missing in .env")
if not API_JWT:
    raise SystemExit("❌ API_JWT is missing in .env")

# proxy (optional)
PROXY_URL = None
if PROXY_FILE.exists():
    _txt = PROXY_FILE.read_text(encoding="utf-8").strip()
    if _txt:
        PROXY_URL = _txt
        try:
            u = urlparse(PROXY_URL)
            display = f"{u.scheme}://{u.hostname or ''}{':' + str(u.port) if u.port else ''}"
        except Exception:
            display = "<proxy configured>"
        logging.info(f"Using API proxy: {display}")

def get_proxies():
    return {"http": PROXY_URL, "https": PROXY_URL} if PROXY_URL else None

# =========================
# DB
# =========================
def db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA busy_timeout=30000;")
    except Exception:
        pass
    conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            chat_id INTEGER PRIMARY KEY,
            pending TEXT,
            temp_bill TEXT,
            home_msg_id INTEGER
        )
    """)
    try:
        conn.execute("ALTER TABLE users ADD COLUMN home_msg_id INTEGER")
    except Exception:
        pass

    conn.execute("""
        CREATE TABLE IF NOT EXISTS bills (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            bill_id TEXT NOT NULL,
            UNIQUE(chat_id, name)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS bill_alerts (
            chat_id INTEGER NOT NULL,
            bill_id TEXT NOT NULL,
            a1h INTEGER NOT NULL DEFAULT 0,
            a10m INTEGER NOT NULL DEFAULT 0,
            a1201 INTEGER NOT NULL DEFAULT 0,
            PRIMARY KEY (chat_id, bill_id)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS sent_alerts (
            chat_id INTEGER NOT NULL,
            bill_id TEXT NOT NULL,
            kind TEXT NOT NULL,
            jdate TEXT NOT NULL,
            uniq TEXT NOT NULL,
            PRIMARY KEY (chat_id, bill_id, kind, jdate, uniq)
        )
    """)
    return conn

def get_user_row(chat_id: int) -> Dict:
    with db() as conn:
        row = conn.execute("SELECT pending,temp_bill,home_msg_id FROM users WHERE chat_id=?", (chat_id,)).fetchone()
        if not row:
            return {"pending": None, "temp_bill": None, "home_msg_id": None}
        return {"pending": row[0], "temp_bill": row[1], "home_msg_id": row[2]}

def set_pending(chat_id: int, value: Optional[str]):
    with db() as conn:
        exists = conn.execute("SELECT 1 FROM users WHERE chat_id=?", (chat_id,)).fetchone()
        if exists:
            conn.execute("UPDATE users SET pending=? WHERE chat_id=?", (value, chat_id))
        else:
            conn.execute("INSERT INTO users(chat_id, pending, temp_bill, home_msg_id) VALUES (?,?,NULL,NULL)", (chat_id, value))
        conn.commit()

def set_temp_bill(chat_id: int, bill: Optional[str]):
    with db() as conn:
        exists = conn.execute("SELECT 1 FROM users WHERE chat_id=?", (chat_id,)).fetchone()
        if exists:
            conn.execute("UPDATE users SET temp_bill=? WHERE chat_id=?", (bill, chat_id))
        else:
            conn.execute("INSERT INTO users(chat_id, pending, temp_bill, home_msg_id) VALUES (NULL, NULL, ?, NULL)", (bill,))
        conn.commit()

def set_home_msg_id(chat_id: int, mid: int):
    with db() as conn:
        exists = conn.execute("SELECT 1 FROM users WHERE chat_id=?", (chat_id,)).fetchone()
        if exists:
            conn.execute("UPDATE users SET home_msg_id=? WHERE chat_id=?", (mid, chat_id))
        else:
            conn.execute("INSERT INTO users(chat_id, pending, temp_bill, home_msg_id) VALUES (?,NULL,NULL,?)", (chat_id, mid))
        conn.commit()

def add_or_update_bill(chat_id: int, name: str, bill_id: str) -> Tuple[bool, str]:
    with db() as conn:
        try:
            conn.execute(
                "INSERT INTO bills(chat_id, name, bill_id) VALUES (?,?,?) "
                "ON CONFLICT(chat_id, name) DO UPDATE SET bill_id=excluded.bill_id",
                (chat_id, name, bill_id)
            )
            conn.execute("INSERT OR IGNORE INTO bill_alerts(chat_id, bill_id) VALUES (?,?)", (chat_id, bill_id))
            conn.commit()
            return True, "saved"
        except Exception as e:
            return False, str(e)

def list_bills(chat_id: int) -> List[Dict]:
    with db() as conn:
        rows = conn.execute("SELECT id, name, bill_id FROM bills WHERE chat_id=? ORDER BY id DESC", (chat_id,)).fetchall()
        return [{"id": r[0], "name": r[1], "bill_id": r[2]} for r in rows]

def get_alerts(chat_id: int, bill_id: str) -> Dict[str, int]:
    with db() as conn:
        row = conn.execute("SELECT a1h,a10m,a1201 FROM bill_alerts WHERE chat_id=? AND bill_id=?", (chat_id, bill_id)).fetchone()
        if not row: return {"a1h":0,"a10m":0,"a1201":0}
        return {"a1h": row[0], "a10m": row[1], "a1201": row[2]}

def set_alert(chat_id: int, bill_id: str, key: str, value: int):
    if key not in {"a1h","a10m","a1201"}:
        raise ValueError("invalid alert key")
    with db() as conn:
        conn.execute("INSERT OR IGNORE INTO bill_alerts(chat_id,bill_id) VALUES (?,?)", (chat_id, bill_id))
        conn.execute(f"UPDATE bill_alerts SET {key}=? WHERE chat_id=? AND bill_id=?", (value, chat_id, bill_id))
        conn.commit()

def mark_sent(chat_id: int, bill_id: str, kind: str, jdate: str, uniq: str) -> bool:
    try:
        with db() as conn:
            conn.execute("INSERT INTO sent_alerts(chat_id,bill_id,kind,jdate,uniq) VALUES (?,?,?,?,?)",
                         (chat_id, bill_id, kind, jdate, uniq))
            conn.commit()
            return True
    except Exception:
        return False

def delete_bill_and_related(chat_id: int, bill_id: str) -> bool:
    try:
        with db() as conn:
            owned = conn.execute("SELECT 1 FROM bills WHERE chat_id=? AND bill_id=?", (chat_id, bill_id)).fetchone()
            if not owned: return False
            conn.execute("DELETE FROM bill_alerts WHERE chat_id=? AND bill_id=?", (chat_id, bill_id))
            conn.execute("DELETE FROM sent_alerts WHERE chat_id=? AND bill_id=?", (chat_id, bill_id))
            conn.execute("DELETE FROM bills WHERE chat_id=? AND bill_id=?", (chat_id, bill_id))
            conn.commit()
        cache_delete_bill(bill_id); now_cache_delete_bill(bill_id)
        return True
    except Exception as e:
        logging.warning(f"delete_bill failed: {e}")
        return False

# =========================
# Caches
# =========================
_cache: Dict[Tuple[str,str], Tuple[float,List[Dict]]] = {}
_now_cache: Dict[str, Tuple[float, List[Dict]]] = {}

def cache_get(bill_id: str, jdate: str) -> Optional[List[Dict]]:
    key = (bill_id, jdate)
    entry = _cache.get(key)
    if not entry: return None
    ts, items = entry
    if datetime.datetime.now(datetime.timezone.utc).timestamp() - ts <= CACHE_TTL_SECONDS:
        return items
    return None

def cache_set(bill_id: str, jdate: str, items: List[Dict]):
    _cache[(bill_id, jdate)] = (datetime.datetime.now(datetime.timezone.utc).timestamp(), items)

def cache_delete_bill(bill_id: str):
    for k in list(_cache.keys()):
        if k[0] == bill_id:
            _cache.pop(k, None)

def cache_sweep():
    now_ts = datetime.datetime.now(datetime.timezone.utc).timestamp()
    for k,(ts,_) in list(_cache.items()):
        if now_ts - ts > CACHE_TTL_SECONDS*2:
            _cache.pop(k,None)

def now_cache_get(bill_id: str):
    entry = _now_cache.get(bill_id)
    if not entry: return None
    ts, items = entry
    if datetime.datetime.now(datetime.timezone.utc).timestamp() - ts <= NOW_CACHE_TTL_SECONDS:
        return items
    return None

def now_cache_set(bill_id: str, items: List[Dict]):
    _now_cache[bill_id] = (datetime.datetime.now(datetime.timezone.utc).timestamp(), items)

def now_cache_delete_bill(bill_id: str):
    _now_cache.pop(bill_id, None)

# =========================
# Jalaali dates
# =========================
def _jalCal(jy):
    breaks=[-61,9,38,199,426,686,756,818,1111,1181,1210,1635,2060,2097,2192,2262,2324,2394,2456,3178]
    gy=jy+621; leapJ=-14; jp=breaks[0]
    for j in range(1,len(breaks)):
        jm=breaks[j]; jump=jm-jp
        if jy<jm:
            N=jy-jp; leapJ+=(N//33)*8+((N%33)+3)//4
            if (jump%33)==4 and jump-N==4: leapJ+=1
            leapG=(gy//4)-((gy//100+1)*3//4)-150
            march=20+leapJ-leapG
            if jump-N<6: N=N-jump+((jump+4)//33)*33
            leap=(((N+1)%33)-1)%4; return leap,march
        leapJ+=(jump//33)*8+(jump%33)//4; jp=jm
    N=jy-jp; leapJ+=(N//33)*8+((N%33)+3)//4
    leapG=(gy//4)-((gy//100+1)*3//4)-150; march=20+leapJ-leapG
    leap=(((N+1)%33)-1)%4; return leap,march

def _g2d(gy,gm,gd):
    a=(14-gm)//12; y=gy+4800-a; m=gm+12*a-3
    return gd+((153*m+2)//5)+365*y+y//4-y//100+y//400-32045

def gregorian_to_jalali(gy,gm,gd):
    jy=gy-621; leap,march=_jalCal(jy); jdn1f=_g2d(gy,3,march); k=_g2d(gy,gm,gd)-jdn1f
    if k>=0:
        if k<=185: jm=1+k//31; jd=1+k%31; return jy,jm,jd
        k-=186
    else:
        jy-=1; leap,march=_jalCal(jy); jdn1f=_g2d(gy-1,3,march); k=_g2d(gy,gm,gd)-jdn1f
    jm=7+k//30; jd=1+k%30; return jy,jm,jd

def jalali_from_date(d: datetime.date) -> str:
    jy,jm,jd = gregorian_to_jalali(d.year,d.month,d.day)
    return f"{jy}/{jm:02d}/{jd:02d}"

def jalali_today() -> str:
    return jalali_from_date(datetime.datetime.now(TZ_TEHRAN).date())

def jalali_tomorrow() -> str:
    return jalali_from_date((datetime.datetime.now(TZ_TEHRAN)+datetime.timedelta(days=1)).date())

def jalali_yesterday() -> str:
    return jalali_from_date((datetime.datetime.now(TZ_TEHRAN)-datetime.timedelta(days=1)).date())

# =========================
# API calls
# =========================
def fetch_blackouts_raw(bill_id: str, from_date: str, to_date: str):
    headers={"Authorization":f"Bearer {API_JWT}","Content-Type":"application/json","Accept":"application/json"}
    payload={"bill_id":str(bill_id),"from_date":from_date,"to_date":to_date}
    try:
        r=requests.post(API_URL, headers=headers, json=payload, proxies=get_proxies(), timeout=40)
    except Exception as e:
        return None, f"Network error: {e}"
    if not r.ok:
        txt=""
        try: txt=r.text[:200]
        except Exception: pass
        return None, f"Request failed: {r.status_code} {txt}"
    try:
        return r.json(), None
    except Exception as e:
        return None, f"Invalid JSON: {e}"

def fetch_blackouts_live_raw(bill_id: str):
    headers={"Authorization":f"Bearer {API_JWT}","Content-Type":"application/json","Accept":"application/json"}
    payload={"bill_id":str(bill_id)}
    try:
        r=requests.post(API_URL_CURRENT, headers=headers, json=payload, proxies=get_proxies(), timeout=30)
    except Exception as e:
        return None, f"Network error: {e}"
    if not r.ok:
        txt=""
        try: txt=r.text[:200]
        except Exception: pass
        return None, f"Request failed: {r.status_code} {txt}"
    try:
        return r.json(), None
    except Exception as e:
        return None, f"Invalid JSON: {e}"

async def fetch_blackouts(bill_id: str, from_date: str, to_date: str):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, lambda: fetch_blackouts_raw(bill_id, from_date, to_date))

async def fetch_blackouts_live(bill_id: str):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, lambda: fetch_blackouts_live_raw(bill_id))

# =========================
# Formatting & helpers
# =========================
def _hm_to_minutes(hm: str) -> Optional[int]:
    try:
        h,m = hm.split(":"); return int(h)*60+int(m)
    except Exception: return None

def _duration_minutes_for_item(it) -> int:
    s=_hm_to_minutes(it.get("outage_start_time") or it.get("outage_time") or "")
    e=_hm_to_minutes(it.get("outage_stop_time") or "")
    if s is None or e is None: return 0
    if e>=s: return max(0, e-s)
    return (24*60 - s) + e

def _format_total_minutes(total_min: int) -> str:
    h,m=divmod(total_min,60)
    if h and m: return f"{h} ساعت و {m} دقیقه"
    if h: return f"{h} ساعت"
    return f"{m} دقیقه"

def format_blackouts(items, header_line, today_note=False):
    total_min = sum(_duration_minutes_for_item(x) for x in items)
    total_line = f"⏱ مجموع مدت خاموشی‌ها: {_format_total_minutes(total_min)}"
    if not items:
        base = f"{header_line}\n{total_line}\nهیچ خاموشیِ برنامه‌ریزی‌شده‌ای یافت نشد."
        if today_note:
            base += "\n\nℹ️ ممکن است خاموشی اتفاق افتاده باشد، اما به پایان رسیده و اکنون دیگر در لیست نیست."
        return base
    lines=[header_line, total_line, f"تعداد {len(items)} مورد:", ""]
    for x in items[:20]:
        date = x.get("outage_date") or x.get("reg_date") or ""
        start = x.get("outage_start_time") or x.get("outage_time") or ""
        end = x.get("outage_stop_time") or ""
        addr = x.get("outage_address") or x.get("address") or ""
        reason = x.get("reason_outage") or ""
        lines.append(f"• {date}  {start}–{end}\n  {addr}  ({reason})")
    if len(items)>20:
        lines.append(f"\n… و {len(items)-20} مورد دیگر.")
    return "\n".join(lines)

def filter_current_outages_cross_day(items, now_hm: str, j_today: str, j_yesterday: str):
    now_m=_hm_to_minutes(now_hm)
    if now_m is None: return []
    cur=[]
    for it in items:
        d=(it.get("outage_date") or it.get("reg_date") or "")
        if d not in (j_today,j_yesterday): continue
        s=_hm_to_minutes(it.get("outage_start_time") or it.get("outage_time") or "")
        e=_hm_to_minutes(it.get("outage_stop_time") or "")
        if s is None or e is None: continue
        if d==j_today:
            if e>=s:
                if s<=now_m<e: cur.append(it)
            else:
                if now_m>=s: cur.append(it)
        else:  # yesterday
            if e<s and now_m<e: cur.append(it)
    return cur

def now_hhmm_tehran():
    t=datetime.datetime.now(TZ_TEHRAN).time()
    return f"{t.hour:02d}:{t.minute:02d}"

# =========================
# UI (Inline keyboards)
# =========================
def main_menu():
    rows = [
        [InlineKeyboardButton("🔴 خاموشی‌های جاری", callback_data="ask:now:0")],
        [InlineKeyboardButton("⚡️ قطعی امروز", callback_data="ask:today:0"),
         InlineKeyboardButton("🌤 قطعی فردا", callback_data="ask:tomorrow:0")],
        [InlineKeyboardButton("📋 همه‌ی خاموشی‌ها", callback_data="ask:all:0")],
        [InlineKeyboardButton("🔔 مدیریت هشدارها", callback_data="alerts:0")],
        [InlineKeyboardButton("➕ افزودن قبض", callback_data="addbill")],
        [InlineKeyboardButton("🗑 حذف قبض", callback_data="delbill:0")],
    ]
    return InlineKeyboardMarkup(rows)

def back_kb():
    return InlineKeyboardMarkup([[InlineKeyboardButton("بازگشت ◀️", callback_data="home")]])

def bill_picker_keyboard(bills: List[Dict], qtype: str, page: int, per_page: int = 8):
    total=len(bills); start=page*per_page; end=min(start+per_page,total)
    rows=[]
    for b in bills[start:end]:
        rows.append([InlineKeyboardButton(f"{b['name']} • {b['bill_id']}", callback_data=f"q:{qtype}:{b['bill_id']}")])
    nav=[]
    if page>0: nav.append(InlineKeyboardButton("⬅️ قبلی", callback_data=f"ask:{qtype}:{page-1}"))
    if end<total: nav.append(InlineKeyboardButton("بعدی ➡️", callback_data=f"ask:{qtype}:{page+1}"))
    if nav: rows.append(nav)
    rows.append([InlineKeyboardButton("بازگشت ◀️", callback_data="home")])
    return InlineKeyboardMarkup(rows)

def alerts_list_keyboard(bills: List[Dict], page: int, per_page: int = 8):
    total=len(bills); start=page*per_page; end=min(start+per_page,total)
    rows=[]
    for b in bills[start:end]:
        rows.append([InlineKeyboardButton(f"🔔 {b['name']} • {b['bill_id']}", callback_data=f"alertcfg:{b['bill_id']}")])
    nav=[]
    if page>0: nav.append(InlineKeyboardButton("⬅️ قبلی", callback_data=f"alerts:{page-1}"))
    if end<total: nav.append(InlineKeyboardButton("بعدی ➡️", callback_data=f"alerts:{page+1}"))
    if nav: rows.append(nav)
    rows.append([InlineKeyboardButton("بازگشت ◀️", callback_data="home")])
    return InlineKeyboardMarkup(rows)

def delete_list_keyboard(bills: List[Dict], page: int, per_page: int = 8):
    total=len(bills); start=page*per_page; end=min(start+per_page,total)
    rows=[]
    for b in bills[start:end]:
        rows.append([InlineKeyboardButton(f"🗑 حذف {b['name']} • {b['bill_id']}", callback_data=f"delpick:{b['bill_id']}")])
    nav=[]
    if page>0: nav.append(InlineKeyboardButton("⬅️ قبلی", callback_data=f"delbill:{page-1}"))
    if end<total: nav.append(InlineKeyboardButton("بعدی ➡️", callback_data=f"delbill:{page+1}"))
    if nav: rows.append(nav)
    rows.append([InlineKeyboardButton("بازگشت ◀️", callback_data="home")])
    return InlineKeyboardMarkup(rows)

def delete_confirm_keyboard(bill_id: str):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ بله، حذف کن", callback_data=f"del:yes:{bill_id}"),
         InlineKeyboardButton("❌ نه، منصرف شدم", callback_data="del:no")],
        [InlineKeyboardButton("بازگشت ◀️", callback_data="home")]
    ])

def alert_cfg_keyboard(chat_id: int, bill_id: str):
    st = get_alerts(chat_id, bill_id)
    onoff = lambda v: "✅ روشن" if v else "❌ خاموش"
    rows = [
        [InlineKeyboardButton(f"⏱ ۱ ساعت قبل • {onoff(st['a1h'])}", callback_data=f"toggle:a1h:{bill_id}")],
        [InlineKeyboardButton(f"⏳ ۱۰ دقیقه قبل • {onoff(st['a10m'])}", callback_data=f"toggle:a10m:{bill_id}")],
        [InlineKeyboardButton(f"🕛 راس ۰۰:۰۱ • {onoff(st['a1201'])}", callback_data=f"toggle:a1201:{bill_id}")],
        [InlineKeyboardButton("بازگشت ◀️", callback_data="home")],
    ]
    return InlineKeyboardMarkup(rows)

def list_active_bills_with_alerts():
    with db() as conn:
        rows = conn.execute("""
        SELECT b.chat_id, b.name, b.bill_id, a.a1h, a.a10m, a.a1201
        FROM bills b
        JOIN bill_alerts a ON a.chat_id=b.chat_id AND a.bill_id=b.bill_id
        WHERE a.a1h=1 OR a.a10m=1 OR a.a1201=1
        """).fetchall()
    return [{"chat_id":r[0], "name":r[1], "bill_id":r[2], "a1h":r[3], "a10m":r[4], "a1201":r[5]} for r in rows]

# =========================
# Single-message helpers
# =========================
HOME_TEXT_TEMPLATE = (
    "⚡️ ربات اعلام «خاموشی‌های برنامه‌ریزی‌شده» (زمان تهران)\n\n"
    "دکمه‌ها:\n"
    "• 🔴 خاموشی‌های جاری (زنده)\n"
    "• ⚡️ قطعی امروز / 🌤 فردا / 📋 همه\n"
    "• 🔔 مدیریت هشدارها (۱ساعت قبل/۱۰دقیقه/۰۰:۰۱)\n"
    "• ➕ افزودن قبض\n"
    "• 🗑 حذف قبض\n\n"
    "قبض‌ها: {bills_line}"
)

async def ensure_home_message(update_or_context, chat_id: int) -> int:
    bot = update_or_context.bot if hasattr(update_or_context, "bot") else update_or_context.application.bot
    u = get_user_row(chat_id)
    mid = u.get("home_msg_id")
    bills = list_bills(chat_id)
    bills_line = "، ".join([b["name"] for b in bills[:6]]) + ("…" if len(bills) > 6 else "")
    if not bills: bills_line = "فعلاً هیچ قبضی ذخیره نشده است."
    text = HOME_TEXT_TEMPLATE.format(bills_line=bills_line)

    if mid:
        try:
            await bot.edit_message_text(chat_id=chat_id, message_id=mid, text=text, reply_markup=main_menu())
            return mid
        except Exception:
            pass
    msg = await bot.send_message(chat_id=chat_id, text=text, reply_markup=main_menu())
    set_home_msg_id(chat_id, msg.message_id)
    return msg.message_id

async def edit_main(update_or_context, chat_id: int, text: str, reply_markup=None):
    bot = update_or_context.bot if hasattr(update_or_context, "bot") else update_or_context.application.bot
    u = get_user_row(chat_id)
    mid = u.get("home_msg_id")
    if mid:
        try:
            await bot.edit_message_text(chat_id=chat_id, message_id=mid, text=text, reply_markup=reply_markup or back_kb())
            return
        except Exception:
            pass
    msg = await bot.send_message(chat_id=chat_id, text=text, reply_markup=reply_markup or back_kb())
    set_home_msg_id(chat_id, msg.message_id)

# =========================
# Handlers
# =========================
async def post_init(app):
    await app.bot.set_my_commands([("start","شروع / نمایش منو")])
    await app.bot.set_chat_menu_button(menu_button=MenuButtonCommands())

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    await ensure_home_message(context, chat_id)

async def on_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    chat_id = query.message.chat.id
    data = (query.data or "").strip()
    await query.answer()

    if data == "home":
        await ensure_home_message(context, chat_id)
        return

    if data == "addbill":
        set_pending(chat_id, "await_bill_id")
        set_temp_bill(chat_id, None)
        return await edit_main(context, chat_id, "شماره قبض را ارسال کنید (فقط اعداد):")  # back only

    if data.startswith("alerts:"):
        page = int(data.split(":")[1]) if ":" in data else 0
        bills = list_bills(chat_id)
        if not bills:
            return await edit_main(context, chat_id, "اول قبض اضافه کن: «➕ افزودن قبض».")
        kb = alerts_list_keyboard(bills, page)  # custom kb WITH its own back
        return await edit_main(context, chat_id, "قبضی که می‌خواهی هشدارش را تنظیم کنی انتخاب کن:", reply_markup=kb)

    if data.startswith("alertcfg:"):
        bill_id = data.split(":")[1]
        kb = alert_cfg_keyboard(chat_id, bill_id)  # custom kb WITH back
        return await edit_main(context, chat_id, f"تنظیم هشدار برای قبض {bill_id}:", reply_markup=kb)

    if data.startswith("toggle:"):
        try:
            _, key, bill_id = data.split(":")
            if key not in {"a1h","a10m","a1201"}: raise ValueError
        except Exception:
            return await edit_main(context, chat_id, "درخواست نامعتبر بود.")
        cur = get_alerts(chat_id, bill_id).get(key,0)
        set_alert(chat_id, bill_id, key, 0 if cur else 1)
        kb = alert_cfg_keyboard(chat_id, bill_id)
        return await edit_main(context, chat_id, "بروز شد.", reply_markup=kb)

    if data.startswith("ask:"):
        parts=data.split(":")
        qtype = parts[1] if len(parts)>1 else ""
        page = int(parts[2]) if len(parts)>2 and parts[2].isdigit() else 0
        if qtype not in {"now","today","tomorrow","all"}:
            return await edit_main(context, chat_id, "درخواست نامعتبر بود.")
        bills = list_bills(chat_id)
        if not bills:
            return await edit_main(context, chat_id, "اول یک قبض اضافه کن: «➕ افزودن قبض».")
        title = {
            "now":"یک قبض برای «خاموشی‌های جاری (زنده)» انتخاب کن:",
            "today":"یک قبض برای «امروز» انتخاب کن:",
            "tomorrow":"یک قبض برای «فردا» انتخاب کن:",
            "all":"یک قبض برای «همهٔ خاموشی‌ها» انتخاب کن:",
        }[qtype]
        kb = bill_picker_keyboard(bills, qtype, page)  # custom kb WITH back
        return await edit_main(context, chat_id, title, reply_markup=kb)

    if data.startswith("q:"):
        try:
            _, qtype, bill_id = data.split(":",2)
        except ValueError:
            return await edit_main(context, chat_id, "دستور نامعتبر بود.")

        if qtype == "now":
            live_items = now_cache_get(bill_id)
            if live_items is None:
                resp_live, err_live = await fetch_blackouts_live(bill_id)
                if resp_live:
                    live_items = resp_live.get("data", []) if isinstance(resp_live, dict) else []
                    now_cache_set(bill_id, live_items)
                else:
                    live_items = None
            if isinstance(live_items, list):
                j_today = jalali_today()
                msg = format_blackouts(live_items, f"🕒 خاموشی‌های جاری (زنده، {j_today})", today_note=True)
                return await edit_main(context, chat_id, msg, reply_markup=back_kb())

            j_today = jalali_today(); j_yesterday = jalali_yesterday()
            resp_t, err_t = await fetch_blackouts(bill_id, j_today, j_today)
            resp_y, err_y = await fetch_blackouts(bill_id, j_yesterday, j_yesterday)
            if not resp_t and not resp_y:
                return await edit_main(context, chat_id, f"❌ {err_t or err_y}")
            items_t = resp_t.get("data", []) if isinstance(resp_t, dict) else []
            items_y = resp_y.get("data", []) if isinstance(resp_y, dict) else []
            raw = (items_y or []) + (items_t or [])
            now_items = filter_current_outages_cross_day(raw, now_hhmm_tehran(), j_today, j_yesterday)
            msg = format_blackouts(now_items, f"🕒 خاموشی‌های جاری (پشتیبان برنامه‌ریزی‌شده، {j_today})", today_note=True)
            return await edit_main(context, chat_id, msg, reply_markup=back_kb())

        if qtype == "today":
            d = jalali_today()
            resp, err = await fetch_blackouts(bill_id, d, d)
            if not resp:
                return await edit_main(context, chat_id, f"❌ {err}")
            raw = resp.get("data", []) if isinstance(resp, dict) else []
            items = [it for it in raw if (it.get("outage_date") or it.get("reg_date") or "") == d]
            msg = format_blackouts(items, f"🗓 فقط امروز: {d}", today_note=True)
            return await edit_main(context, chat_id, msg, reply_markup=back_kb())

        if qtype == "tomorrow":
            d = jalali_tomorrow()
            resp, err = await fetch_blackouts(bill_id, d, d)
            if not resp:
                return await edit_main(context, chat_id, f"❌ {err}")
            raw = resp.get("data", []) if isinstance(resp, dict) else []
            items = [it for it in raw if (it.get("outage_date") or it.get("reg_date") or "") == d]
            msg = format_blackouts(items, f"🗓 فقط فردا: {d}")
            return await edit_main(context, chat_id, msg, reply_markup=back_kb())

        if qtype == "all":
            d = jalali_today()
            resp, err = await fetch_blackouts(bill_id, d, FAR_FUTURE_DATE)
            if not resp:
                return await edit_main(context, chat_id, f"❌ {err}")
            raw = resp.get("data", []) if isinstance(resp, dict) else []
            msg = format_blackouts(raw, f"🗓 از {d} تا {FAR_FUTURE_DATE}")
            return await edit_main(context, chat_id, msg, reply_markup=back_kb())

    if data.startswith("delbill:"):
        page = int(data.split(":")[1]) if ":" in data else 0
        bills = list_bills(chat_id)
        if not bills:
            return await edit_main(context, chat_id, "قبضی برای حذف وجود ندارد.")
        kb = delete_list_keyboard(bills, page)  # custom kb WITH back
        return await edit_main(context, chat_id, "کدام قبض حذف شود؟", reply_markup=kb)

    if data.startswith("delpick:"):
        bill_id = data.split(":")[1]
        name = next((b["name"] for b in list_bills(chat_id) if b["bill_id"]==bill_id), bill_id)
        kb = delete_confirm_keyboard(bill_id)  # custom kb WITH back
        return await edit_main(context, chat_id,
                               f"آیا مطمئنی می‌خواهی قبض «{name} • {bill_id}» را حذف کنی؟\n"
                               f"با حذف، هشدارها و سوابق مرتبط هم پاک می‌شوند.", reply_markup=kb)

    if data.startswith("del:"):
        parts = data.split(":")
        if len(parts)>=2 and parts[1]=="no":
            return await edit_main(context, chat_id, "حذف لغو شد.")
        if len(parts)>=3 and parts[1]=="yes":
            bill_id = parts[2]
            ok = delete_bill_and_related(chat_id, bill_id)
            if ok:
                return await edit_main(context, chat_id, "✅ قبض و همهٔ هشدارها و سوابق مرتبط حذف شدند.")
            else:
                return await edit_main(context, chat_id, "❌ خطا در حذف قبض یا قبضی با این شناسه یافت نشد.")

    await edit_main(context, chat_id, "دستور نامعتبر بود.")

async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    u = get_user_row(chat_id)
    pending = u["pending"]

    if pending == "await_bill_id":
        bill = (update.message.text or "").strip()
        if not bill.isdigit() or len(bill) < 6:
            return await edit_main(context, chat_id, "فرمت شماره قبض معتبر نیست. دوباره ارسال کن (فقط اعداد).")
        set_temp_bill(chat_id, bill)
        set_pending(chat_id, "await_bill_name")
        return await edit_main(context, chat_id, "نام دلخواه برای این قبض را ارسال کن (مثلاً «خانه»، «دفتر»):")

    if pending == "await_bill_name":
        name = (update.message.text or "").strip()
        if not name:
            return await edit_main(context, chat_id, "نام نمی‌تواند خالی باشد. یک نام کوتاه و قابل تشخیص وارد کن.")
        temp_bill = get_user_row(chat_id)["temp_bill"]
        if not temp_bill:
            set_pending(chat_id, None)
            return await edit_main(context, chat_id, "اشکال موقت در افزودن قبض. دوباره «➕ افزودن قبض» را بزن.")
        ok, msg = add_or_update_bill(chat_id, name, temp_bill)
        set_pending(chat_id, None); set_temp_bill(chat_id, None)
        if ok:
            return await edit_main(context, chat_id, f"✅ قبض «{name}» با شماره {temp_bill} ذخیره شد.")
        else:
            return await edit_main(context, chat_id, f"❌ خطا در ذخیره قبض: {msg}")

    # otherwise go Home
    await ensure_home_message(context, chat_id)

# =========================
# Scheduler jobs
# =========================
def format_digest(items, bill_name, jdate):
    return format_blackouts(items, f"🕛 خلاصهٔ خاموشی‌های امروز ({bill_name}) - {jdate}")

async def daily_digest_job(context: ContextTypes.DEFAULT_TYPE):
    bot = context.bot
    j_today = jalali_today()
    active = list_active_bills_with_alerts()
    if not active: return
    for b in active:
        if not b["a1201"]:  # only users who enabled 00:01 digest
            continue

        items = cache_get(b["bill_id"], j_today)
        if items is None:
            resp, err = await fetch_blackouts(b["bill_id"], j_today, j_today)
            if not resp:
                continue
            raw = resp.get("data", []) if isinstance(resp, dict) else []
            # ✅ STRICT filter to only today's items (API may return future rows):
            items = [it for it in raw if (it.get("outage_date") or it.get("reg_date") or "") == j_today]
            cache_set(b["bill_id"], j_today, items)

        if mark_sent(b["chat_id"], b["bill_id"], "1201", j_today, "digest"):
            try:
                await bot.send_message(
                    chat_id=b["chat_id"],
                    text=format_digest(items, b["name"], j_today)
                )
            except Exception as e:
                logging.warning(f"digest send failed: {e}")
    cache_sweep()

async def cleanup_old_alerts(context: ContextTypes.DEFAULT_TYPE):
    cutoff_j = jalali_from_date((datetime.datetime.now(TZ_TEHRAN) - datetime.timedelta(days=SENT_ALERTS_RETENTION_DAYS)).date())
    with db() as conn:
        try:
            conn.execute("DELETE FROM sent_alerts WHERE jdate < ?", (cutoff_j,)); conn.commit()
        except Exception as e:
            logging.warning(f"cleanup_old_alerts failed: {e}")

async def alerts_tick(context: ContextTypes.DEFAULT_TYPE):
    bot=context.bot
    now_dt = datetime.datetime.now(TZ_TEHRAN)
    now_hm = f"{now_dt.hour:02d}:{now_dt.minute:02d}"
    now_m = _hm_to_minutes(now_hm) or 0
    j_today = jalali_today()
    j_tomorrow = jalali_tomorrow()

    active = list_active_bills_with_alerts()
    if not active: return

    in_window = lambda target, now, w=ALERT_WINDOW_MINUTES: target <= now < target + w

    for b in active:
        want_1h = bool(b["a1h"]); want_10m = bool(b["a10m"])
        if not (want_1h or want_10m): continue

        items_today = cache_get(b["bill_id"], j_today)
        if items_today is None:
            resp, err = await fetch_blackouts(b["bill_id"], j_today, j_today)
            if not resp: continue
            raw = resp.get("data", []) if isinstance(resp, dict) else []
            # ✅ فقط امروز:
            items_today = [it for it in raw if (it.get("outage_date") or it.get("reg_date") or "") == j_today]
            cache_set(b["bill_id"], j_today, items_today)

        for it in items_today:
            item_date = (it.get("outage_date") or it.get("reg_date") or "")
            if item_date != j_today:
                continue
            uniq = str(it.get("outage_number") or f"{item_date}-{it.get('outage_start_time')}-{it.get('outage_stop_time')}-{it.get('outage_address')}")
            s_hm = it.get("outage_start_time") or it.get("outage_time") or ""
            e_hm = it.get("outage_stop_time") or ""
            s = _hm_to_minutes(s_hm)
            if s is None: continue

            if want_1h and s - 60 >= 0 and in_window(s-60, now_m):
                if mark_sent(b["chat_id"], b["bill_id"], "1h", item_date, uniq):
                    try:
                        await bot.send_message(chat_id=b["chat_id"],
                            text=(f"⏱ یادآوری ۱ ساعت قبل ({b['name']})\n"
                                  f"امروز {item_date}، {s_hm}–{e_hm}\n"
                                  f"{it.get('outage_address') or it.get('address') or ''}"))
                    except Exception as e:
                        logging.warning(f"send 1h failed: {e}")

            if want_10m and s - 10 >= 0 and in_window(s-10, now_m):
                if mark_sent(b["chat_id"], b["bill_id"], "10m", item_date, uniq):
                    try:
                        await bot.send_message(chat_id=b["chat_id"],
                            text=(f"⏳ یادآوری ۱۰ دقیقه قبل ({b['name']})\n"
                                  f"امروز {item_date}، {s_hm}–{e_hm}\n"
                                  f"{it.get('outage_address') or it.get('address') or ''}"))
                    except Exception as e:
                        logging.warning(f"send 10m failed: {e}")

        # tomorrow near midnight (prev-day reminders)
        if want_1h or want_10m:
            items_tom = cache_get(b["bill_id"], j_tomorrow)
            if items_tom is None:
                resp, err = await fetch_blackouts(b["bill_id"], j_tomorrow, j_tomorrow)
                raw_tom = (resp.get("data", []) if isinstance(resp, dict) else []) if resp else []
                # ✅ فقط فردا:
                items_tom = [it for it in raw_tom if (it.get("outage_date") or it.get("reg_date") or "") == j_tomorrow]
                cache_set(b["bill_id"], j_tomorrow, items_tom)

            for it in items_tom:
                item_date = (it.get("outage_date") or it.get("reg_date") or "")
                if item_date != j_tomorrow: continue
                uniq = str(it.get("outage_number") or f"{item_date}-{it.get('outage_start_time')}-{it.get('outage_stop_time')}-{it.get('outage_address')}")
                s_hm = it.get("outage_start_time") or it.get("outage_time") or ""
                e_hm = it.get("outage_stop_time") or ""
                s = _hm_to_minutes(s_hm)
                if s is None: continue

                if want_1h and s < 60:
                    t1_prev = 24*60 - (60 - s)
                    if in_window(t1_prev, now_m):
                        if mark_sent(b["chat_id"], b["bill_id"], "1h", item_date, uniq):
                            try:
                                await bot.send_message(chat_id=b["chat_id"],
                                    text=(f"⏱ یادآوری ۱ ساعت قبل ({b['name']})\n"
                                          f"فردا {item_date}، {s_hm}–{e_hm}\n"
                                          f"{it.get('outage_address') or it.get('address') or ''}"))
                            except Exception as e:
                                logging.warning(f"send 1h prev-day failed: {e}")

                if want_10m and s < 10:
                    t10_prev = 24*60 - (10 - s)
                    if in_window(t10_prev, now_m):
                        if mark_sent(b["chat_id"], b["bill_id"], "10m", item_date, uniq):
                            try:
                                await bot.send_message(chat_id=b["chat_id"],
                                    text=(f"⏳ یادآوری ۱۰ دقیقه قبل ({b['name']})\n"
                                          f"فردا {item_date}، {s_hm}–{e_hm}\n"
                                          f"{it.get('outage_address') or it.get('address') or ''}"))
                            except Exception as e:
                                logging.warning(f"send 10m prev-day failed: {e}")

    cache_sweep()

# =========================
# Boot
# =========================
def run():
    application = ApplicationBuilder().token(TELEGRAM_TOKEN).post_init(post_init).build()
    application.add_handler(CommandHandler("start", cmd_start))
    application.add_handler(CallbackQueryHandler(on_button))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))

    application.job_queue.run_repeating(alerts_tick, interval=60, first=0, name="alerts_tick")
    application.job_queue.run_daily(daily_digest_job, time=datetime.time(0,1,tzinfo=TZ_TEHRAN), name="daily_digest_job")
    application.job_queue.run_daily(cleanup_old_alerts, time=datetime.time(3,0,tzinfo=TZ_TEHRAN), name="cleanup_old_alerts")

    application.run_polling(allowed_updates=["message","callback_query"])

if __name__ == "__main__":
    DB_PATH.touch(exist_ok=True)
    run()
