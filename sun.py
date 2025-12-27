from aiogram.types import FSInputFile, Message
from aiogram.enums import ParseMode
import asyncio
import html
import os
import random
import sqlite3
import subprocess
import threading
import time
import psutil
import json
import aiohttp
from datetime import datetime, timedelta
from functools import wraps
import pytz
from aiogram import Bot, Dispatcher, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command
from aiogram.types import Message, InlineKeyboardButton, InlineKeyboardMarkup
import logging
from apscheduler.schedulers.asyncio import AsyncIOScheduler
BASE_DIR = "/root/denvkl"
REMOTE_VPS_URL = "http://103.178.235.219:5000"
API_SECRET_KEY = os.getenv("API_SECRET_KEY", "bot_secret_key_12345")
SCRIPT_VIP_DIRECT = [
    "vip_0.py",
    "vip_1.py",
    "vip_2.py",
    "vip_3.py",
    "vip_4.py",
    "vip_5.py",
    "vip_6.py",
    "vip_7.py",
    "vip_8.py",
    "vip_9.py",
    "vip_10.py",
    "vip_11.py",
    "vip_12.py",
    "vip_13.py",
    "vip_14.py",
    "vip_15.py",
    "vip_16.py",
    "vip_17.py",
    "vip_18.py",
    "vip_19.py",
    "vip_20.py",
    "vip_21.py",
    "vip_22.py",
    "vip_23.py",
    "vip_24.py",
    "vip_25.py",
    "vip_26.py",
]
SCRIPT_SMS_DIRECT = [
    "sms_1.py",
    "sms_2.py",
    "sms_3.py",
    "sms_4.py",
    "sms_5.py",
    "sms_6.py",
    "sms_7.py",
    "sms_8.py",
    "sms_9.py",
]
SCRIPT_CALL_DIRECT_LOCAL = ["spam_11.py", "callcall_0.py", "smscall_9.py"]
SCRIPT_SPAM_DIRECT_LOCAL = [
    "smscall_9.py",
    "lenhsieuvip.py",
    "free.py",
    "lenhsieuvip1.py",
]
SCRIPT_FREE_LOCAL = ["test.py", "test1.py", "smscall_3.py", "smsvip.py"]
SCRIPT_CALL_DIRECT_REMOTE = [
    "lenhcall1.py",
    "lenhcall.py",
    "lenhmoi1.py",
    "lenhmoi5.py",
]
SCRIPT_SPAM_DIRECT_REMOTE = ["lenhmoi5.py", "lenhmoi.py", "lenhspam1.py", "07.py"]
SCRIPT_FREE_REMOTE = ["08.py", "06.py"]
SCRIPT_OKI_DIRECT = ["oki.py"]
SCRIPT_CALL_DIRECT = SCRIPT_CALL_DIRECT_LOCAL + SCRIPT_CALL_DIRECT_REMOTE
SCRIPT_SPAM_DIRECT = SCRIPT_SPAM_DIRECT_LOCAL + SCRIPT_SPAM_DIRECT_REMOTE
SCRIPT_FREE = SCRIPT_FREE_LOCAL + SCRIPT_FREE_REMOTE
SCRIPT_VIP_DIRECT_REMOTE = []
SCRIPT_SMS_DIRECT_REMOTE = []
TIMEOUT_MAP = {
    "full": 1200,  # 20 phút
    "vip": 180,  # 3 phút
    "sms": 180,  # 3 phút
    "spam": 300,  # 5 phút
    "call": 300,  # 5 phút
    "free": 100,  # ~1.6 phút
    "oki": 23000,  # 6.5 giờ (23:30 → 6:00)
    "tiktok": 3600,  # 60 phút
    "ngl": 3600,  # 60 phút
}
MA_TOKEN_BOT = os.getenv("BOT_TOKEN", "7945237130:AAFumjQuPd1k_J8VkFz0DsuLpvCW5iB446g")
ID_ADMIN_MAC_DINH = "5365031415"
TEN_ADMIN_MAC_DINH = "Super Admin"
NHOM_CHO_PHEP = [-1002610450062, -1003225919295]
THU_MUC_DU_LIEU = "./data"
os.makedirs(THU_MUC_DU_LIEU, exist_ok=True)
logging.basicConfig(level=logging.CRITICAL, handlers=[])
logger = logging.getLogger(__name__)
logger.disabled = True
DUONG_DAN_DB = os.path.join(THU_MUC_DU_LIEU, "bot_data.db")
USER_PROCESSES = {}
PROCESS_LOCK = threading.Lock()
BOT_USERNAME = None
bot = Bot(token=MA_TOKEN_BOT, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
DB_LOCK = threading.Lock()
OKI_RUNNING = False
OKI_LOCK = threading.Lock()
def tao_ket_noi_db():
    with DB_LOCK:
        conn = sqlite3.connect(DUONG_DAN_DB, timeout=5.0, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        return conn
class OptimizedCache:
    def __init__(self, max_size=100, ttl=300):
        self.cache = {}
        self.timestamps = {}
        self.max_size = max_size
        self.ttl = ttl
    def get(self, key):
        if key in self.cache:
            if time.time() - self.timestamps[key] < self.ttl:
                return self.cache[key]
            else:
                self.cache.pop(key, None)
                self.timestamps.pop(key, None)
        return None
    def set(self, key, value):
        current_time = time.time()
        expired_keys = [
            k for k, t in self.timestamps.items() if current_time - t >= self.ttl
        ]
        for k in expired_keys:
            self.cache.pop(k, None)
            self.timestamps.pop(k, None)
        if len(self.cache) >= self.max_size:
            oldest_keys = sorted(self.timestamps.items(), key=lambda x: x[1])[
                : self.max_size // 3
            ]
            for k, _ in oldest_keys:
                self.cache.pop(k, None)
                self.timestamps.pop(k, None)
        self.cache[key] = value
        self.timestamps[key] = current_time
quyen_cache = OptimizedCache(max_size=50, ttl=600)
cooldown_cache = OptimizedCache(max_size=100, ttl=1200)
COMMAND_COOLDOWNS = {
    "admin": {"default": 0},
    "vip": {
        "sms": 180,
        "call": 180,
        "vip": 240,
        "full": 600,
        "spam": 180,
        "free": 50,
        "img": 90,
        "vid": 90,
        "ngl": 90,
        "tiktok": 1000,
        "default": 1200,
    },
    "super_vip": {
        "sms": 120,
        "call": 120,
        "vip": 240,
        "full": 600,
        "spam": 120,
        "free": 50,
        "img": 90,
        "vid": 90,
        "ngl": 90,
        "tiktok": 100,
        "default": 1000,
    },
    "member": {
        "sms": 180,
        "spam": 200,
        "free": 50,
        "img": 90,
        "vid": 90,
        "ngl": 200,
        "default": 1200,
    },
}
def script_should_run_remote(script_name: str, command_type: str) -> bool:
    if not REMOTE_VPS_URL:
        return False
    remote_lists = {
        "vip": SCRIPT_VIP_DIRECT_REMOTE,
        "sms": SCRIPT_SMS_DIRECT_REMOTE,
        "call": SCRIPT_CALL_DIRECT_REMOTE,
        "spam": SCRIPT_SPAM_DIRECT_REMOTE,
        "free": SCRIPT_FREE_REMOTE,
    }
    remote_scripts = remote_lists.get(command_type, [])
    return script_name in remote_scripts
async def goi_script_vps_khac(command_type: str, phone_numbers: list, user_id: int, script_name: str, **kwargs
):
    if not REMOTE_VPS_URL:
        return False, {"error": "VPS khác chưa được cấu hình"}
    try:
        payload = {
            "command_type": command_type,
            "phone_numbers": phone_numbers,
            "user_id": str(user_id),
            "script_name": script_name,
            "rounds": kwargs.get("rounds"),
        }
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=10)
        ) as session:
            async with session.post(
                f"{REMOTE_VPS_URL}/execute",
                json=payload,
                headers={"Authorization": f"Bearer {API_SECRET_KEY}"},
            ) as resp:
                if resp.status == 200:
                    result = await resp.json()
                    return result.get("success", False), result.get("data", {})
                else:
                    return False, {"error": f"VPS trả về lỗi: {resp.status}"}
    except asyncio.TimeoutError:
        return False, {"error": "Timeout kết nối VPS"}
    except Exception as e:
        pass
def get_carrier(phone):
    if not phone:
        return "Không xác định"
    phone = str(phone).strip()
    if phone.startswith("+84"):
        phone = "0" + phone[3:]
    elif phone.startswith("84"):
        phone = "0" + phone[2:]
    if len(phone) < 3:
        return "Không xác định"
    prefix = phone[:3]
    viettel = {
        "086",
        "096",
        "097",
        "098",
        "032",
        "033",
        "034",
        "035",
        "036",
        "037",
        "038",
        "039",
    }
    mobifone = {"089", "090", "093", "070", "079", "077", "076", "078"}
    vinaphone = {"088", "091", "094", "083", "084", "085", "081", "082"}
    vietnamobile = {"092", "056", "058"}
    gmobile = {"099", "059"}
    if prefix in viettel:
        return "𝑉𝑖𝑒𝑡𝑡𝑒𝑙"
    elif prefix in mobifone:
        return "𝑀𝑜𝑏𝑖𝑓𝑜𝑛𝑒"
    elif prefix in vinaphone:
        return "𝑉𝑖𝑛𝑎𝑝ℎ𝑜𝑛𝑒"
    elif prefix in vietnamobile:
        return "𝑉𝑖𝑒𝑡𝑛𝑎𝑚𝑜𝑏𝑖𝑙𝑒"
    elif prefix in gmobile:
        return "𝐺𝑚𝑜𝑏𝑖𝑙𝑒"
    return "𝐾ℎ𝑜̂𝑛𝑔 𝑥𝑎́𝑐 𝑑𝑖̣𝑛ℎ"
async def execute_with_swap(command_type: str, phone_numbers: list, user_id: int, **kwargs
):
    phone_str = " ".join(phone_numbers)
    if command_type == "full":
        script_name = "pro24h.py"
        script_path = os.path.join(BASE_DIR, script_name)
        cmd = f"python3 {script_path} {phone_str}"
        success, pid = chay_script_don_gian(cmd, user_id, command_type=command_type)
        return success, {
            "script": script_name,
            "pid": pid,
        }
    elif command_type == "vip":
        success_count = 0
        pids = []
        rounds = kwargs.get("rounds", 3)
        for phone in phone_numbers:
            script_name = random.choice(SCRIPT_VIP_DIRECT)
            # Kiểm tra script này có chạy remote không
            if script_should_run_remote(script_name, command_type):
                success, result = await goi_script_vps_khac(
                    command_type, [phone], user_id, script_name, **kwargs
                )
                if success:
                    success_count += 1
                    if "pids" in result:
                        pids.extend(result["pids"])
            else:
                script_path = os.path.join(BASE_DIR, script_name)
                cmd = f"python3 {script_path} {phone} {rounds}"
                success, pid = chay_script_don_gian(
                    cmd, user_id, command_type=command_type
                )
                if success:
                    success_count += 1
                    pids.append(pid)
        return (success_count > 0), {
            "scripts": f"{success_count} scripts",
            "pids": pids,
        }
    script_mapping = {
        "sms": SCRIPT_SMS_DIRECT,
        "call": SCRIPT_CALL_DIRECT,
        "spam": SCRIPT_SPAM_DIRECT,
        "free": SCRIPT_FREE,
    }
    scripts = script_mapping.get(command_type)
    if not scripts:
        return False, {"error": "Không có script"}
    script_name = random.choice(scripts)
    if script_should_run_remote(script_name, command_type):
        return await goi_script_vps_khac(
            command_type, phone_numbers, user_id, script_name, **kwargs
        )
    script_path = os.path.join(BASE_DIR, script_name)
    rounds = kwargs.get("rounds")
    if command_type == "free":
        round_value = 2
    elif command_type in ["call", "spam"]:
        round_value = 2
    else:
        round_value = None
    if round_value is not None:
        cmd = f"python3 {script_path} {phone_str} {round_value}"
    else:
        cmd = f"python3 {script_path} {phone_str}"
    success, pid = chay_script_don_gian(cmd, user_id, command_type=command_type)
    return success, {
        "script": script_name,
        "pid": pid,
    }
async def kiem_tra_vip_het_han():
    while True:
        try:
            await asyncio.sleep(3600)  # Mỗi 1 giờ
            conn = tao_ket_noi_db()
            cursor = conn.cursor()
            cursor.execute(
                "SELECT user_id, name, expiry_date, admin_added_by FROM admin WHERE role = 'vip' AND expiry_date IS NOT NULL"
            )
            vip_users = cursor.fetchall()
            for user in vip_users:
                user_id = user["user_id"]
                expiry_date = user["expiry_date"]
                admin_added_by = user["admin_added_by"]
                try:
                    expiry = datetime.fromisoformat(expiry_date)
                    if datetime.now() > expiry:
                        if admin_added_by == "referral_system":
                            cursor.execute(
                                "SELECT COUNT(*) FROM referrals WHERE referrer_id = ?",
                                (user_id,),
                            )
                            ref_count = cursor.fetchone()[0]
                            if ref_count >= 10:
                                new_expiry = (
                                    datetime.now() + timedelta(days=30)
                                ).isoformat()
                                cursor.execute(
                                    "UPDATE admin SET expiry_date = ? WHERE user_id = ?",
                                    (new_expiry, user_id),
                                )
                                cursor.execute(
                                    "DELETE FROM referrals WHERE referrer_id = ?",
                                    (user_id,),
                                )
                                conn.commit()
                                continue
                        cursor.execute(
                            "DELETE FROM admin WHERE user_id = ?", (user_id,)
                        )
                        conn.commit()
                        quyen_cache.set(user_id, None)
                except:
                    continue
            conn.close()
        except Exception:
            continue
async def kiem_tra_super_vip_het_han():
    while True:
        try:
            await asyncio.sleep(3600)  # Mỗi 1 giờ
            conn = tao_ket_noi_db()
            cursor = conn.cursor()
            cursor.execute(
                "SELECT user_id, name, expiry_date FROM admin WHERE role = 'super_vip' AND expiry_date IS NOT NULL"
            )
            super_vip_users = cursor.fetchall()
            for user in super_vip_users:
                user_id = user["user_id"]
                expiry_date = user["expiry_date"]
                try:
                    expiry = datetime.fromisoformat(expiry_date)
                    if datetime.now() > expiry:
                        cursor.execute(
                            "DELETE FROM admin WHERE user_id = ?", (user_id,)
                        )
                        cursor.execute(
                            "DELETE FROM super_vip_phones WHERE user_id = ?", (user_id,)
                        )
                        conn.commit()
                        quyen_cache.set(user_id, None)
                        await thong_bao_het_han_super_vip(user_id)
                except:
                    continue
            conn.close()
        except Exception:
            # Bỏ logging lỗi để tăng hiệu suất
            continue
async def thong_bao_het_han_vip(user_id):
    try:
        for group_id in NHOM_CHO_PHEP:
            try:
                await bot.send_message(
                    chat_id=group_id,
                    text=f"<blockquote>⏰ 𝑇ℎ𝑜̂𝑛𝑔 𝐵𝑎́𝑜 𝐻𝑒̂́𝑡 𝐻𝑎̣𝑛 𝑉𝐼𝑃 !\n\n"
                    f"🆔 𝑈𝑠𝑒𝑟 𝐼𝐷       :        {user_id}\n"
                    f"📅 𝐷𝑎̃ ℎ𝑒̂́𝑡 ℎ𝑎̣𝑛 𝑣𝑎̀𝑜   :    {datetime.now().strftime('%d/%m/%Y %H:%M')}\n\n"
                    f"🎯 𝐿𝑖𝑒̂𝑛 ℎ𝑒̣̂ 𝐴𝑑𝑚𝑖𝑛 𝑑𝑒̂̉ 𝑔𝑖𝑎 ℎ𝑎̣𝑛 𝑉𝐼𝑃 !</blockquote>",
                    parse_mode=ParseMode.HTML,
                )
                break
            except:
                continue
    except Exception:
        pass
async def thong_bao_het_han_super_vip(user_id):
    try:
        for group_id in NHOM_CHO_PHEP:
            try:
                await bot.send_message(
                    chat_id=group_id,
                    text=f"<blockquote>⏰ 𝑇ℎ𝑜̂𝑛𝑔 𝐵𝑎́𝑜 𝐻𝑒̂́𝑡 𝐻𝑎̣𝑛 𝑆𝑈𝑃𝐸𝑅 𝑉𝐼𝑃 !\n\n"
                    f"🆔 𝑈𝑠𝑒𝑟 𝐼𝐷       :        {user_id}\n"
                    f"📅 𝐷𝑎̃ ℎ𝑒̂́𝑡 ℎ𝑎̣𝑛 𝑣𝑎̀𝑜   :    {datetime.now().strftime('%d/%m/%Y %H:%M')}\n\n"
                    f"🎯 𝐿𝑖𝑒̂𝑛 ℎ𝑒̣̂ 𝐴𝑑𝑚𝑖𝑛 𝑑𝑒̂̉ 𝑔𝑖𝑎 ℎ𝑎̣𝑛 𝑆𝑈𝑃𝐸𝑅 𝑉𝐼𝑃 !</blockquote>",
                    parse_mode=ParseMode.HTML,
                )
                break
            except:
                continue
    except Exception:
        pass
USER_PROCESSES = {}
PROCESS_LOCK = threading.Lock()
def chay_script_don_gian(command, user_id=None, timeout=3600, command_type=None):
    try:
        if not command or not user_id:
            return False, None
        if command_type:
            timeout = TIMEOUT_MAP.get(command_type, timeout)
        with PROCESS_LOCK:
            user_procs = USER_PROCESSES.get(user_id, [])
            alive_procs = []
            for p in user_procs:
                if p.poll() is None:
                    alive_procs.append(p)
                else:
                    try:
                        p.terminate()
                        p.wait(timeout=1)
                    except:
                        try:
                            p.kill()
                        except:
                            pass
            USER_PROCESSES[user_id] = alive_procs
            if len(alive_procs) >= 10:
                return False, None
        process = subprocess.Popen(
            command,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            shell=True,
            start_new_session=True,
            cwd=BASE_DIR,
        )
        with PROCESS_LOCK:
            if user_id not in USER_PROCESSES:
                USER_PROCESSES[user_id] = []
            USER_PROCESSES[user_id].append(process)
        def kill_after_timeout():
            time.sleep(timeout)
            try:
                if process.poll() is None:
                    process.terminate()
                    time.sleep(3)
                    if process.poll() is None:
                        process.kill()
            except:
                pass
        timer_thread = threading.Thread(target=kill_after_timeout, daemon=True)
        timer_thread.start()
        return True, process.pid
    except Exception:
        return False, None
def cleanup_dead_processes():
    try:
        with PROCESS_LOCK:
            for user_id in list(USER_PROCESSES.keys()):
                alive_procs = []
                for p in USER_PROCESSES[user_id]:
                    if p.poll() is None:
                        alive_procs.append(p)
                    else:
                        try:
                            p.terminate()
                            p.wait(timeout=1)
                        except:
                            pass
                if alive_procs:
                    USER_PROCESSES[user_id] = alive_procs
                else:
                    USER_PROCESSES.pop(user_id, None)
        current_time = time.time()
        killed = 0
        all_scripts = set(
            SCRIPT_VIP_DIRECT
            + SCRIPT_SMS_DIRECT
            + SCRIPT_CALL_DIRECT
            + SCRIPT_SPAM_DIRECT
            + SCRIPT_FREE
            + ["tcp.py", "tt.py", "spamngl.py", "pro24h.py"]
        )
        for proc in psutil.process_iter(
            ["pid", "name", "cmdline", "create_time", "status", "cpu_percent"]
        ):
            try:
                if "python" not in proc.info["name"].lower():
                    continue
                cmdline = proc.info.get("cmdline", [])
                if len(cmdline) < 2:
                    continue
                script_name = os.path.basename(cmdline[1]) if cmdline[1] else ""
                if script_name not in all_scripts:
                    continue
                age_minutes = (current_time - proc.info.get("create_time", 0)) / 60
                process_status = proc.info.get("status", "")
                should_kill = False
                if script_name == "pro24h.py":  # Lệnh /full
                    if age_minutes > 60:  # Kill sau 60 phút (theo yêu cầu)
                        should_kill = True
                else:
                    if age_minutes > 5:  # Các lệnh khác kill sau 5 phút
                        should_kill = True
                if process_status in [psutil.STATUS_ZOMBIE, psutil.STATUS_STOPPED]:
                    should_kill = True
                elif age_minutes > 30:
                    try:
                        cpu_percent = proc.cpu_percent(interval=1)
                        if cpu_percent == 0.0:  # Không sử dụng CPU
                            should_kill = True
                    except:
                        should_kill = True  # Nếu không đọc được CPU thì cũng kill
                if should_kill:
                    try:
                        proc.terminate()
                        try:
                            proc.wait(timeout=3)
                        except psutil.TimeoutExpired:
                            proc.kill()
                            proc.wait(timeout=2)
                        killed += 1
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        pass
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.TimeoutExpired):
                continue
        if killed > 0:
            pass
    except Exception:
        pass
def force_kill_zombies():
    try:
        killed_count = 0
        all_scripts = set(
            SCRIPT_VIP_DIRECT
            + SCRIPT_SMS_DIRECT
            + SCRIPT_CALL_DIRECT
            + SCRIPT_SPAM_DIRECT
            + SCRIPT_FREE
            + ["tcp.py", "tt.py", "spamngl.py", "pro24h.py"]
        )
        for proc in psutil.process_iter(
            ["pid", "name", "cmdline", "create_time", "status"]
        ):
            try:
                if "python" not in proc.info["name"].lower():
                    continue
                cmdline = proc.info.get("cmdline", [])
                if len(cmdline) < 2:
                    continue
                script_name = os.path.basename(cmdline[1]) if cmdline[1] else ""
                if script_name not in all_scripts:
                    continue
                age_minutes = (time.time() - proc.info.get("create_time", 0)) / 60
                process_status = proc.info.get("status", "")
                if (
                    process_status in [psutil.STATUS_ZOMBIE, psutil.STATUS_STOPPED]
                    or age_minutes > 30
                ):
                    try:
                        os.kill(proc.pid, 9)  # SIGKILL
                        killed_count += 1
                    except (psutil.NoSuchProcess, psutil.AccessDenied, OSError):
                        pass
            except:
                continue
    except Exception:
        pass
async def schedule_cleanup():
    while True:
        try:
            await asyncio.sleep(120)  # 2 phút như VPS2
            cleanup_dead_processes()
            force_kill_zombies()
        except Exception:
            continue
def khoi_tao_database():
    try:
        conn = tao_ket_noi_db()
        cursor = conn.cursor()
        cursor.execute(
            """
                                                CREATE TABLE IF NOT EXISTS admin (
                                                                user_id TEXT PRIMARY KEY,
                                                                name TEXT,
                                                                role TEXT,
                                                                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                                                expiry_date TIMESTAMP,
                                                                admin_added_by TEXT
                                                )
                                """
        )
        try:
            cursor.execute("ALTER TABLE admin ADD COLUMN expiry_date TIMESTAMP")
        except sqlite3.OperationalError:
            pass
        try:
            cursor.execute("ALTER TABLE admin ADD COLUMN admin_added_by TEXT")
        except sqlite3.OperationalError:
            pass
        cursor.execute(
            """
                                                CREATE TABLE IF NOT EXISTS referrals (
                                                                id INTEGER PRIMARY KEY AUTOINCREMENT,
                                                                referrer_id TEXT NOT NULL,
                                                                referred_id TEXT NOT NULL,
                                                                joined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                                                UNIQUE(referrer_id, referred_id)
                                                )
                                """
        )
        cursor.execute(
            """
                                                CREATE TABLE IF NOT EXISTS super_vip_phones (
                                                                id INTEGER PRIMARY KEY AUTOINCREMENT,
                                                                user_id TEXT NOT NULL,
                                                                phone_number TEXT NOT NULL,
                                                                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                                                UNIQUE(user_id, phone_number)
                                                )
                                """
        )
        conn.commit()
        conn.close()
    except Exception:
        pass
def khoi_tao_admin_mac_dinh():
    try:
        conn = tao_ket_noi_db()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT user_id FROM admin WHERE user_id = ?", (ID_ADMIN_MAC_DINH,)
        )
        if not cursor.fetchone():
            cursor.execute(
                "INSERT INTO admin (user_id, name, role) VALUES (?, ?, ?)",
                (ID_ADMIN_MAC_DINH, TEN_ADMIN_MAC_DINH, "admin"),
            )
            conn.commit()
        conn.close()
    except Exception:
        pass
def doc_file_js(ten_file):
    try:
        if not os.path.exists(ten_file):
            return []
        with open(ten_file, "r", encoding="utf-8") as file:
            noi_dung = file.read()
        import re
        pattern = r"\[([^\]]+)\]"
        match = re.search(pattern, noi_dung, re.DOTALL)
        if match:
            array_content = match.group(1)
            urls = []
            for line in array_content.split("\n"):
                line = line.strip()
                if line.startswith('"') and line.endswith('",'):
                    url = line[1:-2]
                    urls.append(url)
                elif line.startswith('"') and line.endswith('"'):
                    url = line[1:-1]
                    urls.append(url)
            return urls
        return []
    except Exception:
        return []
def lay_cap_do_quyen_nguoi_dung(user_id):
    user_id = str(user_id)
    if user_id == ID_ADMIN_MAC_DINH:
        return "admin"
    cached = quyen_cache.get(user_id)
    if cached:
        return cached
    try:
        conn = tao_ket_noi_db()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT role, expiry_date FROM admin WHERE user_id = ? LIMIT 1", (user_id,)
        )
        result = cursor.fetchone()
        if result:
            role = result["role"]
            expiry_date = result["expiry_date"]
            if role in ("vip", "super_vip") and expiry_date:
                try:
                    expiry = datetime.fromisoformat(expiry_date)
                    if datetime.now() > expiry:
                        cursor.execute(
                            "DELETE FROM admin WHERE user_id = ?", (user_id,)
                        )
                        if role == "super_vip":
                            cursor.execute(
                                "DELETE FROM super_vip_phones WHERE user_id = ?", (user_id,)
                            )
                        conn.commit()
                        conn.close()
                        quyen_cache.set(user_id, "member")
                        asyncio.create_task(thong_bao_het_han_vip(user_id))
                        return "member"
                except:
                    pass
            conn.close()
            quyen_cache.set(user_id, role)
            return role
        conn.close()
        quyen_cache.set(user_id, "member")
        return "member"
    except Exception:
        return "member"
def la_admin(user_id):
    return lay_cap_do_quyen_nguoi_dung(user_id) == "admin"
def la_vip_vinh_vien(user_id):
    cap_do = lay_cap_do_quyen_nguoi_dung(user_id)
    return cap_do in ("admin", "vip", "super_vip")
def la_so_dien_thoai_hop_le(so_dien_thoai):
    if not so_dien_thoai or not so_dien_thoai.isdigit():
        return False
    if len(so_dien_thoai) not in [10, 11]:
        return False
    if len(so_dien_thoai) == 10 and so_dien_thoai[0] == "0":
        return True
    if len(so_dien_thoai) == 11 and so_dien_thoai[:2] == "84":
        return True
    return False
def check_cooldown(user_id, command):
    if la_admin(user_id):
        return False, 0
    key = f"{command}:{user_id}"
    last_use = cooldown_cache.get(key)
    if not last_use:
        return False, 0
    quyen = lay_cap_do_quyen_nguoi_dung(user_id)
    user_cooldowns = COMMAND_COOLDOWNS.get(quyen, COMMAND_COOLDOWNS["member"])
    cooldown_time = user_cooldowns.get(command, user_cooldowns.get("default", 1200))
    elapsed = time.time() - last_use
    if elapsed < cooldown_time:
        return True, cooldown_time - elapsed
    return False, 0
def set_cooldown(user_id, command):
    if not la_admin(user_id):
        key = f"{command}:{user_id}"
        cooldown_cache.set(key, time.time())
def lay_gioi_han_so_dien_thoai(user_id):
    cap_do = lay_cap_do_quyen_nguoi_dung(user_id)
    return {"admin": 50, "vip": 50, "member": 2}.get(cap_do, 2)
def lay_thoi_gian_vn():
    try:
        mui_gio_vn = pytz.timezone("Asia/Ho_Chi_Minh")
        hien_tai = datetime.now(mui_gio_vn)
        return hien_tai.strftime("%H:%M:%S"), hien_tai.strftime("%d/%m/%Y")
    except:
        hien_tai = datetime.now()
        return hien_tai.strftime("%H:%M:%S"), hien_tai.strftime("%d/%m/%Y")
def escape_html(text):
    if text is None:
        return ""
    return html.escape(str(text))
def dinh_dang_thoi_gian_cooldown(giay):
    if giay <= 0:
        return "0 giây"
    if giay < 60:
        return f"{int(giay)} giây"
    phut = int(giay // 60)
    giay_con_lai = int(giay % 60)
    if giay_con_lai == 0:
        return f"{phut} phút"
    else:
        return f"{phut} phút {giay_con_lai} giây"
def dinh_dang_lien_ket_nguoi_dung(user):
    try:
        if not user:
            return "Người dùng không rõ"
        user_id = user.id
        ten_day_du = user.full_name
        if ten_day_du:
            return f'<a href="tg://user?id={user_id}">{escape_html(ten_day_du)}</a>'
        else:
            return f'<a href="tg://user?id={user_id}">ID: {user_id}</a>'
    except:
        return "Người dùng không rõ"
def lay_tieu_de_quyen(user_id):
    cap_do = lay_cap_do_quyen_nguoi_dung(user_id)
    user_id_str = str(user_id)
    if cap_do == "admin":
        if user_id_str == "5301816713":
            return "🧑🏻‍🚀 𝑸𝒖𝒂̉𝒏 𝑳𝒚́"
        else:
            return "🥷🏿 • 𝓐𝓭𝓶𝓲𝓷"
    tieu_de = {
        "super_vip": "🏆 𝑺𝒖𝒑𝒆𝒓𝑽𝑰𝑷",
        "vip": "🧞‍♂️ 🅥🅘🅟 🧜🏻‍",
        "member": " ༉ 𝑀𝑒𝑚𝑏𝑒𝑟𝑠 ༉ ",
    }
    return tieu_de.get(cap_do, tieu_de["member"])
def tao_keyboard_lien_ket_nhom():
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🧑🏻‍🚀 𝑸𝒖𝒂̉𝒏 𝑳𝒚́", url="https://t.me/@foxlyly45"
                ),
                InlineKeyboardButton(
                    text=" 𝓐𝓭𝓶𝓲𝓷  🥷🏿", url="https://t.me/@nonameoaivcl"
                ),
            ]
        ]
    )
    return keyboard
def cooldown_decorator(func):
    @wraps(func)
    async def wrapper(message: Message, *args, **kwargs):
        if not message.from_user:
            return False
        user_id = message.from_user.id
        func_name = func.__name__
        if func_name.startswith("xu_ly_"):
            command = func_name.replace("xu_ly_", "").replace("_", "")
            command_mapping = {
                "randomanh": "img",
                "randomvideo": "vid",
                "checkid": "checkid",
                "themvip": "themvip",
                "xoavip": "xoavip",
                "themadmin": "themadmin",
                "xoaadmin": "xoaadmin",
            }
            command = command_mapping.get(command, command)
        else:
            command = func_name
        is_cooldown, remaining = check_cooldown(user_id, command)
        if is_cooldown:
            time_str = dinh_dang_thoi_gian_cooldown(remaining)
            user = message.from_user
            tieu_de = lay_tieu_de_quyen(user_id)
            lien_ket_nguoi_dung = dinh_dang_lien_ket_nguoi_dung(user)
            chuoi_gio, chuoi_ngay = lay_thoi_gian_vn()
            await gui_phan_hoi(
                message,
                f"{tieu_de}.         :          {lien_ket_nguoi_dung}\n"
                f"🕐 𝐶𝑜̀𝑛 𝑙𝑎̣𝑖           :          {time_str}\n"
                f"🎯 𝑉𝑢̛̀𝑎 𝑙𝑜̀𝑛𝑔 𝑐ℎ𝑜̛̀ 𝑑𝑒̂̉ 𝑠𝑢̛̉ 𝑑𝑢̣𝑛𝑔 𝑙𝑒̣̂𝑛ℎ 𝑛𝑎̀𝑦 𝑡𝑖𝑒̂́𝑝 !",
                xoa_tin_nguoi_dung=True,
                tu_dong_xoa_sau_giay=10,
            )
            return False
        result = await func(message, *args, **kwargs)
        if result is True:
            set_cooldown(user_id, command)
        return result
    return wrapper
def chi_nhom(func):
    @wraps(func)
    async def wrapper(message: Message, *args, **kwargs):
        if not message.from_user:
            return False
        # Admin luôn được phép, kể cả trong DM
        if la_admin(message.from_user.id):
            return await func(message, *args, **kwargs)
        # Các user khác (bao gồm VIP/Super VIP) chỉ được dùng trong nhóm cho phép
        if not message.chat or message.chat.id not in NHOM_CHO_PHEP:
            return False
        return await func(message, *args, **kwargs)
    return wrapper
def chi_admin(func):
    @wraps(func)
    async def wrapper(message: Message, *args, **kwargs):
        if not message.from_user or not la_admin(message.from_user.id):
            user = message.from_user
            user_id = user.id
            tieu_de = lay_tieu_de_quyen(user_id)
            lien_ket_nguoi_dung = dinh_dang_lien_ket_nguoi_dung(user)
            chuoi_gio, chuoi_ngay = lay_thoi_gian_vn()
            await gui_phan_hoi(
                message,
                f"{tieu_de}         :           {lien_ket_nguoi_dung}\n"
                f"🙅🏼 𝐿𝑒̣̂𝑛ℎ 𝑛𝑎̀𝑦 𝑐ℎ𝑖̉ 𝑑𝑎̀𝑛ℎ 𝑐ℎ𝑜 𝐴𝑑𝑚𝑖𝑛 !",
                xoa_tin_nguoi_dung=True,
                tu_dong_xoa_sau_giay=10,
            )
            return False
        return await func(message, *args, **kwargs)
    return wrapper
def chi_vip_vinh_vien(func):
    @wraps(func)
    async def wrapper(message: Message, *args, **kwargs):
        if not message.from_user or not la_vip_vinh_vien(message.from_user.id):
            user = message.from_user
            user_id = user.id
            tieu_de = lay_tieu_de_quyen(user_id)
            lien_ket_nguoi_dung = dinh_dang_lien_ket_nguoi_dung(user)
            chuoi_gio, chuoi_ngay = lay_thoi_gian_vn()

            ref_link = (
                f"https://t.me/{BOT_USERNAME}?start=ref_{user_id}"
                if BOT_USERNAME
                else "https://t.me/spamsmscall1"
            )
            await gui_phan_hoi(
                message,
                f"{tieu_de}         :         {lien_ket_nguoi_dung}\n"
                f"🆔 𝑀ã 𝐼𝐷         :          {user_id}\n"
                f"🙅🏼 𝐿𝑒̣̂𝑛ℎ 𝑛𝑎̀𝑦 𝑐ℎ𝑖̉ 𝑑𝑎̀𝑛ℎ 𝑐ℎ𝑜 𝑉𝐼𝑃 !\n\n"
                f"🎯 𝐶𝑎́𝑐ℎ 𝑙𝑒̂𝑛 𝑉𝐼𝑃 𝑀𝐼𝐸̂̃𝑁 𝑃𝐻𝐼́ :\n\n"
                f"1️⃣ 𝐶ℎ𝑖𝑎 𝑠𝑒̉ 𝑙𝑖𝑛𝑘 𝑐𝑢̉𝑎 𝑏𝑎̣𝑛 : \n{ref_link}\n\n"
                f"2️⃣ 𝑀𝑜̛̀𝑖 10 𝑛𝑔𝑢̛𝑜̛̀𝑖 𝑡ℎ𝑎𝑚 𝑔𝑖𝑎 𝑛ℎ𝑜́𝑚 𝑞𝑢𝑎 𝑙𝑖𝑛𝑘 𝑐𝑢̉𝑎 𝑏𝑎̣𝑛.\n\n"
                f"3️⃣ 𝑇𝑢̛̣ 𝑑𝑜̣̂𝑛𝑔 𝑙𝑒̂𝑛 𝑉𝐼𝑃 𝑠𝑎𝑢 𝑘ℎ𝑖 𝑑𝑢̉ 10 𝑛𝑔𝑢̛𝑜̛̀𝑖 !\n\n"
                f"💡 𝐺𝑜̃ /myuse 𝑑𝑒̂̉ 𝑙𝑎̂́𝑦 𝑙𝑖𝑛𝑘 𝑚𝑜̛̀𝑖 𝑐𝑢̉𝑎 𝑏𝑎̣𝑛 !",
                xoa_tin_nguoi_dung=True,
                tu_dong_xoa_sau_giay=20,
            )
            return False
        return await func(message, *args, **kwargs)
    return wrapper
def chi_super_vip(func):
    @wraps(func)
    async def wrapper(message: Message, *args, **kwargs):
        if not message.from_user:
            return False
        role = lay_cap_do_quyen_nguoi_dung(message.from_user.id)
        if role != "super_vip":
            user = message.from_user
            user_id = user.id
            tieu_de = lay_tieu_de_quyen(user_id)
            lien_ket_nguoi_dung = dinh_dang_lien_ket_nguoi_dung(user)
            chuoi_gio, chuoi_ngay = lay_thoi_gian_vn()
            await gui_phan_hoi(
                message,
                f"{tieu_de}      :         {lien_ket_nguoi_dung}\n"
                f"🙅🏼 𝐿𝑒̣̂𝑛ℎ 𝑛𝑎̀𝑦 𝑐ℎ𝑖̉ 𝑑𝑎̀𝑛ℎ 𝑐ℎ𝑜 𝑆𝑈𝑃𝐸𝑅 𝑉𝐼𝑃 !",
                xoa_tin_nguoi_dung=True,
                tu_dong_xoa_sau_giay=10,
            )
            return False
        return await func(message, *args, **kwargs)
    return wrapper
def chi_admin_hoac_super_vip(func):
    @wraps(func)
    async def wrapper(message: Message, *args, **kwargs):
        if not message.from_user:
            return False
        is_admin = la_admin(message.from_user.id)
        role = lay_cap_do_quyen_nguoi_dung(message.from_user.id)
        if not is_admin and role != "super_vip":
            user = message.from_user
            user_id = user.id
            tieu_de = lay_tieu_de_quyen(user_id)
            lien_ket_nguoi_dung = dinh_dang_lien_ket_nguoi_dung(user)
            chuoi_gio, chuoi_ngay = lay_thoi_gian_vn()
            await gui_phan_hoi(
                message,
                f" {tieu_de}         :            {lien_ket_nguoi_dung}\n"
                f"🙅🏼 𝐿𝑒̣̂𝑛ℎ 𝑛𝑎̀𝑦 𝑐ℎ𝑖̉ 𝑑𝑎̀𝑛ℎ 𝑐ℎ𝑜 𝐴𝑑𝑚𝑖𝑛 và 𝑆𝑈𝑃𝐸𝑅 𝑉𝐼𝑃 !",
                xoa_tin_nguoi_dung=True,
                tu_dong_xoa_sau_giay=10,
            )
            return False
        return await func(message, *args, **kwargs)
    return wrapper
@chi_nhom
@chi_admin_hoac_super_vip
async def xu_ly_addphone(message: Message):
    global OKI_RUNNING
    if not message.from_user:
        return False
    with OKI_LOCK:
        dang_chay = OKI_RUNNING
    if dang_chay:
        user = message.from_user
        lien_ket_nguoi_dung = dinh_dang_lien_ket_nguoi_dung(user)
        await gui_phan_hoi(
            message,
            f"🚧 𝐶𝑎́𝑐 𝑠𝑜̂́ 𝑐𝑢̉𝑎 𝑏𝑎̣𝑛 𝑑𝑎𝑛𝑔 𝑡𝑟𝑜𝑛𝑔 𝑞𝑢𝑎́ 𝑡𝑟𝑖̀𝑛ℎ 𝑐ℎ𝑎̣𝑦.\n\n"
            f"🙅🏼{lien_ket_nguoi_dung} 𝑉𝑢𝑖 𝑙𝑜̀𝑛𝑔 𝑞𝑢𝑎𝑦 𝑙𝑎̣𝑖 𝑣𝑎̀𝑜 𝟼ℎ𝟹𝟶 𝑑𝑒̂̉ 𝑐𝑎̣̂𝑝 𝑛ℎ𝑎̣̂𝑡 𝑙𝑎̣𝑖 𝑑𝑎𝑛ℎ 𝑠𝑎́𝑐ℎ !",
            xoa_tin_nguoi_dung=True,
            tu_dong_xoa_sau_giay=10,
        )
        return False
    user_id = str(message.from_user.id)
    cac_tham_so = trich_xuat_tham_so(message)
    if not cac_tham_so:
        await gui_phan_hoi(
            message,
            "🫡 𝐶𝑢́ 𝑝ℎ𝑎́𝑝: /tso 𝟶𝟿𝟾𝟿𝟸𝟿𝟿𝟿𝟿𝟶, 𝑇𝑜̂́𝑖 𝑑𝑎 𝟹 𝑠𝑜̂́ 𝑐ℎ𝑜 𝑚𝑜̂̃𝑖 𝑆𝑢𝑝𝑒𝑟",
            xoa_tin_nguoi_dung=True,
            tu_dong_xoa_sau_giay=10
        )
        return False
    phone = cac_tham_so[0].strip()
    if not la_so_dien_thoai_hop_le(phone):
        await gui_phan_hoi(
            message,
            "🫡 Số điện thoại không hợp lệ!",
            xoa_tin_nguoi_dung=True,
            tu_dong_xoa_sau_giay=10
        )
        return False
    try:
        conn = tao_ket_noi_db()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT COUNT(*) FROM super_vip_phones WHERE user_id = ?", (user_id,)
        )
        count = cursor.fetchone()[0]
        if count >= 3:
            await gui_phan_hoi(
                message,
                "🫡 Tối đa 3 số điện thoại!",
                xoa_tin_nguoi_dung=True,
                tu_dong_xoa_sau_giay=10
            )
            conn.close()
            return False
        cursor.execute(
            "INSERT OR REPLACE INTO super_vip_phones (user_id, phone_number) VALUES (?, ?)",
            (user_id, phone),
        )
        conn.commit()
        conn.close()
        user = message.from_user
        lien_ket_nguoi_dung = dinh_dang_lien_ket_nguoi_dung(user)
        await gui_phan_hoi(
            message,
            f" {lien_ket_nguoi_dung} 𝑑𝑎̃ 𝑡ℎ𝑒̂𝑚 𝑠𝑜̂́: {phone}",
            xoa_tin_nguoi_dung=True,
            tu_dong_xoa_sau_giay=10
        )
    except Exception as e:
        await gui_phan_hoi(
            message, f"❌ Lỗi: {str(e)}", xoa_tin_nguoi_dung=True, tu_dong_xoa_sau_giay=8
        )
        return False

@chi_nhom
@chi_admin_hoac_super_vip
async def xu_ly_delphone(message: Message):
    global OKI_RUNNING
    if not message.from_user:
        return False
    with OKI_LOCK:
        dang_chay = OKI_RUNNING
    if dang_chay:
        user = message.from_user
        lien_ket_nguoi_dung = dinh_dang_lien_ket_nguoi_dung(user)
        await gui_phan_hoi(
            message,
            f"🚧 𝐶𝑎́𝑐 𝑠𝑜̂́ 𝑐𝑢̉𝑎 𝑏𝑎̣𝑛 𝑑𝑎𝑛𝑔 𝑡𝑟𝑜𝑛𝑔 𝑞𝑢𝑎́ 𝑡𝑟𝑖̀𝑛ℎ 𝑐ℎ𝑎̣𝑦.\n\n"
            f"🙅🏼{lien_ket_nguoi_dung} 𝑉𝑢𝑖 𝑙𝑜̀𝑛𝑔 𝑞𝑢𝑎𝑦 𝑙𝑎̣𝑖 𝑣𝑎̀𝑜 𝟼ℎ𝟹𝟶 𝑑𝑒̂̉ 𝑐𝑎̣̂𝑝 𝑛ℎ𝑎̣̂𝑡 𝑙𝑎̣𝑖 𝑑𝑎𝑛ℎ 𝑠𝑎́𝑐ℎ !",
            xoa_tin_nguoi_dung=True,
            tu_dong_xoa_sau_giay=10,
        )
        return False
    user_id = str(message.from_user.id)
    cac_tham_so = trich_xuat_tham_so(message)
    if not cac_tham_so:
        await gui_phan_hoi(
            message,
            "🫡 𝐶𝑢́ 𝑝ℎ𝑎́𝑝: /𝑑𝑒𝑙𝑠𝑜 𝟶𝟿𝟾𝟿𝟸𝟿𝟿𝟿𝟿𝟶",
            xoa_tin_nguoi_dung=True,
            tu_dong_xoa_sau_giay=10
        )
        return False
    phone = cac_tham_so[0].strip()
    try:
        conn = tao_ket_noi_db()
        cursor = conn.cursor()
        cursor.execute(
            "DELETE FROM super_vip_phones WHERE user_id = ? AND phone_number = ?",
            (user_id, phone),
        )
        conn.commit()
        conn.close()
        user = message.from_user
        lien_ket_nguoi_dung = dinh_dang_lien_ket_nguoi_dung(user)
        await gui_phan_hoi(
            message,
            f" {lien_ket_nguoi_dung} 𝑑𝑎̃ 𝑥𝑜𝑎́ 𝑠𝑜̂́: {phone}",
            xoa_tin_nguoi_dung=True,
            tu_dong_xoa_sau_giay=10
        )
    except Exception as e:
        await gui_phan_hoi(
            message, f"❌ Lỗi: {str(e)}", xoa_tin_nguoi_dung=True, tu_dong_xoa_sau_giay=8
        )
        return False

@chi_nhom
@chi_admin_hoac_super_vip
async def xu_ly_sper(message: Message):
    global OKI_RUNNING
    if not message.from_user:
        return False

    # Lấy trạng thái OKI nhưng vẫn cho phép xem danh sách
    with OKI_LOCK:
        dang_chay = OKI_RUNNING

    user = message.from_user
    user_id = str(user.id)
    tieu_de = lay_tieu_de_quyen(user_id)
    lien_ket_nguoi_dung = dinh_dang_lien_ket_nguoi_dung(user)

    # Lấy danh sách số của user
    try:
        conn = tao_ket_noi_db()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT phone_number FROM super_vip_phones WHERE user_id = ? ORDER BY added_at DESC",
            (user_id,),
        )
        phones = cursor.fetchall()
        conn.close()

        # Tạo nội dung hiển thị
        status_msg = ""
        if dang_chay:
            status_msg = "\n🚧 𝑇𝑟𝑢̛𝑜̛𝑛𝑔 𝑡𝑟𝑖̀𝑛ℎ 𝑑𝑎𝑛𝑔 𝑐ℎ𝑎̣𝑦, 𝑘ℎ𝑜̂𝑛𝑔 𝑡ℎ𝑒̂̉ 𝑡ℎ𝑒̂𝑚 𝑑𝑢̛𝑜̛̣𝑐 𝑠𝑜̂́ 𝑣𝑎̀𝑜 𝑙𝑢́𝑐 𝑛𝑎̀𝑦 !"

        noi_dung = f"""{tieu_de}        :        {lien_ket_nguoi_dung}
🆔 𝑀ã 𝐼𝐷        :         {user_id}{status_msg}

🎯 𝑄𝑢𝑎̉𝑛 𝑙𝑖́ 𝑑𝑎𝑛ℎ 𝑠𝑎́𝑐ℎ 𝑐𝑢̉𝑎 𝐒𝐮𝐩𝐞𝐫 𝐕𝐈𝐏:"""

        if not dang_chay:
            noi_dung += """
 • /tso  [𝑠𝑜̂́]    -    𝑇ℎ𝑒̂𝑚 𝑠𝑜̂́ 𝑚𝑜̛́𝑖
 • /delso  [𝑠𝑜̂́]  -    𝑋𝑜́𝑎 𝑠𝑜̂́ 𝑘ℎ𝑜̉𝑖 𝑑𝑎𝑛ℎ 𝑠𝑎́𝑐ℎ"""

        noi_dung += f"""

📜 𝐷𝑎𝑛ℎ 𝑠𝑎́𝑐ℎ 𝑠𝑜̂́ ℎ𝑖𝑒̣̂𝑛 𝑡𝑎̣𝑖 ({len(phones)}/3):"""

        if phones:
            noi_dung += "\n\n"
            for i, phone in enumerate(phones, 1):
                carrier = get_carrier(phone['phone_number'])
                noi_dung += f"  {i}. 📞 {phone['phone_number']} - {carrier}\n"

            if dang_chay:
                noi_dung += f"\n🛸 𝐷𝑎𝑛ℎ 𝑠𝑎́𝑐ℎ 𝑐𝑎́𝑐 𝑠𝑜̂́ 𝑑𝑎𝑛𝑔 𝑐ℎ𝑎̣𝑦 𝑡𝑢̛̣ 𝑑𝑜̣̂𝑛𝑔 𝟸𝟹:𝟹𝟶-𝟶𝟼:𝟶𝟶"
            else:
                noi_dung += f"\n 𝐿𝑒̂𝑛 𝑙𝑖̣𝑐ℎ 𝑐ℎ𝑎̣𝑦 𝑡𝑢̛̣ 𝑑𝑜̣̂𝑛𝑔 ℎ𝑎̀𝑛𝑔 𝑑𝑒̂𝑚 𝑣𝑎̀𝑜 𝑙𝑢́𝑐 𝟸𝟹ℎ:𝟹𝟶𝑝"
        else:
            noi_dung += "\n\n 𝐵𝑎̣𝑛 𝑐ℎ𝑢̛𝑎 𝑐𝑜́ 𝑠𝑜̂́ 𝑛𝑎̀𝑜 𝑡𝑟𝑜𝑛𝑔 𝑑𝑎𝑛ℎ 𝑠𝑎́𝑐ℎ"
            if not dang_chay:
                noi_dung += "\n💡 𝐷𝑢̀𝑛𝑔 /tso 𝑠𝑜̂́ 𝑑𝑖𝑒̣̂𝑛 𝑡ℎ𝑜𝑎̣𝑖. 𝐷𝑒̂̉ 𝑡ℎ𝑒̂𝑚 𝑠𝑜̂́ 𝑚𝑜̛́𝑖 𝑣𝑎̀𝑜 𝑑𝑎𝑛ℎ 𝑠𝑎́𝑐ℎ"

        await gui_phan_hoi(
            message, noi_dung, xoa_tin_nguoi_dung=True, luu_vinh_vien=True, co_keyboard=True
        )
        return True
    except Exception as e:
        await gui_phan_hoi(
            message, f"❌ Lỗi: {str(e)}", xoa_tin_nguoi_dung=True, tu_dong_xoa_sau_giay=10
        )
        return False

def _lay_super_vip_phones():
    try:
        conn = tao_ket_noi_db()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT user_id, GROUP_CONCAT(phone_number, ', ') as phones, COUNT(*) as count FROM super_vip_phones GROUP BY user_id"
        )
        results = cursor.fetchall()
        conn.close()
        return results
    except Exception as e:
        return []
async def xu_ly_oki_schedule():
    global OKI_RUNNING
    try:
        with OKI_LOCK:
            OKI_RUNNING = True
        results = _lay_super_vip_phones()
        if not results:
            with OKI_LOCK:
                OKI_RUNNING = False
            return
        await _gui_thong_bao_oki_bat_dau_len_nhom()
        for row in results:
            user_id = row["user_id"]
            try:
                await _thuc_thi_oki_scripts_via_fla(user_id)
                await asyncio.sleep(2)
            except Exception:
                pass

    except Exception:
        with OKI_LOCK:
            OKI_RUNNING = False
async def _gui_thong_bao_oki_bat_dau_len_nhom():
    try:
        # Lấy thời gian hiện tại
        chuoi_gio, chuoi_ngay = lay_thoi_gian_vn()

        # Đếm tổng số điện thoại
        results = _lay_super_vip_phones()
        phone_count = sum(row["count"] for row in results) if results else 0

        thong_bao = (
            f"<blockquote>🛸 𝐵𝑎̆́𝑡 𝑑𝑎̂̀𝑢 𝑐ℎ𝑎̣𝑦 𝑡𝑟𝑢̛𝑜̛𝑛𝑔 𝑡𝑟𝑖̀𝑛ℎ ! \n\n"
            f"📜 𝑇𝑜̂̉𝑛𝑔 𝑠𝑜̂́         :           {phone_count}\n\n"            
            f"🕐 𝑇ℎ𝑜̛̀𝑖 𝑔𝑖𝑎𝑛        :          {chuoi_gio}\n"
            f"📅 𝑁𝑔𝑎̀𝑦           :           {chuoi_ngay}\n\n"
            f"🚀 𝐶𝑎́𝑐 𝑠𝑜̂́ 𝑠𝑒̃ 𝑘ℎ𝑜̂𝑛𝑔 𝑡ℎ𝑒̂̉ 𝑡ℎ𝑎𝑦 𝑑𝑜̂̉𝑖 𝑐ℎ𝑜 𝑑𝑒̂́𝑛 𝑘ℎ𝑖 𝑞𝑢𝑎́ 𝑡𝑟𝑖̀𝑛ℎ ℎ𝑜𝑎̀𝑛 𝑡𝑎̂́𝑡 𝑣𝑎̀𝑜 𝟼ℎ𝟹𝟶 !</blockquote>"
        )
        for group_id in NHOM_CHO_PHEP:
            try:
                await bot.send_message(
                    chat_id=group_id,
                    text=thong_bao,
                    parse_mode=ParseMode.HTML,
                )
                break
            except:
                continue
    except Exception:
        pass
async def _thuc_thi_oki_scripts_via_fla(user_id):
    try:
        conn = tao_ket_noi_db()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT phone_number FROM super_vip_phones WHERE user_id = ?",
            (user_id,),
        )
        phones_data = cursor.fetchall()
        conn.close()
        phone_list = [p["phone_number"] for p in phones_data]
        if not phone_list:
            return False, "No phones found"

        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
                payload = {
                    "command_type": "oki",
                    "phone_numbers": phone_list,
                    "user_id": str(user_id),
                    "script_name": "oki.py",
                    "rounds": 999,
                }
                async with session.post(
                    f"{REMOTE_VPS_URL}/execute",
                    json=payload,
                    headers={"Authorization": f"Bearer {API_SECRET_KEY}"},
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as resp:
                    if resp.status == 200:
                        result = await resp.json()
                        if result.get("success"):
                            return True, f"Success: {len(phone_list)} phones"
                        else:
                            return False, f"API Error: {result.get('data', {}).get('error', 'Unknown')}"
                    else:
                        return False, f"HTTP Error: {resp.status}"
        except asyncio.TimeoutError:
            return False, "Timeout connecting to API"
        except Exception as e:
            return False, f"Request failed: {str(e)}"
    except Exception as e:
        return False, f"Database error: {str(e)}"
async def xu_ly_oki_cleanup_6h():
    """Job chạy lúc 6:00 sáng để cleanup tất cả OKI processes và thông báo"""
    global OKI_RUNNING
    try:
        with OKI_LOCK:
            OKI_RUNNING = False

        # Force kill tất cả oki.py processes
        killed_count = 0
        try:
            import psutil
            for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
                try:
                    if "python" not in proc.info["name"].lower():
                        continue
                    cmdline = proc.info.get("cmdline", [])
                    if len(cmdline) < 2:
                        continue
                    script_name = os.path.basename(cmdline[1]) if cmdline[1] else ""
                    if script_name == "oki.py":
                        try:
                            proc.kill()
                            proc.wait(timeout=3)
                            killed_count += 1
                        except:
                            pass
                except:
                    continue
        except:
            pass

        with PROCESS_LOCK:
            USER_PROCESSES.clear()

        await _gui_thong_bao_oki_hoan_thanh_len_nhom(killed_count)

    except Exception:
        pass

async def _gui_thong_bao_oki_hoan_thanh_len_nhom(killed_count=0):
    try:
        chuoi_gio, chuoi_ngay = lay_thoi_gian_vn()
        thong_bao = (
            f"<blockquote>🔊 𝐷𝑎̃ ℎ𝑜𝑎̀𝑛 𝑡ℎ𝑎̀𝑛𝑛 𝑡𝑟𝑢̛𝑜̛𝑛𝑔 𝑡𝑟𝑖̀𝑛ℎ 𝑐ℎ𝑎̣𝑦 𝑡𝑢̛̣ 𝑑𝑜̣̂𝑛𝑔 !\n"
            f"🧹 𝐷𝑢̛̀𝑛𝑔 𝑣𝑎̀ 𝑑𝑜̣𝑛 𝑑𝑒̣𝑝 {killed_count} 𝑡𝑖𝑒̂́𝑛 𝑡𝑟𝑖̀𝑛ℎ \n\n"
            f"⏱ 𝑇ℎ𝑜̛̀𝑖 𝑔𝑖𝑎𝑛   :    {chuoi_gio} - {chuoi_ngay} \n"
            f"⏳ 𝐶ℎ𝑢 𝑘ỳ 𝑡𝑖ế𝑝 𝑡ℎ𝑒𝑜 𝑣𝑎̀𝑜 : 𝟐𝟑𝒉𝟑𝟎 ℎ𝑎̀𝑛𝑔 đ𝑒̂𝑚 𝑂𝑘𝑎𝑦 !</blockquote>"
        )
        for group_id in NHOM_CHO_PHEP:
            try:
                await bot.send_message(
                    chat_id=group_id,
                    text=thong_bao,
                    parse_mode=ParseMode.HTML,
                )
                break
            except:
                continue
    except Exception:
        pass
async def gui_thong_bao_len_nhom(noi_dung: str):
    try:
        for group_id in NHOM_CHO_PHEP:
            try:
                await bot.send_message(
                    chat_id=group_id,
                    text=f"<blockquote>{noi_dung}</blockquote>",
                    parse_mode=ParseMode.HTML,
                )
                break
            except:
                continue
    except Exception:
        pass

async def chay_script_async(cmd, user_id):
    try:
        success, _ = chay_script_don_gian(
            cmd, user_id, timeout=3600, command_type="full"
        )
        return success
    except:
        return False
async def gui_phan_hoi(
    message: Message,
    noi_dung: str,
    xoa_tin_nguoi_dung=True,
    tu_dong_xoa_sau_giay=10
    luu_vinh_vien=False,
    co_keyboard=False,
    photo_path=None,
):
    try:
        chat_id = message.chat.id
        keyboard = tao_keyboard_lien_ket_nhom() if co_keyboard else None
        if photo_path:
            text = f"<blockquote>{noi_dung.strip()}</blockquote>"
            sent_message = await bot.send_photo(
                chat_id=chat_id,
                photo=FSInputFile(photo_path),
                caption=text,
                parse_mode=ParseMode.HTML,
                reply_markup=keyboard
            )
        else:
            #  KHÔNG ẢNH → GIỮ NGUYÊN LOGIC CŨ CỦA MÀY
            text = f"<blockquote>{noi_dung.strip()}</blockquote>"
            sent_message = await bot.send_message(
                chat_id=chat_id,
                text=text,
                parse_mode=ParseMode.HTML,
                reply_markup=keyboard
            )
        if xoa_tin_nguoi_dung:
            try:
                await bot.delete_message(chat_id, message.message_id)
            except:
                pass
        if tu_dong_xoa_sau_giay > 0 and not luu_vinh_vien:
            asyncio.create_task(
                tu_dong_xoa_tin_nhan(
                    sent_message.chat.id,
                    sent_message.message_id,
                    tu_dong_xoa_sau_giay
                )
            )
        return sent_message
    except Exception as e:
        print(f"[GUI_PHAN_HOI_ERROR] {e}")
        return None
async def tu_dong_xoa_tin_nhan(chat_id, message_id, tre=10):
    try:
        await asyncio.sleep(tre)
        await bot.delete_message(chat_id=chat_id, message_id=message_id)
    except:
        pass
def them_vip(user_id, ten, admin_added_by=None):
    try:
        from datetime import timedelta
        conn = tao_ket_noi_db()
        cursor = conn.cursor()
        expiry = datetime.now() + timedelta(days=30)
        cursor.execute(
            "INSERT OR REPLACE INTO admin (user_id, name, role, expiry_date, admin_added_by) VALUES (?, ?, ?, ?, ?)",
            (
                str(user_id),
                ten,
                "vip",
                expiry,
                str(admin_added_by) if admin_added_by else None,
            ),
        )
        conn.commit()
        conn.close()
        quyen_cache.set(str(user_id), "vip")
    except Exception:
        pass
def them_super_vip(user_id, ten, days=30, admin_added_by=None):
    try:
        from datetime import timedelta
        conn = tao_ket_noi_db()
        cursor = conn.cursor()
        expiry = datetime.now() + timedelta(days=days)
        cursor.execute(
            "INSERT OR REPLACE INTO admin (user_id, name, role, expiry_date, admin_added_by) VALUES (?, ?, ?, ?, ?)",
            (
                str(user_id),
                ten,
                "super_vip",
                expiry,
                str(admin_added_by) if admin_added_by else None,
            ),
        )
        conn.commit()
        conn.close()
        quyen_cache.set(str(user_id), "super_vip")
    except Exception:
        pass
def them_admin(user_id, ten):
    try:
        conn = tao_ket_noi_db()
        cursor = conn.cursor()
        cursor.execute(
            "INSERT OR REPLACE INTO admin (user_id, name, role) VALUES (?, ?, ?)",
            (str(user_id), ten, "admin"),
        )
        conn.commit()
        conn.close()
        quyen_cache.set(str(user_id), "admin")
    except Exception:
        pass
def trich_xuat_tham_so(message: Message):
    if not message.text:
        return []
    return message.text.split()[1:]
@chi_nhom
async def xu_ly_sta(message: Message):
    if not message.from_user:
        return False
    user = message.from_user
    user_id = str(user.id)
    if message.text and " " in message.text:
        parts = message.text.split()
        if len(parts) > 1 and parts[1].startswith("ref_"):
            referrer_id = parts[1].replace("ref_", "")
            # Không cho tự refer chính mình
            if referrer_id != user_id:
                try:
                    conn = tao_ket_noi_db()
                    cursor = conn.cursor()
                    # Kiểm tra xem đã được refer chưa
                    cursor.execute(
                        "SELECT COUNT(*) FROM referrals WHERE referred_id = ?",
                        (user_id,),
                    )
                    if cursor.fetchone()[0] == 0:
                        # Lưu referral
                        cursor.execute(
                            "INSERT OR IGNORE INTO referrals (referrer_id, referred_id) VALUES (?, ?)",
                            (referrer_id, user_id),
                        )
                        conn.commit()
                        cursor.execute(
                            "SELECT COUNT(*) FROM referrals WHERE referrer_id = ?",
                            (referrer_id,),
                        )
                        count = cursor.fetchone()[0]
                        if count >= 10:
                            cursor.execute(
                                "SELECT user_id FROM admin WHERE user_id = ? AND role = 'vip'",
                                (referrer_id,),
                            )
                            if not cursor.fetchone():
                                from datetime import timedelta
                                expiry_date = (
                                    datetime.now() + timedelta(days=30)
                                ).isoformat()
                                cursor.execute(
                                    "INSERT OR REPLACE INTO admin (user_id, name, role, expiry_date, admin_added_by) VALUES (?, ?, ?, ?, ?)",
                                    (
                                        referrer_id,
                                        "Auto VIP",
                                        "vip",
                                        expiry_date,
                                        "referral_system",
                                    ),
                                )
                                conn.commit()
                                quyen_cache.set(referrer_id, "vip")
                                try:
                                    await bot.send_message(
                                        chat_id=int(referrer_id),
                                        text=f"<blockquote>🎉 𝐶ℎ𝑢́𝑐 𝑚𝑢̛̀𝑛𝑔 𝑏𝑎̣𝑛 𝑑𝑎̃ 𝑙𝑒̂𝑛 𝑉𝐼𝑃 !\n\n"
                                        f"𝐵𝑎̣𝑛 𝑑𝑎̃ 𝑚𝑜̛̀𝑖 𝑑𝑢̉ {count} 𝑛𝑔𝑢̛𝑜̛̀𝑖 𝑡ℎ𝑎𝑚 𝑔𝑖𝑎 𝑛ℎ𝑜́𝑚\n"
                                        f"𝑇𝑢̛̣ 𝑑𝑜̣̂𝑛𝑔 𝑙𝑒̂𝑛 𝑉𝐼𝑃 30 𝑛𝑔𝑎̀𝑦\n"
                                        f"𝐻𝑒̂́𝑡 ℎ𝑎̣𝑛       :      {(datetime.now() + timedelta(days=30)).strftime('%d/%m/%Y')}</blockquote>",
                                        parse_mode=ParseMode.HTML,
                                    )
                                except:
                                    pass
                    conn.close()
                except Exception:
                    pass
    tieu_de = lay_tieu_de_quyen(user_id)
    lien_ket_nguoi_dung = dinh_dang_lien_ket_nguoi_dung(user)
    noi_dung = f"""{tieu_de}      :      {lien_ket_nguoi_dung}

🚀 𝐿𝐸̣̂𝑁𝐻 𝐶𝑂̛ 𝐵𝐴̉𝑁 :
 • /ping        -      𝑋𝑒𝑚 𝑇𝑟𝑎̣𝑛𝑔 𝑇ℎ𝑎́𝑖
 • /checkid     -      𝑋𝑒𝑚 𝑇ℎ𝑜̂𝑛𝑔 𝑇𝑖𝑛 𝐼𝐷
 • /free        -       𝑆𝑝𝑎𝑚 𝑆𝑀𝑆
 • /sms        -      𝑆𝑀𝑆 (2 𝑠𝑜̂́ 𝑀𝑒𝑚𝑏𝑒𝑟, 50 𝑠𝑜̂́ 𝑉𝐼𝑃)
 • /img        -        𝑅𝑎𝑛𝑑𝑜𝑚 𝐴̉𝑛ℎ
 • /vid        -        𝑅𝑎𝑛𝑑𝑜𝑚 𝑉𝑖𝑑𝑒𝑜
 • /ngl        -        𝑆𝑝𝑎𝑚 𝑁𝐺𝐿

🔥 𝐿𝐸̣̂𝑁𝐻 𝑉𝐼𝑃
 • /spam        -       𝑆𝑝𝑎𝑚 𝑆𝑀𝑆 𝑍𝑎𝑙𝑜
 • /call        -       𝐺𝑜̣𝑖 1 𝑆𝑜̂́
 • /vip         -       𝑆𝑀𝑆 𝐶𝑎𝑙𝑙 10 𝑠𝑜̂́
 • /full        -       𝐶ℎ𝑎̣𝑦 𝐹𝑢𝑙𝑙 ②④ⓗ
 • /tiktok      -      𝑇𝑎̆𝑛𝑔 𝑉𝑖𝑒𝑤 𝑇𝑖𝑘𝑇𝑜𝑘

🏆 𝐿𝐸̣̂𝑁𝐻 𝑆𝑈𝑃𝐸𝑅 𝑉𝐼𝑃
 • /sper       -       𝐴𝑢𝑡𝑜 𝑐ℎ𝑎̣𝑦 𝑑𝑒̂𝑚 𝟸𝟺/𝟽

🌀 𝑀𝑈𝑂̂́𝑁 𝐿𝐸̂𝑁 𝑉𝐼𝑃 𝑀𝐼𝐸̂̃𝑁 𝑃𝐻𝐼́ ? 
   𝐴̂́𝑛 𝑣𝑎̀𝑜 𝑑𝑎̂𝑦 ➤ /myuse 📍
"""
    await gui_phan_hoi(
        message, noi_dung, xoa_tin_nguoi_dung=True, luu_vinh_vien=True, co_keyboard=True
    )
    return True
@cooldown_decorator
@chi_nhom
async def xu_ly_ping(message: Message):
    logger.info(
        f"📥 /ping từ user {message.from_user.id if message.from_user else 'unknown'}"
    )
    if not message.from_user:
        return False
    user = message.from_user
    user_id = user.id
    tieu_de = lay_tieu_de_quyen(user_id)
    lien_ket_nguoi_dung = dinh_dang_lien_ket_nguoi_dung(user)
    noi_dung = (
        f"{tieu_de}         :       {lien_ket_nguoi_dung}\n\n"
        f"🆔 𝑀ã 𝐼𝐷          :       {user_id}\n\n"
        f"🤖 𝑇𝑟𝑎̣𝑛𝑔 𝑡ℎ𝑎́𝑖 𝐵𝑜𝑡    :      ℎ𝑜𝑎̣𝑡 𝑑𝑜̣̂𝑛𝑔 🛰️\n\n"
        f"🚀 𝑆𝐴̆̃𝑁 𝑆𝐴̀𝑁𝐺 𝑁𝐻𝐴̣̂𝑁 𝐿𝐸̣̂𝑁𝐻 !  🎯\n"
    )
    await gui_phan_hoi(
        message, noi_dung, xoa_tin_nguoi_dung=True, luu_vinh_vien=True, co_keyboard=True
    )
    return True
@cooldown_decorator
@chi_nhom
async def xu_ly_sms(message: Message):
    if not message.from_user:
        return False
    user = message.from_user
    user_id = user.id
    cac_tham_so = trich_xuat_tham_so(message)
    if not cac_tham_so:
        gioi_han_so = lay_gioi_han_so_dien_thoai(user_id)
        cap_do = lay_cap_do_quyen_nguoi_dung(user_id)
        gioi_han = 50 if cap_do in ("admin", "vip") else 2
        await gui_phan_hoi(
            message,
            f"🫡 /sms 𝑇𝑜̂́𝑖 𝑑𝑎 {gioi_han} 𝑠𝑜̂́ 𝑑𝑖𝑒̣̂𝑛 𝑡ℎ𝑜𝑎̣𝑖 𝑐𝑢̀𝑛𝑔 𝑙𝑢́𝑐 𝑡ℎ𝑒𝑜 𝑞𝑢𝑦𝑒̂̀𝑛 ℎ𝑖𝑒̣̂𝑛 𝑡𝑎̣𝑖 𝑐𝑢̉𝑎 𝑏𝑎̣𝑛 !",
            xoa_tin_nguoi_dung=True,
            tu_dong_xoa_sau_giay=10
        )
        return False
    cap_do = lay_cap_do_quyen_nguoi_dung(user_id)
    gioi_han = 50 if cap_do in ("admin", "vip") else 2
    if len(cac_tham_so) > gioi_han:
        await gui_phan_hoi(
            message,
            f"🫡 Bạn chỉ được phép nhập tối đa {gioi_han} số!",
            xoa_tin_nguoi_dung=True,
            tu_dong_xoa_sau_giay=10
        )
        return False
    cac_so_hop_le = []
    for so in cac_tham_so:
        so = so.strip()
        if la_so_dien_thoai_hop_le(so):
            cac_so_hop_le.append(so)
    if not cac_so_hop_le:
        await gui_phan_hoi(
            message,
            "🫡 Các số điện thoại không hợp lệ!",
            xoa_tin_nguoi_dung=True,
            tu_dong_xoa_sau_giay=10
        )
        return False
    thanh_cong, exec_info = await execute_with_swap("sms", cac_so_hop_le, user_id)
    if not thanh_cong:
        await gui_phan_hoi(
            message,
            "🫡 Không thể khởi tạo tiến trình SMS!",
            xoa_tin_nguoi_dung=True,
            tu_dong_xoa_sau_giay=10
        )
        return False
    tieu_de = lay_tieu_de_quyen(user_id)
    lien_ket_nguoi_dung = dinh_dang_lien_ket_nguoi_dung(user)
    noi_dung = (
        f"{tieu_de}        :        {lien_ket_nguoi_dung}\n"
        f"🆔 𝑀ã 𝐼𝐷              :       {user_id}\n"
        f"📲 𝑁ℎ𝑎̣̂𝑝 𝑇𝑎𝑦        :        {len(cac_so_hop_le)} 𝑠𝑜̂́ 𝐻𝑜̛̣𝑝 𝑙𝑒̣̂\n"
        f"⚡ 𝑇𝑖𝑒̂́𝑛 𝑡𝑟𝑖̀𝑛ℎ        :         𝐿𝑎̂̀𝑛 𝑙𝑢̛𝑜̛̣𝑡\n"
        f"🪩 𝑉𝑖̣ 𝑡𝑟𝑖́                :        𝑉/𝑁 𝑂𝑛𝑙𝑖𝑛𝑒\n\n"
        f"🚀 𝐿𝑒̣̂𝑛ℎ ✧𝐒𝐌𝐒✧ 𝑑𝑎̃ 𝑐ℎ𝑎̣𝑦 𝑡ℎ𝑎̀𝑛ℎ 𝑐𝑜̂𝑛𝑔\n"
        f" 𝐶ℎ𝑎̣𝑦 𝑐𝑎̀𝑛𝑔 𝑛ℎ𝑖𝑒̂̀𝑢 𝑠𝑜̂́ 𝑐𝑎̀𝑛𝑔 𝑡𝑜̂́𝑡 ! 🫡\n"
    )
    await gui_phan_hoi(
        message,
        noi_dung,
        xoa_tin_nguoi_dung=True,
        luu_vinh_vien=True,
        co_keyboard=True,
        photo_path="imagehack.jpg",
    )
    return True
@cooldown_decorator
@chi_nhom
@chi_vip_vinh_vien
async def xu_ly_spam(message: Message):
    if not message.from_user:
        return False
    user = message.from_user
    user_id = user.id
    cac_tham_so = trich_xuat_tham_so(message)
    if len(cac_tham_so) != 1:
        await gui_phan_hoi(
            message,
            "🫡 𝑪𝒖́ 𝒑𝒉𝒂́𝒑: /𝒔𝒑𝒂𝒎 𝟎𝟗𝟗𝟗𝟖𝟖𝟖𝟗𝟗𝟗 !",
            xoa_tin_nguoi_dung=True,
            tu_dong_xoa_sau_giay=10
        )
        return False
    so_dien_thoai = cac_tham_so[0].strip()
    if not la_so_dien_thoai_hop_le(so_dien_thoai):
        await gui_phan_hoi(
            message,
            "🫡 Số điện thoại không hợp lệ!",
            xoa_tin_nguoi_dung=True,
            tu_dong_xoa_sau_giay=10
        )
        return False
    thanh_cong, exec_info = await execute_with_swap(
        "spam", [so_dien_thoai], user_id, rounds=1
    )
    if not thanh_cong:
        await gui_phan_hoi(
            message,
            "🫡 Lỗi khi khởi động tiến trình!",
            xoa_tin_nguoi_dung=True,
            tu_dong_xoa_sau_giay=10
        )
        return False
    tieu_de = lay_tieu_de_quyen(user_id)
    lien_ket_nguoi_dung = dinh_dang_lien_ket_nguoi_dung(user)
    noi_dung = (
        f"{tieu_de}       :        {lien_ket_nguoi_dung}\n"
        f"🆔 𝑀ã 𝐼𝐷              :       {user_id}\n"
        f"📲 𝑃ℎ𝑜𝑛𝑒 𝑉𝑁        :        {so_dien_thoai}\n"
        f"🛰️ 𝑁ℎ𝑎̀ 𝑚𝑎̣𝑛𝑔       :         {get_carrier(so_dien_thoai)}\n"
        f"🪩 𝑉𝑖̣ 𝑡𝑟𝑖́                :        𝑉/𝑁 𝑂𝑛𝑙𝑖𝑛𝑒\n\n"
        f"🚀 𝐿𝑒̣̂𝑛ℎ ✧𝐒𝐏𝐀𝐌✧ 𝑑𝑎̃ 𝑐ℎ𝑎̣𝑦 𝑡ℎ𝑎̀𝑛ℎ 𝑐𝑜̂𝑛𝑔\n"
        f" 𝑇𝑎̆𝑛𝑔 𝑡𝑜̂́𝑐 𝑔𝑢̛̉𝑖 𝑡𝑖𝑛 𝑟𝑎́𝑐 𝑙𝑖𝑒̂𝑛 𝑡𝑢̣𝑐 ! 🫡\n"
    )
    await gui_phan_hoi(
        message,
        noi_dung,
        xoa_tin_nguoi_dung=True,
        luu_vinh_vien=True,
        co_keyboard=True,
        photo_path="imagehack.jpg",
    )
    return True
@cooldown_decorator
@chi_nhom
async def xu_ly_free(message: Message):
    if not message.from_user:
        return False
    user = message.from_user
    user_id = user.id
    cac_tham_so = trich_xuat_tham_so(message)
    if not cac_tham_so:
        await gui_phan_hoi(
            message,
            "🫡 /free 𝟶𝟿𝟶𝟿𝟷𝟸𝟹𝟺𝟻 - 𝐶ℎ𝑖̉ 𝟷 𝑠𝑜̂́ 𝑑𝑖𝑒̣̂𝑛 𝑡ℎ𝑜𝑎̣𝑖 !",
            xoa_tin_nguoi_dung=True,
            tu_dong_xoa_sau_giay=10
        )
        return False
    if len(cac_tham_so) > 1:
        await gui_phan_hoi(
            message,
            "🫡 Lệnh /free chỉ nhận 1 số điện thoại!",
            xoa_tin_nguoi_dung=True,
            tu_dong_xoa_sau_giay=10
        )
        return False
    cac_so_hop_le = []
    for so in cac_tham_so:
        so = so.strip()
        if la_so_dien_thoai_hop_le(so):
            cac_so_hop_le.append(so)
    if not cac_so_hop_le:
        await gui_phan_hoi(
            message,
            "🫡 Các số điện thoại không hợp lệ!",
            xoa_tin_nguoi_dung=True,
            tu_dong_xoa_sau_giay=10
        )
        return False
    so_dien_thoai = cac_so_hop_le[0]
    thanh_cong, exec_info = await execute_with_swap(
        "free", [so_dien_thoai], user_id, rounds=2
    )
    if not thanh_cong:
        await gui_phan_hoi(
            message,
            "🫡 Lỗi khi khởi động tiến trình!",
            xoa_tin_nguoi_dung=True,
            tu_dong_xoa_sau_giay=10
        )
        return False
    tieu_de = lay_tieu_de_quyen(user_id)
    lien_ket_nguoi_dung = dinh_dang_lien_ket_nguoi_dung(user)
    noi_dung = (
        f"{tieu_de}        :        {lien_ket_nguoi_dung}\n"
        f"🆔 𝑀ã 𝐼𝐷              :       {user_id}\n"
        f"📲 𝑃ℎ𝑜𝑛𝑒 𝑉𝑁        :        {so_dien_thoai}\n"
        f"🛰️ 𝑁ℎ𝑎̀ 𝑚𝑎̣𝑛𝑔       :        {get_carrier(so_dien_thoai)}\n"
        f"🪩 𝑉𝑖̣ 𝑡𝑟𝑖́                :        𝑉/𝑁 𝑂𝑛𝑙𝑖𝑛𝑒\n\n"
        f"🚀 𝐿𝑒̣̂𝑛ℎ ✧𝐅𝐫𝐞𝐞✧ 𝑑𝑎̃ 𝑐ℎ𝑎̣𝑦 𝑡ℎ𝑎̀𝑛ℎ 𝑐𝑜̂𝑛𝑔 \n"
        f" 𝐺𝑖𝑎̉𝑚 𝑡ℎ𝑜̛̀𝑖 𝑔𝑖𝑎𝑛 𝑥𝑢𝑜̂́𝑛𝑔 𝑐𝑜̀𝑛 𝟻𝟶𝑠 !🎯\n"
    )
    await gui_phan_hoi(
        message,
        noi_dung,
        xoa_tin_nguoi_dung=True,
        luu_vinh_vien=True,
        co_keyboard=True,
        photo_path="imagehack.jpg",
    )
    return True
@cooldown_decorator
@chi_nhom
@chi_vip_vinh_vien
async def xu_ly_vip(message: Message):
    if not message.from_user:
        return False
    user = message.from_user
    user_id = user.id
    cac_tham_so = trich_xuat_tham_so(message)
    if not cac_tham_so:
        await gui_phan_hoi(
            message,
            "🫡 /vip 0989299990...𝑇𝑜̂́𝑖 𝑑𝑎 10 𝑠𝑜̂́ !",
            xoa_tin_nguoi_dung=True,
            tu_dong_xoa_sau_giay=10
        )
        return False
    if len(cac_tham_so) > 10:
        await gui_phan_hoi(
            message,
            "🫡 𝐿𝑒̣̂𝑛ℎ /vip 𝑐ℎ𝑖̉ 𝑐ℎ𝑜 𝑝ℎ𝑒́𝑝 𝑡𝑜̂́𝑖 𝑑𝑎 𝟷𝟶 𝑠𝑜̂́!",
            xoa_tin_nguoi_dung=True,
            tu_dong_xoa_sau_giay=10
        )
        return False
    cac_so_hop_le = []
    for so in cac_tham_so[:10]:
        so = so.strip()
        if la_so_dien_thoai_hop_le(so):
            cac_so_hop_le.append(so)
    if not cac_so_hop_le:
        await gui_phan_hoi(
            message,
            "🫡 Các số điện thoại không hợp lệ!",
            xoa_tin_nguoi_dung=True,
            tu_dong_xoa_sau_giay=10
        )
        return False
    thanh_cong, exec_info = await execute_with_swap(
        "vip", cac_so_hop_le, user_id, rounds=3
    )
    if not thanh_cong:
        await gui_phan_hoi(
            message,
            f"🫡 Không thể khởi tạo tiến trình VIP nào!",
            xoa_tin_nguoi_dung=True,
            tu_dong_xoa_sau_giay=10
        )
        return False
    tieu_de = lay_tieu_de_quyen(user_id)
    lien_ket_nguoi_dung = dinh_dang_lien_ket_nguoi_dung(user)
    noi_dung = (
        f"{tieu_de}       :       {lien_ket_nguoi_dung}\n"
        f"🆔 𝑀ã 𝐼𝐷             :       {user_id}\n"
        f"📲 𝑁ℎ𝑎̣̂𝑝 𝑇𝑎𝑦        :       {len(cac_so_hop_le)} 𝑠𝑜̂́ 𝐻𝑜̛̣𝑝 𝑙𝑒̣̂\n"
        f"⚡ 𝑇𝑖𝑒̂́𝑛 𝑡𝑟𝑖̀𝑛ℎ        :       𝑆𝑜𝑛𝑔 𝑠𝑜𝑛𝑔\n"
        f"🪩 𝑉𝑖̣ 𝑡𝑟𝑖́                :        𝑉/𝑁 𝑂𝑛𝑙𝑖𝑛𝑒\n\n"
        f"🚀 𝐿𝑒̣̂𝑛ℎ ✧𝐕𝐈𝐏✧ 𝑑𝑎̃ 𝑐ℎ𝑎̣𝑦 𝑡ℎ𝑎̀𝑛ℎ 𝑐𝑜̂𝑛𝑔\n"
        f" 𝑇ℎ𝑜̛̀𝑖 𝑔𝑖𝑎𝑛 𝑐ℎ𝑎̣𝑦 30 𝑝ℎ𝑢́𝑡... ! 🫡 \n"
    )
    await gui_phan_hoi(
        message,
        noi_dung,
        xoa_tin_nguoi_dung=True,
        luu_vinh_vien=True,
        co_keyboard=True,
        photo_path="imagehack.jpg",
    )
    return True
@cooldown_decorator
@chi_nhom
@chi_vip_vinh_vien
async def xu_ly_call(message: Message):
    if not message.from_user:
        return False
    user = message.from_user
    user_id = user.id
    cac_tham_so = trich_xuat_tham_so(message)
    if len(cac_tham_so) != 1:
        await gui_phan_hoi(
            message,
            "🫡 𝐶𝑢́ 𝑝ℎ𝑎́𝑝: /𝑐𝑎𝑙𝑙 0989226998 1 𝑠𝑜̂́ 𝑚𝑜̂̃𝑖 𝑙𝑎̂̀𝑛 !",
            xoa_tin_nguoi_dung=True,
            tu_dong_xoa_sau_giay=10
        )
        return False
    so_dien_thoai = cac_tham_so[0].strip()
    if not la_so_dien_thoai_hop_le(so_dien_thoai):
        await gui_phan_hoi(
            message,
            "🫡 Số điện thoại không hợp lệ!",
            xoa_tin_nguoi_dung=True,
            tu_dong_xoa_sau_giay=10
        )
        return False
    thanh_cong, exec_info = await execute_with_swap(
        "call", [so_dien_thoai], user_id, rounds=1
    )
    if not thanh_cong:
        await gui_phan_hoi(
            message,
            "🫡 Lỗi khi khởi động tiến trình!",
            xoa_tin_nguoi_dung=True,
            tu_dong_xoa_sau_giay=10
        )
        return False
    tieu_de = lay_tieu_de_quyen(user_id)
    lien_ket_nguoi_dung = dinh_dang_lien_ket_nguoi_dung(user)
    noi_dung = (
        f"{tieu_de}       :        {lien_ket_nguoi_dung}\n"
        f"🆔 𝑀ã 𝐼𝐷              :       {user_id}\n"
        f"📲 𝑃ℎ𝑜𝑛𝑒 𝑉𝑁        :        {so_dien_thoai}\n"
        f"🛰️ 𝑁ℎ𝑎̀ 𝑚𝑎̣𝑛𝑔       :        {get_carrier(so_dien_thoai)}\n"
        f"🪩 𝑉𝑖̣ 𝑡𝑟𝑖́                :        𝑉/𝑁 𝑂𝑛𝑙𝑖𝑛𝑒\n\n"
        f"🚀 𝐿𝑒̣̂𝑛ℎ ✧𝐂𝐀𝐋𝐋✧ 𝑑𝑎̃ 𝑐ℎ𝑎̣𝑦 𝑡ℎ𝑎̀𝑛ℎ 𝑐𝑜̂𝑛𝑔\n"
        f" 𝑇ℎ𝑜̛̀𝑖 𝑔𝑖𝑎𝑛 𝑐𝑜́ ℎ𝑖𝑒̣̂𝑢 𝑙𝑢̛̣𝑐 10 𝑝ℎ𝑢́𝑡 ! 🫡\n"
    )
    await gui_phan_hoi(
        message,
        noi_dung,
        xoa_tin_nguoi_dung=True,
        luu_vinh_vien=True,
        co_keyboard=True,
        photo_path="imagehack.jpg",
    )
    return True
@cooldown_decorator
@chi_nhom
@chi_vip_vinh_vien
async def xu_ly_full(message: Message):
    if not message.from_user:
        return False
    user = message.from_user
    user_id = user.id
    cac_tham_so = trich_xuat_tham_so(message)
    if not cac_tham_so:
        await gui_phan_hoi(
            message,
            "🫡 ℂ𝕦́ 𝕡𝕙𝕒́𝕡: /𝕗𝕦𝕝𝕝 0909778998....\nℂ𝕙𝕒̣𝕪 𝕝𝕚𝕖̂𝑛 𝕥𝕦̣c 24𝕙 - 𝕍𝕀ℙ 𝕔𝕙𝕚̉ 1 𝕤𝕠̂́ 𝕞𝕠̂̃𝕚 𝕝𝕒̂̀𝕟 !",
            xoa_tin_nguoi_dung=True,
            tu_dong_xoa_sau_giay=10
        )
        return False
    if len(cac_tham_so) > 1:
        await gui_phan_hoi(
            message,
            "🫡 VIP chỉ được phép nhập 1 số cho lệnh full!",
            xoa_tin_nguoi_dung=True,
            tu_dong_xoa_sau_giay=10
        )
        return False
    cac_so_hop_le = []
    for so in cac_tham_so:
        so = so.strip()
        if la_so_dien_thoai_hop_le(so):
            cac_so_hop_le.append(so)
    if not cac_so_hop_le:
        await gui_phan_hoi(
            message,
            "🫡 Các số điện thoại không hợp lệ!",
            xoa_tin_nguoi_dung=True,
            tu_dong_xoa_sau_giay=10
        )
        return False
    so_dien_thoai = cac_so_hop_le[0]
    thanh_cong, exec_info = await execute_with_swap("full", [so_dien_thoai], user_id)
    if not thanh_cong:
        await gui_phan_hoi(
            message,
            "🫡 Không thể khởi tạo tiến trình FULL!",
            xoa_tin_nguoi_dung=True,
            tu_dong_xoa_sau_giay=10
        )
        return False
    tieu_de = lay_tieu_de_quyen(user_id)
    lien_ket_nguoi_dung = dinh_dang_lien_ket_nguoi_dung(user)
    noi_dung = (
        f"{tieu_de}       :        {lien_ket_nguoi_dung}\n"
        f"🆔 𝑀ã 𝐼𝐷              :       {user_id}\n"
        f"📲 𝑃ℎ𝑜𝑛𝑒 𝑉𝑁        :        {so_dien_thoai}\n"
        f"🛰️ 𝑁ℎ𝑎̀ 𝑚𝑎̣𝑛𝑔       :        {get_carrier(so_dien_thoai)}\n"
        f"🪩 𝑉𝑖̣ 𝑡𝑟𝑖́                :        𝑉/𝑁 𝑂𝑛𝑙𝑖𝑛𝑒\n\n"
        f"🚀 𝐿𝑒̣̂𝑛ℎ ✧𝐅𝐮𝐥𝐥 24/7✧ 𝑑𝑎̃ 𝑐ℎ𝑎̣𝑦 𝑡ℎ𝑎̀𝑛ℎ 𝑐𝑜̂𝑛𝑔\n"
        f" 𝑇𝑖𝑛 𝑛ℎ𝑎̆́𝑛 𝑣𝑎̀ 𝑐𝑢𝑜̣̂𝑐 𝑔𝑜̣𝑖 𝑐ℎ𝑎̣𝑚 𝑟𝑎̉𝑖 𝑟𝑎́𝑐 ! 🫡\n"
    )
    await gui_phan_hoi(
        message,
        noi_dung,
        xoa_tin_nguoi_dung=True,
        luu_vinh_vien=True,
        co_keyboard=True,
        photo_path="imagehack.jpg",
    )
    return True
@cooldown_decorator
@chi_nhom
@chi_vip_vinh_vien
async def xu_ly_tiktok(message: Message):
    if not message.from_user:
        return False
    user = message.from_user
    user_id = user.id
    cac_tham_so = trich_xuat_tham_so(message)
    if len(cac_tham_so) != 1:
        await gui_phan_hoi(
            message,
            "🫡 Cú pháp: /tiktok [link video tiktok]",
            xoa_tin_nguoi_dung=True,
            tu_dong_xoa_sau_giay=10
        )
        return False
    link_tiktok = cac_tham_so[0].strip()
    if not ("tiktok.com" in link_tiktok or "vm.tiktok.com" in link_tiktok):
        await gui_phan_hoi(
            message,
            "🫡 Link TikTok không hợp lệ!",
            xoa_tin_nguoi_dung=True,
            tu_dong_xoa_sau_giay=10
        )
        return False
    script_tiktok = "tt.py"
    thanh_cong, pid = chay_script_don_gian(
        f"python3 {script_tiktok} {link_tiktok} 1000", user_id, command_type="tiktok"
    )
    if not thanh_cong:
        await gui_phan_hoi(
            message,
            "🫡 Lỗi khi khởi động lệnh tiktok!",
            xoa_tin_nguoi_dung=True,
            tu_dong_xoa_sau_giay=10
        )
        return False
    tieu_de = lay_tieu_de_quyen(user_id)
    lien_ket_nguoi_dung = dinh_dang_lien_ket_nguoi_dung(user)
    noi_dung = f"""{tieu_de}     :     {lien_ket_nguoi_dung}
🆔 𝑀ã 𝐼𝐷          :        {user_id}
🎬 Link            :       {escape_html(link_tiktok[:30])}
🪩 𝑆𝑒𝑟𝑣𝑒𝑟           :          𝑂𝑛𝑙𝑖𝑛𝑒
🚀 𝐿𝑒̣̂𝑛ℎ 𝑡ℎ𝑎̀𝑛ℎ 𝑐𝑜̂𝑛𝑔〔❨✧𝐓𝐢𝐤𝐓𝐨𝐤✧❩〕"""
    await gui_phan_hoi(
        message, noi_dung, xoa_tin_nguoi_dung=True, luu_vinh_vien=True, co_keyboard=True
    )
    return True
@cooldown_decorator
@chi_nhom
async def xu_ly_ngl(message: Message):
    if not message.from_user:
        return False
    user = message.from_user
    user_id = user.id
    cac_tham_so = trich_xuat_tham_so(message)
    if len(cac_tham_so) != 1:
        await gui_phan_hoi(
            message,
            "🫡 Cú pháp: /ngl [link ngl]",
            xoa_tin_nguoi_dung=True,
            tu_dong_xoa_sau_giay=10
        )
        return False
    link_ngl = cac_tham_so[0].strip()
    if not ("ngl.link" in link_ngl):
        await gui_phan_hoi(
            message,
            "🫡 Link NGL không hợp lệ!",
            xoa_tin_nguoi_dung=True,
            tu_dong_xoa_sau_giay=10
        )
        return False
    script_ngl = "spamngl.py"
    thanh_cong, pid = chay_script_don_gian(
        f"python3 {script_ngl} {link_ngl} 1000", user_id, command_type="ngl"
    )
    if not thanh_cong:
        await gui_phan_hoi(
            message,
            "🫡 Lỗi khi khởi động lệnh NGL!",
            xoa_tin_nguoi_dung=True,
            tu_dong_xoa_sau_giay=10
        )
        return False
    tieu_de = lay_tieu_de_quyen(user_id)
    lien_ket_nguoi_dung = dinh_dang_lien_ket_nguoi_dung(user)
    chuoi_gio, chuoi_ngay = lay_thoi_gian_vn()  # Ensure chuoi_gio is defined
    noi_dung = f"""{tieu_de}     :     {lien_ket_nguoi_dung}
🆔 𝑀ã 𝐼𝐷          :           {user_id}
🛰️ Link           :          {escape_html(link_ngl[:30])}...
🎬 Target         :          1000+ 𝑚𝑒𝑠𝑠𝑎𝑔𝑒𝑠
🕜 𝑇ℎ𝑜̛̀𝑖 𝑔𝑖𝑎𝑛        :          {chuoi_gio}
🚀 𝐿𝑒̣̂𝑛ℎ 𝑡ℎ𝑎̀𝑛ℎ 𝑐𝑜̂𝑛𝑔〔❨✧𝐍𝐆𝐋✧❩〕"""
    await gui_phan_hoi(
        message, noi_dung, xoa_tin_nguoi_dung=True, luu_vinh_vien=True, co_keyboard=True
    )
    return True
@cooldown_decorator
@chi_nhom
async def xu_ly_random_anh(message: Message):
    if not message.from_user:
        return False
    user = message.from_user
    user_id = user.id
    danh_sach_anh = doc_file_js("/root/denvkl/images.js")
    if not danh_sach_anh:
        await gui_phan_hoi(
            message,
            "🫡 Không tìm thấy danh sách ảnh!",
            xoa_tin_nguoi_dung=True,
            tu_dong_xoa_sau_giay=10
        )
        return False
    anh_random = random.choice(danh_sach_anh)
    lien_ket_nguoi_dung = dinh_dang_lien_ket_nguoi_dung(user)
    chuoi_gio, chuoi_ngay = lay_thoi_gian_vn()
    noi_dung = f"🏓 Random Ảnh cho {lien_ket_nguoi_dung}\n⏱️ Thời gian: {chuoi_gio} - {chuoi_ngay}"
    try:
        await asyncio.wait_for(
            bot.send_photo(
                chat_id=message.chat.id,
                photo=anh_random,
                caption=f"<blockquote>{noi_dung}</blockquote>",
                parse_mode=ParseMode.HTML,
            ),
            timeout=30.0,
        )
        try:
            await bot.delete_message(
                chat_id=message.chat.id, message_id=message.message_id
            )
        except Exception:
            pass
        return True
    except asyncio.TimeoutError:
        await gui_phan_hoi(
            message,
            "🫡 Timeout khi tải ảnh! Thử lại sau.",
            xoa_tin_nguoi_dung=True,
            tu_dong_xoa_sau_giay=10
        )
        return False
    except Exception as e:
        # Bỏ logging lỗi để tăng hiệu suất
        if "failed to get HTTP URL content" in str(e) and len(danh_sach_anh) > 1:
            anh_backup = random.choice([a for a in danh_sach_anh if a != anh_random])
            try:
                await bot.send_photo(
                    chat_id=message.chat.id,
                    photo=anh_backup,
                    caption=f"<blockquote>{noi_dung}</blockquote>",
                    parse_mode=ParseMode.HTML,
                )
                return True
            except Exception:
                pass
        await gui_phan_hoi(
            message,
            "🫡 Không thể tải ảnh! URL có thể bị lỗi.",
            xoa_tin_nguoi_dung=True,
            tu_dong_xoa_sau_giay=10
        )
        return False
@cooldown_decorator
@chi_nhom
async def xu_ly_random_video(message: Message):
    if not message.from_user:
        return False
    user = message.from_user
    user_id = user.id
    danh_sach_video = doc_file_js("/root/denvkl/videos.js")
    danh_sach_gif = doc_file_js("/root/denvkl/video2.js")
    tat_ca_video = danh_sach_video + danh_sach_gif
    if not tat_ca_video:
        await gui_phan_hoi(
            message,
            "🫡 Không tìm thấy danh sách video!",
            xoa_tin_nguoi_dung=True,
            tu_dong_xoa_sau_giay=10
        )
        return False
    video_random = random.choice(tat_ca_video)
    lien_ket_nguoi_dung = dinh_dang_lien_ket_nguoi_dung(user)
    chuoi_gio, chuoi_ngay = lay_thoi_gian_vn()
    try:
        if video_random.endswith(".gif") or "giphy" in video_random:
            await asyncio.wait_for(
                bot.send_animation(
                    chat_id=message.chat.id,
                    animation=video_random,
                    caption=f"<blockquote>🎬 𝑅𝑎𝑛𝑑𝑜𝑚 𝐺𝐼𝐹 𝑐ℎ𝑜 {lien_ket_nguoi_dung}\n"
                    f"⏱️ Thời gian: {chuoi_gio} - {chuoi_ngay}</blockquote>",
                    parse_mode=ParseMode.HTML,
                ),
                timeout=45.0,
            )
        else:
            await asyncio.wait_for(
                bot.send_video(
                    chat_id=message.chat.id,
                    video=video_random,
                    caption=f"<blockquote>🎬 𝑅𝑎𝑛𝑑𝑜𝑚 𝑉𝑖𝑑𝑒𝑜 𝑐ℎ𝑜 {lien_ket_nguoi_dung}\n"
                    f"⏱️ Thời gian: {chuoi_gio} - {chuoi_ngay}</blockquote>",
                    parse_mode=ParseMode.HTML,
                ),
                timeout=45.0,
            )
        try:
            await bot.delete_message(
                chat_id=message.chat.id, message_id=message.message_id
            )
        except Exception:
            pass
        return True
    except asyncio.TimeoutError:
        await gui_phan_hoi(
            message,
            "🫡 Timeout khi tải video! File quá lớn.",
            xoa_tin_nguoi_dung=True,
            tu_dong_xoa_sau_giay=10
        )
        return False
    except Exception as e:
        if "failed to get HTTP URL content" in str(e) and len(tat_ca_video) > 1:
            video_backup = random.choice([v for v in tat_ca_video if v != video_random])
            try:
                if video_backup.endswith(".gif") or "giphy" in video_backup:
                    await bot.send_animation(
                        chat_id=message.chat.id,
                        animation=video_backup,
                        caption=f"<blockquote>🎬 Random GIF cho {lien_ket_nguoi_dung}\n"
                        f"⏱️ Thời gian: {chuoi_gio} - {chuoi_ngay}</blockquote>",
                        parse_mode=ParseMode.HTML,
                    )
                else:
                    await bot.send_video(
                        chat_id=message.chat.id,
                        video=video_backup,
                        caption=f"<blockquote>🎬 Random Video cho {lien_ket_nguoi_dung}\n"
                        f"⏱️ Thời gian: {chuoi_gio} - {chuoi_ngay}</blockquote>",
                        parse_mode=ParseMode.HTML,
                    )
                return True
            except Exception:
                pass
        await gui_phan_hoi(
            message,
            "🫡 Không thể tải video! URL có thể bị lỗi.",
            xoa_tin_nguoi_dung=True,
            tu_dong_xoa_sau_giay=10
        )
        return False
@chi_nhom
async def xu_ly_checkid(message: Message):
    if not message.from_user:
        return False
    user = message.from_user
    user_id = user.id
    is_admin = la_admin(user_id)
    target_user = None
    target_user_id = None
    target_user_name = None
    cac_tham_so = trich_xuat_tham_so(message)
    if message.reply_to_message and message.reply_to_message.from_user:
        if is_admin:
            target_user = message.reply_to_message.from_user
            target_user_id = target_user.id
            target_user_name = target_user.full_name or target_user.first_name
    elif len(cac_tham_so) >= 1 and is_admin:
        try:
            target_user_id = int(cac_tham_so[0].strip())
            try:
                chat_info = await bot.get_chat(target_user_id)
                target_user_name = (
                    chat_info.full_name
                    or chat_info.first_name
                    or f"User {target_user_id}"
                )
            except:
                target_user_name = f"User {target_user_id}"
        except ValueError:
            pass
    if target_user_id and is_admin:
        check_user_id = target_user_id
        check_user_name = target_user_name
        lien_ket_nguoi_dung = (
            f'<a href="tg://user?id={check_user_id}">{escape_html(check_user_name)}</a>'
        )
    else:
        check_user_id = user_id
        check_user_name = user.full_name or user.first_name
        lien_ket_nguoi_dung = dinh_dang_lien_ket_nguoi_dung(user)
    cap_do = lay_cap_do_quyen_nguoi_dung(check_user_id)
    tieu_de = lay_tieu_de_quyen(check_user_id)
    expiry_info = ""
    if cap_do == "vip":
        try:
            conn = tao_ket_noi_db()
            cursor = conn.cursor()
            cursor.execute(
                "SELECT expiry_date FROM admin WHERE user_id = ? AND role = 'vip'",
                (str(check_user_id),),
            )
            result = cursor.fetchone()
            conn.close()
            if result and result["expiry_date"]:
                expiry_date = datetime.fromisoformat(result["expiry_date"])
                now = datetime.now()
                remaining = expiry_date - now
                if remaining.total_seconds() > 0:
                    days = remaining.days
                    hours = remaining.seconds // 3600
                    expiry_info = (
                        f"⏰ 𝐶𝑜̀𝑛 𝑙𝑎̣𝑖           :       {days} 𝑛𝑔𝑎̀𝑦 {hours} 𝑔𝑖𝑜̛̀"
                    )
                    expiry_info += f"📅 𝐻𝑒̂́𝑡 ℎ𝑎̣𝑛          :       {expiry_date.strftime('%d/%m/%Y %H:%M')}\n\n"
                else:
                    expiry_info = "\n⚠️ 𝑉𝐼𝑃 đ𝑎̃ ℎ𝑒̂́𝑡 ℎ𝑎̣𝑛!\n\n"
        except:
            pass
    elif cap_do == "super_vip":
        try:
            conn = tao_ket_noi_db()
            cursor = conn.cursor()
            cursor.execute(
                "SELECT expiry_date FROM admin WHERE user_id = ? AND role = 'super_vip'",
                (str(check_user_id),),
            )
            result = cursor.fetchone()
            conn.close()
            if result and result["expiry_date"]:
                expiry_date = datetime.fromisoformat(result["expiry_date"])
                now = datetime.now()
                remaining = expiry_date - now
                if remaining.total_seconds() > 0:
                    days = remaining.days
                    hours = remaining.seconds // 3600
                    expiry_info = (
                        f"⏰ 𝐶𝑜̀𝑛 𝑙𝑎̣𝑖           :       {days} 𝑛𝑔𝑎̀𝑦 {hours} 𝑔𝑖𝑜̛̀"
                    )
                    expiry_info += f"📅 𝐻𝑒̂́𝑡 ℎ𝑎̣𝑛 𝑣𝑎̀𝑜 𝑛𝑔𝑎̀𝑦 {expiry_date.strftime('%d/%m/%Y %H:%M')}\n\n"
                else:
                    expiry_info = "\n⚠️ 𝑆𝑈𝑃𝐸𝑅 𝑉𝐼𝑃 đ𝑎̃ ℎ𝑒̂́𝑡 ℎ𝑎̣𝑛!\n\n"
        except:
            pass
    elif cap_do == "admin":
        expiry_info = " 𝑇𝑢̛̣ 𝐻𝑜𝑎̀𝑛 𝑇ℎ𝑖𝑒̣̂𝑛 ! "
    quyen_text = {
        "admin": "🪬 𝐴𝑑𝑚𝑖𝑛",
        "super_vip": "💸 𝑺𝒖𝒑𝒆𝒓𝑽𝑰𝑷",
        "vip": "🌀 𝑉𝐼𝑃",
        "member": "👤 𝑀𝑒𝑚𝑏𝑒𝑟",
    }.get(cap_do, cap_do)
    noi_dung = f"""{tieu_de}      :       {lien_ket_nguoi_dung}
🆔 𝑀ã 𝐼𝐷            :       {check_user_id}
✨ 𝑄𝑢𝑦𝑒̂̀𝑛            :       {quyen_text}
{expiry_info}
"""
    if cap_do == "member":
        noi_dung += "\n𝐵𝑎̣𝑛 𝑐𝑜́ 𝑡ℎ𝑒̂̉ 𝑑𝑢̀𝑛𝑔 𝑙𝑒̣̂𝑛ℎ 𝑓𝑟𝑒𝑒 𝑣𝑎̀ 𝑠𝑚𝑠 𝑚𝑖𝑒̂̃𝑛 𝑝ℎ𝑖́ !"
    elif cap_do == "vip":
        noi_dung += "\n🎯 𝑆𝑢̛̉ 𝑑𝑢̣𝑛𝑔 đ𝑎̂̀𝑦 đ𝑢̉ 𝑐𝑎́𝑐 𝑙𝑒̣̂𝑛ℎ 𝑉𝐼𝑃!"
    elif cap_do == "super_vip":
        noi_dung += "\n🏆 𝑆𝑢̛̉ 𝑑𝑢̣𝑛𝑔 𝑡𝑎̂́𝑡 𝑐𝑎̉ 𝑘ℎ𝑜̂𝑛𝑔 𝑔𝑖𝑜̛́𝑖 ℎ𝑎̣𝑛 𝑆𝑈𝑃𝐸𝑅 𝑉𝐼𝑃!"
    elif cap_do == "admin":
        noi_dung += "\n🎯 𝑇𝑜𝑎̀𝑛 𝑞𝑢𝑦𝑒̂̀𝑛 𝑞𝑢𝑎̉𝑛 𝑡𝑟𝑖̣ ℎ𝑒̣̂ 𝑡ℎ𝑜̂́𝑛𝑔!"
    await gui_phan_hoi(
        message, noi_dung, xoa_tin_nguoi_dung=True, luu_vinh_vien=True, co_keyboard=True
    )
    return True
@cooldown_decorator
@chi_nhom
async def xu_ly_myref(message: Message):
    if not message.from_user:
        return False
    user = message.from_user
    user_id = str(user.id)
    try:
        conn = tao_ket_noi_db()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT COUNT(*) FROM referrals WHERE referrer_id = ?", (user_id,)
        )
        count = cursor.fetchone()[0]
        conn.close()
        ref_link = (
            f"https://t.me/{BOT_USERNAME}?start=ref_{user_id}"
            if BOT_USERNAME
            else "Đang tải..."
        )
        progress = min(count, 10)
        progress_bar = "█" * progress + "░" * (10 - progress)
        cap_do = lay_cap_do_quyen_nguoi_dung(user_id)
        is_vip = cap_do in ("admin", "vip")
        tieu_de = lay_tieu_de_quyen(user_id)
        lien_ket_nguoi_dung = dinh_dang_lien_ket_nguoi_dung(user)
        noi_dung = f"""{tieu_de}     :      {lien_ket_nguoi_dung}
🔗 𝐿𝑖𝑛𝑘 𝑐𝑢̉𝑎 𝑏𝑎̣𝑛:
{ref_link}
🚀 𝑇𝑖𝑒̂́𝑛 𝑑𝑜̣̂ : {count}/10 𝑛𝑔𝑢̛𝑜̛̀𝑖
{progress_bar}
"""
        if is_vip:
            noi_dung += "𝑩𝒂̣𝒏 𝒅𝒂̃ 𝒍𝒂̀ 𝑽𝑰𝑷 𝒓𝒐̂̀𝒊 !"
        elif count >= 10:
            noi_dung += "🎉 𝑩𝒂̣𝒏 𝒅𝒂̃ 𝒅𝒖̉ 10 𝒏𝒈𝒖̛𝒐̛̀𝒊, 𝒄𝒉𝒖́𝒄 𝒎𝒖̛̀𝒏𝒈 𝒍𝒆̂𝒏 𝑽𝑰𝑷 !"
        else:
            con_lai = 10 - count
            noi_dung += f"💪 𝐶𝑜̀𝑛 {con_lai} 𝑛𝑔𝑢̛𝑜̛̀𝑖 𝑛𝑢̛̃𝑎 𝑙𝑎̀ 𝑙𝑒̂𝑛 𝑉𝐼𝑃 !\n\n📝 𝐶𝑎́𝑐ℎ 𝑙𝑎̀𝑚:\n1️⃣ 𝐶ℎ𝑖𝑎 𝑠𝑒̉ 𝑙𝑖𝑛𝑘 𝑐𝑢̉𝑎 𝑏𝑎̣𝑛\n2️⃣ 𝑀𝑜̛̀𝑖 𝑏𝑎̣𝑛 𝑏𝑒̀ 𝑡ℎ𝑎𝑚 𝑔𝑖𝑎 𝑛ℎ𝑜́𝑚\n3️⃣ 𝐻𝑜̣ c𝑙𝑖𝑐𝑘 𝑣𝑎̀𝑜 𝑙𝑖𝑛𝑘 𝑣𝑎̀ 𝑛ℎ𝑎̣̂𝑝 𝑙𝑒̣̂𝑛ℎ 𝒔𝒕𝒂𝒓𝒕\n"
        await gui_phan_hoi(
            message,
            noi_dung,
            xoa_tin_nguoi_dung=True,
            luu_vinh_vien=True,
            co_keyboard=True,
        )
        return True
    except Exception:
        await gui_phan_hoi(
            message,
            "🫡 Lỗi khi lấy thông tin referral!",
            xoa_tin_nguoi_dung=True,
            tu_dong_xoa_sau_giay=10
        )
        return False
@cooldown_decorator
@chi_nhom
@chi_admin
async def xu_ly_them_vip(message: Message):
    if not message.from_user:
        return False
    user = message.from_user
    cac_tham_so = trich_xuat_tham_so(message)
    if len(cac_tham_so) < 1:
        await gui_phan_hoi(
            message,
            "🫡 Cú pháp: /themvip USER_ID [TÊN]",
            xoa_tin_nguoi_dung=True,
            tu_dong_xoa_sau_giay=10
        )
        return False
    id_muc_tieu = cac_tham_so[0].strip()
    ten_muc_tieu = " ".join(cac_tham_so[1:]) if len(cac_tham_so) > 1 else "VIP User"
    try:
        them_vip(id_muc_tieu, ten_muc_tieu, user.id)
        # Tạo liên kết người dùng cho user được thêm VIP
        lien_ket_muc_tieu = (
            f'<a href="tg://user?id={id_muc_tieu}">{escape_html(ten_muc_tieu)}</a>'
        )
        lien_ket_admin = dinh_dang_lien_ket_nguoi_dung(user)
        noi_dung = (
            f"✨ 𝐷𝑎̃ 𝑡ℎ𝑒̂𝑚 𝑉𝐼𝑃 𝑡ℎ𝑎̀𝑛ℎ 𝑐𝑜̂𝑛𝑔 !\n\n"
            f"🧞‍♂️ 𝑉𝐼𝑃 𝑀𝑜̛́𝑖         :        {lien_ket_muc_tieu}\n"
            f"🆔 𝑈𝑠𝑒𝑟 𝐼𝐷         :        {id_muc_tieu}\n"
            f"💬 𝑇ℎ𝑒̂𝑚 𝑏𝑜̛̉𝑖        :        {lien_ket_admin}\n"
            f"📅 𝑇ℎ𝑜̛̀𝑖 ℎ𝑎̣𝑛        :        30 𝑛𝑔𝑎̀𝑦"
        )
        await gui_phan_hoi(
            message, noi_dung, xoa_tin_nguoi_dung=True, luu_vinh_vien=True
        )
        return True
    except Exception as e:
        await gui_phan_hoi(
            message,
            f"Lỗi khi thêm VIP: {str(e)}",
            xoa_tin_nguoi_dung=True,
            tu_dong_xoa_sau_giay=10
        )
        return False
@cooldown_decorator
@chi_nhom
@chi_admin
async def xu_ly_xoa_vip(message: Message):
    if not message.from_user:
        return False
    user = message.from_user
    cac_tham_so = trich_xuat_tham_so(message)
    if len(cac_tham_so) != 1:
        await gui_phan_hoi(
            message,
            "🫡 Cú pháp: /xoavip USER_ID",
            xoa_tin_nguoi_dung=True,
            tu_dong_xoa_sau_giay=10
        )
        return False
    id_muc_tieu = cac_tham_so[0].strip()
    try:
        conn = tao_ket_noi_db()
        cursor = conn.cursor()
        # Lấy thông tin VIP trước khi xóa
        cursor.execute(
            "SELECT name FROM admin WHERE user_id = ? AND role = 'vip'", (id_muc_tieu,)
        )
        vip_info = cursor.fetchone()
        cursor.execute(
            "DELETE FROM admin WHERE user_id = ? AND role = 'vip'", (id_muc_tieu,)
        )
        so_hang_xoa = cursor.rowcount
        conn.commit()
        conn.close()
        quyen_cache.set(id_muc_tieu, None)
        if so_hang_xoa > 0:
            ten_vip = vip_info[0] if vip_info and vip_info[0] else "VIP User"
            lien_ket_vip_xoa = (
                f'<a href="tg://user?id={id_muc_tieu}">{escape_html(ten_vip)}</a>'
            )
            lien_ket_admin = dinh_dang_lien_ket_nguoi_dung(user)
            noi_dung = (
                f"🗑️ 𝐷𝑎̃ 𝑥𝑜́𝑎 𝑉𝐼𝑃 !\n\n"
                f"👤 𝑁𝑔𝑢̛𝑜̛̀𝑖 𝑏𝑖̣ 𝑥𝑜́𝑎      :       {lien_ket_vip_xoa}\n"
                f"🆔 𝑈𝑠𝑒𝑟 𝐼𝐷          :       {id_muc_tieu}\n\n"
                f"🥷🏿 𝑋𝑜́𝑎 𝑏𝑜̛̉𝑖          :       {lien_ket_admin}"
            )
        else:
            lien_ket_khong_tim_thay = (
                f'<a href="tg://user?id={id_muc_tieu}">User {id_muc_tieu}</a>'
            )
            noi_dung = f"⚠️ 𝐾ℎ𝑜̂𝑛𝑔 𝑡𝑖̀𝑚 𝑡ℎ𝑎̂́𝑦 𝑉𝐼𝑃: {lien_ket_khong_tim_thay}"
        await gui_phan_hoi(
            message, noi_dung, xoa_tin_nguoi_dung=True, luu_vinh_vien=True
        )
        return True
    except Exception as e:
        await gui_phan_hoi(
            message,
            f"Lỗi khi xóa VIP: {str(e)}",
            xoa_tin_nguoi_dung=True,
            tu_dong_xoa_sau_giay=10
        )
        return False
@cooldown_decorator
@chi_nhom
@chi_admin
async def xu_ly_them_super_vip(message: Message):
    if not message.from_user:
        return False
    user = message.from_user
    cac_tham_so = trich_xuat_tham_so(message)
    if len(cac_tham_so) < 1:
        await gui_phan_hoi(
            message,
            "🫡 Cú pháp: /themsuper USER_ID ",
            xoa_tin_nguoi_dung=True,
            tu_dong_xoa_sau_giay=10
        )
        return False
    id_muc_tieu = cac_tham_so[0].strip()
    so_ngay = 30
    if len(cac_tham_so) >= 2:
        try:
            so_ngay = int(cac_tham_so[1].strip())
        except:
            pass
    ten_muc_tieu = (
        " ".join(cac_tham_so[2:])
        if len(cac_tham_so) > 2
        else " ".join(cac_tham_so[1:])
        if len(cac_tham_so) > 1
        else "Super VIP"
    )
    try:
        them_super_vip(id_muc_tieu, ten_muc_tieu, days=so_ngay, admin_added_by=user.id)
        lien_ket_muc_tieu = (
            f'<a href="tg://user?id={id_muc_tieu}">{escape_html(ten_muc_tieu)}</a>'
        )
        lien_ket_admin = dinh_dang_lien_ket_nguoi_dung(user)
        noi_dung = (
            f"✨ 𝐷𝑎̃ 𝑡ℎ𝑒̂𝑚 𝑆𝑈𝑃𝐸𝑅 𝑉𝐼𝑃 𝑡ℎ𝑎̀𝑛ℎ 𝑐𝑜̂𝑛𝑔 !\n\n"
            f"🏆 𝑆𝑈𝑃𝐸𝑅 𝑉𝐼𝑃 𝑀𝑜̛́𝑖     :         {lien_ket_muc_tieu}\n"
            f"🆔 𝑈𝑠𝑒𝑟 𝐼𝐷           :         {id_muc_tieu}\n"
            f"💬 𝑇ℎ𝑒̂𝑚 𝑏𝑜̛̉𝑖          :         {lien_ket_admin}\n"
            f"📅 𝑇ℎ𝑜̛̀𝑖 ℎ𝑎̣𝑛          :         {so_ngay} 𝑛𝑔𝑎̀𝑦"
        )
        await gui_phan_hoi(
            message, noi_dung, xoa_tin_nguoi_dung=True, luu_vinh_vien=True
        )
        return True
    except Exception as e:
        await gui_phan_hoi(
            message,
            f"Lỗi khi thêm SUPER VIP: {str(e)}",
            xoa_tin_nguoi_dung=True,
            tu_dong_xoa_sau_giay=10
        )
        return False
@cooldown_decorator
@chi_nhom
@chi_admin
async def xu_ly_xoa_super_vip(message: Message):
    if not message.from_user:
        return False
    user = message.from_user
    cac_tham_so = trich_xuat_tham_so(message)
    if len(cac_tham_so) != 1:
        await gui_phan_hoi(
            message,
            "🫡 Cú pháp: /xoasuper USER_ID",
            xoa_tin_nguoi_dung=True,
            tu_dong_xoa_sau_giay=10
        )
        return False
    id_muc_tieu = cac_tham_so[0].strip()
    try:
        conn = tao_ket_noi_db()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT name FROM admin WHERE user_id = ? AND role = 'super_vip'",
            (id_muc_tieu,),
        )
        vip_info = cursor.fetchone()
        cursor.execute(
            "DELETE FROM admin WHERE user_id = ? AND role = 'super_vip'", (id_muc_tieu,)
        )
        so_hang_xoa = cursor.rowcount
        conn.commit()
        conn.close()
        quyen_cache.set(id_muc_tieu, None)
        if so_hang_xoa > 0:
            ten_vip = vip_info[0] if vip_info and vip_info[0] else "Super VIP"
            lien_ket_vip_xoa = (
                f'<a href="tg://user?id={id_muc_tieu}">{escape_html(ten_vip)}</a>'
            )
            lien_ket_admin = dinh_dang_lien_ket_nguoi_dung(user)
            noi_dung = (
                f"🗑️ 𝐷𝑎̃ 𝑥𝑜́𝑎 𝑆𝑈𝑃𝐸𝑅 𝑉𝐼𝑃 !\n\n"
                f"👤 𝑁𝑔𝑢̛𝑜̛̀𝑖 𝑏𝑖̣ 𝑥𝑜́𝑎     :       {lien_ket_vip_xoa}\n"
                f"🆔 𝑈𝑠𝑒𝑟 𝐼𝐷          :        {id_muc_tieu}\n\n"
                f"🥷🏿 𝑋𝑜́𝑎 𝑏𝑜̛̉𝑖          :         {lien_ket_admin}"
            ) 
        else:
            lien_ket_khong_tim_thay = (
                f'<a href="tg://user?id={id_muc_tieu}">User {id_muc_tieu}</a>'
            )
            noi_dung = f"⚠️ 𝐾ℎ𝑜̂𝑛𝑔 𝑡𝑖̀𝑚 𝑡ℎ𝑎̂́𝑦 𝑆𝑈𝑃𝐸𝑅 𝑉𝐼𝑃: {lien_ket_khong_tim_thay}"
        await gui_phan_hoi(
            message, noi_dung, xoa_tin_nguoi_dung=True, luu_vinh_vien=True
        )
        return True
    except Exception as e:
        await gui_phan_hoi(
            message,
            f"Lỗi khi xóa SUPER VIP: {str(e)}",
            xoa_tin_nguoi_dung=True,
            tu_dong_xoa_sau_giay=10
        )
        return False
@cooldown_decorator
@chi_nhom
@chi_admin
async def xu_ly_them_admin(message: Message):
    if not message.from_user:
        return False
    user = message.from_user
    cac_tham_so = trich_xuat_tham_so(message)
    if len(cac_tham_so) < 1:
        await gui_phan_hoi(
            message,
            "🫡 Cú pháp: /themadmin USER_ID [TÊN]",
            xoa_tin_nguoi_dung=True,
            tu_dong_xoa_sau_giay=10
        )
        return False
    id_muc_tieu = cac_tham_so[0].strip()
    ten_muc_tieu = " ".join(cac_tham_so[1:]) if len(cac_tham_so) > 1 else "Admin Thêm"
    try:
        them_admin(id_muc_tieu, ten_muc_tieu)
        noi_dung = f"Đã thêm Admin: {id_muc_tieu} - {ten_muc_tieu}"
        await gui_phan_hoi(
            message, noi_dung, xoa_tin_nguoi_dung=True, luu_vinh_vien=True
        )
        return True
    except Exception as e:
        await gui_phan_hoi(
            message,
            f"Lỗi khi thêm Admin: {str(e)}",
            xoa_tin_nguoi_dung=True,
            tu_dong_xoa_sau_giay=10
        )
        return False
@cooldown_decorator
@chi_nhom
@chi_admin
async def xu_ly_xoa_admin(message: Message):
    if not message.from_user:
        return False
    user = message.from_user
    cac_tham_so = trich_xuat_tham_so(message)
    if len(cac_tham_so) != 1:
        await gui_phan_hoi(
            message,
            "🫡 Cú pháp: /xoaadmin USER_ID",
            xoa_tin_nguoi_dung=True,
            tu_dong_xoa_sau_giay=10
        )
        return False
    id_muc_tieu = cac_tham_so[0].strip()
    if id_muc_tieu == ID_ADMIN_MAC_DINH:
        await gui_phan_hoi(
            message,
            "Không thể xóa Super Admin!",
            xoa_tin_nguoi_dung=True,
            tu_dong_xoa_sau_giay=10
        )
        return False
    try:
        conn = tao_ket_noi_db()
        cursor = conn.cursor()
        cursor.execute(
            "DELETE FROM admin WHERE user_id = ? AND role = 'admin'", (id_muc_tieu,)
        )
        so_hang_xoa = cursor.rowcount
        conn.commit()
        conn.close()
        quyen_cache.set(id_muc_tieu, None)
        if so_hang_xoa > 0:
            noi_dung = f"Đã xóa Admin: {id_muc_tieu}"
        else:
            noi_dung = f"Không tìm thấy Admin: {id_muc_tieu}"
        await gui_phan_hoi(
            message, noi_dung, xoa_tin_nguoi_dung=True, luu_vinh_vien=True
        )
        return True
    except Exception as e:
        await gui_phan_hoi(
            message,
            f"Lỗi khi xóa Admin: {str(e)}",
            xoa_tin_nguoi_dung=True,
            tu_dong_xoa_sau_giay=10
        )
        return False
@chi_nhom
@chi_admin
async def xu_ly_cleanup(message: Message):
    if not message.from_user:
        return False
    user = message.from_user
    lien_ket_admin = dinh_dang_lien_ket_nguoi_dung(user)
    thong_bao = await bot.send_message(
        chat_id=message.chat.id,
        text="<blockquote>🧹 𝐵𝑎̆́𝑡 𝑑𝑎̂̀𝑢 𝑑𝑜̣𝑛 𝑑𝑒̣̃𝑝 𝑉𝑃𝑆...\n⏳ 𝑉𝑢̛̀𝑎 𝑙𝑜̀𝑛𝑔 𝑐ℎ𝑜̛̀...</blockquote>",
        parse_mode=ParseMode.HTML,
    )
    try:
        await bot.delete_message(chat_id=message.chat.id, message_id=message.message_id)
    except:
        pass
    cleanup_stats = {
        "processes_killed": 0,
        "log_files_deleted": 0,
        "cache_cleared": 0,
        "temp_files_deleted": 0,
        "space_freed": 0,
    }
    try:
        all_scripts = set(
            SCRIPT_VIP_DIRECT
            + SCRIPT_SMS_DIRECT
            + SCRIPT_CALL_DIRECT
            + SCRIPT_SPAM_DIRECT
            + SCRIPT_FREE
            + ["tcp.py", "tt.py", "spamngl.py", "pro24h.py"]
        )
        for proc in psutil.process_iter(["pid", "name", "cmdline"]):
            try:
                if "python" not in proc.info["name"].lower():
                    continue
                cmdline = proc.info.get("cmdline", [])
                if len(cmdline) < 2:
                    continue
                script_name = os.path.basename(cmdline[1]) if cmdline[1] else ""
                if script_name in all_scripts:
                    try:
                        proc.kill()
                        proc.wait(timeout=2)
                        cleanup_stats["processes_killed"] += 1
                    except:
                        pass
            except:
                continue
        with PROCESS_LOCK:
            USER_PROCESSES.clear()
        log_extensions = [".log", ".out", ".err"]
        for root, dirs, files in os.walk("/root"):
            for file in files:
                if any(file.endswith(ext) for ext in log_extensions):
                    try:
                        file_path = os.path.join(root, file)
                        file_stat = os.stat(file_path)
                        file_age = time.time() - file_stat.st_mtime
                        if file_age > 86400:  # 24 hours
                            file_size = file_stat.st_size
                            os.remove(file_path)
                            cleanup_stats["log_files_deleted"] += 1
                            cleanup_stats["space_freed"] += file_size
                    except:
                        pass
        cache_dirs = ["/root/.cache", "/tmp", "/var/tmp"]
        for cache_dir in cache_dirs:
            if os.path.exists(cache_dir):
                try:
                    for root, dirs, files in os.walk(cache_dir):
                        for file in files:
                            try:
                                file_path = os.path.join(root, file)
                                # Chỉ xóa file Python cache và file tạm
                                if (
                                    file.endswith(".pyc")
                                    or file.endswith(".pyo")
                                    or file.startswith("tmp")
                                    or "__pycache__" in root
                                ):
                                    file_size = os.path.getsize(file_path)
                                    os.remove(file_path)
                                    cleanup_stats["cache_cleared"] += 1
                                    cleanup_stats["space_freed"] += file_size
                            except:
                                pass
                        for dir_name in dirs[:]:
                            if dir_name == "__pycache__":
                                try:
                                    dir_path = os.path.join(root, dir_name)
                                    if not os.listdir(dir_path):
                                        os.rmdir(dir_path)
                                except:
                                    pass
                except:
                    pass
        if os.path.exists(BASE_DIR):
            for file in os.listdir(BASE_DIR):
                if (
                    file.endswith(".tmp")
                    or file.endswith(".bak")
                    or file.startswith(".")
                ):
                    try:
                        file_path = os.path.join(BASE_DIR, file)
                        if os.path.isfile(file_path):
                            file_size = os.path.getsize(file_path)
                            os.remove(file_path)
                            cleanup_stats["temp_files_deleted"] += 1
                            cleanup_stats["space_freed"] += file_size
                    except:
                        pass
        try:
            conn = tao_ket_noi_db()
            cursor = conn.cursor()
            cursor.execute("VACUUM")
            cursor.execute("REINDEX")
            conn.commit()
            conn.close()
        except:
            pass
        quyen_cache.cache.clear()
        quyen_cache.timestamps.clear()
        cooldown_cache.cache.clear()
        cooldown_cache.timestamps.clear()
        space_freed_mb = cleanup_stats["space_freed"] / (1024 * 1024)
        # Thông báo kết quả
        noi_dung = f"""🧹 𝐷𝑜̣𝑛 𝑑𝑒̣̃𝑝 𝑉𝑃𝑆 ℎ𝑜𝑎̀𝑛 𝑡𝑎̂́𝑡 !
• 🥷🏿 𝑇ℎ𝑢̛̣𝑐 ℎ𝑖𝑒̣̂𝑛 𝑏𝑜̛̉𝑖 : {lien_ket_admin}
• 🚀 𝑉𝑃𝑆 𝑑𝑎̃ 𝑑𝑢̛𝑜̛̣𝑐 𝑡𝑜̂́𝑖 𝑢̛u ℎ𝑜́𝑎 !"""
        # Cập nhật thông báo
        await bot.edit_message_text(
            chat_id=thong_bao.chat.id,
            message_id=thong_bao.message_id,
            text=f"<blockquote>{noi_dung}</blockquote>",
            parse_mode=ParseMode.HTML,
        )
        asyncio.create_task(
            tu_dong_xoa_tin_nhan(thong_bao.chat.id, thong_bao.message_id, 30)
        )
        return True
    except Exception as e:
        try:
            await bot.edit_message_text(
                chat_id=thong_bao.chat.id,
                message_id=thong_bao.message_id,
                text=f"<blockquote>❌ 𝐿𝑜̂̃𝑖 𝑘ℎ𝑖 𝑑𝑜𝑛 𝑑𝑒̣̃𝑝: {str(e)}</blockquote>",
                parse_mode=ParseMode.HTML,
            )
        except:
            pass
        return False
@chi_nhom
@chi_admin
async def xu_ly_xem_danh_sach_vip(message: Message):
    try:
        conn = tao_ket_noi_db()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT user_id, name, admin_added_by, expiry_date, role FROM admin WHERE role IN ('vip', 'super_vip')"
        )
        all_vip_list = cursor.fetchall()
        conn.close()
        if not all_vip_list:
            await gui_phan_hoi(
                message,
                "Chưa có VIP nào trong hệ thống!",
                xoa_tin_nguoi_dung=True,
                tu_dong_xoa_sau_giay=10
            )
            return False
        vip_list = [v for v in all_vip_list if v[4] == "vip"]
        super_vip_list = [v for v in all_vip_list if v[4] == "super_vip"]
        noi_dung = "📋 𝐷𝐴𝑁𝐻 𝑆𝐴́𝐶𝐻 𝑉𝐼𝑃:\n\n"
        if super_vip_list:
            noi_dung += "🏆 𝑆𝑈𝑃𝐸𝑅 𝑉𝐼𝑃:\n\n"
            for i, svip in enumerate(super_vip_list, 1):
                user_id = svip[0]
                user_name = svip[1] if svip[1] else f"User_{user_id[-4:]}"
                lien_ket_nguoi_dung = (
                    f'<a href="tg://user?id={user_id}">{escape_html(user_name)}</a>'
                )
                expiry_str = ""
                if svip[3]:
                    try:
                        expiry = datetime.fromisoformat(svip[3])
                        now = datetime.now()
                        if expiry > now:
                            days_left = (expiry - now).days
                            hours_left = (expiry - now).seconds // 3600
                            if days_left > 0:
                                expiry_str = f" ({days_left} ngày)"
                            else:
                                expiry_str = f" ({hours_left} giờ)"
                        else:
                            expiry_str = " (Hết hạn)"
                    except:
                        expiry_str = " (Lỗi date)"
                noi_dung += (
                    f"  🏆 {lien_ket_nguoi_dung}{expiry_str}\n     🆔 {user_id}\n\n"
                )
        concucach_vips = [v for v in vip_list if v[2] == "5365031415"]
        if concucach_vips:
            noi_dung += "🥷🏿 𝐶𝑜𝑛 𝐶𝑢̉ 𝐶𝑎̣̆𝑐:\n\n"
            for i, vip in enumerate(concucach_vips, 1):
                user_id = vip[0]
                user_name = vip[1] if vip[1] else f"User_{user_id[-4:]}"
                lien_ket_nguoi_dung = (
                    f'<a href="tg://user?id={user_id}">{escape_html(user_name)}</a>'
                )
                expiry_str = ""
                if vip[3]:
                    try:
                        expiry = datetime.fromisoformat(vip[3])
                        now = datetime.now()
                        if expiry > now:
                            days_left = (expiry - now).days
                            hours_left = (expiry - now).seconds // 3600
                            if days_left > 0:
                                expiry_str = f" ({days_left} ngày)"
                            else:
                                expiry_str = f" ({hours_left} giờ)"
                        else:
                            expiry_str = " (Hết hạn)"
                    except:
                        expiry_str = " (Lỗi date)"
                noi_dung += (
                    f"  🧞‍♂️ {lien_ket_nguoi_dung}{expiry_str}\n     🆔 {user_id}\n\n"
                )
        duyen_vips = [v for v in vip_list if v[2] == "5301816713"]
        if duyen_vips:
            noi_dung += "🧑🏻‍🚀 𝑉𝐼𝑃 𝑐𝑢̉𝑎 𝐷𝑢𝑦𝑒̂𝑛:\n\n"
            for i, vip in enumerate(duyen_vips, 1):
                user_id = vip[0]
                user_name = vip[1] if vip[1] else f"User_{user_id[-4:]}"
                lien_ket_nguoi_dung = (
                    f'<a href="tg://user?id={user_id}">{escape_html(user_name)}</a>'
                )
                expiry_str = ""
                if vip[3]:
                    try:
                        expiry = datetime.fromisoformat(vip[3])
                        now = datetime.now()
                        if expiry > now:
                            days_left = (expiry - now).days
                            hours_left = (expiry - now).seconds // 3600
                            if days_left > 0:
                                expiry_str = f" ({days_left} ngày)"
                            else:
                                expiry_str = f" ({hours_left} giờ)"
                        else:
                            expiry_str = " (Hết hạn)"
                    except:
                        expiry_str = " (Lỗi date)"
                noi_dung += (
                    f"  🧞‍♂️ {lien_ket_nguoi_dung}{expiry_str}\n     🆔 {user_id}\n\n"
                )
        other_vips = [v for v in vip_list if v[2] not in ["5365031415", "5301816713"]]
        if other_vips:
            noi_dung += "🧞‍♂️ 𝑉𝐼𝑃 𝐾ℎ𝑎́𝑐:\n\n"
            for i, vip in enumerate(other_vips, 1):
                user_id = vip[0]
                user_name = vip[1] if vip[1] else f"User_{user_id[-4:]}"
                lien_ket_nguoi_dung = (
                    f'<a href="tg://user?id={user_id}">{escape_html(user_name)}</a>'
                )
                expiry_str = ""
                if vip[3]:
                    try:
                        expiry = datetime.fromisoformat(vip[3])
                        now = datetime.now()
                        if expiry > now:
                            days_left = (expiry - now).days
                            hours_left = (expiry - now).seconds // 3600
                            if days_left > 0:
                                expiry_str = f" ({days_left} ngày)"
                            else:
                                expiry_str = f" ({hours_left} giờ)"
                        else:
                            expiry_str = " (Hết hạn)"
                    except:
                        expiry_str = " (Lỗi date)"
                noi_dung += (
                    f"  🧞‍♂️ {lien_ket_nguoi_dung}{expiry_str}\n     🆔 {user_id}\n\n"
                )
        noi_dung += f"Tổng: {len(vip_list)} VIP + {len(super_vip_list)} SUPER VIP = {len(all_vip_list)} Thành Viên"
        await gui_phan_hoi(
            message, noi_dung, xoa_tin_nguoi_dung=True, luu_vinh_vien=True
        )
        return True
    except Exception as e:
        await gui_phan_hoi(
            message,
            f"Lỗi khi lấy danh sách: {str(e)}",
            xoa_tin_nguoi_dung=True,
            tu_dong_xoa_sau_giay=10
        )
        return False
async def xu_ly_tin_nhan_khong_phai_lenh(message: Message):
    try:
        if not message.from_user or message.from_user.is_bot:
            return
        if not message.text:
            return
        user_id = message.from_user.id
        if message.chat.id not in NHOM_CHO_PHEP:
            return
        try:
            await bot.delete_message(
                chat_id=message.chat.id, message_id=message.message_id
            )
        except:
            pass
        if message.text.startswith("/"):
            cac_lenh_hop_le = [
                "/sta",
                "/start",
                "/vip",
                "/checkid",
                "/call",
                "/sms",
                "/spam",
                "/ping",
                "/full",
                "/img",
                "/vid",
                "/themvip",
                "/xoavip",
                "/myuse",
                "/themadmin",
                "/xoaadmin",
                "/listvip",
                "/tiktok",
                "/ngl",
                "/cleanup",
                "/free",
                "/addsuper",
                "/delsuprr",
                "/tso",
                "/delso",
                "/sper",                           
            ]
            lenh = message.text.split()[0].split("@")[0].lower()
            if lenh not in cac_lenh_hop_le:
                try:
                    phan_hoi = await bot.send_message(
                        chat_id=message.chat.id,
                        text="<blockquote>🏓 𝐿𝑒̣̂𝑛ℎ 𝑘ℎ𝑜̂𝑛𝑔 ℎ𝑜̛̣𝑝 𝑙𝑒̣̂ !\n𝐺𝑜̃ /sta 𝑑𝑒̂̉ 𝑥𝑒𝑚 𝑑𝑎𝑛ℎ 𝑠𝑎́𝑐ℎ 𝑙𝑒̣̂𝑛ℎ</blockquote>",
                        parse_mode=ParseMode.HTML,
                    )
                    asyncio.create_task(
                        tu_dong_xoa_tin_nhan(phan_hoi.chat.id, phan_hoi.message_id, 5)
                    )
                except:
                    pass
    except Exception as e:
        logger.error(f"Lỗi xu_ly_tin_nhan_khong_phai_lenh: {e}")
async def xu_ly_start_rieng(message: Message):
    if not message.from_user:
        return False
    if message.chat.type != "private":
        return False
    user = message.from_user
    user_id = str(user.id)
    if message.text and " " in message.text:
        parts = message.text.split()
        if len(parts) > 1 and parts[1].startswith("ref_"):
            referrer_id = parts[1].replace("ref_", "")
            if referrer_id != user_id:
                try:
                    conn = tao_ket_noi_db()
                    cursor = conn.cursor()
                    cursor.execute(
                        "SELECT COUNT(*) FROM referrals WHERE referred_id = ?",
                        (user_id,),
                    )
                    if cursor.fetchone()[0] == 0:
                        # Lưu referral
                        cursor.execute(
                            "INSERT OR IGNORE INTO referrals (referrer_id, referred_id) VALUES (?, ?)",
                            (referrer_id, user_id),
                        )
                        conn.commit()
                        cursor.execute(
                            "SELECT COUNT(*) FROM referrals WHERE referrer_id = ?",
                            (referrer_id,),
                        )
                        count = cursor.fetchone()[0]
                        if count >= 10:
                            cursor.execute(
                                "SELECT user_id FROM admin WHERE user_id = ? AND role = 'vip'",
                                (referrer_id,),
                            )
                            if not cursor.fetchone():
                                from datetime import timedelta
                                expiry_date = (
                                    datetime.now() + timedelta(days=30)
                                ).isoformat()
                                cursor.execute(
                                    "INSERT OR REPLACE INTO admin (user_id, name, role, expiry_date, admin_added_by) VALUES (?, ?, ?, ?, ?)",
                                    (
                                        referrer_id,
                                        "Auto VIP",
                                        "vip",
                                        expiry_date,
                                        "referral_system",
                                    ),
                                )
                                conn.commit()
                                quyen_cache.set(referrer_id, "vip")
                                try:
                                    await bot.send_message(
                                        chat_id=int(referrer_id),
                                        text=f"<blockquote>🎉 𝐶ℎ𝑢́𝑐 𝑚𝑢̛̀𝑛𝑔 𝑏𝑎̣𝑛 𝑑𝑎̃ 𝑙𝑒̂𝑛 𝑉𝐼𝑃 !\n\n"
                                        f"𝐵𝑎̣𝑛 𝑑𝑎̃ 𝑚𝑜̛̀𝑖 𝑑𝑢̉ {count} 𝑛𝑔𝑢̛𝑜̛̀𝑖 𝑡ℎ𝑎𝑚 𝑔𝑖𝑎 𝑛ℎ𝑜́𝑚\n"
                                        f"𝑇𝑢̛̣ 𝑑𝑜̣𝑛𝑔 𝑙𝑒̂𝑛 𝑉𝐼𝑃 30 𝑛𝑔𝑎̀𝑦\n"
                                        f"𝐻𝑒̂́𝑡 ℎ𝑎̣𝑛  :  {(datetime.now() + timedelta(days=30)).strftime('%d/%m/%Y')}</blockquote>",
                                        parse_mode=ParseMode.HTML,
                                    )
                                except:
                                    pass
                    conn.close()
                    await bot.send_message(
                        chat_id=message.chat.id,
                        text=f"<blockquote>🎉 𝐶ℎ𝑎̀𝑜 𝑚𝑢̛̀𝑛𝑔 𝑏𝑎̣𝑛 𝑑𝑒̂́𝑛 𝑣𝑜̛́𝑖 𝐵𝑜𝑡 !\n\n"
                        f"🏓 𝐵𝑎̣𝑛 𝑑𝑎̃ 𝑡ℎ𝑎𝑚 𝑔𝑖𝑎 𝑞𝑢𝑎 𝑙𝑖𝑛𝑘 𝑚𝑜̛̀𝑖\n"
                        f"🔥 𝐻𝑎̃𝑦 𝑣𝑎̀𝑜 𝑛ℎ𝑜́𝑚 𝑑𝑒̂̉ 𝑠𝑢̛̉ 𝑑𝑢̣𝑛𝑔 𝑏𝑜𝑡 :\n\n"
                        f"🏠 𝑁ℎ𝑜́𝑚 1  :  t.me/spamsmscall1\n"
                        f"🏠 𝑁ℎ𝑜́𝑚 2  :  t.me/+5SfZpYqgheRlMjVl\n\n"
                        f"💡 𝐺𝑜̃ /sta 𝑡𝑟𝑜𝑛𝑔 𝑛ℎ𝑜́𝑚 𝑑𝑒̂̉ 𝑏𝑎̆́𝑡 𝑑𝑎̂̀𝑢 !</blockquote>",
                        parse_mode=ParseMode.HTML,
                    )
                    return True
                except Exception:
                    pass
    await bot.send_message(
        chat_id=message.chat.id,
        text=f"<blockquote>🤖 𝐶ℎ𝑎̀𝑜 𝑏𝑎̣𝑛 !\n\n"
        f"𝐵𝑜𝑡 𝑛𝑎̀𝑦 ℎ𝑜𝑎̣𝑡 𝑑𝑜̣𝑛𝑔 𝑜̛̉ 𝑡𝑟𝑜𝑛𝑔 𝑛ℎ𝑜́𝑚\n"
        f"𝐻𝑎̃𝑦 𝑣𝑎̀𝑜 𝑛ℎ𝑜́𝑚 𝑑𝑒̂̉ 𝑠𝑢̛̉ 𝑑𝑢̣𝑛𝑔 :\n\n"
        f"🏠 𝑁ℎ𝑜́𝑚 1  :  t.me/spamsmscall1\n"
        f"🏠 𝑁ℎ𝑜́𝑚 2  :  t.me/+5SfZpYqgheRlMjVl\n\n"
        f"💡 𝐺𝑜̃ /sta 𝑡𝑟𝑜𝑛𝑔 𝑛ℎ𝑜́𝑚 𝑑𝑒̂̉ 𝑥𝑒𝑚 ℎ𝑢̛𝑜̛́𝑛𝑔 𝑑𝑎̂̃𝑛 !</blockquote>",
        parse_mode=ParseMode.HTML,
    )
    return True
def create_router():
    router = Router()
    router.message.register(
        xu_ly_start_rieng, Command("start")
    )  # Handler cho chat riêng
    router.message.register(xu_ly_sta, Command("sta"))
    router.message.register(xu_ly_ping, Command("ping"))
    router.message.register(xu_ly_sms, Command("sms"))
    router.message.register(xu_ly_spam, Command("spam"))
    router.message.register(xu_ly_free, Command("free"))
    router.message.register(xu_ly_vip, Command("vip"))
    router.message.register(xu_ly_call, Command("call"))
    router.message.register(xu_ly_full, Command("full"))
    router.message.register(xu_ly_tiktok, Command("tiktok"))
    router.message.register(xu_ly_ngl, Command("ngl"))
    router.message.register(xu_ly_checkid, Command("checkid"))
    router.message.register(xu_ly_myref, Command("myuse"))
    router.message.register(xu_ly_them_vip, Command("themvip"))
    router.message.register(xu_ly_random_anh, Command("img"))
    router.message.register(xu_ly_xoa_vip, Command("xoavip"))
    router.message.register(xu_ly_random_video, Command("vid"))
    router.message.register(xu_ly_them_admin, Command("themadmin"))
    router.message.register(xu_ly_xoa_admin, Command("xoaadmin"))
    router.message.register(xu_ly_xem_danh_sach_vip, Command("listvip"))
    router.message.register(xu_ly_cleanup, Command("cleanup"))
    router.message.register(xu_ly_addphone, Command("tso"))
    router.message.register(xu_ly_delphone, Command("delso"))
    router.message.register(xu_ly_them_super_vip, Command("addsuper"))
    router.message.register(xu_ly_xoa_super_vip, Command("delsuper"))
    router.message.register(xu_ly_sper, Command("sper"))
    router.message.register(xu_ly_tin_nhan_khong_phai_lenh)
    return router
async def main():
    try:
        khoi_tao_database()
        khoi_tao_admin_mac_dinh()
        dp = Dispatcher()
        router = create_router()
        dp.include_router(router)
        try:
            bot_info = await bot.get_me()
            global BOT_USERNAME
            BOT_USERNAME = bot_info.username
        except Exception as e:
            raise
        scheduler = AsyncIOScheduler(timezone=pytz.timezone("Asia/Ho_Chi_Minh"))
        scheduler.add_job(
            xu_ly_oki_schedule, "cron", hour=23, minute=30, id="oki_schedule"
        )
        scheduler.add_job(
            xu_ly_oki_cleanup_6h, "cron", hour=6, minute=0, id="oki_cleanup_6h"
        )
        scheduler.add_job(
            cleanup_dead_processes, "cron", hour=6, minute=5, id="general_cleanup"
        )
        scheduler.start()
        cleanup_task = asyncio.create_task(schedule_cleanup())
        vip_check_task = asyncio.create_task(kiem_tra_vip_het_han())
        try:
            await dp.start_polling(
                bot,
                drop_pending_updates=True,
                timeout=30,
                relax=0.1,
                fast=True,
                allowed_updates=["message"],
            )
        finally:
            cleanup_task.cancel()
            vip_check_task.cancel()
            scheduler.shutdown()
            try:
                await cleanup_task
            except asyncio.CancelledError:
                pass
            try:
                await vip_check_task
            except asyncio.CancelledError:
                pass
    except Exception as e:
        cleanup_dead_processes()
        raise
    finally:
        cleanup_dead_processes()
if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        cleanup_dead_processes()
    except Exception as e:
        cleanup_dead_processes()
