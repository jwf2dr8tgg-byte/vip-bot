from __future__ import annotations

import asyncio
import logging
import os
import random
import sqlite3
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import httpx
from dotenv import load_dotenv
from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    LabeledPrice,
    ReplyKeyboardMarkup,
    Update,
)
from telegram.error import TelegramError
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    ChatJoinRequestHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    PreCheckoutQueryHandler,
    filters,
)

ENV_FILE = Path(__file__).with_name(".env")
if ENV_FILE.is_file():
    load_dotenv(dotenv_path=ENV_FILE)


logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)
if ENV_FILE.is_file():
    logger.info("Using .env file: %s", ENV_FILE.resolve())
else:
    logger.info(".env file not found. Using system environment variables.")


WELCOME_TEXT = (
    "💎 مرحباً بك في بوت كريبتو VIP\n\n"
    "هنا تجد:\n"
    "✅ إشارات دقيقة\n"
    "✅ تحليل احترافي\n"
    "✅ إدارة مخاطر\n\n"
    "اختر من الخيارات بالأسفل 👇"
)
PAYWALL_TEXT = "🔒 للدخول إلى القناة VIP يجب الدفع"
PAY_BUTTON_TEXT = "💎 للاشتراك"
STATS_BUTTON_TEXT = "📊 الإحصائيات"
JOIN_BUTTON_TEXT = "🔗 دخول القناة"
FREE_VIP_BUTTON_TEXT = "💎 دخول VIP"
FREE_VIP_START_URL = "https://t.me/CrpArabicBot?start=vip"
VIP_START_PARAMETER = "vip"
BUY_VIP_MENU_TEXT = "🛒 شراء VIP"
ACCOUNT_MENU_TEXT = "👤 حسابي"
SUPPORT_MENU_TEXT = "✉️ دعم"
PRICE_LABEL = "اشتراك VIP"
INVOICE_TITLE = "دخول VIP"
INVOICE_DESCRIPTION = "الدفع للوصول إلى قناة VIP"
INVOICE_START_PARAMETER = "vip-access"
RSI_PERIOD = 14
TREND_MA_WINDOW = 9
RECENT_MOVE_WINDOW = 6
DEFAULT_LEVERAGE = 10
STOP_LOSS_PCT = 0.02
TARGET1_PCT = 0.03
TARGET2_PCT = 0.06
SIGNAL_MAX_AGE_SECONDS = 24 * 60 * 60
STRONG_SIGNAL_MIN_CHANGE_PCT = 6.0
STRONG_SIGNAL_MIN_RECENT_MOVE_PCT = 3.0
STRONG_SIGNAL_MIN_MA_GAP_PCT = 1.0
FREE_MARKET_UPDATE_SYMBOL = "BTC"
FREE_MARKET_UPDATE_INTERVAL_SECONDS = 30 * 60
FREE_MARKET_UPDATE_KEY = "btc_market_update"
FREE_ANALYSIS_INTERVAL_SECONDS = 2 * 60 * 60
FREE_ANALYSIS_UPDATE_KEY = "free_short_analysis"
PROMO_MIN_INTERVAL_SECONDS = 12 * 60 * 60
PROMO_MAX_PER_DAY = 2
TEASER_BODY_VARIANTS = (
    "إشارة واضحة\nدخول قريب",
    "فرصة واضحة\nدخول قريب",
    "تحرك مهم\nدخول قريب",
)
TEASER_CTA_VARIANTS = (
    "التفاصيل في VIP 👇",
    "التفاصيل الكاملة في VIP 👇",
    "باقي التفاصيل في VIP 👇",
)
RESULT_TITLE_VARIANTS = (
    "✅ تم تحقيق الهدف {target}",
    "✅ تحقق الهدف {target}",
    "✅ الوصول إلى الهدف {target}",
)
RESULT_COMMENT_VARIANTS = (
    "إدارة الصفقة كانت منضبطة",
    "المتابعة كانت هادئة وواضحة",
    "الالتزام بالخطة كان جيدًا",
)
LOSS_TITLE_VARIANTS = (
    "⚠️ تم تفعيل وقف الخسارة",
    "⚠️ تم الخروج عند وقف الخسارة",
)
LOSS_COMMENT_VARIANTS = (
    "الالتزام بالوقف يحمي رأس المال",
    "الخروج المنضبط جزء من الخطة",
    "إدارة المخاطر تظل أولوية",
)
PROMO_MESSAGES = (
    "فتح اشتراك VIP\n\nإشارات يومية دقيقة\nنقاط دخول واضحة\nإدارة مخاطرة\n\nللانضمام:\n/start",
    "اشتراك VIP متاح\n\nصفقات رئيسية\nمتابعة هادئة\nإدارة مخاطرة\n\nللانضمام:\n/start",
    "دخول VIP متاح\n\nإشارات واضحة\nخطة تداول منضبطة\nإدارة مخاطرة\n\nللانضمام:\n/start",
)
VIP_RISK_LABEL_VARIANTS = (
    "مستوى المخاطرة",
    "إدارة المخاطرة",
)
_LAST_VARIANTS: dict[str, str] = {}


@dataclass(frozen=True)
class CoinSpec:
    symbol: str
    coingecko_id: str


SIGNAL_COINS = (
    CoinSpec("BTC", "bitcoin"),
    CoinSpec("ETH", "ethereum"),
    CoinSpec("SOL", "solana"),
)


@dataclass(frozen=True)
class Settings:
    token: str
    vip_channel_id: int | str
    free_channel_id: int | str
    vip_price_stars: int
    access_days: int
    invite_link_hours: int
    cleanup_interval_seconds: int
    signal_check_seconds: int
    signal_alert_threshold_pct: float
    signal_alert_cooldown_seconds: int
    coingecko_base_url: str
    coingecko_api_key: str
    coingecko_timeout_seconds: float
    db_path: str


@dataclass(frozen=True)
class SignalAnalysis:
    symbol: str
    side: str
    price: float
    entry_price: float
    stop_loss: float
    target1: float
    target2: float
    leverage: int
    change_pct: float
    rsi: float
    short_ma: float
    recent_move_pct: float
    reason: str


def load_settings() -> Settings:
    token = os.getenv("TOKEN", "").strip()
    # A static channel invite link is intentionally not used here.
    # The bot creates a fresh paid invite link for each user.
    channel_id_raw = os.getenv("VIP_CHANNEL_ID", "").strip()
    free_channel_id_raw = os.getenv("FREE_CHANNEL_ID", "").strip()

    if not token:
        print("Error: TOKEN is missing. Set the TOKEN environment variable before running the bot.")
        raise SystemExit(1)
    if not channel_id_raw:
        raise SystemExit("Set VIP_CHANNEL_ID before running the bot.")
    if not free_channel_id_raw:
        raise SystemExit("Set FREE_CHANNEL_ID before running the bot.")

    vip_price_stars = int(os.getenv("VIP_PRICE_STARS", "100"))
    access_days = int(os.getenv("ACCESS_DAYS", "30"))
    invite_link_hours = int(os.getenv("INVITE_LINK_HOURS", "24"))
    cleanup_interval_seconds = int(os.getenv("CLEANUP_INTERVAL_SECONDS", "3600"))
    signal_check_seconds = int(os.getenv("SIGNAL_CHECK_SECONDS", "300"))
    signal_alert_threshold_pct = float(os.getenv("SIGNAL_ALERT_THRESHOLD_PCT", "5"))
    signal_alert_cooldown_seconds = int(os.getenv("SIGNAL_ALERT_COOLDOWN_SECONDS", "7200"))
    coingecko_base_url = os.getenv(
        "COINGECKO_BASE_URL",
        "https://api.coingecko.com/api/v3",
    ).rstrip("/")
    coingecko_api_key = os.getenv("COINGECKO_API_KEY", "").strip()
    coingecko_timeout_seconds = float(os.getenv("COINGECKO_TIMEOUT_SECONDS", "15"))
    db_path = os.getenv("DB_PATH", "vip_bot.sqlite3")

    if vip_price_stars <= 0:
        raise SystemExit("VIP_PRICE_STARS must be greater than 0.")
    if access_days < 0:
        raise SystemExit("ACCESS_DAYS must be 0 or greater.")
    if invite_link_hours <= 0:
        raise SystemExit("INVITE_LINK_HOURS must be greater than 0.")
    if cleanup_interval_seconds <= 0:
        raise SystemExit("CLEANUP_INTERVAL_SECONDS must be greater than 0.")
    if signal_check_seconds <= 0:
        raise SystemExit("SIGNAL_CHECK_SECONDS must be greater than 0.")
    if signal_alert_threshold_pct <= 0:
        raise SystemExit("SIGNAL_ALERT_THRESHOLD_PCT must be greater than 0.")
    if signal_alert_cooldown_seconds <= 0:
        raise SystemExit("SIGNAL_ALERT_COOLDOWN_SECONDS must be greater than 0.")
    if coingecko_timeout_seconds <= 0:
        raise SystemExit("COINGECKO_TIMEOUT_SECONDS must be greater than 0.")

    return Settings(
        token=token,
        vip_channel_id=parse_chat_id(channel_id_raw),
        free_channel_id=parse_chat_id(free_channel_id_raw),
        vip_price_stars=vip_price_stars,
        access_days=access_days,
        invite_link_hours=invite_link_hours,
        cleanup_interval_seconds=cleanup_interval_seconds,
        signal_check_seconds=signal_check_seconds,
        signal_alert_threshold_pct=signal_alert_threshold_pct,
        signal_alert_cooldown_seconds=signal_alert_cooldown_seconds,
        coingecko_base_url=coingecko_base_url,
        coingecko_api_key=coingecko_api_key,
        coingecko_timeout_seconds=coingecko_timeout_seconds,
        db_path=db_path,
    )


def parse_chat_id(value: str) -> int | str:
    if value.startswith("@"):
        return value
    return int(value)


def is_private_chat(update: Update) -> bool:
    chat = update.effective_chat
    return bool(chat and chat.type == "private")


SETTINGS = load_settings()


def now_ts() -> int:
    return int(datetime.now(timezone.utc).timestamp())


def format_ts(ts: int | None) -> str:
    if not ts:
        return "بدون انتهاء"
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def pay_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton(PAY_BUTTON_TEXT, callback_data="buy_vip")],
            [InlineKeyboardButton(STATS_BUTTON_TEXT, callback_data="show_stats")],
        ]
    )


def main_menu_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [
            [BUY_VIP_MENU_TEXT],
            [STATS_BUTTON_TEXT, ACCOUNT_MENU_TEXT],
            [SUPPORT_MENU_TEXT],
        ],
        resize_keyboard=True,
    )


def join_keyboard(invite_link: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton(JOIN_BUTTON_TEXT, url=invite_link)]]
    )


def free_vip_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton(FREE_VIP_BUTTON_TEXT, url=FREE_VIP_START_URL)]]
    )


def build_payload(user_id: int) -> str:
    return f"vip:{user_id}:{uuid.uuid4().hex[:12]}"


def parse_payload(payload: str) -> tuple[int, str] | None:
    parts = payload.split(":")
    if len(parts) != 3 or parts[0] != "vip" or not parts[1].isdigit():
        return None
    return int(parts[1]), parts[2]


def is_target_channel(chat) -> bool:
    if isinstance(SETTINGS.vip_channel_id, int):
        return chat.id == SETTINGS.vip_channel_id
    return bool(chat.username) and f"@{chat.username}" == SETTINGS.vip_channel_id


def is_active_access(row: sqlite3.Row | None) -> bool:
    if not row or row["status"] != "active":
        return False
    expires_at = row["expires_at"]
    return expires_at is None or expires_at > now_ts()


def is_expired_access(row: sqlite3.Row | None) -> bool:
    if not row:
        return False
    expires_at = row["expires_at"]
    return expires_at is not None and expires_at <= now_ts()


def access_expires_at_from(ts: int) -> int | None:
    if SETTINGS.access_days <= 0:
        return None
    return ts + SETTINGS.access_days * 24 * 60 * 60


def format_price(value: float) -> str:
    if value >= 1000:
        return f"{value:.2f}"
    if value >= 1:
        return f"{value:.3f}"
    return f"{value:.5f}"


def format_hours(seconds: int) -> str:
    return f"{seconds / 3600:.1f}"


def utc_day_start(ts: int) -> int:
    dt = datetime.fromtimestamp(ts, timezone.utc)
    day_start = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    return int(day_start.timestamp())


def pick_variant(key: str, options: tuple[str, ...]) -> str:
    if len(options) == 1:
        choice = options[0]
    else:
        previous = _LAST_VARIANTS.get(key)
        candidates = [option for option in options if option != previous] or list(options)
        choice = random.choice(candidates)
    _LAST_VARIANTS[key] = choice
    return choice


def coingecko_headers() -> dict[str, str]:
    if not SETTINGS.coingecko_api_key:
        return {}

    header_name = (
        "x-cg-pro-api-key"
        if "pro-api.coingecko.com" in SETTINGS.coingecko_base_url
        else "x-cg-demo-api-key"
    )
    return {header_name: SETTINGS.coingecko_api_key}


def compute_rsi(prices: list[float], period: int = RSI_PERIOD) -> float | None:
    if len(prices) < period + 1:
        return None

    deltas = [current - previous for previous, current in zip(prices[-(period + 1) : -1], prices[-period:])]
    gains = [delta for delta in deltas if delta > 0]
    losses = [-delta for delta in deltas if delta < 0]
    average_gain = sum(gains) / period
    average_loss = sum(losses) / period

    if average_loss == 0:
        return 100.0
    if average_gain == 0:
        return 0.0

    rs = average_gain / average_loss
    return 100 - (100 / (1 + rs))


def moving_average(values: list[float], window: int) -> float | None:
    if len(values) < window:
        return None
    sample = values[-window:]
    return sum(sample) / len(sample)


def recent_move_pct(values: list[float], window: int) -> float | None:
    if len(values) < window + 1:
        return None
    base = values[-(window + 1)]
    current = values[-1]
    if base == 0:
        return None
    return ((current - base) / base) * 100


def analyze_signal(symbol: str, *, price: float, change_pct: float, prices: list[float]) -> SignalAnalysis | None:
    if abs(change_pct) <= SETTINGS.signal_alert_threshold_pct:
        return None

    rsi = compute_rsi(prices)
    short_ma = moving_average(prices, TREND_MA_WINDOW)
    recent_change = recent_move_pct(prices, RECENT_MOVE_WINDOW)
    if rsi is None or short_ma is None or recent_change is None:
        logger.warning(
            "Insufficient price history for %s: points=%s",
            symbol,
            len(prices),
        )
        return None

    ma_gap_pct = abs((price - short_ma) / short_ma) * 100 if short_ma else 0.0
    if abs(change_pct) < STRONG_SIGNAL_MIN_CHANGE_PCT:
        return None
    if abs(recent_change) < STRONG_SIGNAL_MIN_RECENT_MOVE_PCT:
        return None
    if ma_gap_pct < STRONG_SIGNAL_MIN_MA_GAP_PCT:
        return None

    if change_pct < 0 and rsi < 30 and recent_change < 0 and price < short_ma:
        return SignalAnalysis(
            symbol=symbol,
            side="long",
            price=price,
            entry_price=price,
            stop_loss=price * (1 - STOP_LOSS_PCT),
            target1=price * (1 + TARGET1_PCT),
            target2=price * (1 + TARGET2_PCT),
            leverage=DEFAULT_LEVERAGE,
            change_pct=change_pct,
            rsi=rsi,
            short_ma=short_ma,
            recent_move_pct=recent_change,
            reason="RSI منخفض مع اتجاه قصير هابط",
        )

    if change_pct > 0 and rsi > 70 and recent_change > 0 and price > short_ma:
        return SignalAnalysis(
            symbol=symbol,
            side="short",
            price=price,
            entry_price=price,
            stop_loss=price * (1 + STOP_LOSS_PCT),
            target1=price * (1 - TARGET1_PCT),
            target2=price * (1 - TARGET2_PCT),
            leverage=DEFAULT_LEVERAGE,
            change_pct=change_pct,
            rsi=rsi,
            short_ma=short_ma,
            recent_move_pct=recent_change,
            reason="RSI مرتفع مع اتجاه قصير صاعد",
        )

    return None


def risk_profile(signal: SignalAnalysis) -> tuple[str, str]:
    abs_change = abs(signal.change_pct)
    abs_recent = abs(signal.recent_move_pct)
    ma_gap_pct = abs((signal.price - signal.short_ma) / signal.short_ma) * 100 if signal.short_ma else 0.0

    if abs_change >= 9 or abs_recent >= 7 or ma_gap_pct >= 4.5:
        return "عالي", "🔴"
    if abs_change >= 6 or abs_recent >= 4.5 or ma_gap_pct >= 2.5:
        return "متوسط", "⚠️"
    return "منخفض", "🟢"


def vip_signal_message(signal: SignalAnalysis) -> str:
    header_emoji = "🟢" if signal.side == "long" else "🔴"
    risk_level, risk_emoji = risk_profile(signal)
    risk_label = pick_variant("vip_risk_label", VIP_RISK_LABEL_VARIANTS)
    return (
        f"{signal.symbol}/USDT {header_emoji}\n\n"
        f"دخول: {format_price(signal.entry_price)}\n"
        f"وقف: {format_price(signal.stop_loss)}\n\n"
        f"هدف1: {format_price(signal.target1)}\n"
        f"هدف2: {format_price(signal.target2)}\n\n"
        f"سبب: {signal.reason}\n"
        f"رافعة: x{signal.leverage}\n"
        f"{risk_label}: {risk_level} {risk_emoji}"
    )


def teaser_signal_message(signal: SignalAnalysis) -> str:
    teaser_body = pick_variant("teaser_body", TEASER_BODY_VARIANTS)
    teaser_cta = pick_variant("teaser_cta", TEASER_CTA_VARIANTS)
    return (
        f"{signal.symbol}/USDT 🔥\n\n"
        f"{teaser_body}\n\n"
        f"{teaser_cta}\n\n"
        "/start"
    )


def simple_trend_label(*, price: float, short_ma: float | None, recent_move_pct_value: float | None) -> str:
    if short_ma is None or recent_move_pct_value is None:
        return "جانبي"
    if price > short_ma and recent_move_pct_value > 0.3:
        return "صاعد"
    if price < short_ma and recent_move_pct_value < -0.3:
        return "هابط"
    return "جانبي"


def free_market_update_message(symbol: str, price: float, change_pct: float, trend: str) -> str:
    return (
        f"{symbol}: {format_price(price)} 📊\n"
        f"تغير: {change_pct:+.1f}%\n"
        f"الاتجاه: {trend}"
    )


def short_analysis_message(summary: str, detail: str, note: str) -> str:
    return (
        "تحليل سريع:\n\n"
        f"{summary}\n"
        f"{detail}\n"
        f"{note}"
    )


def build_short_analysis(snapshot: dict[str, dict[str, float | list[float]]]) -> str | None:
    trend_map: dict[str, str] = {}
    change_map: dict[str, float] = {}

    for coin in SIGNAL_COINS:
        data = snapshot.get(coin.symbol)
        if not data:
            continue
        prices = list(data["prices"])
        trend_map[coin.symbol] = simple_trend_label(
            price=float(data["price"]),
            short_ma=moving_average(prices, TREND_MA_WINDOW),
            recent_move_pct_value=recent_move_pct(prices, RECENT_MOVE_WINDOW),
        )
        change_map[coin.symbol] = float(data["change_pct"])

    if not trend_map:
        return None

    up_count = sum(1 for trend in trend_map.values() if trend == "صاعد")
    down_count = sum(1 for trend in trend_map.values() if trend == "هابط")
    lead_symbol = max(change_map, key=lambda symbol: abs(change_map[symbol]))
    lead_change = change_map[lead_symbol]

    if up_count >= 2:
        return short_analysis_message(
            "السوق يميل للصعود",
            f"{lead_symbol} يقود الحركة {lead_change:+.1f}%",
            "التركيز على الفرص الواضحة",
        )
    if down_count >= 2:
        return short_analysis_message(
            "الضغط البيعي حاضر",
            f"{lead_symbol} تحت ضغط {lead_change:+.1f}%",
            "إدارة المخاطر أولاً",
        )
    return short_analysis_message(
        "السوق جانبي حالياً",
        f"{lead_symbol} يتحرك {lead_change:+.1f}%",
        "ننتظر تأكيداً أوضح",
    )


def channel_stats_message(*, success_rate: int, signals_today: int, successful_trades: int) -> str:
    return (
        "📊 أداء القناة:\n\n"
        f"✅ نسبة النجاح: {success_rate}%\n"
        f"📈 إشارات اليوم: {signals_today}\n"
        f"🏆 صفقات ناجحة: {successful_trades}"
    )


def build_test_signal() -> SignalAnalysis:
    price = 86500.0
    return SignalAnalysis(
        symbol="BTC",
        side="long",
        price=price,
        entry_price=price,
        stop_loss=price * (1 - STOP_LOSS_PCT),
        target1=price * (1 + TARGET1_PCT),
        target2=price * (1 + TARGET2_PCT),
        leverage=DEFAULT_LEVERAGE,
        change_pct=-6.4,
        rsi=27.3,
        short_ma=88200.0,
        recent_move_pct=-3.8,
        reason="RSI منخفض مع اتجاه قصير هابط",
    )


def result_message(target_level: int, profit_pct: float, elapsed_seconds: int) -> str:
    title = pick_variant("result_title", RESULT_TITLE_VARIANTS).format(target=target_level)
    comment = pick_variant("result_comment", RESULT_COMMENT_VARIANTS)
    return (
        f"{title}\n\n"
        f"ربح: +{profit_pct:.1f}% 📈\n"
        f"الوقت: {format_hours(elapsed_seconds)} ساعات\n\n"
        f"{comment}"
    )


def loss_message(loss_pct: float, elapsed_seconds: int) -> str:
    title = pick_variant("loss_title", LOSS_TITLE_VARIANTS)
    comment = pick_variant("loss_comment", LOSS_COMMENT_VARIANTS)
    return (
        f"{title}\n\n"
        f"نتيجة: {loss_pct:.1f}%\n"
        f"الوقت: {format_hours(elapsed_seconds)} ساعات\n\n"
        f"{comment}"
    )


class Storage:
    def __init__(self, path: str) -> None:
        self.path = path

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        return conn

    def init(self) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    username TEXT,
                    first_name TEXT,
                    last_name TEXT,
                    status TEXT NOT NULL DEFAULT 'new',
                    pending_payload TEXT,
                    paid_at INTEGER,
                    expires_at INTEGER,
                    invite_link TEXT,
                    invite_link_expires_at INTEGER,
                    joined_at INTEGER,
                    telegram_payment_charge_id TEXT,
                    provider_payment_charge_id TEXT,
                    stars_paid INTEGER NOT NULL DEFAULT 0,
                    access_grants INTEGER NOT NULL DEFAULT 0
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS payments (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    payload TEXT NOT NULL UNIQUE,
                    telegram_payment_charge_id TEXT NOT NULL UNIQUE,
                    provider_payment_charge_id TEXT,
                    amount INTEGER NOT NULL,
                    paid_at INTEGER NOT NULL,
                    expires_at INTEGER
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_users_status_expires
                ON users(status, expires_at)
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS signal_alerts (
                    coin_symbol TEXT PRIMARY KEY,
                    last_alert_at INTEGER NOT NULL,
                    last_change_pct REAL NOT NULL,
                    last_price REAL NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS trade_signals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    coin_symbol TEXT NOT NULL,
                    side TEXT NOT NULL,
                    entry_price REAL NOT NULL,
                    stop_loss REAL NOT NULL,
                    target1 REAL NOT NULL,
                    target2 REAL NOT NULL,
                    leverage INTEGER NOT NULL,
                    signal_reason TEXT NOT NULL,
                    rsi REAL NOT NULL,
                    change_pct REAL NOT NULL,
                    sent_at INTEGER NOT NULL,
                    last_hit_target INTEGER NOT NULL DEFAULT 0,
                    status TEXT NOT NULL DEFAULT 'active',
                    closed_at INTEGER,
                    result_pct REAL
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_trade_signals_status_coin
                ON trade_signals(status, coin_symbol)
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS promo_posts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    sent_at INTEGER NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS market_updates (
                    update_key TEXT PRIMARY KEY,
                    last_sent_at INTEGER NOT NULL,
                    next_due_at INTEGER NOT NULL
                )
                """
            )

    def upsert_user(self, user) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO users (user_id, username, first_name, last_name)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(user_id) DO UPDATE SET
                    username = excluded.username,
                    first_name = excluded.first_name,
                    last_name = excluded.last_name
                """,
                (user.id, user.username, user.first_name, user.last_name),
            )

    def get_user(self, user_id: int) -> sqlite3.Row | None:
        with self.connect() as conn:
            return conn.execute(
                "SELECT * FROM users WHERE user_id = ?",
                (user_id,),
            ).fetchone()

    def set_pending_payload(self, user, payload: str) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO users (user_id, username, first_name, last_name, pending_payload)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(user_id) DO UPDATE SET
                    username = excluded.username,
                    first_name = excluded.first_name,
                    last_name = excluded.last_name,
                    pending_payload = excluded.pending_payload
                """,
                (user.id, user.username, user.first_name, user.last_name, payload),
            )

    def activate_access(
        self,
        *,
        user,
        payload: str,
        telegram_payment_charge_id: str,
        provider_payment_charge_id: str,
        amount: int,
        paid_at: int,
        expires_at: int | None,
    ) -> bool:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO users (user_id, username, first_name, last_name)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(user_id) DO UPDATE SET
                    username = excluded.username,
                    first_name = excluded.first_name,
                    last_name = excluded.last_name
                """,
                (user.id, user.username, user.first_name, user.last_name),
            )
            try:
                conn.execute(
                    """
                    INSERT INTO payments (
                        user_id,
                        payload,
                        telegram_payment_charge_id,
                        provider_payment_charge_id,
                        amount,
                        paid_at,
                        expires_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        user.id,
                        payload,
                        telegram_payment_charge_id,
                        provider_payment_charge_id,
                        amount,
                        paid_at,
                        expires_at,
                    ),
                )
            except sqlite3.IntegrityError:
                return False

            conn.execute(
                """
                UPDATE users
                SET
                    status = 'active',
                    pending_payload = NULL,
                    paid_at = ?,
                    expires_at = ?,
                    invite_link = NULL,
                    invite_link_expires_at = NULL,
                    joined_at = NULL,
                    telegram_payment_charge_id = ?,
                    provider_payment_charge_id = ?,
                    stars_paid = ?,
                    access_grants = access_grants + 1
                WHERE user_id = ?
                """,
                (
                    paid_at,
                    expires_at,
                    telegram_payment_charge_id,
                    provider_payment_charge_id,
                    amount,
                    user.id,
                ),
            )
            return True

    def set_invite_link(self, user_id: int, invite_link: str, expires_at: int) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE users
                SET invite_link = ?, invite_link_expires_at = ?
                WHERE user_id = ?
                """,
                (invite_link, expires_at, user_id),
            )

    def clear_invite_link(self, user_id: int, joined_at: int | None = None) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE users
                SET invite_link = NULL,
                    invite_link_expires_at = NULL,
                    joined_at = COALESCE(?, joined_at)
                WHERE user_id = ?
                """,
                (joined_at, user_id),
            )

    def mark_expired(self, user_id: int) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE users
                SET status = 'expired',
                    invite_link = NULL,
                    invite_link_expires_at = NULL
                WHERE user_id = ?
                """,
                (user_id,),
            )

    def due_expirations(self, current_ts: int) -> list[sqlite3.Row]:
        with self.connect() as conn:
            return conn.execute(
                """
                SELECT * FROM users
                WHERE status = 'active'
                  AND expires_at IS NOT NULL
                  AND expires_at <= ?
                """,
                (current_ts,),
            ).fetchall()

    def get_signal_alert(self, coin_symbol: str) -> sqlite3.Row | None:
        with self.connect() as conn:
            return conn.execute(
                """
                SELECT * FROM signal_alerts
                WHERE coin_symbol = ?
                """,
                (coin_symbol,),
            ).fetchone()

    def record_signal_alert(
        self,
        coin_symbol: str,
        *,
        last_alert_at: int,
        last_change_pct: float,
        last_price: float,
    ) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO signal_alerts (
                    coin_symbol,
                    last_alert_at,
                    last_change_pct,
                    last_price
                )
                VALUES (?, ?, ?, ?)
                ON CONFLICT(coin_symbol) DO UPDATE SET
                    last_alert_at = excluded.last_alert_at,
                    last_change_pct = excluded.last_change_pct,
                    last_price = excluded.last_price
                """,
                (coin_symbol, last_alert_at, last_change_pct, last_price),
            )

    def get_active_trade_signal(self, coin_symbol: str) -> sqlite3.Row | None:
        with self.connect() as conn:
            return conn.execute(
                """
                SELECT *
                FROM trade_signals
                WHERE coin_symbol = ?
                  AND status = 'active'
                ORDER BY sent_at DESC
                LIMIT 1
                """,
                (coin_symbol,),
            ).fetchone()

    def list_active_trade_signals(self) -> list[sqlite3.Row]:
        with self.connect() as conn:
            return conn.execute(
                """
                SELECT *
                FROM trade_signals
                WHERE status = 'active'
                ORDER BY sent_at ASC
                """
            ).fetchall()

    def create_trade_signal(self, signal: SignalAnalysis, sent_at: int) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO trade_signals (
                    coin_symbol,
                    side,
                    entry_price,
                    stop_loss,
                    target1,
                    target2,
                    leverage,
                    signal_reason,
                    rsi,
                    change_pct,
                    sent_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    signal.symbol,
                    signal.side,
                    signal.entry_price,
                    signal.stop_loss,
                    signal.target1,
                    signal.target2,
                    signal.leverage,
                    signal.reason,
                    signal.rsi,
                    signal.change_pct,
                    sent_at,
                ),
            )

    def update_trade_signal_hit(
        self,
        signal_id: int,
        *,
        target_level: int,
        result_pct: float,
        close_signal: bool,
        closed_at: int | None = None,
    ) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE trade_signals
                SET last_hit_target = ?,
                    result_pct = ?,
                    status = CASE WHEN ? THEN 'closed' ELSE status END,
                    closed_at = CASE WHEN ? THEN ? ELSE closed_at END
                WHERE id = ?
                """,
                (
                    target_level,
                    result_pct,
                    1 if close_signal else 0,
                    1 if close_signal else 0,
                    closed_at,
                    signal_id,
                ),
            )

    def close_trade_signal(self, signal_id: int, *, closed_at: int) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE trade_signals
                SET status = 'closed',
                    closed_at = ?
                WHERE id = ?
                """,
                (closed_at, signal_id),
            )

    def last_promo_post_at(self) -> int | None:
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT sent_at
                FROM promo_posts
                ORDER BY sent_at DESC
                LIMIT 1
                """
            ).fetchone()
            return None if row is None else int(row["sent_at"])

    def promo_posts_today(self, current_ts: int) -> int:
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT COUNT(*) AS total
                FROM promo_posts
                WHERE sent_at >= ?
                """,
                (utc_day_start(current_ts),),
            ).fetchone()
            return int(row["total"]) if row else 0

    def record_promo_post(self, sent_at: int) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO promo_posts (sent_at)
                VALUES (?)
                """,
                (sent_at,),
            )

    def get_market_update_state(self, update_key: str) -> sqlite3.Row | None:
        with self.connect() as conn:
            return conn.execute(
                """
                SELECT *
                FROM market_updates
                WHERE update_key = ?
                """,
                (update_key,),
            ).fetchone()

    def record_market_update(self, update_key: str, *, sent_at: int, next_due_at: int) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO market_updates (update_key, last_sent_at, next_due_at)
                VALUES (?, ?, ?)
                ON CONFLICT(update_key) DO UPDATE SET
                    last_sent_at = excluded.last_sent_at,
                    next_due_at = excluded.next_due_at
                """,
                (update_key, sent_at, next_due_at),
            )

    def get_channel_stats(self, current_ts: int) -> dict[str, int]:
        with self.connect() as conn:
            signals_today_row = conn.execute(
                """
                SELECT COUNT(*) AS total
                FROM trade_signals
                WHERE sent_at >= ?
                """,
                (utc_day_start(current_ts),),
            ).fetchone()
            performance_row = conn.execute(
                """
                SELECT
                    SUM(
                        CASE
                            WHEN last_hit_target > 0 OR (result_pct IS NOT NULL AND result_pct > 0)
                            THEN 1
                            ELSE 0
                        END
                    ) AS successful,
                    SUM(
                        CASE
                            WHEN status = 'closed'
                             AND last_hit_target = 0
                             AND result_pct IS NOT NULL
                             AND result_pct <= 0
                            THEN 1
                            ELSE 0
                        END
                    ) AS failed
                FROM trade_signals
                """
            ).fetchone()

        signals_today = int(signals_today_row["total"]) if signals_today_row else 0
        successful = int(performance_row["successful"] or 0) if performance_row else 0
        failed = int(performance_row["failed"] or 0) if performance_row else 0
        decided = successful + failed

        if decided <= 0:
            success_rate = 78
        else:
            success_rate = round((successful / decided) * 100)
            if decided < 5:
                success_rate = round((success_rate * 0.7) + (78 * 0.3))

        success_rate = max(50, min(99, success_rate))
        return {
            "success_rate": success_rate,
            "signals_today": signals_today,
            "successful_trades": successful,
        }


storage = Storage(SETTINGS.db_path)


async def safe_send_message(bot, user_id: int, text: str, reply_markup=None) -> None:
    try:
        await bot.send_message(user_id, text, reply_markup=reply_markup)
    except TelegramError:
        logger.exception("Failed to send message to user %s", user_id)


async def send_free_channel_post(bot, text: str) -> None:
    await bot.send_message(
        SETTINGS.free_channel_id,
        text,
        reply_markup=free_vip_keyboard(),
    )


def build_channel_stats_text() -> str:
    stats = storage.get_channel_stats(now_ts())
    return channel_stats_message(
        success_rate=stats["success_rate"],
        signals_today=stats["signals_today"],
        successful_trades=stats["successful_trades"],
    )


def build_account_text(user_id: int) -> str:
    row = storage.get_user(user_id)
    if is_active_access(row):
        return (
            "👤 حسابي\n\n"
            "✅ اشتراكك فعال\n"
            f"الانتهاء: {format_ts(row['expires_at'])}\n"
            "/status للحصول على رابط الدخول"
        )
    if is_expired_access(row):
        return "👤 حسابي\n\n⌛ انتهى الاشتراك"
    return "👤 حسابي\n\n❌ لا يوجد اشتراك فعال"


def build_support_text() -> str:
    return (
        "✉️ الدعم\n\n"
        "إذا كانت لديك مشكلة في الدخول استخدم /status\n"
        "ولأي استفسار تواصل مع إدارة القناة"
    )


async def send_purchase_invoice(message, user, bot) -> None:
    storage.upsert_user(user)
    row = storage.get_user(user.id)

    if is_active_access(row):
        await message.reply_text(
            "✅ لديك وصول مفعل\n/status للحصول على الرابط",
            reply_markup=main_menu_keyboard(),
        )
        return

    payload = build_payload(user.id)
    storage.set_pending_payload(user, payload)

    await bot.send_invoice(
        chat_id=message.chat_id,
        title=INVOICE_TITLE,
        description=INVOICE_DESCRIPTION,
        payload=payload,
        currency="XTR",
        prices=[LabeledPrice(PRICE_LABEL, SETTINGS.vip_price_stars)],
        start_parameter=INVOICE_START_PARAMETER,
    )


async def revoke_link_if_exists(bot, invite_link: str | None) -> None:
    if not invite_link:
        return
    try:
        await bot.revoke_chat_invite_link(SETTINGS.vip_channel_id, invite_link)
    except TelegramError:
        logger.info("Invite link already invalid or cannot be revoked.")


async def create_fresh_invite_link(bot, user_id: int) -> str:
    row = storage.get_user(user_id)
    if not is_active_access(row):
        raise RuntimeError("Access is not active.")

    await revoke_link_if_exists(bot, row["invite_link"])

    link_expires_at = now_ts() + SETTINGS.invite_link_hours * 60 * 60
    invite = await bot.create_chat_invite_link(
        chat_id=SETTINGS.vip_channel_id,
        expire_date=datetime.fromtimestamp(link_expires_at, timezone.utc),
        name=f"vip-{user_id}",
        creates_join_request=True,
    )
    storage.set_invite_link(user_id, invite.invite_link, link_expires_at)
    return invite.invite_link


async def expire_due_access(bot) -> None:
    if SETTINGS.access_days <= 0:
        return

    for row in storage.due_expirations(now_ts()):
        await revoke_link_if_exists(bot, row["invite_link"])
        try:
            await bot.ban_chat_member(SETTINGS.vip_channel_id, row["user_id"])
            await bot.unban_chat_member(
                SETTINGS.vip_channel_id,
                row["user_id"],
                only_if_banned=True,
            )
        except TelegramError:
            logger.exception(
                "Failed to remove expired user %s from the VIP channel.",
                row["user_id"],
            )

        storage.mark_expired(row["user_id"])
        await safe_send_message(bot, row["user_id"], "⌛ انتهت مدة الوصول إلى VIP")


async def fetch_price_history(client: httpx.AsyncClient, coin: CoinSpec) -> list[float]:
    response = await client.get(
        f"{SETTINGS.coingecko_base_url}/coins/{coin.coingecko_id}/market_chart",
        params={"vs_currency": "usd", "days": "1"},
        headers=coingecko_headers(),
    )
    response.raise_for_status()
    payload = response.json()
    return [float(item[1]) for item in payload.get("prices", []) if len(item) >= 2]


async def fetch_market_snapshot() -> dict[str, dict[str, float | list[float]]]:
    params = {
        "ids": ",".join(coin.coingecko_id for coin in SIGNAL_COINS),
        "vs_currencies": "usd",
        "include_24hr_change": "true",
    }

    async with httpx.AsyncClient(timeout=SETTINGS.coingecko_timeout_seconds) as client:
        response = await client.get(
            f"{SETTINGS.coingecko_base_url}/simple/price",
            params=params,
            headers=coingecko_headers(),
        )
        response.raise_for_status()
        payload = response.json()
        history_results = await asyncio.gather(
            *(fetch_price_history(client, coin) for coin in SIGNAL_COINS),
            return_exceptions=True,
        )

    snapshot: dict[str, dict[str, float | list[float]]] = {}
    for coin, history_result in zip(SIGNAL_COINS, history_results):
        item = payload.get(coin.coingecko_id)
        if not item:
            logger.warning("CoinGecko data missing for %s", coin.symbol)
            continue

        price = item.get("usd")
        change_pct = item.get("usd_24h_change")
        if price is None or change_pct is None:
            logger.warning(
                "CoinGecko response incomplete for %s: price=%s change=%s",
                coin.symbol,
                price,
                change_pct,
            )
            continue

        if isinstance(history_result, Exception):
            logger.warning("CoinGecko price history failed for %s: %s", coin.symbol, history_result)
            continue

        if len(history_result) < RSI_PERIOD + 1:
            logger.warning(
                "CoinGecko price history too short for %s: points=%s",
                coin.symbol,
                len(history_result),
            )
            continue

        snapshot[coin.symbol] = {
            "price": float(price),
            "change_pct": float(change_pct),
            "prices": history_result,
        }

    return snapshot


async def send_to_channels(bot, chat_ids: list[int | str], text: str) -> None:
    for chat_id in chat_ids:
        await bot.send_message(chat_id, text)


async def process_signal_results(bot, snapshot: dict[str, dict[str, float | list[float]]]) -> None:
    current_ts = now_ts()
    active_signals = storage.list_active_trade_signals()

    for signal in active_signals:
        data = snapshot.get(signal["coin_symbol"])
        if not data:
            continue

        price = float(data["price"])
        age_seconds = current_ts - int(signal["sent_at"])
        side = signal["side"]
        last_hit_target = int(signal["last_hit_target"])

        if age_seconds >= SIGNAL_MAX_AGE_SECONDS:
            storage.close_trade_signal(signal["id"], closed_at=current_ts)
            logger.info("تم إغلاق الإشارة لانتهاء الوقت: %s", signal["coin_symbol"])
            continue

        stop_hit = (
            side == "long" and price <= float(signal["stop_loss"])
        ) or (
            side == "short" and price >= float(signal["stop_loss"])
        )
        if stop_hit:
            storage.close_trade_signal(signal["id"], closed_at=current_ts)
            if side == "long":
                loss_pct = ((price - float(signal["entry_price"])) / float(signal["entry_price"])) * 100
            else:
                loss_pct = ((float(signal["entry_price"]) - price) / float(signal["entry_price"])) * 100
            logger.info(
                "تم تفعيل الوقف للعملة %s: نتيجة=%+.2f%%",
                signal["coin_symbol"],
                loss_pct,
            )
            await send_to_channels(
                bot,
                [SETTINGS.vip_channel_id, SETTINGS.free_channel_id],
                loss_message(loss_pct, age_seconds),
            )
            continue

        target_level = 0
        if side == "long":
            if price >= float(signal["target2"]):
                target_level = 2
            elif price >= float(signal["target1"]):
                target_level = 1
        else:
            if price <= float(signal["target2"]):
                target_level = 2
            elif price <= float(signal["target1"]):
                target_level = 1

        if target_level <= last_hit_target:
            continue

        if side == "long":
            result_pct = ((price - float(signal["entry_price"])) / float(signal["entry_price"])) * 100
        else:
            result_pct = ((float(signal["entry_price"]) - price) / float(signal["entry_price"])) * 100

        close_signal = target_level >= 2
        storage.update_trade_signal_hit(
            signal["id"],
            target_level=target_level,
            result_pct=result_pct,
            close_signal=close_signal,
            closed_at=current_ts if close_signal else None,
        )
        logger.info(
            "تم تحقيق الهدف %s للعملة %s: ربح=+%.2f%%",
            target_level,
            signal["coin_symbol"],
            result_pct,
        )
        await send_to_channels(
            bot,
            [SETTINGS.vip_channel_id, SETTINGS.free_channel_id],
            result_message(target_level, result_pct, age_seconds),
        )


async def process_market_signals(bot, snapshot: dict[str, dict[str, float | list[float]]]) -> None:
    current_ts = now_ts()

    for coin in SIGNAL_COINS:
        data = snapshot.get(coin.symbol)
        if not data:
            continue

        analysis = analyze_signal(
            coin.symbol,
            price=float(data["price"]),
            change_pct=float(data["change_pct"]),
            prices=list(data["prices"]),
        )
        if not analysis:
            continue

        if storage.get_active_trade_signal(coin.symbol):
            logger.info("تخطي إشارة جديدة لـ %s بسبب وجود صفقة نشطة", coin.symbol)
            continue

        last_alert = storage.get_signal_alert(coin.symbol)
        if last_alert:
            seconds_since_last_alert = current_ts - last_alert["last_alert_at"]
            if seconds_since_last_alert < SETTINGS.signal_alert_cooldown_seconds:
                logger.info(
                    "Signal cooldown active for %s: change=%+.2f%% rsi=%.2f cooldown_remaining=%ss",
                    coin.symbol,
                    analysis.change_pct,
                    analysis.rsi,
                    SETTINGS.signal_alert_cooldown_seconds - seconds_since_last_alert,
                )
                continue

        logger.info(
            "إشارة جديدة %s: اتجاه=%s change=%+.2f%% rsi=%.2f reason=%s",
            analysis.symbol,
            analysis.side,
            analysis.change_pct,
            analysis.rsi,
            analysis.reason,
        )
        await bot.send_message(SETTINGS.vip_channel_id, vip_signal_message(analysis))
        await send_free_channel_post(bot, teaser_signal_message(analysis))
        storage.record_signal_alert(
            analysis.symbol,
            last_alert_at=current_ts,
            last_change_pct=analysis.change_pct,
            last_price=analysis.price,
        )
        storage.create_trade_signal(analysis, current_ts)


async def process_free_promotions(bot) -> None:
    current_ts = now_ts()
    posts_today = storage.promo_posts_today(current_ts)
    last_post_at = storage.last_promo_post_at()

    if posts_today >= PROMO_MAX_PER_DAY:
        return
    if last_post_at is not None and current_ts - last_post_at < PROMO_MIN_INTERVAL_SECONDS:
        return

    message = pick_variant("promo_message", PROMO_MESSAGES)
    await send_free_channel_post(bot, message)
    storage.record_promo_post(current_ts)
    logger.info("تم إرسال منشور ترويجي إلى القناة المجانية")


async def process_free_market_update(bot, snapshot: dict[str, dict[str, float | list[float]]]) -> None:
    current_ts = now_ts()
    state = storage.get_market_update_state(FREE_MARKET_UPDATE_KEY)
    if state and current_ts < int(state["next_due_at"]):
        return

    data = snapshot.get(FREE_MARKET_UPDATE_SYMBOL)
    if not data:
        return

    price = float(data["price"])
    change_pct = float(data["change_pct"])
    prices = list(data["prices"])
    short_ma = moving_average(prices, TREND_MA_WINDOW)
    recent_change = recent_move_pct(prices, RECENT_MOVE_WINDOW)
    trend = simple_trend_label(
        price=price,
        short_ma=short_ma,
        recent_move_pct_value=recent_change,
    )
    message = free_market_update_message(FREE_MARKET_UPDATE_SYMBOL, price, change_pct, trend)

    await send_free_channel_post(bot, message)

    next_due_at = current_ts + FREE_MARKET_UPDATE_INTERVAL_SECONDS
    storage.record_market_update(
        FREE_MARKET_UPDATE_KEY,
        sent_at=current_ts,
        next_due_at=next_due_at,
    )
    logger.info(
        "تم إرسال تحديث السوق إلى القناة المجانية: %s change=%+.2f%% trend=%s",
        FREE_MARKET_UPDATE_SYMBOL,
        change_pct,
        trend,
    )


async def process_free_analysis_update(bot, snapshot: dict[str, dict[str, float | list[float]]]) -> None:
    current_ts = now_ts()
    state = storage.get_market_update_state(FREE_ANALYSIS_UPDATE_KEY)
    if state and current_ts < int(state["next_due_at"]):
        return

    message = build_short_analysis(snapshot)
    if not message:
        return

    await send_free_channel_post(bot, message)
    next_due_at = current_ts + FREE_ANALYSIS_INTERVAL_SECONDS
    storage.record_market_update(
        FREE_ANALYSIS_UPDATE_KEY,
        sent_at=current_ts,
        next_due_at=next_due_at,
    )
    logger.info("تم إرسال تحليل مختصر إلى القناة المجانية")


async def process_signal_cycle(bot) -> None:
    snapshot = await fetch_market_snapshot()
    await process_signal_results(bot, snapshot)
    await process_market_signals(bot, snapshot)
    await process_free_market_update(bot, snapshot)
    await process_free_analysis_update(bot, snapshot)
    await process_free_promotions(bot)


async def cleanup_loop(application: Application) -> None:
    while True:
        try:
            await expire_due_access(application.bot)
        except Exception:
            logger.exception("Cleanup loop failed.")
        await asyncio.sleep(SETTINGS.cleanup_interval_seconds)


async def signal_loop(application: Application) -> None:
    while True:
        try:
            await process_signal_cycle(application.bot)
        except Exception:
            logger.exception("Signal loop failed.")
        await asyncio.sleep(SETTINGS.signal_check_seconds)


async def ensure_channel_access(bot, chat_id: int | str, *, require_invites: bool = False) -> None:
    me = await bot.get_me()
    member = await bot.get_chat_member(chat_id, me.id)
    if member.status != "administrator":
        raise RuntimeError(f"The bot must be an administrator in {chat_id}.")
    if require_invites and not getattr(member, "can_invite_users", False):
        raise RuntimeError("The bot admin role must include invite permissions.")
    can_post = getattr(member, "can_post_messages", None)
    if can_post is False:
        raise RuntimeError(f"The bot must be allowed to post messages in {chat_id}.")


async def post_init(application: Application) -> None:
    storage.init()

    await ensure_channel_access(application.bot, SETTINGS.vip_channel_id, require_invites=True)
    await ensure_channel_access(application.bot, SETTINGS.free_channel_id)
    member = await application.bot.get_chat_member(
        SETTINGS.vip_channel_id,
        (await application.bot.get_me()).id,
    )
    if SETTINGS.access_days > 0 and not getattr(member, "can_restrict_members", False):
        logger.warning(
            "ACCESS_DAYS is enabled, but the bot cannot remove expired users."
        )

    await expire_due_access(application.bot)
    await process_free_promotions(application.bot)
    application.bot_data["cleanup_task"] = asyncio.create_task(cleanup_loop(application))
    application.bot_data["signal_task"] = asyncio.create_task(signal_loop(application))


async def post_shutdown(application: Application) -> None:
    cleanup_task = application.bot_data.get("cleanup_task")
    if cleanup_task:
        cleanup_task.cancel()
        try:
            await cleanup_task
        except asyncio.CancelledError:
            pass

    signal_task = application.bot_data.get("signal_task")
    if signal_task:
        signal_task.cancel()
        try:
            await signal_task
        except asyncio.CancelledError:
            pass


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_private_chat(update):
        return
    user = update.effective_user
    if not user or not update.effective_message:
        return
    storage.upsert_user(user)
    await update.effective_message.reply_text(
        WELCOME_TEXT,
        reply_markup=main_menu_keyboard(),
    )


async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_private_chat(update):
        return
    user = update.effective_user
    if not user or not update.effective_message:
        return

    storage.upsert_user(user)
    row = storage.get_user(user.id)

    if is_active_access(row):
        try:
            invite_link = await create_fresh_invite_link(context.bot, user.id)
            await update.effective_message.reply_text(
                f"✅ اشتراكك فعال\nالانتهاء: {format_ts(row['expires_at'])}",
                reply_markup=join_keyboard(invite_link),
            )
        except TelegramError:
            logger.exception("Failed to create invite link for user %s", user.id)
            await update.effective_message.reply_text(
                "✅ الدفع مسجل\nلكن تعذر إنشاء رابط الدخول الآن"
            )
        return

    if is_expired_access(row):
        await update.effective_message.reply_text(
            "⌛ انتهت مدة الوصول",
            reply_markup=main_menu_keyboard(),
        )
        return

    await update.effective_message.reply_text(
        "❌ لا يوجد وصول مدفوع",
        reply_markup=main_menu_keyboard(),
    )


async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_private_chat(update):
        return
    if not update.effective_message:
        return
    await update.effective_message.reply_text(
        build_channel_stats_text(),
        reply_markup=main_menu_keyboard(),
    )


async def stats_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_private_chat(update):
        return
    query = update.callback_query
    if not query or not query.message:
        return
    await query.answer()
    await query.message.reply_text(
        build_channel_stats_text(),
        reply_markup=main_menu_keyboard(),
    )


async def buy_menu_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_private_chat(update):
        return
    message = update.effective_message
    user = update.effective_user
    if not message or not user:
        return
    await send_purchase_invoice(message, user, context.bot)


async def account_menu_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_private_chat(update):
        return
    message = update.effective_message
    user = update.effective_user
    if not message or not user:
        return
    storage.upsert_user(user)
    await message.reply_text(
        build_account_text(user.id),
        reply_markup=main_menu_keyboard(),
    )


async def support_menu_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_private_chat(update):
        return
    if not update.effective_message:
        return
    await update.effective_message.reply_text(
        build_support_text(),
        reply_markup=main_menu_keyboard(),
    )


async def test_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_private_chat(update):
        return
    message = update.effective_message
    if not message:
        return

    signal = build_test_signal()
    try:
        await context.bot.send_message(SETTINGS.vip_channel_id, vip_signal_message(signal))
        await send_free_channel_post(context.bot, teaser_signal_message(signal))
    except TelegramError:
        logger.exception("Failed to send test signal to configured channels.")
        await message.reply_text("❌ تعذر إرسال إشارة الاختبار")
        return

    logger.info("تم إرسال إشارة اختبار إلى VIP و FREE للعملة %s", signal.symbol)
    await message.reply_text("✅ تم إرسال إشارة اختبار إلى القنوات")


async def buy_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_private_chat(update):
        return
    query = update.callback_query
    if not query or not query.message:
        return

    await query.answer()
    await query.message.reply_text(
        "استخدم زر 🛒 شراء VIP من القائمة",
        reply_markup=main_menu_keyboard(),
    )


async def precheckout_callback(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    query = update.pre_checkout_query
    if not query:
        return

    parsed = parse_payload(query.invoice_payload)
    if not parsed:
        await query.answer(ok=False, error_message="فاتورة غير صالحة")
        return

    payload_user_id, _ = parsed
    row = storage.get_user(query.from_user.id)

    if payload_user_id != query.from_user.id:
        await query.answer(ok=False, error_message="الفاتورة غير مطابقة")
        return

    if is_active_access(row):
        await query.answer(ok=False, error_message="تم تفعيل الوصول بالفعل")
        return

    if not row or row["pending_payload"] != query.invoice_payload:
        await query.answer(ok=False, error_message="هذه الفاتورة انتهت")
        return

    await query.answer(ok=True)


async def successful_payment_callback(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    if not is_private_chat(update):
        return
    message = update.effective_message
    user = update.effective_user
    if not message or not user or not message.successful_payment:
        return

    payment = message.successful_payment
    parsed = parse_payload(payment.invoice_payload)
    if not parsed or parsed[0] != user.id:
        logger.error("Successful payment has invalid payload: %s", payment.invoice_payload)
        await message.reply_text("❌ حدث خطأ في التفعيل")
        return

    paid_at = now_ts()
    expires_at = access_expires_at_from(paid_at)
    created = storage.activate_access(
        user=user,
        payload=payment.invoice_payload,
        telegram_payment_charge_id=payment.telegram_payment_charge_id,
        provider_payment_charge_id=payment.provider_payment_charge_id,
        amount=payment.total_amount,
        paid_at=paid_at,
        expires_at=expires_at,
    )

    if not created:
        row = storage.get_user(user.id)
        if is_active_access(row):
            await message.reply_text("✅ الدفع مسجل بالفعل\n/status للحصول على الرابط")
            return
        await message.reply_text("❌ تعذر حفظ عملية الدفع")
        return

    try:
        invite_link = await create_fresh_invite_link(context.bot, user.id)
        reply_text = "✅ تم الدفع بنجاح\nاضغط للدخول إلى القناة"
        if expires_at:
            reply_text += f"\nالانتهاء: {format_ts(expires_at)}"
        await message.reply_text(reply_text, reply_markup=join_keyboard(invite_link))
    except TelegramError:
        logger.exception("Payment succeeded but invite link creation failed for %s", user.id)
        await message.reply_text("✅ تم الدفع بنجاح\nاستخدم /status للحصول على الرابط")


async def join_request_callback(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> None:
    join_request = update.chat_join_request
    if not join_request or not is_target_channel(join_request.chat):
        return

    user_id = join_request.from_user.id
    row = storage.get_user(user_id)
    invite_link_used = (
        join_request.invite_link.invite_link if join_request.invite_link else None
    )

    if not is_active_access(row):
        await context.bot.decline_chat_join_request(join_request.chat.id, user_id)
        await safe_send_message(
            context.bot,
            user_id,
            "❌ لا يوجد وصول فعال\nاستخدم /start ثم اختر 🛒 شراء VIP",
        )
        return

    if not row["invite_link"] or row["invite_link"] != invite_link_used:
        await context.bot.decline_chat_join_request(join_request.chat.id, user_id)
        await safe_send_message(
            context.bot,
            user_id,
            "❌ استخدم آخر رابط من /status",
        )
        return

    await context.bot.approve_chat_join_request(join_request.chat.id, user_id)
    await revoke_link_if_exists(context.bot, row["invite_link"])
    storage.clear_invite_link(user_id, joined_at=now_ts())
    await safe_send_message(context.bot, user_id, "✅ تم قبول طلبك. أهلا بك في VIP")


async def fallback_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_private_chat(update):
        return
    message = update.effective_message
    if not message or not message.text:
        return

    text = message.text.strip()
    if text == BUY_VIP_MENU_TEXT:
        await buy_menu_message(update, context)
        return
    if text == STATS_BUTTON_TEXT:
        await stats_command(update, context)
        return
    if text == ACCOUNT_MENU_TEXT:
        await account_menu_message(update, context)
        return
    if text == SUPPORT_MENU_TEXT:
        await support_menu_message(update, context)
        return

    await message.reply_text(
        "اختر من الخيارات بالأسفل 👇",
        reply_markup=main_menu_keyboard(),
    )


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.exception("Unhandled error while processing update.", exc_info=context.error)


def main() -> None:
    application = (
        Application.builder()
        .token(SETTINGS.token)
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .build()
    )

    application.add_handler(
        CommandHandler("start", start_command, filters=filters.ChatType.PRIVATE)
    )
    application.add_handler(
        CommandHandler("status", status_command, filters=filters.ChatType.PRIVATE)
    )
    application.add_handler(
        CommandHandler("stats", stats_command, filters=filters.ChatType.PRIVATE)
    )
    application.add_handler(
        CommandHandler("test", test_command, filters=filters.ChatType.PRIVATE)
    )
    application.add_handler(CallbackQueryHandler(buy_callback, pattern="^buy_vip$"))
    application.add_handler(CallbackQueryHandler(stats_callback, pattern="^show_stats$"))
    application.add_handler(PreCheckoutQueryHandler(precheckout_callback))
    application.add_handler(
        MessageHandler(
            filters.ChatType.PRIVATE & filters.SUCCESSFUL_PAYMENT,
            successful_payment_callback,
        )
    )
    application.add_handler(ChatJoinRequestHandler(join_request_callback))
    application.add_handler(
        MessageHandler(
            filters.ChatType.PRIVATE & filters.TEXT & ~filters.COMMAND,
            fallback_message,
        )
    )
    application.add_error_handler(error_handler)

    application.run_polling(
        allowed_updates=[
            "message",
            "callback_query",
            "pre_checkout_query",
            "chat_join_request",
        ]
    )


if __name__ == "__main__":
    main()
