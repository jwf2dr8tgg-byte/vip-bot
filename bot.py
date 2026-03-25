from __future__ import annotations

import asyncio
import io
import logging
import os
import random
import sqlite3
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import httpx
from dotenv import load_dotenv

try:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
except ImportError:
    plt = None

from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    LabeledPrice,
    ReplyKeyboardMarkup,
    Update,
)
from telegram.error import Conflict, TelegramError
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
VIP_DISPLAY_PRICE_AED = 100
VIP_DISCOUNT_PERCENT = 50
PRICE_LABEL = "اشتراك VIP | خصم 50%"
INVOICE_TITLE = "دخول VIP | خصم 50%"
INVOICE_DESCRIPTION = "اشتراك VIP بخصم 50% | السعر الحالي ما يعادل 100 درهم من النجوم"
INVOICE_START_PARAMETER = "vip-access"
RSI_PERIOD = 14
TREND_MA_WINDOW = 9
RECENT_MOVE_WINDOW = 6
LOCAL_PRICE_HISTORY_LIMIT = 96
SUPPORT_RESISTANCE_WINDOW = 24
DEFAULT_LEVERAGE = 10
STOP_LOSS_PCT = 0.02
TARGET1_PCT = 0.03
TARGET2_PCT = 0.06
SIGNAL_MAX_AGE_SECONDS = 24 * 60 * 60
STRONG_SIGNAL_MIN_CHANGE_PCT = 6.0
STRONG_SIGNAL_MIN_RECENT_MOVE_PCT = 3.0
STRONG_SIGNAL_MIN_MA_GAP_PCT = 1.0
VIP_PROGRESS_UPDATE_THRESHOLD = 0.55
VIP_PROGRESS_MIN_AGE_SECONDS = 15 * 60
FREE_MARKET_UPDATE_SYMBOL = "BTC"
FREE_MARKET_UPDATE_INTERVAL_SECONDS = 30 * 60
FREE_MARKET_UPDATE_KEY = "btc_market_update"
FREE_ANALYSIS_INTERVAL_SECONDS = 2 * 60 * 60
FREE_ANALYSIS_UPDATE_KEY = "free_short_analysis"
FREE_TOP_MOVERS_MIN_INTERVAL_SECONDS = 60 * 60
FREE_TOP_MOVERS_MAX_INTERVAL_SECONDS = 2 * 60 * 60
FREE_TOP_MOVERS_KEY = "free_top_movers"
FREE_LEVELS_INTERVAL_SECONDS = 2 * 60 * 60
FREE_LEVELS_KEY = "free_support_resistance"
FREE_FEAR_GREED_INTERVAL_SECONDS = 4 * 60 * 60
FREE_FEAR_GREED_KEY = "free_fear_greed"
FREE_DAILY_SUMMARY_KEY = "free_daily_summary"
FREE_WHALE_MIN_INTERVAL_SECONDS = 4 * 60 * 60
FREE_WHALE_MAX_INTERVAL_SECONDS = 8 * 60 * 60
FREE_WHALE_KEY = "free_whale_alert"
FREE_CHART_MIN_INTERVAL_SECONDS = 4 * 60 * 60
FREE_CHART_MAX_INTERVAL_SECONDS = 8 * 60 * 60
FREE_CHART_KEY = "free_chart_update"
MARKET_OVERVIEW_LIMIT = 25
FEAR_GREED_API_URL = "https://api.alternative.me/fng/"
PROMO_MIN_INTERVAL_SECONDS = 12 * 60 * 60
PROMO_MAX_PER_DAY = 2
POLLING_CONFLICT_RETRY_SECONDS = 20
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
    CoinSpec("BNB", "binancecoin"),
    CoinSpec("XRP", "ripple"),
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
    change_1h: float | None
    change_4h: float | None
    rsi: float
    short_ma: float
    recent_move_pct: float
    reason: str
    support_level: float
    resistance_level: float
    context: str
    timing: str
    confidence: str


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


def support_resistance_levels(values: list[float], window: int = SUPPORT_RESISTANCE_WINDOW) -> tuple[float, float] | None:
    if not values:
        return None
    sample = values[-window:] if len(values) >= window else values
    return min(sample), max(sample)


def direction_label(side: str) -> str:
    return "شراء" if side == "long" else "بيع"


def confidence_label(
    *,
    side: str,
    price: float,
    change_pct: float,
    rsi: float,
    short_ma: float,
    recent_change: float,
    support: float,
    resistance: float,
) -> str:
    score = 0
    if abs(change_pct) >= 8:
        score += 2
    elif abs(change_pct) >= 6:
        score += 1

    if abs(recent_change) >= 4.5:
        score += 1

    ma_gap_pct = abs((price - short_ma) / short_ma) * 100 if short_ma else 0.0
    if ma_gap_pct >= 2.5:
        score += 1

    if side == "long":
        if rsi <= 25:
            score += 2
        elif rsi <= 28:
            score += 1
        if price <= support * 1.015:
            score += 1
    else:
        if rsi >= 75:
            score += 2
        elif rsi >= 72:
            score += 1
        if price >= resistance * 0.985:
            score += 1

    if score >= 5:
        return "مرتفعة"
    if score >= 3:
        return "جيدة"
    return "متوسطة"


def market_context_text(
    *,
    side: str,
    trend: str,
    support: float,
    resistance: float,
) -> str:
    if side == "long":
        hold_text = f"الثبات فوق {format_price(support)} صاعد"
        break_text = f"الكسر أسفل {format_price(support)} هابط"
        return f"{hold_text} | {break_text}"

    hold_text = f"البقاء دون {format_price(resistance)} هابط"
    break_text = f"الاختراق فوق {format_price(resistance)} صاعد"
    return f"{hold_text} | {break_text}"


def entry_timing_text(*, side: str, price: float, support: float, resistance: float) -> str:
    span = max(resistance - support, price * 0.005)
    if side == "long":
        distance = max(price - support, 0.0)
        if distance <= span * 0.35:
            return "دخول الآن قرب الدعم"
        return f"انتظار ثبات أقرب إلى {format_price(support)}"

    distance = max(resistance - price, 0.0)
    if distance <= span * 0.35:
        return "دخول الآن قرب المقاومة"
    return f"انتظار رفض أوضح قرب {format_price(resistance)}"


def analyze_signal(symbol: str, *, price: float, change_pct: float, prices: list[float]) -> SignalAnalysis | None:
    if abs(change_pct) <= SETTINGS.signal_alert_threshold_pct:
        return None

    rsi = compute_rsi(prices)
    short_ma = moving_average(prices, TREND_MA_WINDOW)
    recent_change = recent_move_pct(prices, RECENT_MOVE_WINDOW)
    levels = support_resistance_levels(prices)
    if rsi is None or short_ma is None or recent_change is None or not levels:
        logger.warning(
            "Insufficient price history for %s: points=%s",
            symbol,
            len(prices),
        )
        return None
    support, resistance = levels

    ma_gap_pct = abs((price - short_ma) / short_ma) * 100 if short_ma else 0.0
    if abs(change_pct) < STRONG_SIGNAL_MIN_CHANGE_PCT:
        return None
    if abs(recent_change) < STRONG_SIGNAL_MIN_RECENT_MOVE_PCT:
        return None
    if ma_gap_pct < STRONG_SIGNAL_MIN_MA_GAP_PCT:
        return None

    trend = simple_trend_label(
        price=price,
        short_ma=short_ma,
        recent_move_pct_value=recent_change,
    )
    change_1h = intraday_change_pct(prices, 1)
    change_4h = intraday_change_pct(prices, 4)

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
            change_1h=change_1h,
            change_4h=change_4h,
            rsi=rsi,
            short_ma=short_ma,
            recent_move_pct=recent_change,
            reason="RSI منخفض مع اتجاه قصير هابط",
            support_level=support,
            resistance_level=resistance,
            context=market_context_text(
                side="long",
                trend=trend,
                support=support,
                resistance=resistance,
            ),
            timing=entry_timing_text(
                side="long",
                price=price,
                support=support,
                resistance=resistance,
            ),
            confidence=confidence_label(
                side="long",
                price=price,
                change_pct=change_pct,
                rsi=rsi,
                short_ma=short_ma,
                recent_change=recent_change,
                support=support,
                resistance=resistance,
            ),
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
            change_1h=change_1h,
            change_4h=change_4h,
            rsi=rsi,
            short_ma=short_ma,
            recent_move_pct=recent_change,
            reason="RSI مرتفع مع اتجاه قصير صاعد",
            support_level=support,
            resistance_level=resistance,
            context=market_context_text(
                side="short",
                trend=trend,
                support=support,
                resistance=resistance,
            ),
            timing=entry_timing_text(
                side="short",
                price=price,
                support=support,
                resistance=resistance,
            ),
            confidence=confidence_label(
                side="short",
                price=price,
                change_pct=change_pct,
                rsi=rsi,
                short_ma=short_ma,
                recent_change=recent_change,
                support=support,
                resistance=resistance,
            ),
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


def confidence_emoji(label: str) -> str:
    if label == "مرتفعة":
        return "🎯"
    if label == "جيدة":
        return "✅"
    return "📌"


def trade_progress_ratio(*, side: str, entry_price: float, target1: float, price: float) -> float:
    total_distance = (target1 - entry_price) if side == "long" else (entry_price - target1)
    if total_distance <= 0:
        return 0.0

    moved_distance = (price - entry_price) if side == "long" else (entry_price - price)
    return max(0.0, moved_distance / total_distance)


def intraday_change_pct(values: list[float], hours: int) -> float | None:
    if len(values) < 2:
        return None

    if len(values) >= 120:
        steps = max(1, round(hours * max(len(values) - 1, 1) / 168))
    else:
        steps = max(1, round(hours * 2))

    if len(values) <= steps:
        return None

    base = values[-(steps + 1)]
    current = values[-1]
    if base == 0:
        return None
    return ((current - base) / base) * 100


def format_intraday_change(value: float | None) -> str:
    if value is None:
        return "--"
    return f"{value:+.1f}%"


def free_post_icon(change_24h: float) -> str:
    if change_24h >= 0:
        return "📈"
    return "📉"


def free_vip_hook_text() -> str:
    return "التحليل الكامل + مستويات الدخول الدقيقة + الأهداف + الستوب موجود في قناة الـ VIP فقط"


def free_vip_cta_text() -> str:
    return f"انضم للـ VIP الآن لتحصل على الإشارات الكاملة 👇\n{FREE_VIP_START_URL}"


def build_free_teaser_post(
    *,
    symbol: str,
    price: float,
    prices: list[float],
    change_24h: float,
    notes: list[str],
    outlook: str,
    change_1h_override: float | None = None,
    change_4h_override: float | None = None,
) -> str:
    change_1h = change_1h_override if change_1h_override is not None else intraday_change_pct(prices, 1)
    change_4h = change_4h_override if change_4h_override is not None else intraday_change_pct(prices, 4)
    icon = free_post_icon(change_24h)
    context_data = snapshot_context(
        symbol,
        {
            "price": price,
            "change_pct": change_24h,
            "prices": prices,
        },
    )
    lines = [
        f"{symbol}/USDT {icon}",
        f"السعر: {format_price(price)} | 1س {format_intraday_change(change_1h)} | 4س {format_intraday_change(change_4h)} | 24س {change_24h:+.1f}%",
        "",
    ]
    for note in notes[:2]:
        if note:
            lines.append(f"• {note}")

    if context_data:
        side = str(context_data["side"])
        support = float(context_data["support"])
        resistance = float(context_data["resistance"])
        lines.extend(
            [
                "",
                f"الميل: {direction_label(side)} | الجودة: {context_data['confidence']}",
                f"منطقة المتابعة: {free_watch_zone_text(side=side, support=support, resistance=resistance, price=price)}",
                f"التصرف: {context_data['timing']}",
            ]
        )

    lines.extend(
        [
            "",
            f"توقعي: {outlook}",
            "",
            f"{free_vip_hook_text()}",
            "",
            f"{free_vip_cta_text()}",
        ]
    )
    return (
        "\n".join(lines)
    )


def free_watch_zone_text(*, side: str, support: float, resistance: float, price: float) -> str:
    span = max(resistance - support, price * 0.008)
    if side == "long":
        upper = min(resistance, support + span * 0.6)
        if upper <= support:
            upper = price
        return f"{format_price(support)} - {format_price(upper)}"

    lower = max(support, resistance - span * 0.6)
    if lower >= resistance:
        lower = price
    return f"{format_price(lower)} - {format_price(resistance)}"


def snapshot_direction(
    *,
    price: float,
    change_pct: float,
    short_ma: float | None,
    recent_change: float | None,
    rsi: float | None,
) -> str:
    if rsi is not None:
        if rsi <= 35:
            return "long"
        if rsi >= 65:
            return "short"
    if short_ma is not None and recent_change is not None:
        if price >= short_ma and recent_change >= 0:
            return "long"
        if price <= short_ma and recent_change <= 0:
            return "short"
    return "long" if change_pct >= 0 else "short"


def snapshot_reason(
    *,
    trend: str,
    rsi: float | None,
    support: float,
    resistance: float,
    short_ma: float | None,
) -> str:
    if rsi is not None and rsi <= 35:
        return f"RSI منخفض قرب دعم {format_price(support)}"
    if rsi is not None and rsi >= 65:
        return f"RSI مرتفع قرب مقاومة {format_price(resistance)}"
    if trend == "صاعد":
        return f"الزخم القصير صاعد أعلى {format_price(short_ma or support)}"
    if trend == "هابط":
        return f"الزخم القصير هابط دون {format_price(short_ma or resistance)}"
    return f"السعر داخل نطاق بين {format_price(support)} و {format_price(resistance)}"


def snapshot_confidence(
    *,
    side: str,
    price: float,
    change_pct: float,
    rsi: float | None,
    short_ma: float | None,
    recent_change: float | None,
    support: float,
    resistance: float,
) -> str:
    if rsi is None or short_ma is None or recent_change is None:
        return "متوسطة"
    return confidence_label(
        side=side,
        price=price,
        change_pct=change_pct,
        rsi=rsi,
        short_ma=short_ma,
        recent_change=recent_change,
        support=support,
        resistance=resistance,
    )


def snapshot_risk(
    *,
    price: float,
    change_pct: float,
    short_ma: float | None,
    recent_change: float | None,
) -> tuple[str, str]:
    abs_change = abs(change_pct)
    abs_recent = abs(recent_change or 0.0)
    ma_gap_pct = abs((price - short_ma) / short_ma) * 100 if short_ma else 0.0
    if abs_change >= 8 or abs_recent >= 5 or ma_gap_pct >= 4:
        return "عالي", "🔴"
    if abs_change >= 4 or abs_recent >= 2.5 or ma_gap_pct >= 2:
        return "متوسط", "⚠️"
    return "منخفض", "🟢"


def snapshot_context(
    symbol: str,
    data: dict[str, float | list[float]],
) -> dict[str, object] | None:
    prices = [float(value) for value in list(data["prices"])]
    if not prices:
        return None

    price = float(data["price"])
    change_pct = float(data["change_pct"])
    short_ma = moving_average(prices, TREND_MA_WINDOW)
    recent_change = recent_move_pct(prices, RECENT_MOVE_WINDOW)
    rsi = compute_rsi(prices)
    support, resistance = support_resistance_levels(prices) or (price, price)
    trend = simple_trend_label(
        price=price,
        short_ma=short_ma,
        recent_move_pct_value=recent_change,
    )
    side = snapshot_direction(
        price=price,
        change_pct=change_pct,
        short_ma=short_ma,
        recent_change=recent_change,
        rsi=rsi,
    )
    return {
        "symbol": symbol,
        "prices": prices,
        "price": price,
        "change_pct": change_pct,
        "change_1h": intraday_change_pct(prices, 1),
        "change_4h": intraday_change_pct(prices, 4),
        "short_ma": short_ma,
        "recent_change": recent_change,
        "rsi": rsi,
        "support": support,
        "resistance": resistance,
        "trend": trend,
        "side": side,
        "reason": snapshot_reason(
            trend=trend,
            rsi=rsi,
            support=support,
            resistance=resistance,
            short_ma=short_ma,
        ),
        "context": market_context_text(
            side=side,
            trend=trend,
            support=support,
            resistance=resistance,
        ),
        "timing": entry_timing_text(
            side=side,
            price=price,
            support=support,
            resistance=resistance,
        ),
        "confidence": snapshot_confidence(
            side=side,
            price=price,
            change_pct=change_pct,
            rsi=rsi,
            short_ma=short_ma,
            recent_change=recent_change,
            support=support,
            resistance=resistance,
        ),
        "risk": snapshot_risk(
            price=price,
            change_pct=change_pct,
            short_ma=short_ma,
            recent_change=recent_change,
        ),
    }


def build_vip_insight_post(
    *,
    title: str,
    symbol: str,
    price: float,
    prices: list[float],
    change_pct: float,
    detail_lines: list[str],
    outlook: str,
) -> str:
    context_data = snapshot_context(
        symbol,
        {
            "price": price,
            "change_pct": change_pct,
            "prices": prices,
        },
    )
    if not context_data:
        return (
            f"{title}\n\n"
            f"{symbol}/USDT\n"
            f"السعر: {format_price(price)}\n"
            f"24س: {change_pct:+.1f}%\n\n"
            f"الرؤية: {outlook}"
        )

    risk_level, risk_emoji = context_data["risk"]
    confidence = str(context_data["confidence"])
    rsi_value = context_data["rsi"]
    rsi_text = f"{float(rsi_value):.1f}" if isinstance(rsi_value, (int, float)) else "--"
    details = "\n".join(f"• {line}" for line in detail_lines[:2] if line)
    return (
        f"{title}\n\n"
        f"{symbol}/USDT\n"
        f"السعر: {format_price(price)} | 1س {format_intraday_change(context_data['change_1h'])} | 4س {format_intraday_change(context_data['change_4h'])} | 24س {change_pct:+.1f}%\n"
        f"الاتجاه: {direction_label(str(context_data['side']))} | الثقة: {confidence} {confidence_emoji(confidence)}\n"
        f"المخاطرة: {risk_level} {risk_emoji}\n"
        f"RSI: {rsi_text} | الترند: {context_data['trend']}\n"
        f"دعم: {format_price(float(context_data['support']))} | مقاومة: {format_price(float(context_data['resistance']))}\n\n"
        f"{details}\n\n"
        f"السيناريو: {context_data['context']}\n"
        f"التوقيت: {context_data['timing']}\n"
        f"الرؤية: {outlook}"
    )


def snapshot_signal_bias(*, price: float, short_ma: float | None, rsi: float | None) -> str:
    if short_ma is None or rsi is None:
        return "أرى حركة حساسة وتحتاج تأكيد"
    if price > short_ma and rsi >= 55:
        return "أتوقع استمرار الصعود بحذر"
    if price < short_ma and rsi <= 45:
        return "أرى ضغطًا بيعيًا واحتمال استمرار الهبوط"
    return "أرى السوق في منطقة حرجة ويحتاج تأكيد"


def vip_signal_message(signal: SignalAnalysis) -> str:
    header_emoji = "🟢" if signal.side == "long" else "🔴"
    risk_level, risk_emoji = risk_profile(signal)
    risk_label = pick_variant("vip_risk_label", VIP_RISK_LABEL_VARIANTS)
    confidence = f"{signal.confidence} {confidence_emoji(signal.confidence)}"
    return (
        f"{signal.symbol}/USDT {header_emoji}\n\n"
        f"الاتجاه: {direction_label(signal.side)}\n"
        f"دخول: {format_price(signal.entry_price)}\n"
        f"وقف: {format_price(signal.stop_loss)}\n\n"
        f"هدف1: {format_price(signal.target1)}\n"
        f"هدف2: {format_price(signal.target2)}\n\n"
        f"رافعة: x{signal.leverage}\n"
        f"{risk_label}: {risk_level} {risk_emoji}\n"
        f"الثقة: {confidence}\n\n"
        f"التحليل: {signal.reason}\n"
        f"دعم: {format_price(signal.support_level)} | مقاومة: {format_price(signal.resistance_level)}\n"
        f"السيناريو: {signal.context}\n"
        f"التوقيت: {signal.timing}\n\n"
        "إدارة الصفقة:\n"
        "40% عند هدف1\n"
        "نقل الوقف إلى الدخول\n"
        "ترك الباقي لهدف2"
    )


def vip_trade_progress_message(signal: sqlite3.Row, price: float, progress_ratio: float) -> str:
    return (
        f"{signal['coin_symbol']}/USDT متابعة VIP\n\n"
        f"السعر الحالي: {format_price(price)}\n"
        f"التقدم: {progress_ratio * 100:.0f}% نحو الهدف1\n\n"
        "التوجيه:\n"
        "لا ملاحقة جديدة الآن\n"
        "الإبقاء على الوقف كما هو"
    )


def vip_target_management_message(signal: sqlite3.Row, target_level: int, price: float) -> str:
    if target_level == 1:
        return (
            f"{signal['coin_symbol']}/USDT متابعة VIP\n\n"
            f"تم الوصول إلى الهدف1 عند {format_price(price)}\n\n"
            "الإجراء:\n"
            "تخفيف 40%\n"
            "نقل الوقف إلى الدخول\n"
            "مراقبة الهدف2"
        )

    return (
        f"{signal['coin_symbol']}/USDT متابعة VIP\n\n"
        f"تم الوصول إلى الهدف2 عند {format_price(price)}\n\n"
        "الإجراء:\n"
        "إغلاق المتبقي\n"
        "تثبيت الربح بالكامل"
    )


def vip_stop_followup_message(signal: sqlite3.Row, price: float) -> str:
    return (
        f"{signal['coin_symbol']}/USDT متابعة VIP\n\n"
        f"تم إلغاء الفكرة الحالية عند {format_price(price)}\n\n"
        "الإجراء:\n"
        "خروج كامل\n"
        "انتظار تمركز جديد"
    )


def teaser_signal_message(signal: SignalAnalysis) -> str:
    notes = [
        signal.reason,
        f"منطقة مهمة بين {format_price(signal.support_level)} و {format_price(signal.resistance_level)}",
    ]
    if signal.side == "long":
        outlook = f"أرى فرصة قوية إذا حافظ السعر على {format_price(signal.support_level)}"
    else:
        outlook = f"أرى ضغط بيع إذا بقي السعر دون {format_price(signal.resistance_level)}"

    return build_free_teaser_post(
        symbol=signal.symbol,
        price=signal.price,
        prices=[signal.price],
        change_24h=signal.change_pct,
        notes=notes,
        outlook=outlook,
        change_1h_override=signal.change_1h,
        change_4h_override=signal.change_4h,
    )


def simple_trend_label(*, price: float, short_ma: float | None, recent_move_pct_value: float | None) -> str:
    if short_ma is None or recent_move_pct_value is None:
        return "جانبي"
    if price > short_ma and recent_move_pct_value > 0.3:
        return "صاعد"
    if price < short_ma and recent_move_pct_value < -0.3:
        return "هابط"
    return "جانبي"


def free_market_update_message(
    symbol: str,
    price: float,
    prices: list[float],
    change_pct: float,
    trend: str,
) -> str:
    short_ma = moving_average(prices, TREND_MA_WINDOW)
    rsi = compute_rsi(prices)
    support, resistance = support_resistance_levels(prices) or (price, price)
    notes = [
        f"الاتجاه القصير {trend} مع تمركز حول {format_price(short_ma or price)}",
        f"الدعم الأقرب {format_price(support)} والمقاومة {format_price(resistance)}",
    ]
    return build_free_teaser_post(
        symbol=symbol,
        price=price,
        prices=prices,
        change_24h=change_pct,
        notes=notes,
        outlook=snapshot_signal_bias(price=price, short_ma=short_ma, rsi=rsi),
    )


def vip_market_update_message(
    symbol: str,
    price: float,
    prices: list[float],
    change_pct: float,
    trend: str,
) -> str:
    short_ma = moving_average(prices, TREND_MA_WINDOW)
    support, resistance = support_resistance_levels(prices) or (price, price)
    detail_lines = [
        f"الزخم اللحظي {trend} مع تمركز حول {format_price(short_ma or price)}",
        f"المراقبة الآن بين دعم {format_price(support)} ومقاومة {format_price(resistance)}",
    ]
    return build_vip_insight_post(
        title="تحديث VIP 📊",
        symbol=symbol,
        price=price,
        prices=prices,
        change_pct=change_pct,
        detail_lines=detail_lines,
        outlook=snapshot_signal_bias(price=price, short_ma=short_ma, rsi=compute_rsi(prices)),
    )


def short_analysis_message(summary: str, detail: str, note: str) -> str:
    return (
        "تحليل سريع:\n\n"
        f"{summary}\n"
        f"{detail}\n"
        f"{note}"
    )


def top_movers_message(
    gainers: list[tuple[str, float]],
    losers: list[tuple[str, float]],
) -> str:
    gainers_text = "\n".join(f"{symbol} {change:+.1f}%" for symbol, change in gainers) or "لا يوجد"
    losers_text = "\n".join(f"{symbol} {change:+.1f}%" for symbol, change in losers) or "لا يوجد"
    return (
        "الأبرز في السوق 📈\n\n"
        f"صاعد:\n{gainers_text}\n\n"
        f"هابط:\n{losers_text}"
    )


def support_resistance_message(
    symbol: str,
    trend: str,
    support: float,
    resistance: float,
    reward_pct: float,
    risk_pct: float,
) -> str:
    return (
        f"{symbol} مستويات فنية\n\n"
        f"الاتجاه: {trend}\n"
        f"دعم: {format_price(support)}\n"
        f"مقاومة: {format_price(resistance)}\n\n"
        f"عائد محتمل: +{reward_pct:.1f}%\n"
        f"مخاطرة: -{risk_pct:.1f}%"
    )


def fear_greed_message(value: int, label: str) -> str:
    return (
        "الخوف والطمع 🧠\n\n"
        f"المؤشر: {value}\n"
        f"الحالة: {label}"
    )


def daily_summary_message(total: int, wins: int, losses: int) -> str:
    return (
        "ملخص اليوم 📘\n\n"
        f"النتائج: {total}\n"
        f"رابحة: {wins}\n"
        f"خاسرة: {losses}"
    )


def whale_alert_message(symbol: str, change_pct: float, volume_ratio_pct: float) -> str:
    side_line = "شراء واضح" if change_pct > 0 else "بيع واضح"
    return (
        "تنبيه سيولة 🐋\n\n"
        f"{symbol} {change_pct:+.1f}%\n"
        f"{side_line}\n"
        f"نشاط: {volume_ratio_pct:.1f}%"
    )


def build_short_analysis(snapshot: dict[str, dict[str, float | list[float]]]) -> str | None:
    available = [
        (coin.symbol, snapshot.get(coin.symbol))
        for coin in SIGNAL_COINS
        if snapshot.get(coin.symbol)
    ]
    if not available:
        return None

    style = pick_variant(
        "free_analysis_style",
        ("btc_eth", "coin_of_day", "main_pair", "end_day"),
    )

    if style == "btc_eth":
        btc = snapshot.get("BTC")
        eth = snapshot.get("ETH")
        if not btc or not eth:
            style = "coin_of_day"
        else:
            btc_prices = list(btc["prices"])
            eth_change = float(eth["change_pct"])
            btc_price = float(btc["price"])
            btc_change = float(btc["change_pct"])
            notes = [
                f"BTC يتحرك حول {format_price(btc_price)} مع زخم {btc_change:+.1f}%",
                f"ETH يتحرك {eth_change:+.1f}% وهذا يؤكد مزاج السوق العام",
            ]
            return build_free_teaser_post(
                symbol="BTC",
                price=btc_price,
                prices=btc_prices,
                change_24h=btc_change,
                notes=notes,
                outlook=snapshot_signal_bias(
                    price=btc_price,
                    short_ma=moving_average(btc_prices, TREND_MA_WINDOW),
                    rsi=compute_rsi(btc_prices),
                ),
            )

    if style == "main_pair":
        symbol = "ETH" if snapshot.get("ETH") else available[0][0]
    elif style == "end_day":
        symbol = "BTC" if snapshot.get("BTC") else available[0][0]
    else:
        symbol = max(
            (symbol for symbol, _ in available),
            key=lambda item: abs(float(snapshot[item]["change_pct"])),
        )

    data = snapshot[symbol]
    prices = list(data["prices"])
    price = float(data["price"])
    change_24h = float(data["change_pct"])
    trend = simple_trend_label(
        price=price,
        short_ma=moving_average(prices, TREND_MA_WINDOW),
        recent_move_pct_value=recent_move_pct(prices, RECENT_MOVE_WINDOW),
    )
    support, resistance = support_resistance_levels(prices) or (price, price)

    if style == "end_day":
        notes = [
            f"الحركة الحالية {trend} قرب {format_price(price)}",
            f"المستوى الحاسم بين {format_price(support)} و {format_price(resistance)}",
        ]
        outlook = "أتوقع جلسة تالية أوضح إذا استمر الثبات الحالي"
    else:
        notes = [
            f"العملة الأوضح الآن {symbol} بحركة {change_24h:+.1f}%",
            f"تتحرك بين دعم {format_price(support)} ومقاومة {format_price(resistance)}",
        ]
        outlook = snapshot_signal_bias(
            price=price,
            short_ma=moving_average(prices, TREND_MA_WINDOW),
            rsi=compute_rsi(prices),
        )

    return build_free_teaser_post(
        symbol=symbol,
        price=price,
        prices=prices,
        change_24h=change_24h,
        notes=notes,
        outlook=outlook,
    )


def build_vip_analysis_update(snapshot: dict[str, dict[str, float | list[float]]]) -> str | None:
    available = [
        (coin.symbol, snapshot.get(coin.symbol))
        for coin in SIGNAL_COINS
        if snapshot.get(coin.symbol)
    ]
    if not available:
        return None

    symbol = max(
        (coin_symbol for coin_symbol, _ in available),
        key=lambda item: abs(float(snapshot[item]["change_pct"])),
    )
    data = snapshot[symbol]
    context_data = snapshot_context(symbol, data)
    if not context_data:
        return None

    detail_lines = [
        str(context_data["reason"]),
        f"السوق يتحرك الآن بنغمة {context_data['trend']}",
    ]
    if str(context_data["side"]) == "long":
        outlook = f"أرى فرصة شراء إذا حافظ السعر على {format_price(float(context_data['support']))}"
    else:
        outlook = f"أرى ضغط بيع إذا بقي السعر دون {format_price(float(context_data['resistance']))}"

    return build_vip_insight_post(
        title="تحليل VIP سريع",
        symbol=symbol,
        price=float(context_data["price"]),
        prices=list(context_data["prices"]),
        change_pct=float(context_data["change_pct"]),
        detail_lines=detail_lines,
        outlook=outlook,
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
        change_1h=+1.1,
        change_4h=-2.2,
        rsi=27.3,
        short_ma=88200.0,
        recent_move_pct=-3.8,
        reason="RSI منخفض مع اتجاه قصير هابط",
        support_level=85800.0,
        resistance_level=88950.0,
        context="الثبات فوق 85800.00 صاعد | الكسر أسفل 85800.00 هابط",
        timing="دخول الآن قرب الدعم",
        confidence="مرتفعة",
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


def fear_greed_label_ar(label: str) -> str:
    mapping = {
        "Extreme Fear": "خوف شديد",
        "Fear": "خوف",
        "Neutral": "محايد",
        "Greed": "طمع",
        "Extreme Greed": "طمع شديد",
    }
    return mapping.get(label, "محايد")


def choose_levels_coin(snapshot: dict[str, dict[str, float | list[float]]]) -> tuple[str, dict[str, float | list[float]]] | None:
    if not snapshot:
        return None
    symbol = max(snapshot, key=lambda item: abs(float(snapshot[item]["change_pct"])))
    return symbol, snapshot[symbol]


def build_support_resistance_post(
    snapshot: dict[str, dict[str, float | list[float]]]
) -> str | None:
    selection = choose_levels_coin(snapshot)
    if not selection:
        return None

    symbol, data = selection
    prices = [float(value) for value in list(data["prices"])]
    if len(prices) < TREND_MA_WINDOW + 1:
        return None

    window = prices[-24:] if len(prices) >= 24 else prices
    price = float(data["price"])
    support = min(window)
    resistance = max(window)
    trend = simple_trend_label(
        price=price,
        short_ma=moving_average(prices, TREND_MA_WINDOW),
        recent_move_pct_value=recent_move_pct(prices, RECENT_MOVE_WINDOW),
    )
    notes = [
        f"دعم مهم قرب {format_price(support)} ومقاومة عند {format_price(resistance)}",
        f"أي كسر لهذا النطاق سيحدد الحركة التالية بشكل أوضح",
    ]
    if trend == "صاعد":
        outlook = f"أرى فرصة استمرار إذا حافظ السعر على {format_price(support)}"
    elif trend == "هابط":
        outlook = f"أرى ضغطًا بيعيًا إذا فشل السعر في استعادة {format_price(resistance)}"
    else:
        outlook = "أرى السوق متماسكًا والانتظار أفضل حتى يظهر اتجاه أوضح"

    return build_free_teaser_post(
        symbol=symbol,
        price=price,
        prices=prices,
        change_24h=float(data["change_pct"]),
        notes=notes,
        outlook=outlook,
    )


def build_vip_levels_update(
    snapshot: dict[str, dict[str, float | list[float]]]
) -> str | None:
    selection = choose_levels_coin(snapshot)
    if not selection:
        return None

    symbol, data = selection
    context_data = snapshot_context(symbol, data)
    if not context_data:
        return None

    detail_lines = [
        f"المستوى الحاسم الآن بين {format_price(float(context_data['support']))} و {format_price(float(context_data['resistance']))}",
        str(context_data["reason"]),
    ]
    if str(context_data["side"]) == "long":
        outlook = "أفضلية الشراء تبقى قائمة إذا استمر الثبات الحالي"
    else:
        outlook = "أفضلية الحذر قائمة ما دام الرفض مستمرًا قرب المقاومة"

    return build_vip_insight_post(
        title="مستويات VIP",
        symbol=symbol,
        price=float(context_data["price"]),
        prices=list(context_data["prices"]),
        change_pct=float(context_data["change_pct"]),
        detail_lines=detail_lines,
        outlook=outlook,
    )


def build_top_movers_post(
    overview: list[dict],
    snapshot: dict[str, dict[str, float | list[float]]],
) -> str | None:
    movers: list[tuple[str, float]] = []
    for item in overview:
        change_pct = item.get("price_change_percentage_24h")
        symbol = item.get("symbol")
        if change_pct is None or not symbol:
            continue
        movers.append((str(symbol).upper(), float(change_pct)))

    if not movers:
        return None

    gainers = sorted((item for item in movers if item[1] > 0), key=lambda item: item[1], reverse=True)[:2]
    losers = sorted((item for item in movers if item[1] < 0), key=lambda item: item[1])[:2]
    if not gainers and not losers:
        return None

    lead_symbol = None
    if gainers:
        lead_symbol = gainers[0][0]
    elif losers:
        lead_symbol = losers[0][0]
    if not lead_symbol or lead_symbol not in snapshot:
        return None

    data = snapshot[lead_symbol]
    prices = list(data["prices"])
    price = float(data["price"])
    notes = [
        f"أقوى صاعد: {gainers[0][0]} {gainers[0][1]:+.1f}%" if gainers else f"أقوى هابط: {losers[0][0]} {losers[0][1]:+.1f}%",
        f"أضعف أداء: {losers[0][0]} {losers[0][1]:+.1f}%" if losers else "باقي العملات تتحرك بهدوء نسبي",
    ]
    outlook = "أرى فرصة متابعة للعملة الأقوى إذا استمر الزخم الحالي"
    return build_free_teaser_post(
        symbol=lead_symbol,
        price=price,
        prices=prices,
        change_24h=float(data["change_pct"]),
        notes=notes,
        outlook=outlook,
    )


def build_vip_top_movers_update(
    overview: list[dict],
    snapshot: dict[str, dict[str, float | list[float]]],
) -> str | None:
    movers: list[tuple[str, float]] = []
    for item in overview:
        change_pct = item.get("price_change_percentage_24h")
        symbol = item.get("symbol")
        if change_pct is None or not symbol:
            continue
        movers.append((str(symbol).upper(), float(change_pct)))

    if not movers:
        return None

    gainers = sorted((item for item in movers if item[1] > 0), key=lambda item: item[1], reverse=True)[:2]
    losers = sorted((item for item in movers if item[1] < 0), key=lambda item: item[1])[:2]
    lead_symbol = gainers[0][0] if gainers else losers[0][0] if losers else None
    if not lead_symbol or lead_symbol not in snapshot:
        return None

    data = snapshot[lead_symbol]
    detail_lines = [
        f"الأقوى الآن: {gainers[0][0]} {gainers[0][1]:+.1f}%" if gainers else f"الأوضح هبوطًا: {losers[0][0]} {losers[0][1]:+.1f}%",
        f"الأضعف: {losers[0][0]} {losers[0][1]:+.1f}%" if losers else "باقي السوق أكثر هدوءًا",
    ]
    return build_vip_insight_post(
        title="قراءة تدفق السوق VIP",
        symbol=lead_symbol,
        price=float(data["price"]),
        prices=list(data["prices"]),
        change_pct=float(data["change_pct"]),
        detail_lines=detail_lines,
        outlook="أتابع العملة الأقوى فقط إذا بقي الزخم مدعومًا بالحجم",
    )


def build_market_overview_from_snapshot(
    snapshot: dict[str, dict[str, float | list[float] | str | None]]
) -> list[dict]:
    overview: list[dict] = []
    for coin in SIGNAL_COINS:
        data = snapshot.get(coin.symbol)
        if not data:
            continue
        overview.append(
            {
                "id": coin.coingecko_id,
                "symbol": coin.symbol.lower(),
                "price_change_percentage_24h": data.get("change_pct"),
                "total_volume": data.get("total_volume"),
                "market_cap": data.get("market_cap"),
            }
        )
    return overview


def find_whale_candidate(overview: list[dict]) -> tuple[str, float, float] | None:
    tracked_ids = {coin.coingecko_id: coin.symbol for coin in SIGNAL_COINS}
    best_candidate: tuple[str, float, float] | None = None
    best_score = 0.0

    for item in overview:
        coin_id = str(item.get("id") or "")
        symbol = tracked_ids.get(coin_id)
        change_pct = item.get("price_change_percentage_24h")
        total_volume = item.get("total_volume")
        market_cap = item.get("market_cap")
        if not symbol or change_pct is None or not total_volume or not market_cap:
            continue

        volume_ratio = float(total_volume) / float(market_cap) if market_cap else 0.0
        change_value = float(change_pct)
        if abs(change_value) < 4.0 or volume_ratio < 0.045:
            continue

        score = abs(change_value) * volume_ratio
        if score > best_score:
            best_score = score
            best_candidate = (symbol, change_value, volume_ratio * 100)

    return best_candidate


def build_fear_greed_post(
    snapshot: dict[str, dict[str, float | list[float]]],
    value: int,
    label: str,
) -> str | None:
    data = snapshot.get("BTC")
    if not data:
        return None

    prices = list(data["prices"])
    price = float(data["price"])
    notes = [
        f"مؤشر الخوف والطمع عند {value} وهذا يعكس حالة {label}",
        "BTC يبقى أفضل مقياس لاتجاه السوق في هذه المرحلة",
    ]
    if value <= 25:
        outlook = "أرى فرصة ارتداد قائمة لكن التنفيذ يحتاج تأكيد"
    elif value >= 75:
        outlook = "أرى احتمالات تصحيح إذا ضعف الزخم الحالي"
    else:
        outlook = "أرى السوق متوازنًا والفرص تحتاج انتقاءً أفضل"

    return build_free_teaser_post(
        symbol="BTC",
        price=price,
        prices=prices,
        change_24h=float(data["change_pct"]),
        notes=notes,
        outlook=outlook,
    )


def build_vip_fear_greed_update(
    snapshot: dict[str, dict[str, float | list[float]]],
    value: int,
    label: str,
) -> str | None:
    data = snapshot.get("BTC")
    if not data:
        return None

    detail_lines = [
        f"الخوف والطمع عند {value} وحالة السوق {label}",
        "قراءة المزاج العام تدعم اختيار الفرص وليس مطاردة الحركة",
    ]
    if value <= 25:
        outlook = "أفضلية الارتداد قائمة لكن التنفيذ يحتاج ثباتًا فعليًا"
    elif value >= 75:
        outlook = "مناطق الطمع تبرر تشديد الإدارة وانتظار دخول أوضح"
    else:
        outlook = "القراءة متوازنة والاتجاه القادم سيحسمه الكسر أو الثبات"

    return build_vip_insight_post(
        title="مزاج السوق VIP",
        symbol="BTC",
        price=float(data["price"]),
        prices=list(data["prices"]),
        change_pct=float(data["change_pct"]),
        detail_lines=detail_lines,
        outlook=outlook,
    )


def build_daily_summary_post(
    snapshot: dict[str, dict[str, float | list[float]]],
    total: int,
    wins: int,
    losses: int,
) -> str | None:
    data = snapshot.get("BTC") or next(iter(snapshot.values()), None)
    if not data:
        return None

    symbol = "BTC" if snapshot.get("BTC") else next(iter(snapshot))
    prices = list(data["prices"])
    price = float(data["price"])
    notes = [
        f"ملخص اليوم: {wins} رابحة مقابل {losses} خاسرة من أصل {total}",
        "النتيجة تعطي صورة سريعة عن جودة الحركة الحالية في السوق",
    ]
    if wins > losses:
        outlook = "أرى أن المزاج العام إيجابي لكن الانتقاء مهم"
    elif losses > wins:
        outlook = "أرى أن السوق حذر ويحتاج صفقات أكثر انتقائية"
    else:
        outlook = "أرى توازنًا واضحًا والسوق ينتظر محفزًا أقوى"

    return build_free_teaser_post(
        symbol=symbol,
        price=price,
        prices=prices,
        change_24h=float(data["change_pct"]),
        notes=notes,
        outlook=outlook,
    )


def build_vip_daily_summary_update(
    snapshot: dict[str, dict[str, float | list[float]]],
    total: int,
    wins: int,
    losses: int,
) -> str | None:
    data = snapshot.get("BTC") or next(iter(snapshot.values()), None)
    if not data:
        return None

    symbol = "BTC" if snapshot.get("BTC") else next(iter(snapshot))
    detail_lines = [
        f"إجمالي الصفقات اليوم {total} منها {wins} رابحة و {losses} خاسرة",
        "القراءة اليومية تساعد على ضبط الإيقاع وعدم زيادة المخاطرة",
    ]
    if wins > losses:
        outlook = "الأداء اليومي جيد لكن الأفضلية تبقى للصفقات المنتقاة"
    elif losses > wins:
        outlook = "الأداء حذر اليوم، لذلك الانتقاء والانضباط أهم من التكرار"
    else:
        outlook = "توازن النتائج يعني أن السوق ما زال يحتاج فلترة أعلى"

    return build_vip_insight_post(
        title="ملخص VIP اليومي",
        symbol=symbol,
        price=float(data["price"]),
        prices=list(data["prices"]),
        change_pct=float(data["change_pct"]),
        detail_lines=detail_lines,
        outlook=outlook,
    )


def build_whale_alert_post(
    snapshot: dict[str, dict[str, float | list[float]]],
    symbol: str,
    change_pct: float,
    volume_ratio_pct: float,
) -> str | None:
    data = snapshot.get(symbol)
    if not data:
        return None

    prices = list(data["prices"])
    price = float(data["price"])
    notes = [
        f"نشاط سيولة غير معتاد بقوة {volume_ratio_pct:.1f}%",
        "الحركة مدعومة بحجم تداول ملفت ويستحق المتابعة",
    ]
    if change_pct > 0:
        outlook = "أرى فرصة متابعة إذا استمر الثبات فوق المستوى الحالي"
    else:
        outlook = "أرى أن الضغط قائم ما لم يظهر امتصاص واضح للبيع"

    return build_free_teaser_post(
        symbol=symbol,
        price=price,
        prices=prices,
        change_24h=float(data["change_pct"]),
        notes=notes,
        outlook=outlook,
    )


def build_vip_whale_alert_update(
    snapshot: dict[str, dict[str, float | list[float]]],
    symbol: str,
    change_pct: float,
    volume_ratio_pct: float,
) -> str | None:
    data = snapshot.get(symbol)
    if not data:
        return None

    detail_lines = [
        f"نشاط سيولة مرتفع بنسبة {volume_ratio_pct:.1f}% مع حركة {change_pct:+.1f}%",
        "هذا النوع من التدفق قد يسبق حركة متابعة إذا حافظ السعر على مستواه الحالي",
    ]
    if change_pct > 0:
        outlook = "أراقب استمرار الصعود فقط إذا لم يفقد السعر زخمه في الساعات القادمة"
    else:
        outlook = "أتعامل معها كإشارة حذر حتى يظهر امتصاص واضح للضغط البيعي"

    return build_vip_insight_post(
        title="تنبيه VIP للتدفق",
        symbol=symbol,
        price=float(data["price"]),
        prices=list(data["prices"]),
        change_pct=float(data["change_pct"]),
        detail_lines=detail_lines,
        outlook=outlook,
    )


def choose_chart_coin(
    snapshot: dict[str, dict[str, float | list[float]]]
) -> tuple[str, dict[str, float | list[float]]] | None:
    if not snapshot:
        return None
    if snapshot.get("BTC") and random.random() < 0.6:
        return "BTC", snapshot["BTC"]
    selection = choose_levels_coin(snapshot)
    if selection:
        return selection
    symbol = next(iter(snapshot))
    return symbol, snapshot[symbol]


def build_free_chart_caption(
    snapshot: dict[str, dict[str, float | list[float]]],
    symbol: str,
) -> str | None:
    data = snapshot.get(symbol)
    if not data:
        return None

    context_data = snapshot_context(symbol, data)
    if not context_data:
        return None

    support = float(context_data["support"])
    resistance = float(context_data["resistance"])
    notes = [
        str(context_data["reason"]),
        f"النطاق الحالي بين {format_price(support)} و {format_price(resistance)}",
    ]
    if str(context_data["side"]) == "long":
        outlook = f"أرى فرصة متابعة إذا حافظ السعر على {format_price(support)}"
    else:
        outlook = f"أرى ضغطًا قائمًا إذا بقي السعر دون {format_price(resistance)}"

    return build_free_teaser_post(
        symbol=symbol,
        price=float(context_data["price"]),
        prices=list(context_data["prices"]),
        change_24h=float(context_data["change_pct"]),
        notes=notes,
        outlook=outlook,
    )


def build_vip_chart_caption(
    snapshot: dict[str, dict[str, float | list[float]]],
    symbol: str,
) -> str | None:
    data = snapshot.get(symbol)
    if not data:
        return None

    context_data = snapshot_context(symbol, data)
    if not context_data:
        return None

    support = float(context_data["support"])
    resistance = float(context_data["resistance"])
    detail_lines = [
        str(context_data["reason"]),
        f"النطاق الحاسم الآن بين {format_price(support)} و {format_price(resistance)}",
    ]
    if str(context_data["side"]) == "long":
        outlook = f"أفضلية الحركة تبقى إيجابية إذا استمر الثبات فوق {format_price(support)}"
    else:
        outlook = f"أفضلية الحذر قائمة إذا استمر الرفض دون {format_price(resistance)}"

    return build_vip_insight_post(
        title="شارت VIP",
        symbol=symbol,
        price=float(context_data["price"]),
        prices=list(context_data["prices"]),
        change_pct=float(context_data["change_pct"]),
        detail_lines=detail_lines,
        outlook=outlook,
    )


def render_price_chart(
    *,
    symbol: str,
    prices: list[float],
    support: float,
    resistance: float,
) -> io.BytesIO | None:
    if plt is None or len(prices) < 2:
        return None

    figure, axis = plt.subplots(figsize=(8, 4.6), dpi=140)
    figure.patch.set_facecolor("#f7f4ed")
    axis.set_facecolor("#fffdf8")
    x_values = list(range(len(prices)))
    axis.plot(x_values, prices, color="#0f766e", linewidth=2.4)
    axis.fill_between(x_values, prices, min(prices), color="#99f6e4", alpha=0.18)
    axis.axhline(support, color="#2563eb", linestyle="--", linewidth=1.2)
    axis.axhline(resistance, color="#dc2626", linestyle="--", linewidth=1.2)
    axis.scatter([len(prices) - 1], [prices[-1]], color="#111827", s=24, zorder=3)
    axis.set_title(f"{symbol}/USDT", color="#111827", fontsize=14, fontweight="bold")
    axis.grid(alpha=0.18)
    axis.spines["top"].set_visible(False)
    axis.spines["right"].set_visible(False)
    axis.tick_params(labelsize=8, colors="#374151")
    axis.text(
        0.02,
        0.96,
        f"دعم {format_price(support)}",
        transform=axis.transAxes,
        ha="left",
        va="top",
        fontsize=8,
        color="#2563eb",
    )
    axis.text(
        0.98,
        0.96,
        f"مقاومة {format_price(resistance)}",
        transform=axis.transAxes,
        ha="right",
        va="top",
        fontsize=8,
        color="#dc2626",
    )
    figure.tight_layout()

    buffer = io.BytesIO()
    figure.savefig(buffer, format="png", bbox_inches="tight")
    plt.close(figure)
    buffer.seek(0)
    buffer.name = f"{symbol.lower()}_chart.png"
    return buffer


def build_free_promo_post(
    snapshot: dict[str, dict[str, float | list[float]]],
) -> str | None:
    data = snapshot.get("BTC") or snapshot.get("ETH")
    if not data:
        return None

    symbol = "BTC" if snapshot.get("BTC") else "ETH"
    prices = list(data["prices"])
    price = float(data["price"])
    notes = [
        "السوق عند منطقة مهمة والفرص الحقيقية تحتاج تنفيذًا أدق",
        "في القناة المجانية نعطي القراءة السريعة فقط بدون تفاصيل الدخول الكاملة",
    ]
    outlook = "أرى فرصًا واضحة لكن التنفيذ الأفضل يبقى داخل قناة الـ VIP"
    return build_free_teaser_post(
        symbol=symbol,
        price=price,
        prices=prices,
        change_24h=float(data["change_pct"]),
        notes=notes,
        outlook=outlook,
    )


class Storage:
    def __init__(self, path: str) -> None:
        self.path = path

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        return conn

    def ensure_column(
        self,
        conn: sqlite3.Connection,
        table_name: str,
        column_name: str,
        column_sql: str,
    ) -> None:
        columns = {
            str(row["name"])
            for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()
        }
        if column_name not in columns:
            try:
                conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_sql}")
            except sqlite3.OperationalError as exc:
                if "duplicate column name" not in str(exc).lower():
                    raise

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
                    last_management_stage INTEGER NOT NULL DEFAULT 0,
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
            self.ensure_column(
                conn,
                "trade_signals",
                "last_management_stage",
                "INTEGER NOT NULL DEFAULT 0",
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
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS market_price_samples (
                    coin_symbol TEXT NOT NULL,
                    sampled_at INTEGER NOT NULL,
                    price REAL NOT NULL,
                    change_pct REAL NOT NULL,
                    PRIMARY KEY (coin_symbol, sampled_at)
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_market_price_samples_coin_time
                ON market_price_samples(coin_symbol, sampled_at DESC)
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

    def record_market_price_sample(
        self,
        coin_symbol: str,
        sampled_at: int,
        price: float,
        change_pct: float,
    ) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO market_price_samples (
                    coin_symbol,
                    sampled_at,
                    price,
                    change_pct
                )
                VALUES (?, ?, ?, ?)
                """,
                (coin_symbol, sampled_at, price, change_pct),
            )
            conn.execute(
                """
                DELETE FROM market_price_samples
                WHERE coin_symbol = ?
                  AND sampled_at NOT IN (
                      SELECT sampled_at
                      FROM market_price_samples
                      WHERE coin_symbol = ?
                      ORDER BY sampled_at DESC
                      LIMIT ?
                  )
                """,
                (coin_symbol, coin_symbol, LOCAL_PRICE_HISTORY_LIMIT),
            )

    def list_recent_market_prices(self, coin_symbol: str, limit: int = LOCAL_PRICE_HISTORY_LIMIT) -> list[float]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT price
                FROM market_price_samples
                WHERE coin_symbol = ?
                ORDER BY sampled_at DESC
                LIMIT ?
                """,
                (coin_symbol, limit),
            ).fetchall()
        return [float(row["price"]) for row in reversed(rows)]

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

    def update_management_stage(self, signal_id: int, stage: int) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE trade_signals
                SET last_management_stage = CASE
                    WHEN last_management_stage > ? THEN last_management_stage
                    ELSE ?
                END
                WHERE id = ?
                """,
                (stage, stage, signal_id),
            )

    def move_trade_stop_to_entry(self, signal_id: int) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE trade_signals
                SET stop_loss = entry_price
                WHERE id = ?
                """,
                (signal_id,),
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

    def get_daily_result_summary(self, current_ts: int) -> dict[str, int]:
        day_start = utc_day_start(current_ts)
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    SUM(CASE WHEN result_pct > 0 THEN 1 ELSE 0 END) AS wins,
                    SUM(CASE WHEN result_pct <= 0 THEN 1 ELSE 0 END) AS losses
                FROM trade_signals
                WHERE closed_at IS NOT NULL
                  AND closed_at >= ?
                """,
                (day_start,),
            ).fetchone()

        return {
            "total": int(row["total"] or 0) if row else 0,
            "wins": int(row["wins"] or 0) if row else 0,
            "losses": int(row["losses"] or 0) if row else 0,
        }


storage = Storage(SETTINGS.db_path)
storage.init()


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


async def send_free_channel_photo(bot, photo, caption: str) -> None:
    await bot.send_photo(
        SETTINGS.free_channel_id,
        photo=photo,
        caption=caption,
        reply_markup=free_vip_keyboard(),
    )


async def send_vip_channel_photo(bot, photo, caption: str, *, post_type: str) -> bool:
    logger.info("Sending %s photo to VIP channel: vip_channel_id=%s", post_type, SETTINGS.vip_channel_id)
    try:
        sent_message = await bot.send_photo(
            SETTINGS.vip_channel_id,
            photo=photo,
            caption=caption,
        )
    except TelegramError as exc:
        logger.exception(
            "Failed to send %s photo to VIP channel: vip_channel_id=%s error=%s",
            post_type,
            SETTINGS.vip_channel_id,
            exc,
        )
        return False
    logger.info(
        "VIP photo send success: post_type=%s vip_channel_id=%s message_id=%s",
        post_type,
        SETTINGS.vip_channel_id,
        sent_message.message_id,
    )
    return True


async def send_vip_channel_post(bot, text: str, *, post_type: str) -> bool:
    logger.info("Sending %s to VIP channel: vip_channel_id=%s", post_type, SETTINGS.vip_channel_id)
    try:
        sent_message = await bot.send_message(SETTINGS.vip_channel_id, text)
    except TelegramError as exc:
        logger.exception(
            "Failed to send %s to VIP channel: vip_channel_id=%s error=%s",
            post_type,
            SETTINGS.vip_channel_id,
            exc,
        )
        return False
    logger.info(
        "VIP send success: post_type=%s vip_channel_id=%s message_id=%s",
        post_type,
        SETTINGS.vip_channel_id,
        sent_message.message_id,
    )
    return True


async def send_generated_signal_posts(bot, analysis: SignalAnalysis) -> bool:
    logger.info(
        "Sending FULL signal to VIP channel: chat_id=%s symbol=%s",
        SETTINGS.vip_channel_id,
        analysis.symbol,
    )
    vip_sent = await send_vip_channel_post(
        bot,
        vip_signal_message(analysis),
        post_type=f"FULL signal {analysis.symbol}",
    )
    if not vip_sent:
        return False

    try:
        await send_free_channel_post(bot, teaser_signal_message(analysis))
    except TelegramError:
        logger.exception(
            "Failed to send teaser signal to FREE channel: chat_id=%s symbol=%s",
            SETTINGS.free_channel_id,
            analysis.symbol,
        )
    return True


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


def vip_offer_message() -> str:
    return (
        "💎 اشتراك VIP\n\n"
        f"السعر الحالي: ما يعادل {VIP_DISPLAY_PRICE_AED} درهم من النجوم\n"
        f"خصم {VIP_DISCOUNT_PERCENT}% لفترة محدودة\n\n"
        "اضغط على الفاتورة التالية لإتمام الاشتراك"
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

    await message.reply_text(
        vip_offer_message(),
        reply_markup=main_menu_keyboard(),
    )

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


def merge_local_price_history(coin_symbol: str, current_price: float, prices: list[float] | None = None) -> list[float]:
    history = storage.list_recent_market_prices(coin_symbol, LOCAL_PRICE_HISTORY_LIMIT)
    series = [float(value) for value in (prices or [])]
    if not series:
        series = history
    elif history:
        for price in history:
            if len(series) >= LOCAL_PRICE_HISTORY_LIMIT:
                break
            series.insert(0, price)
    if not series or series[-1] != current_price:
        series.append(current_price)
    return series[-LOCAL_PRICE_HISTORY_LIMIT:]


async def fetch_simple_price_snapshot(
    client: httpx.AsyncClient,
) -> dict[str, dict[str, float | list[float]]]:
    response = await client.get(
        f"{SETTINGS.coingecko_base_url}/simple/price",
        params={
            "ids": ",".join(coin.coingecko_id for coin in SIGNAL_COINS),
            "vs_currencies": "usd",
            "include_24hr_change": "true",
        },
        headers=coingecko_headers(),
    )
    response.raise_for_status()
    payload = response.json()

    snapshot: dict[str, dict[str, float | list[float]]] = {}
    for coin in SIGNAL_COINS:
        item = payload.get(coin.coingecko_id)
        if not item:
            logger.warning("CoinGecko simple price missing for %s", coin.symbol)
            continue

        price = item.get("usd")
        change_pct = item.get("usd_24h_change")
        if price is None or change_pct is None:
            logger.warning(
                "CoinGecko simple price incomplete for %s: price=%s change=%s",
                coin.symbol,
                price,
                change_pct,
            )
            continue

        snapshot[coin.symbol] = {
            "price": float(price),
            "change_pct": float(change_pct),
            "prices": merge_local_price_history(coin.symbol, float(price)),
        }
    return snapshot


async def fetch_market_overview(limit: int = MARKET_OVERVIEW_LIMIT) -> list[dict]:
    async with httpx.AsyncClient(timeout=SETTINGS.coingecko_timeout_seconds) as client:
        response = await client.get(
            f"{SETTINGS.coingecko_base_url}/coins/markets",
            params={
                "vs_currency": "usd",
                "order": "market_cap_desc",
                "per_page": str(limit),
                "page": "1",
                "sparkline": "false",
                "price_change_percentage": "24h",
            },
            headers=coingecko_headers(),
        )
        response.raise_for_status()
        payload = response.json()

    return payload if isinstance(payload, list) else []


async def fetch_fear_greed_index() -> tuple[int, str] | None:
    async with httpx.AsyncClient(timeout=SETTINGS.coingecko_timeout_seconds) as client:
        response = await client.get(
            FEAR_GREED_API_URL,
            params={"limit": "1", "format": "json"},
        )
        response.raise_for_status()
        payload = response.json()

    data = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(data, list) or not data:
        return None

    current = data[0]
    value = current.get("value")
    label = current.get("value_classification", "")
    if value is None:
        return None
    return int(value), fear_greed_label_ar(str(label))


async def fetch_market_snapshot() -> dict[str, dict[str, float | list[float]]]:
    async with httpx.AsyncClient(timeout=SETTINGS.coingecko_timeout_seconds) as client:
        snapshot: dict[str, dict[str, float | list[float]]]
        try:
            response = await client.get(
                f"{SETTINGS.coingecko_base_url}/coins/markets",
                params={
                    "ids": ",".join(coin.coingecko_id for coin in SIGNAL_COINS),
                    "vs_currency": "usd",
                    "price_change_percentage": "24h",
                    "sparkline": "true",
                },
                headers=coingecko_headers(),
            )
            response.raise_for_status()
            payload = response.json()

            snapshot = {}
            payload_by_id = {
                str(item.get("id")): item
                for item in payload
                if isinstance(item, dict) and item.get("id")
            }
            for coin in SIGNAL_COINS:
                item = payload_by_id.get(coin.coingecko_id)
                if not item:
                    logger.warning("CoinGecko data missing for %s", coin.symbol)
                    continue

                price = item.get("current_price")
                change_pct = item.get("price_change_percentage_24h")
                sparkline = item.get("sparkline_in_7d") or {}
                prices = sparkline.get("price") if isinstance(sparkline, dict) else None
                if price is None or change_pct is None:
                    logger.warning(
                        "CoinGecko response incomplete for %s: price=%s change=%s",
                        coin.symbol,
                        price,
                        change_pct,
                    )
                    continue

                snapshot[coin.symbol] = {
                    "price": float(price),
                    "change_pct": float(change_pct),
                    "prices": merge_local_price_history(
                        coin.symbol,
                        float(price),
                        [float(value) for value in prices] if isinstance(prices, list) else None,
                    ),
                    "market_cap": (
                        float(item["market_cap"])
                        if item.get("market_cap") is not None
                        else None
                    ),
                    "total_volume": (
                        float(item["total_volume"])
                        if item.get("total_volume") is not None
                        else None
                    ),
                }
        except httpx.HTTPError as exc:
            logger.warning("CoinGecko bulk market fetch failed, using simple price fallback: %s", exc)
            snapshot = await fetch_simple_price_snapshot(client)

    sampled_at = now_ts()
    for coin_symbol, data in snapshot.items():
        storage.record_market_price_sample(
            coin_symbol,
            sampled_at,
            float(data["price"]),
            float(data["change_pct"]),
        )
    return snapshot


def random_due_in(min_seconds: int, max_seconds: int) -> int:
    return random.randint(min_seconds, max_seconds)


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
        last_management_stage = int(signal["last_management_stage"] or 0)

        progress_ratio = trade_progress_ratio(
            side=side,
            entry_price=float(signal["entry_price"]),
            target1=float(signal["target1"]),
            price=price,
        )
        if (
            last_management_stage < 1
            and age_seconds >= VIP_PROGRESS_MIN_AGE_SECONDS
            and VIP_PROGRESS_UPDATE_THRESHOLD <= progress_ratio < 1
        ):
            await send_vip_channel_post(
                bot,
                vip_trade_progress_message(signal, price, progress_ratio),
                post_type=f"VIP progress {signal['coin_symbol']}",
            )
            storage.update_management_stage(signal["id"], 1)
            last_management_stage = 1

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
            await send_vip_channel_post(
                bot,
                vip_stop_followup_message(signal, price),
                post_type=f"VIP stop follow-up {signal['coin_symbol']}",
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
        if target_level == 1:
            storage.move_trade_stop_to_entry(signal["id"])
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
        await send_vip_channel_post(
            bot,
            vip_target_management_message(signal, target_level, price),
            post_type=f"VIP target follow-up {signal['coin_symbol']} T{target_level}",
        )
        storage.update_management_stage(signal["id"], 2 if target_level == 1 else 3)


async def process_market_signals(bot, snapshot: dict[str, dict[str, float | list[float]]]) -> None:
    current_ts = now_ts()
    candidate_signals = 0
    blocked_signals = 0
    sent_signals = 0

    for coin in SIGNAL_COINS:
        logger.info("Processing coin: symbol=%s coingecko_id=%s", coin.symbol, coin.coingecko_id)
        data = snapshot.get(coin.symbol)
        if not data:
            logger.warning("No snapshot data available for %s", coin.symbol)
            continue

        analysis = analyze_signal(
            coin.symbol,
            price=float(data["price"]),
            change_pct=float(data["change_pct"]),
            prices=list(data["prices"]),
        )
        if not analysis:
            continue
        candidate_signals += 1

        if storage.get_active_trade_signal(coin.symbol):
            logger.info("تخطي إشارة جديدة لـ %s بسبب وجود صفقة نشطة", coin.symbol)
            blocked_signals += 1
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
                blocked_signals += 1
                continue

        logger.info(
            "إشارة جديدة %s: اتجاه=%s change=%+.2f%% rsi=%.2f reason=%s",
            analysis.symbol,
            analysis.side,
            analysis.change_pct,
            analysis.rsi,
            analysis.reason,
        )
        vip_sent = await send_generated_signal_posts(bot, analysis)
        if not vip_sent:
            logger.warning("VIP signal send failed for %s", analysis.symbol)
            continue
        sent_signals += 1
        logger.info(
            "Signal sent for %s: vip_channel_id=%s free_channel_id=%s",
            analysis.symbol,
            SETTINGS.vip_channel_id,
            SETTINGS.free_channel_id,
        )
        storage.record_signal_alert(
            analysis.symbol,
            last_alert_at=current_ts,
            last_change_pct=analysis.change_pct,
            last_price=analysis.price,
        )
        storage.create_trade_signal(analysis, current_ts)

    logger.info(
        "Market signal processing complete: candidates=%s blocked=%s vip_sent=%s",
        candidate_signals,
        blocked_signals,
        sent_signals,
    )


async def process_free_promotions(
    bot,
    snapshot: dict[str, dict[str, float | list[float]]],
) -> None:
    current_ts = now_ts()
    posts_today = storage.promo_posts_today(current_ts)
    last_post_at = storage.last_promo_post_at()

    if posts_today >= PROMO_MAX_PER_DAY:
        return
    if last_post_at is not None and current_ts - last_post_at < PROMO_MIN_INTERVAL_SECONDS:
        return

    message = build_free_promo_post(snapshot)
    if not message:
        return
    await send_free_channel_post(bot, message)
    storage.record_promo_post(current_ts)
    logger.info("تم إرسال منشور ترويجي إلى القناة المجانية")


async def process_free_top_movers(
    bot,
    overview: list[dict],
    snapshot: dict[str, dict[str, float | list[float]]],
) -> list[dict]:
    current_ts = now_ts()
    state = storage.get_market_update_state(FREE_TOP_MOVERS_KEY)
    if state and current_ts < int(state["next_due_at"]):
        return overview

    message = build_top_movers_post(overview, snapshot)
    vip_message = build_vip_top_movers_update(overview, snapshot)
    if not message and not vip_message:
        storage.record_market_update(
            FREE_TOP_MOVERS_KEY,
            sent_at=current_ts,
            next_due_at=current_ts + random_due_in(FREE_TOP_MOVERS_MIN_INTERVAL_SECONDS, FREE_TOP_MOVERS_MAX_INTERVAL_SECONDS),
        )
        return overview

    if vip_message:
        await send_vip_channel_post(bot, vip_message, post_type="top movers update")
    if message:
        await send_free_channel_post(bot, message)
    storage.record_market_update(
        FREE_TOP_MOVERS_KEY,
        sent_at=current_ts,
        next_due_at=current_ts + random_due_in(FREE_TOP_MOVERS_MIN_INTERVAL_SECONDS, FREE_TOP_MOVERS_MAX_INTERVAL_SECONDS),
    )
    logger.info("تم إرسال قائمة الصاعدين والهابطين إلى VIP و FREE")
    return overview


async def process_free_levels_update(
    bot,
    snapshot: dict[str, dict[str, float | list[float]]],
) -> None:
    current_ts = now_ts()
    state = storage.get_market_update_state(FREE_LEVELS_KEY)
    if state and current_ts < int(state["next_due_at"]):
        return

    message = build_support_resistance_post(snapshot)
    vip_message = build_vip_levels_update(snapshot)
    if not message and not vip_message:
        return

    if vip_message:
        await send_vip_channel_post(bot, vip_message, post_type="levels update")
    if message:
        await send_free_channel_post(bot, message)
    storage.record_market_update(
        FREE_LEVELS_KEY,
        sent_at=current_ts,
        next_due_at=current_ts + FREE_LEVELS_INTERVAL_SECONDS,
    )
    logger.info("تم إرسال مستويات الدعم والمقاومة إلى VIP و FREE")


async def process_free_fear_greed(
    bot,
    snapshot: dict[str, dict[str, float | list[float]]],
) -> None:
    current_ts = now_ts()
    state = storage.get_market_update_state(FREE_FEAR_GREED_KEY)
    if state and current_ts < int(state["next_due_at"]):
        return

    try:
        data = await fetch_fear_greed_index()
    except Exception:
        logger.exception("Failed to fetch fear and greed index.")
        return

    if not data:
        return

    value, label = data
    message = build_fear_greed_post(snapshot, value, label)
    vip_message = build_vip_fear_greed_update(snapshot, value, label)
    if not message and not vip_message:
        return
    if vip_message:
        await send_vip_channel_post(bot, vip_message, post_type="fear greed update")
    if message:
        await send_free_channel_post(bot, message)
    storage.record_market_update(
        FREE_FEAR_GREED_KEY,
        sent_at=current_ts,
        next_due_at=current_ts + FREE_FEAR_GREED_INTERVAL_SECONDS,
    )
    logger.info("تم إرسال مؤشر الخوف والطمع إلى VIP و FREE")


async def process_free_daily_summary(
    bot,
    snapshot: dict[str, dict[str, float | list[float]]],
) -> None:
    current_ts = now_ts()
    state = storage.get_market_update_state(FREE_DAILY_SUMMARY_KEY)
    if state and current_ts < int(state["next_due_at"]):
        return

    summary = storage.get_daily_result_summary(current_ts)
    message = build_daily_summary_post(
        snapshot,
        summary["total"],
        summary["wins"],
        summary["losses"],
    )
    vip_message = build_vip_daily_summary_update(
        snapshot,
        summary["total"],
        summary["wins"],
        summary["losses"],
    )
    if not message and not vip_message:
        return
    if vip_message:
        await send_vip_channel_post(bot, vip_message, post_type="daily summary update")
    if message:
        await send_free_channel_post(bot, message)
    next_due_at = utc_day_start(current_ts) + 24 * 60 * 60
    storage.record_market_update(
        FREE_DAILY_SUMMARY_KEY,
        sent_at=current_ts,
        next_due_at=next_due_at,
    )
    logger.info("تم إرسال الملخص اليومي إلى VIP و FREE")


async def process_free_whale_alert(
    bot,
    overview: list[dict],
    snapshot: dict[str, dict[str, float | list[float]]],
) -> list[dict]:
    current_ts = now_ts()
    state = storage.get_market_update_state(FREE_WHALE_KEY)
    if state and current_ts < int(state["next_due_at"]):
        return overview

    candidate = find_whale_candidate(overview)
    if not candidate:
        storage.record_market_update(
            FREE_WHALE_KEY,
            sent_at=current_ts,
            next_due_at=current_ts + FREE_WHALE_MIN_INTERVAL_SECONDS,
        )
        return overview

    symbol, change_pct, volume_ratio_pct = candidate
    message = build_whale_alert_post(snapshot, symbol, change_pct, volume_ratio_pct)
    vip_message = build_vip_whale_alert_update(snapshot, symbol, change_pct, volume_ratio_pct)
    if not message and not vip_message:
        return overview
    if vip_message:
        await send_vip_channel_post(bot, vip_message, post_type="whale alert update")
    if message:
        await send_free_channel_post(bot, message)
    storage.record_market_update(
        FREE_WHALE_KEY,
        sent_at=current_ts,
        next_due_at=current_ts + random_due_in(FREE_WHALE_MIN_INTERVAL_SECONDS, FREE_WHALE_MAX_INTERVAL_SECONDS),
    )
    logger.info(
        "تم إرسال تنبيه سيولة إلى القناة المجانية: %s change=%+.2f%% volume_ratio=%.2f%%",
        symbol,
        change_pct,
        volume_ratio_pct,
    )
    return overview


async def process_free_chart_update(
    bot,
    snapshot: dict[str, dict[str, float | list[float]]],
) -> None:
    current_ts = now_ts()
    state = storage.get_market_update_state(FREE_CHART_KEY)
    if state and current_ts < int(state["next_due_at"]):
        return

    selection = choose_chart_coin(snapshot)
    if not selection:
        return

    symbol, data = selection
    context_data = snapshot_context(symbol, data)
    if not context_data:
        return

    chart = render_price_chart(
        symbol=symbol,
        prices=list(context_data["prices"]),
        support=float(context_data["support"]),
        resistance=float(context_data["resistance"]),
    )
    caption = build_free_chart_caption(snapshot, symbol)
    vip_caption = build_vip_chart_caption(snapshot, symbol)
    if not chart or (not caption and not vip_caption):
        storage.record_market_update(
            FREE_CHART_KEY,
            sent_at=current_ts,
            next_due_at=current_ts + random_due_in(FREE_CHART_MIN_INTERVAL_SECONDS, FREE_CHART_MAX_INTERVAL_SECONDS),
        )
        if plt is None:
            logger.warning("Chart update skipped because matplotlib is not installed.")
        return

    try:
        chart_bytes = chart.getvalue()
        if vip_caption:
            vip_chart = io.BytesIO(chart_bytes)
            vip_chart.name = chart.name
            try:
                await send_vip_channel_photo(bot, vip_chart, vip_caption, post_type="chart update")
            finally:
                vip_chart.close()
        if caption:
            free_chart = io.BytesIO(chart_bytes)
            free_chart.name = chart.name
            try:
                await send_free_channel_photo(bot, free_chart, caption)
            finally:
                free_chart.close()
    finally:
        chart.close()

    storage.record_market_update(
        FREE_CHART_KEY,
        sent_at=current_ts,
        next_due_at=current_ts + random_due_in(FREE_CHART_MIN_INTERVAL_SECONDS, FREE_CHART_MAX_INTERVAL_SECONDS),
    )
    logger.info("تم إرسال شارت تحليلي إلى VIP و FREE: %s", symbol)


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
    message = free_market_update_message(
        FREE_MARKET_UPDATE_SYMBOL,
        price,
        prices,
        change_pct,
        trend,
    )
    vip_message = vip_market_update_message(
        FREE_MARKET_UPDATE_SYMBOL,
        price,
        prices,
        change_pct,
        trend,
    )

    await send_vip_channel_post(bot, vip_message, post_type="market update")
    await send_free_channel_post(bot, message)

    next_due_at = current_ts + FREE_MARKET_UPDATE_INTERVAL_SECONDS
    storage.record_market_update(
        FREE_MARKET_UPDATE_KEY,
        sent_at=current_ts,
        next_due_at=next_due_at,
    )
    logger.info(
        "تم إرسال تحديث السوق إلى VIP و FREE: %s change=%+.2f%% trend=%s",
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
    vip_message = build_vip_analysis_update(snapshot)
    if not message and not vip_message:
        return

    if vip_message:
        await send_vip_channel_post(bot, vip_message, post_type="analysis update")
    if message:
        await send_free_channel_post(bot, message)
    next_due_at = current_ts + FREE_ANALYSIS_INTERVAL_SECONDS
    storage.record_market_update(
        FREE_ANALYSIS_UPDATE_KEY,
        sent_at=current_ts,
        next_due_at=next_due_at,
    )
    logger.info("تم إرسال تحليل مختصر إلى VIP و FREE")


async def process_signal_cycle(bot) -> None:
    logger.info(
        "Running signal cycle: vip_channel_id=%s free_channel_id=%s",
        SETTINGS.vip_channel_id,
        SETTINGS.free_channel_id,
    )
    try:
        snapshot = await fetch_market_snapshot()
    except Exception:
        logger.exception("Market snapshot fetch failed.")
        snapshot = {}
    await process_signal_results(bot, snapshot)
    await process_market_signals(bot, snapshot)
    await process_free_market_update(bot, snapshot)
    await process_free_analysis_update(bot, snapshot)
    overview = build_market_overview_from_snapshot(snapshot)
    overview = await process_free_top_movers(bot, overview, snapshot)
    await process_free_levels_update(bot, snapshot)
    await process_free_fear_greed(bot, snapshot)
    await process_free_daily_summary(bot, snapshot)
    await process_free_whale_alert(bot, overview, snapshot)
    await process_free_chart_update(bot, snapshot)
    await process_free_promotions(bot, snapshot)


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
        await asyncio.sleep(1800)


async def ensure_channel_access(
    bot,
    chat_id: int | str,
    *,
    require_invites: bool = False,
    channel_label: str = "channel",
) -> None:
    me = await bot.get_me()
    member = await bot.get_chat_member(chat_id, me.id)
    if member.status != "administrator":
        raise RuntimeError(f"The bot must be an administrator in the {channel_label} ({chat_id}).")
    if require_invites and not getattr(member, "can_invite_users", False):
        raise RuntimeError(
            f"The bot must have Add Subscribers/Invite Users in the {channel_label} ({chat_id})."
        )
    can_post = getattr(member, "can_post_messages", None)
    if can_post is False:
        raise RuntimeError(
            f"The bot must have Post Messages in the {channel_label} ({chat_id})."
        )
    logger.info(
        "Verified %s access: chat_id=%s can_post_messages=%s can_invite_users=%s",
        channel_label,
        chat_id,
        can_post,
        getattr(member, "can_invite_users", None),
    )


async def post_init(application: Application) -> None:
    storage.init()

    await ensure_channel_access(
        application.bot,
        SETTINGS.vip_channel_id,
        require_invites=True,
        channel_label="VIP channel",
    )
    await ensure_channel_access(
        application.bot,
        SETTINGS.free_channel_id,
        channel_label="FREE channel",
    )
    member = await application.bot.get_chat_member(
        SETTINGS.vip_channel_id,
        (await application.bot.get_me()).id,
    )
    if SETTINGS.access_days > 0 and not getattr(member, "can_restrict_members", False):
        logger.warning(
            "ACCESS_DAYS is enabled, but the bot cannot remove expired users."
        )

    await expire_due_access(application.bot)
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
        vip_sent = await send_generated_signal_posts(context.bot, signal)
    except TelegramError:
        logger.exception("Failed to send test signal to configured channels.")
        await message.reply_text("❌ تعذر إرسال إشارة الاختبار")
        return
    if not vip_sent:
        await message.reply_text("❌ تعذر إرسال الإشارة إلى قناة VIP")
        return

    logger.info("تم إرسال إشارة اختبار إلى VIP و FREE للعملة %s", signal.symbol)
    await message.reply_text("✅ تم إرسال إشارة اختبار إلى القنوات")


async def test_vip_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_private_chat(update):
        return
    message = update.effective_message
    if not message:
        return

    signal = build_test_signal()
    vip_sent = await send_vip_channel_post(
        context.bot,
        vip_signal_message(signal),
        post_type="test VIP message",
    )
    if not vip_sent:
        await message.reply_text("❌ تعذر إرسال اختبار VIP")
        return

    await message.reply_text("✅ تم إرسال اختبار VIP")


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
    if isinstance(context.error, Conflict):
        logger.error(
            "Telegram polling conflict detected. Another bot instance is already running for this token. This instance will stop and retry."
        )
        context.application.bot_data["restart_after_conflict"] = True
        context.application.stop_running()
        return
    logger.exception("Unhandled error while processing update.", exc_info=context.error)


def build_application() -> Application:
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
    application.add_handler(
        CommandHandler("test_vip", test_vip_command, filters=filters.ChatType.PRIVATE)
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
    return application


def main() -> None:
    while True:
        application = build_application()
        application.bot_data["restart_after_conflict"] = False

        application.run_polling(
            allowed_updates=[
                "message",
                "callback_query",
                "pre_checkout_query",
                "chat_join_request",
            ],
            close_loop=False,
        )

        if not application.bot_data.get("restart_after_conflict"):
            break

        logger.warning(
            "Retrying Telegram polling in %s seconds after conflict.",
            POLLING_CONFLICT_RETRY_SECONDS,
        )
        time.sleep(POLLING_CONFLICT_RETRY_SECONDS)


if __name__ == "__main__":
    main()
