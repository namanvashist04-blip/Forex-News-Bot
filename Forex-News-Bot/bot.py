import asyncio
from datetime import datetime, timezone, timedelta, time
import logging
import os
import re
import aiohttp
import aiosqlite
import discord
from discord import app_commands
from discord.ext import commands, tasks
from dotenv import load_dotenv
import feedparser

# Load environment variables
load_dotenv()

# --- CONFIGURATION ---
TOKEN = os.getenv("DISCORD_BOT_TOKEN")
CHANNEL_ID = int(os.getenv("CHANNEL_ID", "0"))
RUN_MODE = os.getenv("RUN_MODE", "continuous").strip().lower()
CURRENCIES_RAW = os.getenv("TARGET_CURRENCIES", "USD,EUR,GBP,JPY,AUD,CAD,CHF,NZD")
TARGET_CURRENCIES = [c.strip().upper() for c in CURRENCIES_RAW.split(",") if c.strip()]
CHECK_INTERVAL_SECONDS = int(os.getenv("CHECK_INTERVAL_SECONDS", "60"))

CALENDAR_URL = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"
RSS_URLS = [
    "https://www.forexlive.com/feed/news",
    "https://www.fxstreet.com/rss/news"
]
DB_FILE = "trading_bot.db"
HTTP_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
}

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("ForexNewsBot")

# Bot Setup with Commands & Slash tree
intents = discord.Intents.default()
bot = commands.Bot(command_prefix="!", intents=intents)

# --- DATABASE LAYER ---
async def init_db():
    async with aiosqlite.connect(DB_FILE) as db:
        # Multi-stage alerts table (supports 24h, 1h, 15m)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS sent_alerts (
                event_id TEXT,
                alert_type TEXT,
                sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (event_id, alert_type)
            )
        """)
        # Seen breaking news RSS table
        await db.execute("""
            CREATE TABLE IF NOT EXISTS seen_news (
                news_id TEXT PRIMARY KEY,
                seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        # Backward compatibility check for old sent_events table
        await db.execute("""
            CREATE TABLE IF NOT EXISTS sent_events (
                event_id TEXT PRIMARY KEY
            )
        """)
        await db.commit()

async def is_alert_sent(event_id: str, alert_type: str) -> bool:
    async with aiosqlite.connect(DB_FILE) as db:
        # Check new multi-stage table
        async with db.execute(
            "SELECT 1 FROM sent_alerts WHERE event_id = ? AND alert_type = ?", 
            (event_id, alert_type)
        ) as cursor:
            if await cursor.fetchone() is not None:
                return True
        # Check old table if 24h
        if alert_type == "24h":
            async with db.execute(
                "SELECT 1 FROM sent_events WHERE event_id = ?", 
                (event_id,)
            ) as cursor:
                if await cursor.fetchone() is not None:
                    return True
    return False

async def mark_alert_sent(event_id: str, alert_type: str):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute(
            "INSERT OR IGNORE INTO sent_alerts (event_id, alert_type) VALUES (?, ?)", 
            (event_id, alert_type)
        )
        if alert_type == "24h":
            await db.execute("INSERT OR IGNORE INTO sent_events (event_id) VALUES (?)", (event_id,))
        await db.commit()

async def is_news_seen(news_id: str) -> bool:
    async with aiosqlite.connect(DB_FILE) as db:
        async with db.execute("SELECT 1 FROM seen_news WHERE news_id = ?", (news_id,)) as cursor:
            return await cursor.fetchone() is not None

async def mark_news_seen(news_id: str):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("INSERT OR IGNORE INTO seen_news (news_id) VALUES (?)", (news_id,))
        await db.commit()

async def get_db_stats():
    async with aiosqlite.connect(DB_FILE) as db:
        async with db.execute("SELECT COUNT(*) FROM sent_alerts") as c1:
            alerts_count = (await c1.fetchone())[0]
        async with db.execute("SELECT COUNT(*) FROM seen_news") as c2:
            news_count = (await c2.fetchone())[0]
    return alerts_count, news_count

# --- UI COMPONENTS ---
class AlertView(discord.ui.View):
    def __init__(self, currency: str):
        super().__init__(timeout=None)
        symbol = f"{currency}USD" if currency != "USD" else "DXY"
        self.add_item(discord.ui.Button(
            label=f"📈 View {currency} Chart", 
            url=f"https://www.tradingview.com/chart/?symbol={symbol}", 
            style=discord.ButtonStyle.link
        ))

# --- MARKET ANALYSIS HELPERS ---
def clean_html_text(raw_html: str) -> str:
    clean = re.sub(r'<[^>]+>', '', raw_html)
    return clean.strip()

def check_session_overlap(event_time_utc: datetime) -> bool:
    """London (08:00-16:00 UTC) and NY (13:00-21:00 UTC) overlap is 13:00 to 16:00 UTC."""
    overlap_start = time(13, 0)
    overlap_end = time(16, 0)
    return overlap_start <= event_time_utc.time() <= overlap_end

def get_smc_volatility(event_title: str) -> str:
    high_impact_keywords = ["CPI", "NFP", "Non-Farm", "FOMC", "Fed Interest Rate", "ECB Rate", "BOE Rate", "GDP", "Inflation", "Retail Sales", "Unemployment"]
    if any(keyword.lower() in event_title.lower() for keyword in high_impact_keywords):
        return "🔥 High (50-100+ Pips | Liquidity Sweeps & High Slippage Expected)"
    return "⚡ Moderate (20-50 Pips | Normal Expansion)"

async def analyze_sentiment(headline: str) -> str:
    lower_head = headline.lower()
    bearish_words = ["cut", "dovish", "stimulus", "contraction", "drop", "falls", "slump", "easing", "misses", "slowdown", "bearish", "plunges"]
    bullish_words = ["hike", "hawkish", "growth", "expansion", "surge", "beats", "rises", "tightening", "strong", "higher", "bullish", "jump"]
    
    bear_score = sum(1 for word in bearish_words if word in lower_head)
    bull_score = sum(1 for word in bullish_words if word in lower_head)
    
    if bear_score > bull_score:
        return "📉 **Bearish Bias Expected** (Institutional Selling Pressure)"
    elif bull_score > bear_score:
        return "📈 **Bullish Bias Expected** (Institutional Buying Pressure)"
    return "⚖️ **Neutral / High Volatility** (Two-way liquidity hunt possible)"

# --- DATA FETCHING ---
async def fetch_calendar_events():
    try:
        timeout = aiohttp.ClientTimeout(total=15)
        async with aiohttp.ClientSession(headers=HTTP_HEADERS, timeout=timeout) as session:
            async with session.get(CALENDAR_URL) as response:
                if response.status == 200:
                    return await response.json()
                logger.warning(f"Failed to fetch calendar, status code: {response.status}")
    except Exception as e:
        logger.error(f"Error fetching calendar: {e}")
    return []

async def fetch_breaking_news_entries():
    all_entries = []
    timeout = aiohttp.ClientTimeout(total=10)
    async with aiohttp.ClientSession(headers=HTTP_HEADERS, timeout=timeout) as session:
        for url in RSS_URLS:
            try:
                async with session.get(url) as response:
                    if response.status == 200:
                        text = await response.text()
                        feed = feedparser.parse(text)
                        if feed.entries:
                            all_entries.extend(feed.entries[:5])
            except Exception as e:
                logger.warning(f"Failed to fetch RSS from {url}: {e}")
    return all_entries

# --- ALERT CHECKER LOGIC ---
async def process_calendar_alerts(channel: discord.TextChannel):
    events = await fetch_calendar_events()
    if not events:
        return

    now = datetime.now(timezone.utc)

    for event in events:
        impact = event.get("impact", "")
        country = event.get("country", "")
        title = event.get("title", "")
        raw_date = event.get("date", "")

        if impact != "High" or country not in TARGET_CURRENCIES:
            continue

        try:
            event_time = datetime.fromisoformat(raw_date)
            if event_time.tzinfo is None:
                event_time = event_time.replace(tzinfo=timezone.utc)
        except Exception:
            continue

        time_diff = event_time - now
        total_seconds = time_diff.total_seconds()
        event_id = f"alert_{country}_{title}_{raw_date}"

        # Define Alert Stages:
        # 1. 24-Hour Warning: 23h 50m to 24h 10m
        # 2. 1-Hour Reminder: 50m to 65m
        # 3. 15-Minute Imminent Alert: 5m to 18m
        stages = [
            ("24h", timedelta(hours=23, minutes=50).total_seconds(), timedelta(hours=24, minutes=10).total_seconds(), 0x3498DB, "🗓️ 24-HOUR ADVANCE WARNING"),
            ("1h", timedelta(minutes=50).total_seconds(), timedelta(minutes=65).total_seconds(), 0xE67E22, "⏰ 1-HOUR EVENT REMINDER"),
            ("15m", timedelta(minutes=5).total_seconds(), timedelta(minutes=18).total_seconds(), 0xE74C3C, "🚨 15-MIN HIGH IMPACT IMMINENT"),
        ]

        for alert_type, min_sec, max_sec, color, stage_title in stages:
            if min_sec <= total_seconds <= max_sec:
                if await is_alert_sent(event_id, alert_type):
                    continue

                session_overlap = "🔥 **London / NY Overlap (Peak Volume & Slippage Risk)**" if check_session_overlap(event_time) else "Standard Session"
                volatility = get_smc_volatility(title)
                unix_ts = int(event_time.timestamp())

                embed = discord.Embed(
                    title=f"{stage_title}: {title}",
                    description=f"**Currency:** `{country}`\n**Scheduled Time:** <t:{unix_ts}:F> (<t:{unix_ts}:R>)\n**Session Condition:** {session_overlap}",
                    color=color
                )
                embed.add_field(name="🎯 Volatility Expectation", value=volatility, inline=False)
                embed.add_field(name="Forecast", value=event.get("forecast") or "N/A", inline=True)
                embed.add_field(name="Previous", value=event.get("previous") or "N/A", inline=True)
                embed.add_field(name="Impact", value="🔴 High Impact", inline=True)
                embed.set_footer(text="Forex News Bot • Powered by ForexFactory Data")

                try:
                    await channel.send(embed=embed, view=AlertView(country))
                    await mark_alert_sent(event_id, alert_type)
                    logger.info(f"Sent {alert_type} alert for {country} - {title}")
                except Exception as e:
                    logger.error(f"Failed to send calendar alert to channel: {e}")

async def process_breaking_news(channel: discord.TextChannel):
    entries = await fetch_breaking_news_entries()
    if not entries:
        return

    shock_keywords = [
        "emergency", "rate cut", "rate hike", "intervention", "unplanned",
        "flash crash", "war", "sanction", "inflation surge", "bank failure",
        "geopolitical", "central bank", "crisis", "default", "liquidity"
    ]

    for entry in entries:
        news_id = getattr(entry, "id", getattr(entry, "link", entry.title))
        if await is_news_seen(news_id):
            continue

        title = entry.title
        lower_title = title.lower()

        if any(kw in lower_title for kw in shock_keywords):
            sentiment = await analyze_sentiment(title)
            summary_raw = getattr(entry, "summary", "")
            clean_summary = clean_html_text(summary_raw)[:250]
            
            embed = discord.Embed(
                title="🚨 FLASH BREAKING MARKET EVENT",
                description=f"### [{title}]({entry.link})\n\n{clean_summary}...",
                color=0x9B59B6
            )
            embed.add_field(name="🤖 Market Sentiment Bias", value=sentiment, inline=False)
            embed.set_footer(text="Breaking Forex News Alert • Real-time Feeds")

            try:
                await channel.send(embed=embed, view=AlertView("USD"))
                await mark_news_seen(news_id)
                logger.info(f"Sent breaking news alert: {title}")
            except Exception as e:
                logger.error(f"Failed to send breaking news alert: {e}")

# --- RECURRING MONITOR TASK (Continuous Mode) ---
@tasks.loop(seconds=CHECK_INTERVAL_SECONDS)
async def monitor_task():
    channel = bot.get_channel(CHANNEL_ID)
    if not channel:
        return
    await process_calendar_alerts(channel)
    await process_breaking_news(channel)

@monitor_task.before_loop
async def before_monitor_task():
    await bot.wait_until_ready()

# --- BOT EVENTS ---
@bot.event
async def on_ready():
    logger.info(f"Bot logged in as {bot.user} (ID: {bot.user.id})")
    await init_db()

    # Sync Slash Commands
    try:
        synced = await bot.tree.sync()
        logger.info(f"Synced {len(synced)} application slash commands.")
    except Exception as e:
        logger.error(f"Failed to sync slash commands: {e}")

    channel = bot.get_channel(CHANNEL_ID)
    if not channel:
        logger.warning(f"Target Channel with ID '{CHANNEL_ID}' not found or bot lacks permission.")

    if RUN_MODE == "cron":
        logger.info("Executing single run for Cron/GitHub Actions...")
        if channel:
            await process_calendar_alerts(channel)
            await process_breaking_news(channel)
        logger.info("Cron check finished. Closing bot instance.")
        await bot.close()
    else:
        if not monitor_task.is_running():
            monitor_task.start()
        logger.info(f"24/7 Continuous background monitoring started (interval: {CHECK_INTERVAL_SECONDS}s).")

# --- INTERACTIVE SLASH COMMANDS ---
@bot.tree.command(name="today", description="View all High-Impact Forex events scheduled for today")
async def slash_today(interaction: discord.Interaction):
    await interaction.response.defer()
    events = await fetch_calendar_events()
    now = datetime.now(timezone.utc)
    today_date_str = now.strftime("%Y-%m-%d")

    matching_events = []
    for ev in events:
        if ev.get("impact") == "High" and ev.get("country") in TARGET_CURRENCIES:
            try:
                ev_time = datetime.fromisoformat(ev.get("date", ""))
                if ev_time.strftime("%Y-%m-%d") == today_date_str:
                    matching_events.append((ev, ev_time))
            except Exception:
                continue

    if not matching_events:
        await interaction.followup.send("✅ **No High-Impact events scheduled for today** for your target currencies.")
        return

    embed = discord.Embed(
        title=f"📅 High-Impact Forex Events Today ({today_date_str})",
        color=0x2ECC71,
        description=f"Showing high-impact events for: `{', '.join(TARGET_CURRENCIES)}`"
    )

    for ev, ev_time in matching_events[:10]:
        unix_ts = int(ev_time.timestamp())
        forecast = ev.get("forecast") or "N/A"
        prev = ev.get("previous") or "N/A"
        overlap = "🔥 London/NY Overlap" if check_session_overlap(ev_time) else "Standard Session"
        
        embed.add_field(
            name=f"🔴 [{ev.get('country')}] {ev.get('title')}",
            value=f"**Time:** <t:{unix_ts}:t> (<t:{unix_ts}:R>)\n**Forecast:** `{forecast}` | **Previous:** `{prev}`\n**Session:** {overlap}",
            inline=False
        )

    embed.set_footer(text="Forex News Bot • Use /upcoming for tomorrow's events")
    await interaction.followup.send(embed=embed)

@bot.tree.command(name="upcoming", description="View upcoming High-Impact Forex events in the next 24-48 hours")
async def slash_upcoming(interaction: discord.Interaction):
    await interaction.response.defer()
    events = await fetch_calendar_events()
    now = datetime.now(timezone.utc)
    future_limit = now + timedelta(hours=48)

    matching_events = []
    for ev in events:
        if ev.get("impact") == "High" and ev.get("country") in TARGET_CURRENCIES:
            try:
                ev_time = datetime.fromisoformat(ev.get("date", ""))
                if now <= ev_time <= future_limit:
                    matching_events.append((ev, ev_time))
            except Exception:
                continue

    if not matching_events:
        await interaction.followup.send("✅ **No High-Impact events in the next 48 hours** for your target currencies.")
        return

    embed = discord.Embed(
        title="⏳ Upcoming High-Impact Forex Events (Next 48 Hours)",
        color=0x3498DB,
        description=f"Target Currencies: `{', '.join(TARGET_CURRENCIES)}`"
    )

    for ev, ev_time in matching_events[:10]:
        unix_ts = int(ev_time.timestamp())
        forecast = ev.get("forecast") or "N/A"
        prev = ev.get("previous") or "N/A"
        volatility = get_smc_volatility(ev.get('title', ''))

        embed.add_field(
            name=f"🔴 [{ev.get('country')}] {ev.get('title')}",
            value=f"**Time:** <t:{unix_ts}:F> (<t:{unix_ts}:R>)\n**Forecast:** `{forecast}` | **Previous:** `{prev}`\n**Expected Move:** {volatility.split('(')[0]}",
            inline=False
        )

    embed.set_footer(text="Forex News Bot • Pro Tip: Watch for Liquidity Grabs at key release times")
    await interaction.followup.send(embed=embed)

@bot.tree.command(name="news", description="Fetch latest breaking Forex news headlines and sentiment")
async def slash_news(interaction: discord.Interaction):
    await interaction.response.defer()
    entries = await fetch_breaking_news_entries()

    if not entries:
        await interaction.followup.send("⚠️ No news items found currently.")
        return

    embed = discord.Embed(
        title="📰 Latest Forex & Macro Market Headlines",
        color=0xF1C40F,
        description="Recent headlines from ForexLive & FXStreet with AI Sentiment:"
    )

    for entry in entries[:6]:
        sentiment = await analyze_sentiment(entry.title)
        embed.add_field(
            name=f"📌 {entry.title}",
            value=f"{sentiment}\n[Read Full Story]({entry.link})",
            inline=False
        )

    embed.set_footer(text="Forex News Bot • Breaking Market Intelligence")
    await interaction.followup.send(embed=embed)

@bot.tree.command(name="status", description="Check Forex News Bot status, DB stats, and monitored currencies")
async def slash_status(interaction: discord.Interaction):
    alerts_count, news_count = await get_db_stats()
    latency_ms = round(bot.latency * 1000, 1)

    embed = discord.Embed(
        title="🤖 Forex News Bot Status & Diagnostics",
        color=0x2ECC71
    )
    embed.add_field(name="Bot Latency", value=f"`{latency_ms} ms`", inline=True)
    embed.add_field(name="Running Mode", value=f"`{RUN_MODE.upper()}`", inline=True)
    embed.add_field(name="Check Interval", value=f"`{CHECK_INTERVAL_SECONDS}s`", inline=True)
    embed.add_field(name="Monitored Currencies", value=f"`{', '.join(TARGET_CURRENCIES)}`", inline=False)
    embed.add_field(name="Database Records", value=f"• Sent Alerts: `{alerts_count}`\n• Seen News: `{news_count}`", inline=False)
    embed.set_footer(text="Forex News Bot System Health")
    await interaction.response.send_message(embed=embed)

# --- STARTUP ENTRY POINT ---
if __name__ == "__main__":
    if not TOKEN:
        logger.error("Error: DISCORD_BOT_TOKEN is missing! Please set it in your .env file or environment variables.")
        exit(1)
    
    bot.run(TOKEN)