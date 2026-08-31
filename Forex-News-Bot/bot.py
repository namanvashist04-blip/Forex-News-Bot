import asyncio
from datetime import datetime, timezone, timedelta, time
import logging
import os
import re
import time as time_module
import aiohttp
from aiohttp import web
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
DEFAULT_CHANNEL_ID = int(os.getenv("CHANNEL_ID", "0"))
RUN_MODE = os.getenv("RUN_MODE", "continuous").strip().lower()
CURRENCIES_RAW = os.getenv("TARGET_CURRENCIES", "USD,EUR,GBP,JPY,AUD,CAD,CHF,NZD")
TARGET_CURRENCIES = [c.strip().upper() for c in CURRENCIES_RAW.split(",") if c.strip()]
CHECK_INTERVAL_SECONDS = int(os.getenv("CHECK_INTERVAL_SECONDS", "60"))
PORT = int(os.getenv("PORT", "10000"))
RENDER_EXTERNAL_URL = os.getenv("RENDER_EXTERNAL_URL", "")

CALENDAR_URL = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"
RSS_URLS = [
    "https://www.forexlive.com/feed/news",
    "https://www.fxstreet.com/rss/news"
]
DB_FILE = "trading_bot.db"
HTTP_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
}

# In-Memory Cache to ensure 100% resilience against API rate limits
cached_calendar_events = []
last_calendar_fetch = datetime.min.replace(tzinfo=timezone.utc)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("ForexNewsBot")

# Bot Setup with Commands & Slash tree
intents = discord.Intents.default()
bot = commands.Bot(command_prefix="!", intents=intents)

# --- 1. SELF-HEALING HTTP HEALTHCHECK & SELF-PINGER ---
async def start_health_check_server():
    """Binds to PORT and self-pings to keep server alive 24/7 on Free Cloud tiers."""
    app = web.Application()
    async def handle_health(request):
        return web.Response(text="Forex News Bot is Healthy & Live 24/7!", content_type="text/plain")
    app.router.add_get("/", handle_health)
    app.router.add_get("/health", handle_health)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    logger.info(f"Health check HTTP server active on port {PORT}")

@tasks.loop(minutes=4)
async def self_ping_task():
    """Self-ping loop every 4 minutes to guarantee the instance never sleeps."""
    target_url = RENDER_EXTERNAL_URL or f"http://127.0.0.1:{PORT}/health"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(target_url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                if resp.status == 200:
                    logger.debug("Self-ping successful: Instance kept awake.")
    except Exception:
        pass

# --- 2. RESILIENT DATABASE LAYER ---
async def init_db():
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS bot_config (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS sent_alerts (
                event_id TEXT,
                alert_type TEXT,
                sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (event_id, alert_type)
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS seen_news (
                news_id TEXT PRIMARY KEY,
                seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await db.commit()

async def get_alert_channel_id() -> int:
    try:
        async with aiosqlite.connect(DB_FILE) as db:
            async with db.execute("SELECT value FROM bot_config WHERE key = 'alert_channel_id'") as cursor:
                row = await cursor.fetchone()
                if row and row[0]:
                    return int(row[0])
    except Exception:
        pass
    return DEFAULT_CHANNEL_ID

async def set_alert_channel_id(channel_id: int):
    async with aiosqlite.connect(DB_FILE) as db:
        await db.execute(
            "INSERT INTO bot_config (key, value) VALUES ('alert_channel_id', ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (str(channel_id),)
        )
        await db.commit()

async def is_alert_sent(event_id: str, alert_type: str) -> bool:
    try:
        async with aiosqlite.connect(DB_FILE) as db:
            async with db.execute(
                "SELECT 1 FROM sent_alerts WHERE event_id = ? AND alert_type = ?", 
                (event_id, alert_type)
            ) as cursor:
                return (await cursor.fetchone()) is not None
    except Exception:
        return False

async def mark_alert_sent(event_id: str, alert_type: str):
    try:
        async with aiosqlite.connect(DB_FILE) as db:
            await db.execute(
                "INSERT OR IGNORE INTO sent_alerts (event_id, alert_type) VALUES (?, ?)", 
                (event_id, alert_type)
            )
            await db.commit()
    except Exception:
        pass

async def is_news_seen(news_id: str) -> bool:
    try:
        async with aiosqlite.connect(DB_FILE) as db:
            async with db.execute("SELECT 1 FROM seen_news WHERE news_id = ?", (news_id,)) as cursor:
                return (await cursor.fetchone()) is not None
    except Exception:
        return False

async def mark_news_seen(news_id: str):
    try:
        async with aiosqlite.connect(DB_FILE) as db:
            await db.execute("INSERT OR IGNORE INTO seen_news (news_id) VALUES (?, ?)", (news_id,))
            await db.commit()
    except Exception:
        pass

async def get_db_stats():
    try:
        async with aiosqlite.connect(DB_FILE) as db:
            async with db.execute("SELECT COUNT(*) FROM sent_alerts") as c1:
                alerts_count = (await c1.fetchone())[0]
            async with db.execute("SELECT COUNT(*) FROM seen_news") as c2:
                news_count = (await c2.fetchone())[0]
        return alerts_count, news_count
    except Exception:
        return 0, 0

# --- 3. UI COMPONENTS & HELPERS ---
class AlertView(discord.ui.View):
    def __init__(self, currency: str):
        super().__init__(timeout=None)
        symbol = f"{currency}USD" if currency != "USD" else "DXY"
        self.add_item(discord.ui.Button(
            label=f"📈 View {currency} Chart", 
            url=f"https://www.tradingview.com/chart/?symbol={symbol}", 
            style=discord.ButtonStyle.link
        ))

def clean_html_text(raw_html: str) -> str:
    clean = re.sub(r'<[^>]+>', '', raw_html)
    return clean.strip()

def check_session_overlap(event_time_utc: datetime) -> bool:
    overlap_start = time(13, 0)
    overlap_end = time(16, 0)
    return overlap_start <= event_time_utc.time() <= overlap_end

def get_smc_volatility(event_title: str) -> str:
    high_impact_keywords = ["CPI", "NFP", "Non-Farm", "FOMC", "Fed Interest Rate", "ECB Rate", "BOE Rate", "GDP", "Inflation", "Retail Sales", "Unemployment"]
    if any(keyword.lower() in event_title.lower() for keyword in high_impact_keywords):
        return "🔥 High (50-100+ Pips | Liquidity Sweeps Expected)"
    return "⚡ Moderate (20-50 Pips | Normal Expansion)"

async def analyze_sentiment(headline: str) -> str:
    lower_head = headline.lower()
    bearish_words = ["cut", "dovish", "stimulus", "contraction", "drop", "falls", "slump", "easing", "misses", "slowdown", "bearish", "plunges", "crash", "loss"]
    bullish_words = ["hike", "hawkish", "growth", "expansion", "surge", "beats", "rises", "tightening", "strong", "higher", "bullish", "jump", "profit"]
    
    bear_score = sum(1 for word in bearish_words if word in lower_head)
    bull_score = sum(1 for word in bullish_words if word in lower_head)
    
    if bear_score > bull_score:
        return "📉 **Bearish Bias Expected**"
    elif bull_score > bear_score:
        return "📈 **Bullish Bias Expected**"
    return "⚖️ **Neutral / High Volatility**"

# --- 4. FAILOVER DATA FETCHING ---
async def fetch_calendar_events():
    global cached_calendar_events, last_calendar_fetch
    now = datetime.now(timezone.utc)
    
    # 5-minute memory cache to prevent 429 rate limits
    if cached_calendar_events and (now - last_calendar_fetch).total_seconds() < 300:
        return cached_calendar_events

    try:
        timeout = aiohttp.ClientTimeout(total=15)
        async with aiohttp.ClientSession(headers=HTTP_HEADERS, timeout=timeout) as session:
            async with session.get(CALENDAR_URL) as response:
                if response.status == 200:
                    data = await response.json()
                    cached_calendar_events = data
                    last_calendar_fetch = now
                    return data
                elif response.status == 429:
                    logger.warning("ForexFactory 429 Rate Limit: Falling back to cached data.")
                    return cached_calendar_events
    except Exception as e:
        logger.error(f"Error fetching calendar: {e}")
    return cached_calendar_events

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

# --- 5. AUTOMATED ALERTS MONITOR ---
async def process_calendar_alerts(channel: discord.TextChannel):
    events = await fetch_calendar_events()
    if not events or not channel:
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

        # Zero-Duplicate Time Guard: Ignore events that have already passed
        if event_time < (now - timedelta(minutes=5)):
            continue

        time_diff = event_time - now
        total_seconds = time_diff.total_seconds()
        event_id = f"alert_{country}_{title}_{raw_date}"

        stages = [
            ("24h", timedelta(hours=23, minutes=45).total_seconds(), timedelta(hours=24, minutes=15).total_seconds(), 0x3498DB, "🗓️ 24-HOUR ADVANCE WARNING"),
            ("1h", timedelta(minutes=45).total_seconds(), timedelta(minutes=70).total_seconds(), 0xE67E22, "⏰ 1-HOUR EVENT REMINDER"),
            ("15m", timedelta(minutes=5).total_seconds(), timedelta(minutes=20).total_seconds(), 0xE74C3C, "🚨 15-MIN HIGH IMPACT IMMINENT"),
        ]

        for alert_type, min_sec, max_sec, color, stage_title in stages:
            if min_sec <= total_seconds <= max_sec:
                if await is_alert_sent(event_id, alert_type):
                    continue

                session_overlap = "🔥 **London / NY Overlap (High Volatility)**" if check_session_overlap(event_time) else "Standard Session"
                volatility = get_smc_volatility(title)
                unix_ts = int(event_time.timestamp())

                embed = discord.Embed(
                    title=f"{stage_title}: {title}",
                    description=f"**Currency:** `{country}`\n**Time:** <t:{unix_ts}:F> (<t:{unix_ts}:R>)\n**Session:** {session_overlap}",
                    color=color
                )
                embed.add_field(name="Expected Move", value=volatility, inline=False)
                embed.add_field(name="Forecast", value=event.get("forecast") or "N/A", inline=True)
                embed.add_field(name="Previous", value=event.get("previous") or "N/A", inline=True)
                embed.add_field(name="Impact", value="🔴 High Impact", inline=True)
                embed.set_footer(text="Forex News Bot • Powered by ForexFactory")

                try:
                    await channel.send(embed=embed, view=AlertView(country))
                    await mark_alert_sent(event_id, alert_type)
                    logger.info(f"Sent {alert_type} alert for {country} - {title}")
                except Exception as e:
                    logger.error(f"Failed to send calendar alert: {e}")

async def process_breaking_news(channel: discord.TextChannel):
    entries = await fetch_breaking_news_entries()
    if not entries or not channel:
        return

    shock_keywords = [
        "emergency", "rate cut", "rate hike", "intervention", "unplanned",
        "flash crash", "crash", "war", "sanction", "inflation surge", "bank failure",
        "geopolitical", "central bank", "crisis", "default", "liquidity", "plunge", "collapse"
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
                title="🚨 FLASH MARKET / CRASH ALERT",
                description=f"### [{title}]({entry.link})\n\n{clean_summary}...",
                color=0xE74C3C
            )
            embed.add_field(name="Market Sentiment", value=sentiment, inline=False)
            embed.set_footer(text="Breaking Forex Alert • Auto-detected")

            try:
                await channel.send(embed=embed, view=AlertView("USD"))
                await mark_news_seen(news_id)
                logger.info(f"Sent breaking news alert: {title}")
            except Exception as e:
                logger.error(f"Failed to send breaking news alert: {e}")

# --- 6. BACKGROUND WORKER TASK ---
@tasks.loop(seconds=CHECK_INTERVAL_SECONDS)
async def monitor_task():
    channel_id = await get_alert_channel_id()
    if not channel_id:
        return
    channel = bot.get_channel(channel_id)
    if not channel:
        return
    await process_calendar_alerts(channel)
    await process_breaking_news(channel)

@monitor_task.before_loop
async def before_monitor_task():
    await bot.wait_until_ready()

# --- 7. BOT EVENTS & AUTO RECONNECT ---
@bot.event
async def on_ready():
    logger.info(f"Bot logged in as {bot.user} (ID: {bot.user.id})")
    await init_db()

    # Sync Slash Commands
    try:
        synced = await bot.tree.sync()
        logger.info(f"Synced {len(synced)} application slash commands.")
    except Exception as e:
        logger.error(f"Slash command sync error: {e}")

    channel_id = await get_alert_channel_id()
    channel = bot.get_channel(channel_id) if channel_id else None
    if not channel:
        logger.warning(f"Target Alert Channel with ID '{channel_id}' not found. Use /setloc in Discord to set channel.")

    # Start healthcheck server and self-pinger
    try:
        await start_health_check_server()
        if not self_ping_task.is_running():
            self_ping_task.start()
    except Exception as e:
        logger.warning(f"Health check setup note: {e}")

    if RUN_MODE == "cron":
        logger.info("Executing single run for Cron...")
        if channel:
            await process_calendar_alerts(channel)
            await process_breaking_news(channel)
        await bot.close()
    else:
        if not monitor_task.is_running():
            monitor_task.start()
        logger.info(f"24/7 Continuous background monitoring active ({CHECK_INTERVAL_SECONDS}s loop).")

# Global Error Handler for Slash Commands so bot never silently crashes
@bot.tree.error
async def on_app_command_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    logger.error(f"Command Error: {error}")
    try:
        if not interaction.response.is_done():
            await interaction.response.send_message("⚠️ An error occurred while processing this command. Please try again.", ephemeral=True)
        else:
            await interaction.followup.send("⚠️ An error occurred while processing this command. Please try again.", ephemeral=True)
    except Exception:
        pass

# --- 8. COMMANDS ---
@bot.tree.command(name="setloc", description="[Admin Only] Set the channel where bot sends automated alerts & crash news")
@app_commands.describe(channel="Select the channel for alerts (leave blank for current channel)")
@app_commands.default_permissions(administrator=True)
async def slash_setloc(interaction: discord.Interaction, channel: discord.TextChannel = None):
    if not interaction.user.guild_permissions.administrator:
        await interaction.response.send_message("❌ **Permission Denied:** Only Server Administrators can use /setloc.", ephemeral=True)
        return

    target_channel = channel or interaction.channel
    await set_alert_channel_id(target_channel.id)

    embed = discord.Embed(
        title="📍 Alert Location Set Successfully!",
        description=f"All **Automated News Warnings & Crash/Shock Alerts** will now be sent to {target_channel.mention}!\n\n*Members can use commands (`/today`, `/news`, etc.) anywhere in the server.*",
        color=0x2ECC71
    )
    embed.set_footer(text="Forex News Bot • Alert Routing Updated")
    await interaction.response.send_message(embed=embed)

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

    embed.set_footer(text="Forex News Bot • Watch for Liquidity Grabs at release times")
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
        description="Recent headlines with sentiment:"
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

@bot.tree.command(name="status", description="Check Forex News Bot status, configured alert channel, and diagnostics")
async def slash_status(interaction: discord.Interaction):
    alerts_count, news_count = await get_db_stats()
    latency_ms = round(bot.latency * 1000, 1)
    channel_id = await get_alert_channel_id()
    channel_display = f"<#{channel_id}>" if channel_id else "`Not Set`"

    embed = discord.Embed(
        title="🤖 Forex News Bot Status & Diagnostics",
        color=0x2ECC71
    )
    embed.add_field(name="Bot Latency", value=f"`{latency_ms} ms`", inline=True)
    embed.add_field(name="Running Mode", value=f"`{RUN_MODE.upper()}`", inline=True)
    embed.add_field(name="Alert Channel (Auto-Posts)", value=channel_display, inline=True)
    embed.add_field(name="Monitored Currencies", value=f"`{', '.join(TARGET_CURRENCIES)}`", inline=False)
    embed.add_field(name="Database Records", value=f"• Sent Alerts: `{alerts_count}`\n• Seen News: `{news_count}`", inline=False)
    embed.set_footer(text="Forex News Bot System Health • Use /setloc to update alert channel")
    await interaction.response.send_message(embed=embed)

# --- 9. RESILIENT ENTRY POINT WITH EXPONENTIAL BACKOFF ---
if __name__ == "__main__":
    if not TOKEN:
        logger.error("Error: DISCORD_BOT_TOKEN is missing!")
        exit(1)

    backoff = 30
    while True:
        try:
            logger.info("Connecting to Discord Gateway...")
            bot.run(TOKEN)
            break
        except discord.errors.HTTPException as e:
            if e.status == 429:
                logger.warning(f"Discord 429 Rate Limit encountered. Auto-cooling down for {backoff}s before reconnecting...")
                time_module.sleep(backoff)
                backoff = min(backoff * 2, 300) # Max 5 min backoff
            else:
                logger.error(f"Discord HTTP Exception: {e}. Retrying in 15s...")
                time_module.sleep(15)
        except Exception as e:
            logger.error(f"Unexpected connection drop: {e}. Reconnecting in 15s...")
            time_module.sleep(15)
