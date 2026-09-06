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
import pytz

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
TIMEZONE_STR = os.getenv("TIMEZONE", "Asia/Kolkata")

CALENDAR_URL = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"
RSS_URLS = [
    "https://www.forexlive.com/feed/news",
    "https://www.fxstreet.com/rss/news"
]
DB_FILE = "trading_bot.db"
HTTP_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
}

# In-Memory Cache for Calendar API
cached_calendar_events = []
last_calendar_fetch = datetime.min.replace(tzinfo=timezone.utc)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("ForexNewsBot")

# Bot Setup
intents = discord.Intents.default()
bot = commands.Bot(command_prefix="!", intents=intents)

# --- 1. HEALTHCHECK & SELF-PINGER ---
async def start_health_check_server():
    """Binds to PORT to keep server alive 24/7 on Free Cloud tiers."""
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

# --- 2. DATABASE LAYER ---
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

async def get_db_stats():
    try:
        async with aiosqlite.connect(DB_FILE) as db:
            async with db.execute("SELECT COUNT(*) FROM sent_alerts") as c1:
                alerts_count = (await c1.fetchone())[0]
        return alerts_count
    except Exception:
        return 0

# --- 3. UI COMPONENTS & HELPERS ---
class AlertView(discord.ui.View):
    def __init__(self, currency: str = "USD"):
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

# --- 4. DATA FETCHING ---
async def fetch_calendar_events():
    global cached_calendar_events, last_calendar_fetch
    now = datetime.now(timezone.utc)
    
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
                    logger.warning("ForexFactory 429 Rate Limit: Using cached data.")
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

# --- 5. DAILY 00:01 MIDNIGHT BRIEFING (@everyone) ---
async def process_daily_morning_briefing(channel: discord.TextChannel):
    """Sends a daily summary at 00:01 (12:01 AM IST) mentioning @everyone with today's scheduled high-impact events."""
    try:
        tz = pytz.timezone(TIMEZONE_STR)
    except Exception:
        tz = pytz.timezone("Asia/Kolkata")

    now_local = datetime.now(tz)
    today_str = now_local.strftime("%Y-%m-%d")
    digest_id = f"daily_digest_{today_str}"

    if now_local.hour == 0 and now_local.minute >= 1:
        if await is_alert_sent(digest_id, "daily_briefing"):
            return

        events = await fetch_calendar_events()
        matching_events = []
        for ev in events:
            if ev.get("impact") == "High" and ev.get("country") in TARGET_CURRENCIES:
                try:
                    ev_time = datetime.fromisoformat(ev.get("date", ""))
                    ev_time_local = ev_time.astimezone(tz) if ev_time.tzinfo else ev_time.replace(tzinfo=timezone.utc).astimezone(tz)
                    if ev_time_local.strftime("%Y-%m-%d") == today_str:
                        matching_events.append((ev, ev_time))
                except Exception:
                    continue

        embed = discord.Embed(
            title=f"📅 High-Impact Forex Events Today ({now_local.strftime('%A, %d %B %Y')})",
            color=0x2ECC71,
            description=f"Showing high-impact events for: `{', '.join(TARGET_CURRENCIES)}`"
        )

        if not matching_events:
            embed.add_field(
                name="✅ Clear Market Day",
                value="Aaj target currencies ke liye koi High-Impact economic event scheduled nahi hai.",
                inline=False
            )
        else:
            for ev, ev_time in matching_events[:12]:
                unix_ts = int(ev_time.timestamp())
                forecast = ev.get("forecast") or "N/A"
                prev = ev.get("previous") or "N/A"
                overlap = "🔥 London/NY Overlap" if check_session_overlap(ev_time) else "Standard Session"

                embed.add_field(
                    name=f"🔴 [{ev.get('country')}] {ev.get('title')}",
                    value=f"**Time:** <t:{unix_ts}:t> (<t:{unix_ts}:R>)\n**Forecast:** `{forecast}` | **Previous:** `{prev}`\n**Session:** {overlap}",
                    inline=False
                )

        embed.set_footer(text="Forex News Bot • Daily 00:01 Briefing")

        try:
            await channel.send(content="@everyone", embed=embed)
            await mark_alert_sent(digest_id, "daily_briefing")
            logger.info(f"Daily 00:01 Midnight Briefing sent successfully for {today_str}")
        except Exception as e:
            logger.error(f"Failed to send daily briefing: {e}")

# --- 6. BACKGROUND WORKER LOOP ---
@tasks.loop(seconds=CHECK_INTERVAL_SECONDS)
async def monitor_task():
    channel_id = await get_alert_channel_id()
    if not channel_id:
        return
    channel = bot.get_channel(channel_id)
    if not channel:
        return
    
    # Only runs the clean daily 00:01 briefing
    await process_daily_morning_briefing(channel)

@monitor_task.before_loop
async def before_monitor_task():
    await bot.wait_until_ready()

# --- 7. BOT EVENTS ---
@bot.event
async def on_ready():
    logger.info(f"Bot logged in as {bot.user} (ID: {bot.user.id})")
    await init_db()

    try:
        synced = await bot.tree.sync()
        logger.info(f"Synced {len(synced)} application slash commands.")
    except Exception as e:
        logger.error(f"Slash command sync error: {e}")

    channel_id = await get_alert_channel_id()
    channel = bot.get_channel(channel_id) if channel_id else None
    if not channel:
        logger.warning(f"Target Alert Channel with ID '{channel_id}' not found. Use /setloc in Discord to set channel.")

    try:
        await start_health_check_server()
        if not self_ping_task.is_running():
            self_ping_task.start()
    except Exception as e:
        logger.warning(f"Health check setup note: {e}")

    if RUN_MODE == "cron":
        logger.info("Executing single run for Cron...")
        if channel:
            await process_daily_morning_briefing(channel)
        await bot.close()
    else:
        if not monitor_task.is_running():
            monitor_task.start()
        logger.info(f"24/7 Monitoring active for Daily 00:01 Briefing.")

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

# --- 8. SLASH COMMANDS ---
@bot.tree.command(name="setloc", description="[Admin Only] Set the channel where bot sends daily 00:01 briefing")
@app_commands.describe(channel="Select the channel for daily briefing (leave blank for current channel)")
@app_commands.default_permissions(administrator=True)
async def slash_setloc(interaction: discord.Interaction, channel: discord.TextChannel = None):
    if not interaction.user.guild_permissions.administrator:
        await interaction.response.send_message("❌ **Permission Denied:** Only Server Administrators can use /setloc.", ephemeral=True)
        return

    target_channel = channel or interaction.channel
    await set_alert_channel_id(target_channel.id)

    embed = discord.Embed(
        title="📍 Daily Briefing Channel Set!",
        description=f"Ab rozana **00:01 (12:01 AM Midnight)** par **`@everyone`** mention ke sath poore din ka High-Impact News Calendar {target_channel.mention} me aayega.\n\n*Members kisi bhi channel me `/today`, `/upcoming`, `/news` commands use kar sakte hain.*",
        color=0x2ECC71
    )
    embed.set_footer(text="Forex News Bot • Configuration Updated")
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
    alerts_count = await get_db_stats()
    latency_ms = round(bot.latency * 1000, 1)
    channel_id = await get_alert_channel_id()
    channel_display = f"<#{channel_id}>" if channel_id else "`Not Set`"

    embed = discord.Embed(
        title="🤖 Forex News Bot Status & Diagnostics",
        color=0x2ECC71
    )
    embed.add_field(name="Bot Latency", value=f"`{latency_ms} ms`", inline=True)
    embed.add_field(name="Running Mode", value=f"`{RUN_MODE.upper()}`", inline=True)
    embed.add_field(name="Daily Briefing Channel", value=channel_display, inline=True)
    embed.add_field(name="Monitored Currencies", value=f"`{', '.join(TARGET_CURRENCIES)}`", inline=False)
    embed.add_field(name="Daily Briefings Sent", value=f"`{alerts_count}`", inline=False)
    embed.set_footer(text="Forex News Bot System Health • Use /setloc to update channel")
    await interaction.response.send_message(embed=embed)

# --- 9. RESILIENT ENTRY POINT ---
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
                logger.warning(f"Discord 429 Rate Limit. Reconnecting in {backoff}s...")
                time_module.sleep(backoff)
                backoff = min(backoff * 2, 300)
            else:
                logger.error(f"Discord HTTP Exception: {e}. Retrying in 15s...")
                time_module.sleep(15)
        except Exception as e:
            logger.error(f"Unexpected drop: {e}. Reconnecting in 15s...")
            time_module.sleep(15)
