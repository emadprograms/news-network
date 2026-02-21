import os
import discord
from discord.ext import commands
import aiohttp
import asyncio
from datetime import datetime, timedelta
from dotenv import load_dotenv

# Load local environment variables if present
load_dotenv()

# Configuration from Environment Variables
DISCORD_TOKEN = os.getenv("DISCORD_BOT_TOKEN")
GITHUB_TOKEN = os.getenv("GITHUB_PAT")
GITHUB_REPO = os.getenv("GITHUB_REPO", "emadprograms/news-network")
WORKFLOW_FILENAME = os.getenv("WORKFLOW_FILENAME", "manual_run.yml")

# Friendly model aliases → actual KeyManager config IDs (free tier only)
MODEL_ALIASES = {
    "flash":    "gemini-2.5-flash-free",
    "lite":     "gemini-2.5-flash-lite-free",
    "3flash":   "gemini-3-flash-free",
}
AVAILABLE_MODELS = list(MODEL_ALIASES.keys())

# Setup intents for message reading
intents = discord.Intents.default()
intents.message_content = True

# Initialize Bot
bot = commands.Bot(command_prefix="!", intents=intents)

@bot.event
async def on_ready():
    print(f'Logged in as {bot.user.name} ({bot.user.id})')
    print('Bot is ready to receive commands.')

@bot.command(name="cleannews")
async def trigger_fetch(ctx, target_date: str = None, model: str = "lite"):
    """Triggers the Clean News LLM Extraction workflow. 
    Usage: !cleannews [YYYY-MM-DD] [model]
    Models: flash (2.5 Flash), lite (2.5 Flash Lite), 3 (3 Flash)"""
    
    # Resolve friendly model alias
    resolved_model = MODEL_ALIASES.get(model.lower())
    if not resolved_model:
        await ctx.send(
            f"❌ **Unknown model:** `{model}`\n"
            f"> Available models: {', '.join([f'`{k}`' for k in AVAILABLE_MODELS])}\n"
            f"> Example: `!cleannews 2026-02-18 flash`"
        )
        return
    
    # 🛡️ Validate date format BEFORE dispatching
    if target_date:
        try:
            parsed = datetime.strptime(target_date, "%Y-%m-%d")
            
            # Allow targeting upcoming trading days (up to 5 days ahead) for weekends/holidays
            today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            max_future = today + timedelta(days=5)
            
            if parsed > max_future:
                await ctx.send(
                    f"❌ **Invalid date:** `{target_date}` is too far in the future.\n"
                    f"> You can target dates up to 5 days ahead to prepare for the next trading session."
                )
                return
                
            target_date = parsed.strftime("%Y-%m-%d")  # Normalize to clean format
        except ValueError:
            await ctx.send(
                f"❌ **Invalid date format:** `{target_date}`\n"
                f"> Expected format: **YYYY-MM-DD** (e.g. `2026-02-18`)\n"
                f"> Please try again with a valid date."
            )
            return
    else:
        # Default to today if none provided in bot visual output (even though main.py handles it)
        target_date = "today"
    
    # Visual feedback focused on News-Network identity
    status_msg = await ctx.send(
        f"🧠 **Connecting to News Network LLM Engine...**\n"
        f"> **Date:** `{target_date}`\n"
        f"> **Model:** `{model}` → `{resolved_model}`\n"
        f"Dispatching signal to GitHub Actions..."
    )
    
    # Prepare GitHub API request
    url = f"https://api.github.com/repos/{GITHUB_REPO}/actions/workflows/{WORKFLOW_FILENAME}/dispatches"
    
    headers = {
        "Accept": "application/vnd.github.v3+json",
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "X-GitHub-Api-Version": "2022-11-28"
    }
    
    # We trigger the workflow on the 'main' branch
    data = {"ref": "main", "inputs": {}}
    
    # Add optional inputs if provided
    if target_date != "today":
        data["inputs"]["target_date"] = target_date
    data["inputs"]["model"] = resolved_model
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=data) as response:
                # GitHub returns 204 No Content on a successful dispatch
                if response.status == 204:
                    await status_msg.edit(content="💠 **Transmission Successful!**\n> **News Network** is initializing... Fetching live link... 📡")
                    print(f"Triggered fetch via Discord user: {ctx.author}")
                    
                    # Try up to 3 times with 4s wait each (total 12s)
                    live_url = None
                    for attempt in range(1, 4):
                        await asyncio.sleep(4)
                        print(f"Attempt {attempt} to fetch live link...")
                        
                        runs_url = f"https://api.github.com/repos/{GITHUB_REPO}/actions/workflows/{WORKFLOW_FILENAME}/runs"
                        async with session.get(runs_url, headers=headers) as runs_resp:
                            if runs_resp.status == 200:
                                runs_data = await runs_resp.json()
                                if runs_data.get("workflow_runs"):
                                    live_url = runs_data["workflow_runs"][0]["html_url"]
                                    break
                            else:
                                print(f"Failed to fetch runs on attempt {attempt}: {runs_resp.status}")
                    
                    final_msg_content = (
                        f"💠 **Transmission Successful!**\n"
                        f"> **News Network LLM Engine** is now distilling data.\n"
                    )
                    if live_url:
                        final_msg_content += f"> 🔗 **[Watch Live Extraction on GitHub]({live_url})**\n\n"
                    else:
                        final_msg_content += f"> (Live link could not be retrieved - check GitHub Actions manually)\n\n"
                        
                    final_msg_content += "> The optimized JSON payload and extraction report will be delivered here shortly. 🧠"
                    
                    await status_msg.edit(content=final_msg_content)
                else:
                    response_json = await response.json() if response.content_type == 'application/json' else {}
                    error_details = response_json.get("message", await response.text())
                    await status_msg.edit(content=f"❌ **Failed to trigger workflow.**\nGitHub API Error ({response.status}): `{error_details}`")
                    print(f"Failed to trigger: {response.status} - {await response.text()}")
            
    except Exception as e:
        await status_msg.edit(content=f"⚠️ **Internal Error:** Could not reach GitHub.\n`{str(e)}`")
        print(f"Exception triggering workflow: {e}")

if __name__ == "__main__":
    if not DISCORD_TOKEN:
        print("CRITICAL: DISCORD_BOT_TOKEN is missing.")
        exit(1)
    if not GITHUB_TOKEN:
        print("CRITICAL: GITHUB_PAT is missing.")
        exit(1)
        
    print("Starting News Network bot...")
    bot.run(DISCORD_TOKEN)
