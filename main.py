import argparse
import sys
import os
import time
import datetime
import re
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
import aiohttp
import asyncio

# Setup paths (ensure imports work)
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from modules.market_utils import MarketCalendar
from modules.db_client import NewsDatabase
from modules.key_manager import KeyManager
from modules.llm_client import GeminiClient
from modules.text_optimizer import optimize_json_for_synthesis
from infisical_sdk import InfisicalSDKClient

# --- UTILITY CONTEXT ---
def normalize_text(text):
    if not text: return ""
    return re.sub(r'[^a-z0-9\s]', '', str(text).lower()).strip()

def find_missing_items(chunk, salvaged_items):
    if not salvaged_items: return chunk
    
    extracted_sets = []
    for item in salvaged_items:
        for h in item.get('source_headlines', []):
            norm = normalize_text(h)
            if norm:
                extracted_sets.append(set(norm.split()))
    
    missing_items = []
    for item in chunk:
        title = item.get('title', 'Unknown')
        norm_title = normalize_text(title)
        if not norm_title:
            missing_items.append(item)
            continue
            
        title_tokens = set(norm_title.split())
        is_found = False
        
        for e_tokens in extracted_sets:
            if not title_tokens: continue
            intersection = title_tokens.intersection(e_tokens)
            overlap = len(intersection) / len(title_tokens)
            if overlap >= 0.85:
                is_found = True
                break
                
        if not is_found:
            missing_items.append(item)
            
    return missing_items

def clean_content(content_list):
    if not content_list: return []
    full_text = " ".join(content_list)
    clean_text = re.sub(r'<[^>]+>', '', full_text)
    
    paragraphs = []
    if len(content_list) <= 1:
        parts = clean_text.split(". ")
        current_p = ""
        for p in parts:
            current_p += p + ". "
            if len(current_p) > 200: 
                paragraphs.append(current_p.strip())
                current_p = ""
        if current_p: paragraphs.append(current_p.strip())
    else:
        paragraphs = [re.sub(r'<[^>]+>', '', c).strip() for c in content_list if c.strip()]
    return paragraphs

def chunk_data(items, max_tokens=10000):
    chunks = []
    current_chunk = []
    current_tokens = 0
    flat_items = []
    limit_chars = int(max_tokens * 2.5)
    
    for item in items:
        body = " ".join(clean_content(item.get('content', [])))
        if len(body) > limit_chars:
            parts = [body[i:i+limit_chars] for i in range(0, len(body), limit_chars)]
            for p_idx, p_text in enumerate(parts):
                new_item = item.copy()
                orig_title = item.get('title', 'No Title')
                new_item['title'] = f"[Part {p_idx+1}/{len(parts)}] {orig_title}"
                new_item['content'] = [p_text]
                flat_items.append(new_item)
        else:
            flat_items.append(item)

    for item in flat_items:
        body = " ".join(clean_content(item.get('content', [])))
        meta = f"{item.get('time')} {item.get('title')} {item.get('publisher')}"
        total_chars = len(body) + len(meta) + 50 
        est_tok = int(total_chars / 2.5)
        
        if (current_tokens + est_tok) > max_tokens and current_chunk:
            chunks.append(current_chunk)
            current_chunk = []
            current_tokens = 0
            
        current_chunk.append(item)
        current_tokens += est_tok
        
    if current_chunk:
        chunks.append(current_chunk)
        
    return chunks

def build_chunk_prompt(chunk, index, total, market_data_text, headline_inventory):
    prompt = f"""SYSTEM NOTICE: This is PART {index} of {total}.

*** ROLE ***
You are a high-fidelity Data Extraction Engine. Your sole purpose is to convert unstructured news text into a structured, machine-readable JSON dataset.

*** DATA INTEGRITY INVENTORY ***
The following headlines are present in this chunk. Every single headline must be accounted for in your output:
{headline_inventory}

*** STRICT CONSTRAINTS ***
1. SOURCE TRACKING: For every item you extract, you MUST populate the 'source_headlines' field with the original headlines from the inventory above that were used to build that item.
2. NO DATA LOSS: Every headline in the inventory must contribute to at least one 'source_headlines' entry in your JSON list.
3. STRICT 1:1 EXTRACTION: Do NOT group headlines. Every single headline from the inventory must have its own unique entry in the 'news_items' array.
4. DEDUPLICATION: If stories are similar, still create separate entries for each unique headline.
5. NO SYNTHESIS: Focus on facts. Extract numbers, tickers, and entities exactly.

*** OUTPUT FORMAT ***
Output a valid JSON object with the following schema:
{{
  "news_items": [
    {{
      "category": "String (EARNINGS, MERGERS_ACQUISITIONS, MACRO_ECONOMY, MARKET_MOVEMENTS, GEOPOLITICS, EXECUTIVE_MOVES, OTHER)",
      "primary_entity": "String (Company/Ticker/Country)",
      "secondary_entities": ["Array of Strings"],
      "event_summary": "String (Fact-based summary)",
      "hard_data": {{ "key": "value" }},
      "quotes": ["Array of Strings"],
      "sentiment_indicated": ["Array of Strings"],
      "is_truncated": Boolean,
      "source_headlines": ["Array of MANDATORY EXACT HEADLINES from the inventory above"]
    }}
  ]
}}

*** INSTRUCTIONS ***
1. Start with '{{' and end with '}}'. No conversational text.
2. Use the Headline Inventory as your checklist.
3. If no data exists for a field, leave it null.

=== MARKET DATA STARTS BELOW ===
{market_data_text}
"""
    return prompt

def repair_json_content(json_str):
    json_str = json_str.strip()
    json_str = re.sub(r'\}\s*\{', '}, {', json_str)
    json_str = re.sub(r'\]\s*\[', '], [', json_str)
    json_str = re.sub(r',\s*\}', '}', json_str)
    json_str = re.sub(r',\s*\]', ']', json_str)
    json_str = re.sub(r'\"\s*\n\s*\"', '", "', json_str)
    json_str = re.sub(r'(\d+|true|false|null)\s*\n\s*\"', r'\1, "', json_str)
    json_str = re.sub(r'\"([a-zA-Z0-9_]+)\"\s+\"([^\"]+)\"', r'"\1": "\2"', json_str)
    return json_str

def salvage_json_items(text: str) -> list:
    if not text: return []
    items = []
    
    start_pattern = r'\{\s*"category":'
    for match in re.finditer(start_pattern, text):
        start_idx = match.start()
        
        brace_count = 0
        end_idx = -1
        for i in range(start_idx, len(text)):
            if text[i] == '{':
                brace_count += 1
            elif text[i] == '}':
                brace_count -= 1
                if brace_count == 0:
                    end_idx = i + 1
                    break
        
        if end_idx != -1:
            try:
                obj_str = text[start_idx:end_idx].strip()
                if obj_str.endswith(','): obj_str = obj_str[:-1].strip()
                
                obj = json.loads(obj_str, strict=False)
                if isinstance(obj, dict) and "category" in obj:
                    items.append(obj)
            except: 
                continue
                
    return items

def extract_chunk_worker_cli(worker_data):
    i_display, chunk, total_chunks, selected_model, depth, ai_client, km = worker_data
    worker_logs = []
    last_raw_content = ""
    api_call_count = 0
    best_parsed_items = []
    
    headline_inventory = ""
    context_for_prompt = ""
    for idx, item in enumerate(chunk, 1):
        t = item.get('time', 'N/A')
        title = item.get('title', 'No Title')
        body = " ".join(clean_content(item.get('content', [])))
        headline_inventory += f"- {title}\n"
        context_for_prompt += f"--- SOURCE {idx} ---\nTITLE: {title}\nTIME: {t}\nCONTENT: {body}\n\n"
    
    display_name = f"Part {i_display}" if depth == 0 else f"Branch {i_display}"
    p = build_chunk_prompt(chunk, i_display, total_chunks, context_for_prompt, headline_inventory)
    
    attempt, max_attempts = 0, 5
    while attempt < max_attempts:
        attempt += 1
        
        if attempt >= 2 and len(chunk) > 1:
            salvaged_so_far = best_parsed_items if best_parsed_items else (salvage_json_items(last_raw_content) if last_raw_content else [])
            missing_items = find_missing_items(chunk, salvaged_so_far)
            
            if not missing_items:
                worker_logs.append(f"✅ [{display_name}] Residue Check: All {len(chunk)} stories accounted for across {len(salvaged_so_far)} items!")
                return (True, salvaged_so_far, worker_logs, api_call_count)
            
            if len(missing_items) < len(chunk):
                worker_logs.append(f"⚡ [{display_name}] Residue detected: Keeping {len(salvaged_so_far)} items. Recovering {len(missing_items)} missing...")
                res_residual = extract_chunk_worker_cli((f"{i_display}.RES", missing_items, total_chunks, selected_model, depth + 1, ai_client, km))
                success_r, items_r, logs_r, calls_r = res_residual
                
                api_call_count += calls_r
                worker_logs.extend(logs_r)
                
                combined_items = salvaged_so_far + items_r
                if success_r:
                    worker_logs.append(f"🌿 [{display_name}] Residual Sync Complete. {len(combined_items)} items total.")
                    return (True, combined_items, worker_logs, api_call_count)
                else:
                    return (False, combined_items, worker_logs, api_call_count)
            
            worker_logs.append(f"⚠️ [{display_name}] Zero-match detected. Branching into 2 sub-parts...")
            mid = len(chunk) // 2
            left_chunk = chunk[:mid]
            right_chunk = chunk[mid:]
            
            res_l = extract_chunk_worker_cli((f"{i_display}.A", left_chunk, total_chunks, selected_model, depth + 1, ai_client, km))
            res_r = extract_chunk_worker_cli((f"{i_display}.B", right_chunk, total_chunks, selected_model, depth + 1, ai_client, km))
            
            success_l, items_l, logs_l, calls_l = res_l
            success_r, items_r, logs_r, calls_r = res_r
            
            api_call_count += (calls_l + calls_r)
            worker_logs.extend(logs_l)
            worker_logs.extend(logs_r)
            
            combined_items = items_l + items_r
            total_success = success_l and success_r
            
            if total_success:
                worker_logs.append(f"🌿 [{display_name}] Branch Sync Complete. {len(combined_items)} items recovered.")
                return (True, combined_items, worker_logs, api_call_count)
            else:
                worker_logs.append(f"❌ [{display_name}] Branch failure persisted.")
                return (False, combined_items, worker_logs, api_call_count)

        try:
            token_est = km.estimate_tokens(p) if km else "N/A"
            worker_logs.append(f"🔹 [{display_name}] Trial {attempt} - Extraction in progress... (~{token_est} tokens)")
            api_call_count += 1
            res = ai_client.generate_content(p, config_id=selected_model)
            
            if res['success']:
                content = res['content']
                last_raw_content = content 
                raw_json = content.strip()
                
                if "```json" in raw_json:
                    match = re.search(r"```json\s*(.*?)\s*```", raw_json, re.DOTALL)
                    if match: raw_json = match.group(1).strip()
                elif "```" in raw_json:
                     match = re.search(r"```\s*(.*?)\s*```", raw_json, re.DOTALL)
                     if match: raw_json = match.group(1).strip()
                
                if not raw_json.startswith("{"):
                    match = re.search(r"(\{.*\})", raw_json, re.DOTALL)
                    if match: raw_json = match.group(1).strip()
                
                raw_json = repair_json_content(raw_json)
                if raw_json.strip().endswith("}") is False:
                     if not raw_json.strip().endswith(('"', ',', '}', ']')): raw_json += '"'
                     if raw_json.count('{') > raw_json.count('}'): raw_json += '}]}'

                try:
                    data = json.loads(raw_json, strict=False)
                    items = data if isinstance(data, list) else data.get("news_items", [])
                    
                    if len(items) > len(best_parsed_items):
                        best_parsed_items = items
                        
                    missing_now = find_missing_items(chunk, items)
                    recovered_count = len(chunk) - len(missing_now)
                    min_req = len(chunk) * 0.95
                    
                    if recovered_count < min_req and attempt < max_attempts:
                        worker_logs.append(f"⚠️ [{display_name}] Low Yield Check: Only {recovered_count}/{len(chunk)} headlines recovered. Retrying...")
                        time.sleep(1)
                        continue 
                        
                    worker_logs.append(f"✅ [{display_name}] Success! {len(items)} items ({recovered_count}/{len(chunk)} headlines). (Key: {res.get('key_name', 'Unknown')})")
                    return (True, items, worker_logs, api_call_count)
                    
                except Exception as json_err:
                    err_str = str(json_err).lower()
                    if any(k in err_str for k in ["delimiter", "double quotes", "expecting value", "unterminated"]):
                        salvaged = salvage_json_items(content)
                        if salvaged:
                            missing_salvage = find_missing_items(chunk, salvaged)
                            recovered_salvage = len(chunk) - len(missing_salvage)
                            if recovered_salvage < min_req and attempt < max_attempts:
                                worker_logs.append(f"⚠️ [{display_name}] Eager Salvage Rejected: Low yield ({recovered_salvage}/{len(chunk)} headlines). Retrying...")
                                time.sleep(1)
                                continue 
                                
                            worker_logs.append(f"⚡ [{display_name}] Eager Salvage: Recovered {recovered_salvage}/{len(chunk)} headlines.")
                            return (True, salvaged, worker_logs, api_call_count)
                    raise json_err
            else:
                err_msg = res['content']
                wait_sec = res.get('wait_seconds', 0)
                if wait_sec > 0:
                    worker_logs.append(f"⏳ [{display_name}] Quota hit (Key: {res.get('key_name', 'Unknown')}). Rotating keys...")
                    time.sleep(1) 
                    continue
                else:
                    worker_logs.append(f"❌ [{display_name}] Trial {attempt} failed: {err_msg} (Key: {res.get('key_name', 'Unknown')})")
                    time.sleep(2)
        except Exception as e:
            worker_logs.append(f"⚠️ [{display_name}] Error: {e}")
            time.sleep(2)
            
    if last_raw_content:
        salvaged = salvage_json_items(last_raw_content)
        if salvaged:
            missing_final = find_missing_items(chunk, salvaged)
            recovered_final = len(chunk) - len(missing_final)
            if recovered_final >= (len(chunk) * 0.95):
                worker_logs.append(f"🩹 [{display_name}] Emergency Salvage: {recovered_final}/{len(chunk)} headlines.")
                return (True, salvaged, worker_logs, api_call_count)
            else:
                worker_logs.append(f"❌ [{display_name}] Emergency Salvage FAILED: Yield too low ({len(salvaged)}/{len(chunk)}).")
            
    return (False, [], worker_logs, api_call_count)


# --- DISCORD INTEGRATION ---
async def send_discord_report(webhook_url, summary_text, optimized_text, file_name, embeds):
    try:
        data = {"embeds": embeds}
        
        # We need to send both the JSON data and a file (the optimized text)
        form = aiohttp.FormData()
        # Ensure we send valid payload JSON as string
        form.add_field('payload_json', json.dumps(data), content_type='application/json')
        # Add the optimized text as a file upload
        form.add_field('file', optimized_text.encode('utf-8'), filename=file_name, content_type='text/plain')

        async with aiohttp.ClientSession() as session:
            async with session.post(webhook_url, data=form) as response:
                if response.status not in (200, 204):
                    print(f"Failed to send to Discord. Status: {response.status}, text: {await response.text()}")
                else:
                    print("✅ Successfully pushed report to Discord!")
    except Exception as e:
        print(f"Error sending to discord: {e}")


def run_extraction(target_date_str, api_preference, target_model, webhook_url):
    print("Initializing Database & Keys via Infisical...")
    
    # 1. Fetch Secrets
    try:
        # For simplicity in GitHub Actions, use INFISICAL_TOKEN if provided for auth, 
        # or fall back to client_id/secret if set in environment.
        infisical = InfisicalSDKClient(host="https://app.infisical.com")
        
        if "INFISICAL_TOKEN" in os.environ:
             infisical.auth.universal_auth.login(
                 client_id=os.environ.get("INFISICAL_CLIENT_ID"),
                 client_secret=os.environ.get("INFISICAL_CLIENT_SECRET")
             )
        else:
             from dotenv import load_dotenv
             load_dotenv()
             # Locally we might have these or STREAMLIT SECRETS 
             # (Actually, let's use the explicit secrets we expect from GitHub Actions or local env)
             inf_client_id = os.environ.get("INFISICAL_CLIENT_ID")
             inf_client_secret = os.environ.get("INFISICAL_CLIENT_SECRET")
             inf_project_id = os.environ.get("INFISICAL_PROJECT_ID")
             
             if not (inf_client_id and inf_client_secret and inf_project_id):
                 print("CRITICAL ERROR: Missing Infisical Credentials.")
                 return
                 
             infisical.auth.universal_auth.login(
                 client_id=inf_client_id,
                 client_secret=inf_client_secret
             )

        news_url = infisical.secrets.get_secret_by_name(secret_name="turso_emadarshadalam_newsdatabase_db_url", project_id=inf_project_id, environment_slug="dev", secret_path="/").secretValue
        news_token = infisical.secrets.get_secret_by_name(secret_name="turso_emadarshadalam_newsdatabase_auth_token", project_id=inf_project_id, environment_slug="dev", secret_path="/").secretValue

        km_url = infisical.secrets.get_secret_by_name(secret_name="turso_emadprograms_analystworkbench_db_url", project_id=inf_project_id, environment_slug="dev", secret_path="/").secretValue
        km_token = infisical.secrets.get_secret_by_name(secret_name="turso_emadprograms_analystworkbench_auth_token", project_id=inf_project_id, environment_slug="dev", secret_path="/").secretValue

        try:
            inf_webhook = infisical.secrets.get_secret_by_name(secret_name="discord_captain_clean_news_webhook_url", project_id=inf_project_id, environment_slug="dev", secret_path="/").secretValue
            if inf_webhook and not webhook_url:
                webhook_url = inf_webhook
        except Exception:
            pass

    except Exception as e:
        print(f"CRITICAL ERROR: Failed to get keys from Infisical - {e}")
        return

    # 2. Init Clients
    db = NewsDatabase(news_url.replace("libsql://", "https://"), news_token)
    km = KeyManager(km_url, km_token)
    ai_client = GeminiClient(km)

    # 3. Determine Date
    if target_date_str:
        session_date = datetime.datetime.strptime(target_date_str, "%Y-%m-%d").date()
    else:
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        session_date = MarketCalendar.get_trading_session_date(now_utc)
        
    session_start, session_end = MarketCalendar.get_session_window(session_date)
    now_naive = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
    if session_start <= now_naive < session_end:
        session_end = now_naive
        
    print(f"Target Date: {session_date}")
    print(f"Lookback Window: {session_start} -> {session_end}")

    items = db.fetch_news_range(session_start.isoformat(), session_end.isoformat())
    print(f"Fetched {len(items)} items.")
    
    if not items:
        print("No items found. Aborting extraction.")
        asyncio.run(send_discord_report(webhook_url, "No items found.", "No items to analyze.", f"{session_date}_network.log", [{
            "title": "⚠️ News Network: No Data",
            "description": f"No data found for the logical session: `{session_date}`.",
            "color": 16753920
        }]))
        return

    preview_text = f"TOTAL NEWS QUANTITY: {len(items)}\n=== START RAW DATA DUMP ===\n\n"
    for idx, item in enumerate(items):
        t = item.get('time', 'N/A')
        title = item.get('title', 'No Title')
        body = " ".join(clean_content(item.get('content', [])))
        preview_text += f"ITEM {idx+1}:\n[{t}] {title}\n{body}\n\n"

    raw_input_tokens = km.estimate_tokens(preview_text)
    chunks = chunk_data(items, max_tokens=10000)
    print(f"Chunked into {len(chunks)} parts.")
    
    all_extracted_items = []
    total_api_calls = 0
    max_threads = min(len(chunks), 15)
    
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        future_to_chunk = {
            executor.submit(extract_chunk_worker_cli, (i+1, chunk, len(chunks), target_model, 0, ai_client, km)): i 
            for i, chunk in enumerate(chunks)
        }
        
        for future in as_completed(future_to_chunk):
            success, chunk_items, logs, calls = future.result()
            total_api_calls += calls
            if success:
                all_extracted_items.extend(chunk_items)
            for l in logs:
                print(l)

    ext_duration = time.time() - start_time
    print(f"Extraction took {ext_duration:.1f}s, Yielded {len(all_extracted_items)} optimized features.")
    
    optimized_text = optimize_json_for_synthesis(all_extracted_items)
    opt_tokens = km.estimate_tokens(optimized_text)
    savings_pct = ((raw_input_tokens - opt_tokens) / raw_input_tokens * 100) if raw_input_tokens > 0 else 0
    
    # 4. Fidelity Check
    original_headlines = [item.get('title', 'Unknown') for item in items]
    norm_original = {normalize_text(h): h for h in original_headlines}
    
    extracted_sources = []
    for item in all_extracted_items:
        sources = item.get('source_headlines', [])
        if isinstance(sources, list):
            extracted_sources.extend(sources)
    
    norm_extracted = {normalize_text(s) for s in extracted_sources}
    
    preserved_titles = []
    lost_titles = []
    
    for h_norm, original_title in norm_original.items():
        is_found = False
        for s_norm in norm_extracted:
            if h_norm == s_norm or h_norm in s_norm or s_norm in h_norm:
                is_found = True
                break
        
        if is_found:
            preserved_titles.append(original_title)
        else:
            lost_titles.append(original_title)
    
    fidelity_score = (len(preserved_titles) / len(original_headlines) * 100) if original_headlines else 0

    # 5. Discord Delivery
    embeds = [{
        "title": f"📰 News Network Report Generated",
        "color": 3447003, # Blueish
        "description": f"Extraction completed for logical trading day **{session_date}**.",
        "fields": [
            {
                "name": "📊 Extraction Metrics",
                "value": (
                    f"**Date:** `{session_date}`\n"
                    f"**Total Articles:** {len(original_headlines)}\n"
                    f"**Fidelity:** `{fidelity_score:.1f}%` ({len(preserved_titles)}/{len(original_headlines)})\n"
                    f"**Extracted Features:** {len(all_extracted_items)}"
                ),
                "inline": True
            },
            {
                "name": "💻 System Info",
                "value": (
                    f"**Model Used:** `{target_model}`\n"
                    f"**API Provider:** `{api_preference}`\n"
                    f"**API Calls Made:** `{total_api_calls}`\n"
                    f"**Duration:** `{ext_duration:.1f}s`"
                ),
                "inline": True
            },
            {
                "name": "💾 Optimization Stats",
                "value": (
                    f"**Input Size:** ~{raw_input_tokens:,} tokens\n"
                    f"**Output Size:** ~{opt_tokens:,} tokens\n"
                    f"**Token Savings:** `- {savings_pct:.1f}%`"
                ),
                "inline": False
            }
        ],
        "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat()
    }]
    
    if len(lost_titles) > 0:
         embeds.append({
             "title": f"⚠️ {len(lost_titles)} Missing Items",
             "description": "The following items were potentially dropped or grouped:",
             "color": 16711680,
             "fields": [{"name": "Items", "value": "\n".join([f"- {i}" for i in sorted(list(set(lost_titles)))])[:1000]}]
         })

    asyncio.run(send_discord_report(webhook_url, "Report ready", optimized_text, f"{session_date}_news.log", embeds))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="News Network Extraction Script")
    parser.add_argument("--date", type=str, help="Target session date (YYYY-MM-DD)", default=None)
    parser.add_argument("--api", type=str, help="Target API preference (gemini, deepseek, etc)", default="gemini")
    parser.add_argument("--model", type=str, help="Target Model Config name", default="gemini-2.5-flash-lite-free")
    parser.add_argument("--webhook", type=str, help="Discord Webhook URL", default=None)
    
    args = parser.parse_args()
    
    webhook_url = args.webhook or os.environ.get("DISCORD_WEBHOOK_URL")
    if not webhook_url:
        print("WARNING: No Discord Webhook URL provided. The log won't be sent.")
        
    print(f"Starting News Extraction: Date={args.date}, API={args.api}, Model={args.model}")
    run_extraction(args.date, args.api, args.model, webhook_url)

