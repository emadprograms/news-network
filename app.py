import streamlit as st
import time
import datetime
import re
from modules.market_utils import MarketCalendar
from modules.db_client import NewsDatabase
from modules.key_manager import KeyManager
from modules.llm_client import GeminiClient
from modules.text_optimizer import optimize_json_for_synthesis
from infisical_sdk import InfisicalSDKClient
import json

# --- CONFIG ---
st.set_page_config(page_title="News Network", page_icon="📰", layout="wide")

# --- INIT SESSION STATE ---
if 'news_data' not in st.session_state:
    st.session_state['news_data'] = []
if 'ai_report' not in st.session_state:
    st.session_state['ai_report'] = ""
if 'data_loaded' not in st.session_state:
    st.session_state['data_loaded'] = False
if 'dry_run_prompts' not in st.session_state: 
    st.session_state['dry_run_prompts'] = []

# --- CUSTOM CSS ---
st.markdown("""
<style>
    .stButton>button {
        width: 100%;
        border-radius: 5px;
        height: 3em; 
    }
</style>
""", unsafe_allow_html=True)


# --- DB / KEY INIT ---
@st.cache_resource
def get_db_connection():
    try:
        infisical_secrets = st.secrets["infisical"]
        infisical = InfisicalSDKClient(host="https://app.infisical.com")
        
        infisical.auth.universal_auth.login(
            client_id=infisical_secrets["client_id"],
            client_secret=infisical_secrets["client_secret"]
        )
        
        # 1. News Database (Headed for data)
        news_url = infisical.secrets.get_secret_by_name(
            secret_name="turso_emadarshadalam_newsdatabase_db_url",
            project_id=infisical_secrets["project_id"],
            environment_slug="dev",
            secret_path="/"
        ).secretValue
        
        news_token = infisical.secrets.get_secret_by_name(
            secret_name="turso_emadarshadalam_newsdatabase_auth_token",
            project_id=infisical_secrets["project_id"],
            environment_slug="dev",
            secret_path="/"
        ).secretValue

        # --- 2. Key Manager Database (Headed for keys) ---
        km_url = infisical.secrets.get_secret_by_name(
            secret_name="turso_emadprograms_analystworkbench_db_url",
            project_id=infisical_secrets["project_id"],
            environment_slug="dev",
            secret_path="/"
        ).secretValue
        
        km_token = infisical.secrets.get_secret_by_name(
            secret_name="turso_emadprograms_analystworkbench_auth_token",
            project_id=infisical_secrets["project_id"],
            environment_slug="dev",
            secret_path="/"
        ).secretValue
        
        db = NewsDatabase(
            news_url.replace("libsql://", "https://"),
            news_token
        )
        return db, km_url, km_token
        
    except Exception as e:
        st.error(f"Failed to initialize DB/Keys: {e}")
        return None, None, None

db, db_url, db_token = get_db_connection()

if db_url and db_token:
    km = KeyManager(db_url, db_token)
    ai_client = GeminiClient(km)
else:
    km = None
    ai_client = None


def normalize_text(text):
    """Global utility for consistent headline matching (keeps spaces)."""
    if not text: return ""
    # Lowercase, remove special chars except spaces
    return re.sub(r'[^a-z0-9\s]', '', str(text).lower()).strip()

def find_missing_items(chunk, salvaged_items):
    """
    Identifies which original news items are missing from the salvaged list.
    Uses Token-Overlap Comparison (Strict Word Match).
    """
    if not salvaged_items: return chunk
    
    # Pre-calculate token sets for extracted headlines
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
            # Use Token Intersection Ratio (Jaccard-ish)
            # Story is 'found' if at least 85% of its title words are in an extracted headline
            if not title_tokens: continue
            intersection = title_tokens.intersection(e_tokens)
            overlap = len(intersection) / len(title_tokens)
            if overlap >= 0.85:
                is_found = True
                break
                
        if not is_found:
            missing_items.append(item)
            
    return missing_items

# --- HELPER FUNCTIONS ---
def clean_content(content_list):
    """Cleans text list into pure paragraphs."""
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

def chunk_data(items, max_tokens=10000): # Aggressive Stability: 10k
    """
    Splits items into chunks. 
    Intelligent Slicing: If an individual item exceeds the limit, it is sliced 
    into multiple parts to ensure zero data loss.
    """
    chunks = []
    current_chunk = []
    current_tokens = 0
    
    # Pre-process items to handle mega-stories (slicing instead of truncation)
    flat_items = []
    limit_chars = int(max_tokens * 2.5)
    
    for item in items:
        body = " ".join(clean_content(item.get('content', [])))
        if len(body) > limit_chars:
            # Slice it
            parts = [body[i:i+limit_chars] for i in range(0, len(body), limit_chars)]
            for p_idx, p_text in enumerate(parts):
                new_item = item.copy()
                orig_title = item.get('title', 'No Title')
                new_item['title'] = f"[Part {p_idx+1}/{len(parts)}] {orig_title}"
                new_item['content'] = [p_text]
                flat_items.append(new_item)
        else:
            flat_items.append(item)

    # Now aggregate into chunks
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
    """
    STRICT DATA ETL PROMPT - V2
    Includes Headline Inventory for Data Integrity Tracking.
    """
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
    """Robust JSON repair for common LLM syntax errors."""
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
    """EMERGENCY FALLBACK: Hunts for individual JSON objects by finding balanced braces."""
    if not text: return []
    items = []
    
    # Find all occurrences of the start pattern
    start_pattern = r'\{\s*"category":'
    for match in re.finditer(start_pattern, text):
        start_idx = match.start()
        
        # Scan forward for balanced closing brace
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
                # Repair common trailing artifacts before parsing
                if obj_str.endswith(','): obj_str = obj_str[:-1].strip()
                
                obj = json.loads(obj_str, strict=False)
                if isinstance(obj, dict) and "category" in obj:
                    items.append(obj)
            except: 
                continue
                
    # --- FRAGMENT SALVAGING (for cut-off responses) ---
    # Find the last '{"category":' that didn't have a matching '}'
    last_item_start = text.rfind('{"category":')
    if last_item_start != -1:
        # Check if this start index was already processed
        if not any(text[last_item_start:].startswith(text[match.start():match.end()]) for match in re.finditer(start_pattern, text) if (match.start() + 10) < len(text)):
             # This is a bit complex, let's just try to patch the last fragment if items is empty or last item is missing
             pass # Basic salvaging above is usually enough, but we can add patch logic if needed.
             
    return items

def extract_chunk_worker(worker_data):
    """Task worker for parallel extraction with Recursive Adaptive Branching."""
    i_display, chunk, total_chunks, selected_model, depth = worker_data
    worker_logs = []
    last_raw_content = ""
    api_call_count = 0
    best_parsed_items = []
    
    # --- PHASE 1: PREPARE PROMPT ---
    headline_inventory = ""
    context_for_prompt = ""
    for idx, item in enumerate(chunk, 1):
        t = item.get('time', 'N/A')
        title = item.get('title', 'No Title')
        body = " ".join(clean_content(item.get('content', [])))
        headline_inventory += f"- {title}\n"
        context_for_prompt += f"--- SOURCE {idx} ---\nTITLE: {title}\nTIME: {t}\nCONTENT: {body}\n\n"
    
    # Adjust display index for sub-parts if we are deep in recursion
    display_name = f"Part {i_display}" if depth == 0 else f"Branch {i_display}"
    
    p = build_chunk_prompt(chunk, i_display, total_chunks, context_for_prompt, headline_inventory)
    
    # --- PHASE 2: TRIAL LOOP ---
    attempt, max_attempts = 0, 5
    while attempt < max_attempts:
        attempt += 1
        
        # --- ADAPTIVE BRANCHING TRIGGER ---
        # Branch if Trial 2+ starts and we still have multiple items
        if attempt >= 2 and len(chunk) > 1:
            # --- TARGETED RESIDUAL EXTRACTION ---
            # Priority: 1. Success-but-low-yield items, 2. Regex-salvaged items
            salvaged_so_far = best_parsed_items if best_parsed_items else (salvage_json_items(last_raw_content) if last_raw_content else [])
            missing_items = find_missing_items(chunk, salvaged_so_far)
            
            if not missing_items:
                worker_logs.append(f"✅ [{display_name}] Residue Check: All {len(chunk)} stories accounted for across {len(salvaged_so_far)} items!")
                return (True, salvaged_so_far, worker_logs, api_call_count)
            
            if len(missing_items) < len(chunk):
                # We have partial success! Only retry the missing ones.
                worker_logs.append(f"⚡ [{display_name}] Residue detected: Keeping {len(salvaged_so_far)} items. Recovering {len(missing_items)} missing...")
                res_residual = extract_chunk_worker((f"{i_display}.RES", missing_items, total_chunks, selected_model, depth + 1))
                success_r, items_r, logs_r, calls_r = res_residual
                
                api_call_count += calls_r
                worker_logs.extend(logs_r)
                
                # Combine results. Note: salvaged_so_far are used as the base.
                combined_items = salvaged_so_far + items_r
                if success_r:
                    worker_logs.append(f"🌿 [{display_name}] Residual Sync Complete. {len(combined_items)} items total.")
                    return (True, combined_items, worker_logs, api_call_count)
                else:
                    return (False, combined_items, worker_logs, api_call_count)
            
            # If we couldn't find a single matching item, fall back to standard blind branching
            worker_logs.append(f"⚠️ [{display_name}] Zero-match detected. Branching into 2 sub-parts...")
            mid = len(chunk) // 2
            left_chunk = chunk[:mid]
            right_chunk = chunk[mid:]
            
            # Recursive calls for sub-parts
            res_l = extract_chunk_worker((f"{i_display}.A", left_chunk, total_chunks, selected_model, depth + 1))
            res_r = extract_chunk_worker((f"{i_display}.B", right_chunk, total_chunks, selected_model, depth + 1))
            
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
                # Even the branch failed? Keep trying or bubble up failure
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
                
                # Cleanup markdown
                if "```json" in raw_json:
                    match = re.search(r"```json\s*(.*?)\s*```", raw_json, re.DOTALL)
                    if match: raw_json = match.group(1).strip()
                elif "```" in raw_json:
                     match = re.search(r"```\s*(.*?)\s*```", raw_json, re.DOTALL)
                     if match: raw_json = match.group(1).strip()
                
                if not raw_json.startswith("{"):
                    match = re.search(r"(\{.*\})", raw_json, re.DOTALL)
                    if match: raw_json = match.group(1).strip()
                
                # Robust repair
                raw_json = repair_json_content(raw_json)
                if raw_json.strip().endswith("}") is False:
                     if not raw_json.strip().endswith(('"', ',', '}', ']')): raw_json += '"'
                     if raw_json.count('{') > raw_json.count('}'): raw_json += '}]}'

                try:
                    data = json.loads(raw_json, strict=False)
                    items = data if isinstance(data, list) else data.get("news_items", [])
                    
                    # Store as best result if it has more items (or better fidelity markers in future)
                    if len(items) > len(best_parsed_items):
                        best_parsed_items = items
                        
                    # --- YIELD ENFORCEMENT (Fidelity-Based) ---
                    # Count actual headlines recovered rather than item count
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
                            # --- YIELD ENFORCEMENT (SALVAGE Fidelity-Based) ---
                            missing_salvage = find_missing_items(chunk, salvaged)
                            recovered_salvage = len(chunk) - len(missing_salvage)
                            if recovered_salvage < min_req and attempt < max_attempts:
                                worker_logs.append(f"⚠️ [{display_name}] Eager Salvage Rejected: Low yield ({recovered_salvage}/{len(chunk)} headlines). Retrying...")
                                time.sleep(1)
                                continue 
                                
                            worker_logs.append(f"⚡ [{display_name}] Eager Salvage: Recovered {recovered_salvage}/{len(chunk)} headlines.")
                            worker_logs.append(f"DEBUG_RAW_CONTENT|{content}")
                            worker_logs.append(f"DEBUG_SALVAGED_ITEMS|{json.dumps(salvaged, indent=2)}")
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
            
    # --- PHASE 3: FINAL EMERGENCY SALVAGE ---
    if last_raw_content:
        salvaged = salvage_json_items(last_raw_content)
        if salvaged:
            missing_final = find_missing_items(chunk, salvaged)
            recovered_final = len(chunk) - len(missing_final)
            if recovered_final >= (len(chunk) * 0.95):
                worker_logs.append(f"🩹 [{display_name}] Emergency Salvage: {recovered_final}/{len(chunk)} headlines.")
                worker_logs.append(f"DEBUG_RAW_CONTENT|{last_raw_content}")
                worker_logs.append(f"DEBUG_SALVAGED_ITEMS|{json.dumps(salvaged, indent=2)}")
                return (True, salvaged, worker_logs, api_call_count)
            else:
                worker_logs.append(f"❌ [{display_name}] Emergency Salvage FAILED: Yield too low ({len(salvaged)}/{len(chunk)}).")
            
    return (False, [], worker_logs, api_call_count)


# ==============================================================================
#  LAY OUT
# ==============================================================================

# 1. HEADER
st.title("📰 News Network Analysis")

# 2. UNIFIED CONTROL PANEL
now_utc = datetime.datetime.now(datetime.timezone.utc)
logical_session = MarketCalendar.get_trading_session_date(now_utc)

with st.container():
    st.subheader("🛠️ Analyst Control Panel")
    st.info(f"🕒 **Current Logical Trading Session:** {logical_session.strftime('%A, %b %d, %Y')}")
    
    # Trading Session (OUTSIDE form for dynamic updates)
    st.markdown("**1. Trading Session**")
    col_s1, col_s2 = st.columns([1, 2])
    with col_s1:
        session_date = st.date_input("Session Date", value=logical_session, help="Select the trading day to analyze.")
    with col_s2:
        # Auto-compute the lookback window using MarketCalendar
        session_start, session_end = MarketCalendar.get_session_window(session_date)
        session_label = MarketCalendar.get_session_label(session_date)
        
        # Cap at current time if session hasn't ended yet
        now_naive = now_utc.replace(tzinfo=None)
        if session_start <= now_naive < session_end:
            session_end = now_naive
            st.caption(f"📅 **{session_label} Session** — Lookback (UTC): {session_start.strftime('%a %b %d, %I:%M %p')} → {session_end.strftime('%a %b %d, %I:%M %p')} (Ongoing)")
        elif now_naive < session_start:
            st.caption(f"📅 **{session_label} Session** — Lookback (UTC): {session_start.strftime('%a %b %d, %I:%M %p')} → {session_end.strftime('%a %b %d, %I:%M %p')} (Upcoming)")
        else:
            st.caption(f"📅 **{session_label} Session** — Lookback (UTC): {session_start.strftime('%a %b %d, %I:%M %p')} → {session_end.strftime('%a %b %d, %I:%M %p')}")

    
    st.divider()

    with st.form("analyst_controls"):
        # Model Selection
        st.markdown("**2. Select Extraction Model**")
        if km:
             model_options = list(km.MODELS_CONFIG.keys())
             ix = 0
             if 'gemini-2.5-flash-lite-free' in model_options:
                 ix = model_options.index('gemini-2.5-flash-lite-free')
             elif 'gemini-2.0-flash-paid' in model_options:
                 ix = model_options.index('gemini-2.0-flash-paid')
             selected_model = st.selectbox("Select Model", options=model_options, index=ix, label_visibility="collapsed")
        else:
            st.error("Keys unavailable")
            selected_model = None

        st.divider()
        
        submitted = st.form_submit_button("▶️ START EXTRACTION", type="primary", use_container_width=True)


# 3. EXECUTION LOGIC
if submitted:
    # A. FETCH DATA
    st.session_state['data_loaded'] = False
    st.session_state['news_data'] = []
    st.session_state['ai_report'] = ""
    
    if db:
        with st.spinner("1/3 Fetching Market Data..."):
             items = db.fetch_news_range(session_start.isoformat(), session_end.isoformat())

             
             st.session_state['news_data'] = items
             st.session_state['data_loaded'] = True
    else:
        st.error("Database connection unavailable.")
        st.stop()

    if not st.session_state['news_data']:
        st.warning("No news found in the selected range.")
    else:
        # --- INSTANT PREVIEW (REQUESTED) ---
        items = st.session_state['news_data']
        st.success(f"📦 Data Fetch Complete: {len(items)} news items found.")
        with st.expander("📋 Emergency Copiable Raw Data Backup", expanded=False):
            preview_text = f"TOTAL NEWS QUANTITY: {len(items)}\n"
            preview_text += "=== START RAW DATA DUMP ===\n\n"
            
            for idx, item in enumerate(items):
                t = item.get('time', 'N/A')
                title = item.get('title', 'No Title')
                body = " ".join(clean_content(item.get('content', [])))
                preview_text += f"ITEM {idx+1}:\n[{t}] {title}\n{body}\n\n"
            
            preview_text += "=== END RAW DATA DUMP ==="
            
            st.info("💡 Copy the raw data below for safe-keeping. This is the exact text being processed by the AI.")
            st.code(preview_text, language="text")

        # B. CHUNK DATA
        # CONSERVATIVE CHUNK SIZE: 10k tokens.
        # This reduces the cognitive load on the AI and minimizes branching overhead.
        chunks = chunk_data(st.session_state['news_data'], max_tokens=10000)
        
        if len(chunks) > 1:
            st.toast(f"Data too large for one prompt. Split into {len(chunks)} parts.")
        
        # C. EXECUTE EXTRACTION
        all_extracted_items = []
        log_container = st.container()
        progress_bar = st.progress(0.0)
        status_text = st.empty()
        
        # Run AI
        if not ai_client:
            st.error("AI Client unavailable.")
        else:
            import json
            # --- LIVE ETL LOGS ---
            log_expander = st.expander("🛠️ Live ETL Logs", expanded=True)
            log_container = log_expander.container()
            
            # --- START PARALLEL EXECUTION ---
            from concurrent.futures import ThreadPoolExecutor, as_completed
            max_threads = min(len(chunks), 15)
            st.info(f"🚀 Scaling throughput: Initializing {max_threads} worker threads (1 per data chunk)...")
            
            all_extracted_items = []
            completed_count = 0
            total_api_calls = 0
            
            with ThreadPoolExecutor(max_workers=max_threads) as executor:
                future_to_chunk = {
                    executor.submit(extract_chunk_worker, (i+1, chunk, len(chunks), selected_model, 0)): i 
                    for i, chunk in enumerate(chunks)
                }
                
                for future in as_completed(future_to_chunk):
                    completed_count += 1
                    success, items, logs, calls = future.result()
                    total_api_calls += calls
                    
                    # Update UI
                    for log_msg in logs:
                        if "✅" in log_msg: log_container.success(log_msg)
                        elif "❌" in log_msg: log_container.error(log_msg)
                        elif "⏳" in log_msg: log_container.warning(log_msg)
                        elif log_msg.startswith("DEBUG_RAW_CONTENT|"):
                            with log_container.expander("🔍 View Salvaged Raw Response"):
                                st.code(log_msg.split("|", 1)[1])
                        elif log_msg.startswith("DEBUG_SALVAGED_ITEMS|"):
                            with log_container.expander("📝 View Extracted Salvaged Items"):
                                st.code(log_msg.split("|", 1)[1], language="json")
                        else: log_container.write(log_msg)
                        
                    if success:
                        all_extracted_items.extend(items)
                    
                    progress_bar.progress(completed_count / len(chunks))
            
            progress_bar.progress(1.0)
            status_text.success("Extraction Complete!")
            time.sleep(1)
            status_text.empty()
            progress_bar.empty()
            
            if all_extracted_items:
                final_dataset = {"news_items": all_extracted_items, "total_entities": len(all_extracted_items)}
                st.session_state['ai_report'] = json.dumps(final_dataset, indent=2)
                st.session_state['json_data'] = all_extracted_items

                st.balloons()
                st.divider()
                st.subheader("✨ Optimized Token-Light Input")
                
                optimized_text = optimize_json_for_synthesis(all_extracted_items)
                
                # --- DATA INTEGRITY TEST ---
                original_headlines = [item.get('title', 'Unknown') for item in st.session_state['news_data']]
                norm_original = {normalize_text(h): h for h in original_headlines}
                
                extracted_sources = []
                for item in all_extracted_items:
                    sources = item.get('source_headlines', [])
                    if isinstance(sources, list):
                        extracted_sources.extend(sources)
                
                norm_extracted = {normalize_text(s) for s in extracted_sources}
                
                # Cross-Containment Matching (Resilient to suffix stripping/minor rephrasing)
                preserved_titles = []
                lost_titles = []
                
                for h_norm, original_title in norm_original.items():
                    is_found = False
                    for s_norm in norm_extracted:
                        # Match if one is a substring of the other or exact match
                        if h_norm == s_norm or h_norm in s_norm or s_norm in h_norm:
                            is_found = True
                            break
                    
                    if is_found:
                        preserved_titles.append(original_title)
                    else:
                        lost_titles.append(original_title)
                
                fidelity_score = (len(preserved_titles) / len(original_headlines) * 100) if original_headlines else 0
                
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("✅ Data Fidelity", f"{fidelity_score:.1f}%", help="Percentage of original headlines successfully processed into the output (normalized matching).")
                with col2:
                    st.metric("📑 Preservation", f"{len(preserved_titles)}/{len(original_headlines)}", help="Total articles captured in final distillation.")
                with col3:
                    st.metric("📡 Total API Calls", f"{total_api_calls}", help="Number of times the AI was queried (including retries and branches).")
                
                if lost_titles:
                    with st.expander(f"⚠️ Warning: {len(lost_titles)} Headlines potentially missing", expanded=False):
                        st.write("The following headlines were either consolidated, identified as duplicates, or missed. Check if they are actually absent from the distillation below:")
                        for title in sorted(list(set(lost_titles))):
                            st.write(f"- {title}")
                else:
                    st.success("🎯 100% Data Integrity: All headlines accounted for.")

                # --- TOKEN SAVINGS VS RAW INPUT ---
                # Calculate total raw tokens from the actual news text being processed
                raw_input_tokens = km.estimate_tokens(preview_text) if km else 0
                opt_tokens = km.estimate_tokens(optimized_text) if km else 0
                savings_pct = ((raw_input_tokens - opt_tokens) / raw_input_tokens * 100) if raw_input_tokens > 0 else 0
                
                st.info(f"💾 **Data Distillation**: Input reduced from ~{raw_input_tokens:,} to ~{opt_tokens:,} tokens (**-{savings_pct:.1f}%**)")
                st.code(optimized_text, language="text")
                st.success("✅ Process Complete. Copy the optimized text above for your manual AI analysis.")

if st.session_state['data_loaded']:
    st.divider()
    
    pass
