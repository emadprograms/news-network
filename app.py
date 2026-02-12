import streamlit as st
import time
import datetime
import re
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
            secret_name="turso_emadarshadalam_newsdatabase_DB_URL",
            project_id=infisical_secrets["project_id"],
            environment_slug="dev",
            secret_path="/"
        ).secretValue
        
        news_token = infisical.secrets.get_secret_by_name(
            secret_name="turso_emadarshadalam_newsdatabase_AUTH_TOKEN",
            project_id=infisical_secrets["project_id"],
            environment_slug="dev",
            secret_path="/"
        ).secretValue

        # 2. Key Manager Database (Headed for keys)
        km_url = infisical.secrets.get_secret_by_name(
            secret_name="turso_emadprograms_analystworkbench_DB_URL",
            project_id=infisical_secrets["project_id"],
            environment_slug="dev",
            secret_path="/"
        ).secretValue
        
        km_token = infisical.secrets.get_secret_by_name(
            secret_name="turso_emadprograms_analystworkbench_AUTH_TOKEN",
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

def chunk_data(items, max_tokens=10000): # Enforce 10k limit
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

def build_chunk_prompt(chunk, index, total, market_data_text):
    """
    STRICT DATA ETL PROMPT - V1
    Forces JSON output for machine-readable datasets.
    """
    prompt = f"""SYSTEM NOTICE: This is PART {index+1} of {total}.

*** ROLE ***
You are a high-fidelity Data Extraction Engine. Your sole purpose is to convert unstructured news text into a structured, machine-readable JSON dataset. You have ZERO creative license. You must not analyze, correlate, or summarize beyond the explicit facts provided in the text.

*** STRICT CONSTRAINTS ***
1. NO CORRELATION: Do not link separate stories unless the text explicitly links them.
2. NO EXTERNAL KNOWLEDGE: Extract only what is in the provided text.
3. NO SYNTHESIS: Do not merge distinct events into a general narrative. Keep them as discrete data objects.
4. DEDUPLICATION: If a story appears multiple times (e.g., across different news wires), extract data only from the most detailed version and ignore the rest.
5. RAW DATA PRIORITY: Prioritize preserving specific numbers (tickers, prices, % changes, revenue, EPS, deal sizes). Do not round these numbers.
6. TRUNCATION HANDLING: If the text chunk ends in the middle of a story, extract what is visible and mark the entry as "TRUNCATED" so the next system knows to look for the rest.

*** OUTPUT FORMAT ***
You must output a valid JSON object containing a list of items. Use the following schema structure:

{{
  "news_items": [
    {{
      "category": "String (Choose one: EARNINGS, MERGERS_ACQUISITIONS, MACRO_ECONOMY, MARKET_MOVEMENTS, GEOPOLITICS, EXECUTIVE_MOVES, OTHER)",
      "primary_entity": "String (Company Name or Ticker or Country)",
      "secondary_entities": ["Array of Strings (Other involved parties)"],
      "event_summary": "String (Concise, factual, one-sentence description of the event)",
      "hard_data": {{
        "key_metric_1": "value",  // e.g., "Revenue": "$12.4B"
        "key_metric_2": "value"   // e.g., "EPS": "$1.20"
      }},
      "guidance_or_forecast": "String (Only if explicitly stated in text)",
      "quotes": ["Array of Strings (Direct quotes from key figures)"],
      "sentiment_indicated": ["Array of Strings (e.g., 'Analyst upgraded to Buy', 'Weak Outlook')"],
      "is_truncated": Boolean
    }}
  ]
}}

*** INSTRUCTIONS ***
1. Process the provided partial dataset.
2. Extract every distinct news event into the JSON format defined above.
3. If no hard data exists for a field, leave it null or empty.
4. Do not output any conversational text before or after the JSON block. Start with '{{' and end with '}}'.

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
    """EMERGENCY FALLBACK: Hunts for individual JSON objects and patches segments."""
    if not text: return []
    items = []
    last_end = 0
    pattern = r'\{\s*"category":.*?\}(?=\s*[,\]\}]|\s*$)'
    for match in re.finditer(pattern, text, re.DOTALL):
        try:
            obj_str = match.group(0).strip()
            if obj_str.count('{') > obj_str.count('}'): obj_str += '}'
            obj = json.loads(obj_str, strict=False)
            if isinstance(obj, dict) and "category" in obj:
                items.append(obj)
                last_end = match.end()
        except: continue
    remaining = text[last_end:].strip()
    if '{"category":' in remaining:
        frag_start = remaining.find('{"category":')
        fragment = remaining[frag_start:].strip()
        patch_variants = ['"}', '}', '"]}', ']}', '"]}}', ']}}', '"} } ] } }']
        for pv in patch_variants:
            try:
                patched = fragment + pv
                if patched.count('{') > patched.count('}'): 
                    patched += '}' * (patched.count('{') - patched.count('}'))
                obj = json.loads(patched, strict=False)
                if isinstance(obj, dict) and "category" in obj:
                    obj["is_truncated"] = True
                    obj["event_summary"] = obj.get("event_summary", "") + " [RECOVERED FRAGMENT]"
                    items.append(obj)
                    break 
            except: continue
    return items

def extract_chunk_worker(worker_data):
    """Task worker for parallel extraction."""
    i, chunk, total_chunks, selected_model = worker_data
    worker_logs = []
    last_raw_content = ""
    context_for_prompt = ""
    for item in chunk:
        t = item.get('time', 'N/A')
        title = item.get('title', 'No Title')
        body = " ".join(clean_content(item.get('content', [])))
        context_for_prompt += f"[{t}] {title}\n{body}\n\n"
    
    p = build_chunk_prompt(chunk, i, total_chunks, context_for_prompt)
    attempt, max_attempts = 0, 5
    while attempt < max_attempts:
        attempt += 1
        try:
            token_est = km.estimate_tokens(p) if km else "N/A"
            worker_logs.append(f"🔹 [Part {i+1}/{total_chunks}] Trial {attempt} - Extraction in progress... (~{token_est} tokens)")
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
                    worker_logs.append(f"✅ [Part {i+1}] Success! {len(items)} items. (Key: {res.get('key_name', 'Unknown')})")
                    return (True, items, worker_logs)
                except Exception as json_err:
                    err_str = str(json_err).lower()
                    if any(k in err_str for k in ["delimiter", "double quotes", "expecting value", "unterminated"]):
                        salvaged = salvage_json_items(content)
                        if salvaged:
                            worker_logs.append(f"⚡ [Part {i+1}] Eager Salvage: Recovered {len(salvaged)} items.")
                            worker_logs.append(f"DEBUG_RAW_CONTENT|{content}")
                            worker_logs.append(f"DEBUG_SALVAGED_ITEMS|{json.dumps(salvaged, indent=2)}")
                            return (True, salvaged, worker_logs)
                    raise json_err
            else:
                err_msg = res['content']
                wait_sec = res.get('wait_seconds', 0)
                if wait_sec > 0:
                    worker_logs.append(f"⏳ [Part {i+1}] Quota hit. Rotating keys...")
                    time.sleep(1) 
                    continue
                else:
                    worker_logs.append(f"❌ [Part {i+1}] Trial {attempt} failed: {err_msg}")
                    time.sleep(2)
        except Exception as e:
            worker_logs.append(f"⚠️ [Part {i+1}] Error: {e}")
            time.sleep(2)
            
    if last_raw_content:
        salvaged = salvage_json_items(last_raw_content)
        if salvaged:
            worker_logs.append(f"🩹 [Part {i+1}] Emergency Salvage: {len(salvaged)} items.")
            worker_logs.append(f"DEBUG_RAW_CONTENT|{last_raw_content}")
            worker_logs.append(f"DEBUG_SALVAGED_ITEMS|{json.dumps(salvaged, indent=2)}")
            return (True, salvaged, worker_logs)
            
    return (False, [], worker_logs)


# ==============================================================================
#  LAY OUT
# ==============================================================================

# 1. HEADER
st.title("📰 News Network Analysis")

# 2. UNIFIED CONTROL PANEL
with st.container():
    st.subheader("🛠️ Analyst Control Panel")
    

    with st.form("analyst_controls"):
        # ROW 1: Time Window
        st.markdown("**1. Select Time Window**")
        col_t1, col_t2, col_t3, col_t4 = st.columns(4)
        with col_t1: start_date = st.date_input("From Date", value=datetime.date.today())
        with col_t2: start_time = st.time_input("From Time", value=datetime.time(0, 0))
        with col_t3: end_date = st.date_input("To Date", value=datetime.date.today())
        with col_t4: end_time = st.time_input("To Time", value=datetime.time(23, 59))
        
        st.divider()

        # ROW 2: Model & Model Setup
        st.markdown("**2. Select Extraction Model**")
        if km:
             model_options = list(km.MODELS_CONFIG.keys())
             ix = 0
             if 'gemini-2.0-flash-paid' in model_options:
                 ix = model_options.index('gemini-2.0-flash-paid')
             elif 'gemini-1.5-flash-paid' in model_options:
                 ix = model_options.index('gemini-1.5-flash-paid')
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
             dt_start = datetime.datetime.combine(start_date, start_time)
             dt_end = datetime.datetime.combine(end_date, end_time)
             items = db.fetch_news_range(dt_start.isoformat(), dt_end.isoformat())
             
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
        with st.expander("📋 Emergency Copiable Raw Data Backup", expanded=True):
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
        # REDUCED CHUNK SIZE: 50k -> 10k for maximum fidelity and zero truncation risk
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
            st.info(f"🚀 Starting Parallel Extraction with {max_threads} worker threads...")
            
            all_extracted_items = []
            completed_count = 0
            
            with ThreadPoolExecutor(max_workers=max_threads) as executor:
                future_to_chunk = {
                    executor.submit(extract_chunk_worker, (i, chunk, len(chunks), selected_model)): i 
                    for i, chunk in enumerate(chunks)
                }
                
                for future in as_completed(future_to_chunk):
                    completed_count += 1
                    success, items, logs = future.result()
                    
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
                
                raw_json_str = json.dumps(all_extracted_items)
                raw_tokens = km.estimate_tokens(raw_json_str) if km else 0
                opt_tokens = km.estimate_tokens(optimized_text) if km else 0
                savings_pct = ((raw_tokens - opt_tokens) / raw_tokens * 100) if raw_tokens > 0 else 0
                
                st.info(f"💾 **Token Savings**: Output reduced from ~{raw_tokens:,} to ~{opt_tokens:,} tokens (**-{savings_pct:.1f}%**)")
                st.code(optimized_text, language="text")
                st.success("✅ Process Complete. Copy the optimized text above for your manual AI analysis.")

if st.session_state['data_loaded']:
    st.divider()
    
    pass
