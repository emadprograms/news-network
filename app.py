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
    """Splits items into chunks. Truncates individual items if they exceed limit."""
    chunks = []
    current_chunk = []
    current_tokens = 0
    
    for item in items:
        body = " ".join(clean_content(item.get('content', [])))
        meta = f"{item.get('time')} {item.get('title')} {item.get('publisher')}"
        
        # Hard truncate individual items to max_tokens to prevent single-item overflow
        limit_chars = int(max_tokens * 2.5)
        if len(body) > limit_chars:
            body = body[:limit_chars] + "... [TRUNCATED]"
            # CRITICAL FIX: Update the item itself so the prompt uses the truncated version
            item = item.copy()
            item['content'] = [body]
            
        total_chars = len(body) + len(meta) + 50 
        est_tok = int(total_chars / 2.5)
        
        # If adding this item exceeds limit, start new chunk
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
      "sentiment_indicated": "String (Only if explicitly stated, e.g., 'Analyst upgraded to Buy')",
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


# ==============================================================================
#  LAY OUT
# ==============================================================================

# 1. HEADER
st.title("📰 News Network Analysis")

# 2. UNIFIED CONTROL PANEL
with st.container():
    st.subheader("🛠️ Analyst Control Panel")
    
    # DEBUG: Key Status
    with st.expander("🔑 System Keys & Status"):
        if km:
            keys = km.get_all_managed_keys()
            if keys:
                clean_keys = []
                for k in keys:
                    clean_keys.append({
                        "Name": k['key_name'],
                        "Tier": k.get('tier', 'free'),
                        "Priority": k.get('priority', 10),
                        "Added": k.get('added_at', 'N/A')
                    })
                st.dataframe(clean_keys)
            else:
                st.warning("No keys found in Database.")
        else:
            st.error("KeyManager not initialized.")

    with st.form("analyst_controls"):
        # ROW 1: Time Window
        st.markdown("**1. Select Time Window**")
        col_t1, col_t2, col_t3, col_t4 = st.columns(4)
        with col_t1: start_date = st.date_input("From Date", value=datetime.date.today())
        with col_t2: start_time = st.time_input("From Time", value=datetime.time(0, 0))
        with col_t3: end_date = st.date_input("To Date", value=datetime.date.today())
        with col_t4: end_time = st.time_input("To Time", value=datetime.time(23, 59))
        
        st.divider()

        # ROW 2: Model Configuration
        st.markdown("**2. AI Extraction Configuration**")
        col_ai1, col_ai2 = st.columns([3, 1])
        
        with col_ai1:
            st.info(
                "💡 **Structured ETL Mode Active**: The system is forced into a high-fidelity data extraction role. "
                "It will output a machine-readable JSON dataset containing earnings, macroEvents, and market movements."
            )
            
        with col_ai2:
            if km:
                 model_options = list(km.MODELS_CONFIG.keys())
                 ix = 0
                 if 'gemini-2.0-flash-paid' in model_options:
                     ix = model_options.index('gemini-2.0-flash-paid')
                 elif 'gemini-1.5-flash-paid' in model_options:
                     ix = model_options.index('gemini-1.5-flash-paid')
                 selected_model = st.selectbox("Select Model", options=model_options, index=ix)
            else:
                st.error("Keys unavailable")
                selected_model = None

        st.divider()
        
        # ROW 3: Execution Mode
        col_exec, col_btn = st.columns([3, 1])
        with col_exec:
             mode = st.radio("Extraction Mode", ["🚀 RUN ETL (Full Extraction)", "🧪 DRY RUN (Test Prompts Only)"], horizontal=True)
        
        with col_btn:
            st.write("") 
            st.write("") 
            submitted = st.form_submit_button("▶️ START EXTRACTION", type="primary")

# 3. EXECUTION LOGIC
if submitted:
    # A. FETCH DATA
    st.session_state['data_loaded'] = False
    st.session_state['news_data'] = []
    st.session_state['ai_report'] = ""
    st.session_state['dry_run_prompts'] = []
    
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
        
        # C. EXECUTE MODE
        if "Dry Run" in mode:
            prompts = []
            for i, chunk in enumerate(chunks):
                # Build context for display
                context_for_prompt = ""
                for item in chunk:
                    t = item.get('time', 'N/A')
                    title = item.get('title', 'No Title')
                    body = " ".join(clean_content(item.get('content', [])))
                    context_for_prompt += f"[{t}] {title}\n{body}\n\n"
                    
                p = build_chunk_prompt(chunk, i, len(chunks), context_for_prompt)
                prompts.append(p)
            st.session_state['dry_run_prompts'] = prompts
            st.toast(f"Dry Run Complete: {len(prompts)} ETL prompts built.")
            
        else:
            # Run AI
            if not ai_client:
                st.error("AI Client unavailable.")
            else:
                import json
                all_extracted_items = []
                last_idx = len(chunks) - 1
                
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                # --- LIVE ETL LOGS ---
                log_expander = st.expander("🛠️ Live ETL Logs", expanded=True)
                log_container = log_expander.container()
                
                def repair_json_content(json_str):
                    """
                    Robust JSON repair for common LLM syntax errors.
                    """
                    # 0. Strip leading/trailing non-json junk that might have slipped through
                    json_str = json_str.strip()
                    
                    # 1. Fix missing commas between objects: } { -> }, {
                    json_str = re.sub(r'\}\s*\{', '}, {', json_str)
                    
                    # 2. Fix missing commas between array items: ] [ -> ], [
                    json_str = re.sub(r'\]\s*\[', '], [', json_str)
                    
                    # 3. Fix Trailing Commas: , } -> } and , ] -> ]
                    # This is the most likely cause of "Expecting property name" errors
                    json_str = re.sub(r',\s*\}', '}', json_str)
                    json_str = re.sub(r',\s*\]', ']', json_str)

                    # 4. Fix missing commas between key-value pairs
                    # Pattern: "val" "key": -> "val", "key":
                    json_str = re.sub(r'\"\s*\n\s*\"', '", "', json_str)
                    
                    # 5. Fix missing commas after literals (numbers, true, false, null)
                    json_str = re.sub(r'(\d+|true|false|null)\s*\n\s*\"', r'\1, "', json_str)
                    
                    # 6. Fix Missing Colons (The "Expecting :" error)
                    # Pattern: "key" "value" -> "key": "value"
                    json_str = re.sub(r'\"([a-zA-Z0-9_]+)\"\s+\"([^\"]+)\"', r'"\1": "\2"', json_str)
                    
                    return json_str

                # --- PARALLEL EXECUTION LOGIC ---
                from concurrent.futures import ThreadPoolExecutor, as_completed

                def extract_chunk_worker(chunk_data):
                    """
                    Worker function to process a single chunk in a separate thread.
                    Returns: (success, items, log_messages)
                    """
                    i, chunk, total_chunks = chunk_data
                    worker_logs = []
                    extracted_items = []
                    
                    # 1. Build context
                    context_for_prompt = ""
                    for item in chunk:
                        t = item.get('time', 'N/A')
                        title = item.get('title', 'No Title')
                        body = " ".join(clean_content(item.get('content', [])))
                        context_for_prompt += f"[{t}] {title}\n{body}\n\n"

                    p = build_chunk_prompt(chunk, i, total_chunks, context_for_prompt)
                    
                    # 2. Execute with Relentless Rotation & Retry
                    attempt = 0
                    max_attempts = 5 # Avoid infinite loops in threads
                    
                    while attempt < max_attempts:
                        attempt += 1
                        try:
                            # Token Est
                            token_est = km.estimate_tokens(p) if km else "N/A"
                            worker_logs.append(f"🔹 [Part {i+1}/{total_chunks}] Trial {attempt} - Extraction in progress... (~{token_est} tokens)")
                            
                            # API Call
                            res = ai_client.generate_content(p, config_id=selected_model)
                            
                            if res['success']:
                                content = res['content']
                                key_used = res.get('key_name', 'Unknown')
                                
                                # --- ROBUST JSON EXTRACTION ---
                                raw_json = content.strip()
                                # Layer 1-3 repairs (inline for compactness or call helper)
                                if "```json" in raw_json:
                                    match = re.search(r"```json\s*(.*?)\s*```", raw_json, re.DOTALL)
                                    if match: raw_json = match.group(1).strip()
                                elif "```" in raw_json:
                                     match = re.search(r"```\s*(.*?)\s*```", raw_json, re.DOTALL)
                                     if match: raw_json = match.group(1).strip()
                                if not raw_json.startswith("{"):
                                    match = re.search(r"(\{.*\})", raw_json, re.DOTALL)
                                    if match: raw_json = match.group(1).strip()
                                if raw_json.endswith("```"): raw_json = raw_json[:-3].strip()
                                
                                raw_json = repair_json_content(raw_json)
                                
                                # Layer 5 repair
                                if raw_json.strip().endswith("}") is False:
                                     if not raw_json.strip().endswith(('"', ',', '}', ']')): raw_json += '"'
                                     if raw_json.count('{') > raw_json.count('}'): raw_json += '}]}'

                                # Parse
                                data = json.loads(raw_json, strict=False)
                                if isinstance(data, list): items = data
                                else: items = data.get("news_items", [])
                                
                                worker_logs.append(f"✅ [Part {i+1}] Success! {len(items)} items. (Key: {key_used})")
                                return (True, items, worker_logs)

                            else:
                                # Failure (429 or other)
                                err_msg = res['content']
                                failed_key = res.get('key_name', 'Unknown')
                                wait_sec = res.get('wait_seconds', 0)
                                
                                if wait_sec > 0:
                                    worker_logs.append(f"⏳ [Part {i+1}] Quota hit for '{failed_key}'. Retrying with new key...")
                                    time.sleep(1) # Small sleep, then rotate
                                    continue
                                else:
                                    worker_logs.append(f"❌ [Part {i+1}] Trial {attempt} failed: {err_msg}")
                                    time.sleep(2)
                                    continue
                                    
                        except Exception as e:
                            worker_logs.append(f"⚠️ [Part {i+1}] Error: {e}")
                            time.sleep(2)
                    
                    worker_logs.append(f"❌ [Part {i+1}] FAILED after {max_attempts} attempts.")
                    return (False, [], worker_logs)

                # --- START PARALLEL EXECUTION ---
                max_threads = min(len(chunks), 15) # Cap at 15 threads or num chunks
                st.info(f"🚀 Starting Parallel Extraction with {max_threads} worker threads...")
                
                completed_count = 0
                
                with ThreadPoolExecutor(max_workers=max_threads) as executor:
                    # Submit all tasks
                    future_to_chunk = {executor.submit(extract_chunk_worker, (i, chunk, len(chunks))): i for i, chunk in enumerate(chunks)}
                    
                    for future in as_completed(future_to_chunk):
                        completed_count += 1
                        success, items, logs = future.result()
                        
                        # Update UI
                        for log_msg in logs:
                            if "✅" in log_msg: log_container.success(log_msg)
                            elif "❌" in log_msg: log_container.error(log_msg)
                            elif "⏳" in log_msg: log_container.warning(log_msg)
                            else: log_container.write(log_msg)
                            
                        if success:
                            all_extracted_items.extend(items)
                        
                        # Update Progress
                        progress_bar.progress(completed_count / len(chunks))
                
                progress_bar.progress(1.0)
                status_text.success("Extraction Complete! Generatng Report...")
                time.sleep(1)
                status_text.empty()
                progress_bar.empty()
                
                if all_extracted_items:
                    # 1. Save Structured JSON for the User
                    final_dataset = {"news_items": all_extracted_items, "total_entities": len(all_extracted_items)}
                    st.session_state['ai_report'] = json.dumps(final_dataset, indent=2)
                    st.session_state['json_data'] = all_extracted_items # Ensure this is set for optimization
                    st.balloons()

                    # 2. RUN FINAL ANALYTICAL SYNTHESIS (Use Token-Optimized Text)
                    st.divider()
                    st.subheader("🤖 AI Market Analyst Report")
                    
                    # OPTIMIZATION: Transform JSON to Dense Text
                    optimized_text = optimize_json_for_synthesis(all_extracted_items)
                    
                    # TOKEN SAVINGS CALCULATION
                    raw_json_str = json.dumps(all_extracted_items)
                    raw_tokens = km.estimate_tokens(raw_json_str) if km else 0
                    opt_tokens = km.estimate_tokens(optimized_text) if km else 0
                    savings = raw_tokens - opt_tokens
                    savings_pct = (savings / raw_tokens * 100) if raw_tokens > 0 else 0
                    
                    with st.expander("🔍 View Optimized Token-Light Input (For AI Analysis)", expanded=False):
                        st.info(f"✨ **Token Savings**: Reduced from ~{raw_tokens:,} to ~{opt_tokens:,} tokens (**-{savings_pct:.1f}%**)")
                        st.text(optimized_text)
                    
                    final_prompt = f"""
                    You are a Senior Market Analyst. 
                    Review the following consolidated news feed (organized by Entity) and generate a comprehensive market report.
                    
                    This input is highly optimized to save tokens. It is grouped by ENTITY.
                    
                    OBJECTIVE:
                    Synthesize the diverse news items into a coherent narrative.
                    
                    Focus on:
                    1. 🚨 Major Earnings Beats/Misses for Key Players
                    2. 🕵️‍♂️ Significant Insider Moves (Clusters of buying/selling)
                    3. 📉 Analyst Sentiment Shifts (Upgrades/Downgrades)
                    4. 🌍 Macro Trends & Sector Rotations
                    
                    Do NOT just list the news again. Connect the dots.
                    
                    DATA:
                    {optimized_text}
                    """
                    
                    with st.spinner("🤖 Writing Final Report..."):
                         # We use a separate key/call for this to ensure we have quota
                        final_res = ai_client.generate_content(final_prompt, config_id=selected_model)
                        if final_res['success']:
                            st.markdown("### 📝 Consolidated Market Analysis")
                            st.markdown(final_res['content'])
                        else:
                            st.error(f"Failed to generate final report: {final_res['content']}")

if st.session_state['data_loaded']:
    st.divider()
    
    # Market Data Preview
    items = st.session_state['news_data']
    with st.expander(f"📊 Market Data Preview ({len(items)} items found)"):
        st.json(items)
        
    # Results Section (DataSet)
    if st.session_state['ai_report']:
        st.subheader("📦 Extracted Structured Dataset")
        
        try:
            import json
            import pandas as pd
            
            # 1. Parse Data
            parsed_data = json.loads(st.session_state['ai_report'])
            items = parsed_data.get("news_items", [])
            
            # 2. Neat Table View (Primary)
            if items:
                st.markdown("### 📋 Executive Summary Table")
                # Flatten hard_data for the table view
                df_rows = []
                for it in items:
                    row = {
                        "Category": it.get("category", "OTHER"),
                        "Primary Entity": it.get("primary_entity", "N/A"),
                        "Summary": it.get("event_summary", "N/A"),
                        "Hard Data": str(it.get("hard_data", {})),
                        "Sentiment": it.get("sentiment_indicated", "N/A")
                    }
                    df_rows.append(row)
                
                df = pd.DataFrame(df_rows)
                st.dataframe(df, use_container_width=True)
            
            # 3. Interactive JSON Tree & Copy Option
            with st.expander("🔍 View/Copy Full JSON Structure"):
                st.info("💡 You can copy the JSON by clicking the icon in the top-right of the code block below.")
                st.json(parsed_data)
                st.divider()
                st.caption("📋 Raw JSON (Ideal for Copying)")
                st.code(st.session_state['ai_report'], language="json")
                
        except Exception as e:
            st.error(f"Display Error: {e}")
            st.code(st.session_state['ai_report'], language="json")
            
        st.download_button(
            "📥 Download JSON Dataset", 
            st.session_state['ai_report'], 
            file_name="extracted_news_data.json",
            mime="application/json"
        )
        
    elif st.session_state['dry_run_prompts']:
        st.subheader(f"🧪 Dry Run Result ({len(st.session_state['dry_run_prompts'])} Parts)")
        
        tabs = st.tabs([f"Part {i+1}" for i in range(len(st.session_state['dry_run_prompts']))])
        
        for i, tab in enumerate(tabs):
            prompt = st.session_state['dry_run_prompts'][i]
            with tab:
                st.info(f"Part {i+1} ETL Prompt - This would be sent for extraction.")
                st.caption(f"Estimated Tokens: {km.estimate_tokens(prompt) if km else 'N/A'}")
                st.code(prompt, language="text")
    
    pass
