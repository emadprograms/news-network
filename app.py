import streamlit as st
import time
import datetime
import re
from modules.db_client import NewsDatabase
from modules.key_manager import KeyManager
from modules.llm_client import GeminiClient
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

def chunk_data(items, max_tokens=220000): # Reverted to 220k
    """Splits items into chunks. Truncates individual items if they exceed limit."""
    chunks = []
    current_chunk = []
    current_tokens = 0
    
    for item in items:
        body = " ".join(clean_content(item.get('content', [])))
        meta = f"{item.get('time')} {item.get('title')} {item.get('publisher')}"
        
        # Hard truncate individual items to max_tokens to prevent single-item overflow
        if len(body) > (max_tokens * 2.5):
            body = body[:int(max_tokens * 2.5)] + "... [TRUNCATED]"
            
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

                for i, chunk in enumerate(chunks):
                    # 1. Build context for this chunk
                    context_for_prompt = ""
                    for item in chunk:
                        t = item.get('time', 'N/A')
                        title = item.get('title', 'No Title')
                        body = " ".join(clean_content(item.get('content', [])))
                        context_for_prompt += f"[{t}] {title}\n{body}\n\n"

                    p = build_chunk_prompt(chunk, i, len(chunks), context_for_prompt)
                    
                    status_msg = f"Extracting Data Part {i+1}/{len(chunks)}..."
                    progress_bar.progress((i) / (len(chunks) if len(chunks) > 0 else 1))
                    
                    # 2. Execute with Relentless Rotation & Retry
                    attempt = 0
                    with st.spinner(f"ETL Engine: {status_msg}"):
                        while True:
                            attempt += 1
                            log_container.write(f"🔹 [Part {i+1}/{len(chunks)}] Trial {attempt} - Extraction in progress...")
                            
                            res = ai_client.generate_content(p, config_id=selected_model)
                            
                            if res['success']:
                                content = res['content']
                                key_used = res.get('key_name', 'Unknown')
                                
                                # --- ROBUST JSON EXTRACTION ---
                                try:
                                    raw_json = content.strip()
                                    
                                    # Layer 1: Markdown blocks
                                    if "```json" in raw_json:
                                        match = re.search(r"```json\s*(.*?)\s*```", raw_json, re.DOTALL)
                                        if match: raw_json = match.group(1).strip()
                                    elif "```" in raw_json:
                                         match = re.search(r"```\s*(.*?)\s*```", raw_json, re.DOTALL)
                                         if match: raw_json = match.group(1).strip()
                                    
                                    # Layer 2: Fallback to first '{' and last '}'
                                    if not raw_json.startswith("{"):
                                        match = re.search(r"(\{.*\})", raw_json, re.DOTALL)
                                        if match: raw_json = match.group(1).strip()
                                    
                                    # Layer 3: Clean up potentially trailing junk
                                    if raw_json.endswith("```"):
                                        raw_json = raw_json[:-3].strip()

                                    # Layer 4: Structural Repair (Aggressive)
                                    raw_json = repair_json_content(raw_json)
                                    
                                    # Layer 5: Ensure brackets are balanced/closed if truncated
                                    if raw_json.strip().endswith("}") is False:
                                         if raw_json.strip().endswith("]"):
                                             pass 
                                         else:
                                             # Try closing the main object if it ends abruptly
                                             if not raw_json.strip().endswith(('"', ',', '}', ']')):
                                                  raw_json += '"' # Close string?
                                             if raw_json.count('{') > raw_json.count('}'):
                                                 raw_json += '}]}' # Attempt to close
                                    
                                    # Parse with strict=False to allow control characters
                                    data = json.loads(raw_json, strict=False)
                                    
                                    # Handle list output instead of dict
                                    if isinstance(data, list):
                                        items = data
                                    else:
                                        items = data.get("news_items", [])
                                        
                                    all_extracted_items.extend(items)
                                    
                                    log_container.success(f"✅ [Part {i+1}] Extracted {len(items)} items using '{key_used}'.")
                                    st.toast(f"✅ Part {i+1} successful.")
                                    break # Success
                                    
                                except Exception as json_err:
                                    log_container.error(f"⚠️ JSON Extraction Failed on Trial {attempt}: {json_err}")
                                    with log_container.expander("🔍 View Raw AI Response"):
                                        st.code(content)
                                    log_container.info("🔄 Rotating to next key in 2s...")
                                    time.sleep(2)
                                    continue # Force retry with next key
                            
                            else:
                                # Failure: Rate Limit or Transient
                                err_msg = res['content']
                                wait_sec = res.get('wait_seconds', 0)
                                if wait_sec > 0:
                                    log_container.warning(f"⏳ Quota hit. Resting {int(wait_sec)}s...")
                                    time.sleep(wait_sec)
                                    continue
                                else:
                                    log_container.error(f"❌ Trial {attempt} failed: {err_msg}")
                                    time.sleep(5)
                                    continue 
                
                progress_bar.progress(1.0)
                status_text.empty()
                
                if all_extracted_items:
                    final_dataset = {"news_items": all_extracted_items, "total_entities": len(all_extracted_items)}
                    st.session_state['ai_report'] = json.dumps(final_dataset, indent=2)
                    st.balloons()


# 4. RESULTS DISPLAY
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
