import streamlit as st
import time
import datetime
import re
from modules.db_client import NewsDatabase
from modules.key_manager import KeyManager
from modules.llm_client import GeminiClient
from infisical_sdk import InfisicalSDKClient

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
    .ticker-wrap {
        width: 100%;
        overflow: hidden;
        background-color: #0e1117;
        color: #00ff41;
        font-family: 'Courier New', Courier, monospace;
        padding: 10px 0;
        white-space: nowrap;
        box-sizing: border-box;
    }
    .ticker {
        display: inline-block;
        animation: marquee 120s linear infinite;
        padding-left: 100%; 
    }
    @keyframes marquee {
        0%   { transform: translate(0, 0); }
        100% { transform: translate(-100%, 0); }
    }
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
        
        db_url_secret = infisical.secrets.get_secret_by_name(
            secret_name="turso_emadarshadalam_newsdatabase_DB_URL",
            project_id=infisical_secrets["project_id"],
            environment_slug="dev",
            secret_path="/"
        ).secretValue
        
        db_token_secret = infisical.secrets.get_secret_by_name(
            secret_name="turso_emadarshadalam_newsdatabase_AUTH_TOKEN",
            project_id=infisical_secrets["project_id"],
            environment_slug="dev",
            secret_path="/"
        ).secretValue
        
        db = NewsDatabase(
            db_url_secret.replace("libsql://", "https://"),
            db_token_secret
        )
        return db, db_url_secret, db_token_secret
        
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
        # 1 token ~ 2.5 chars. Max chars ~ 250k for 100k tokens.
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

def build_prompt_from_items(items, system_instruction):
    context_blob = ""
    for item in items:
        t = item.get('time', 'N/A')
        title = item.get('title', 'No Title')
        src = item.get('publisher', 'Unknown')
        body = " ".join(clean_content(item.get('content', [])))
        context_blob += f"[{t}] {title} ({src})\n{body}\n\n"
        
    return f"{system_instruction}\n\n=== MARKET DATA ===\n{context_blob}"

def build_chunk_prompt(chunk, index, total, system_instruction):
    """Builds a prompt for a specific chunk with explicit Part X of Y instructions."""
    if total == 1:
        return build_prompt_from_items(chunk, system_instruction)
    
    # 0-indexed vs 1-indexed conversion
    # index is 0-based. total is 1-based count.
    # index 0 is Part 1. index (total-1) is Part (total) == LAST.
    
    is_last_part = (index + 1) == total
    
    if is_last_part:
        # FINAL PART PROMPT
        system_notice = (
            f" SYSTEM NOTICE: This is PART {index+1} of {total} (FINAL PART).\n"
            f" INSTRUCTIONS:\n"
            f" 1. Analyze this final dataset.\n"
            f" 2. Combine findings from this part with all previous parts (if available).\n"
            f" 3. GENERATE THE FINAL COMPREHENSIVE REPORT NOW.\n"
            f" {system_instruction}"
        )
    else:
        # INTERMEDIATE PART PROMPT (Stealth Extraction)
        system_notice = (
            f" SYSTEM NOTICE: This is PART {index+1} of {total}.\n"
            f" INSTRUCTIONS:\n"
            f" 1. Analyze this partial dataset.\n"
            f" 2. EXTRACT detailed findings into a block labeled [[MEMORY_BLOCK]]. This is for internal use.\n"
            f" 3. OUTSIDE the block, simply acknowledge receipt: 'Received Part {index+1} of {total}, ready for next part.'\n"
            f" 4. Do NOT generate a final analysis yet.\n"
            f" {system_instruction}"
        )
        
    return build_prompt_from_items(chunk, system_notice)


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
                # hide values
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

        # ROW 2: AI Configuration
        st.markdown("**2. AI Configuration**")
        col_ai1, col_ai2 = st.columns([3, 1])
        
        with col_ai1:
            default_sys = (
                "You are a master financial analyst. Review the provided news items and generate a strategic market summary.\n"
                "Identify key trends, correlation between assets, and potential market moving events.\n"
                "Format the output as a professional briefing."
            )
            system_instruction = st.text_area("System Instruction", value=default_sys, height=100)
            
        with col_ai2:
            if km:
                 model_options = list(km.MODELS_CONFIG.keys())
                 ix = 0
                 if 'gemini-2.0-flash-paid' in model_options:
                     ix = model_options.index('gemini-2.0-flash-paid')
                 elif 'gemini-2.5-flash-free' in model_options:
                     ix = model_options.index('gemini-2.5-flash-free')
                 selected_model = st.selectbox("Select Model", options=model_options, index=ix)
            else:
                st.error("Keys unavailable")
                selected_model = None

        st.divider()
        
        # ROW 3: Execution Mode
        col_exec, col_btn = st.columns([3, 1])
        with col_exec:
             mode = st.radio("Execution Mode", ["🚀 Run Analysis (Fetch + AI)", "🧪 Dry Run (Fetch + Build Prompt Only)"], horizontal=True)
        
        with col_btn:
            st.write("") 
            st.write("") 
            submitted = st.form_submit_button("▶️ EXECUTE WORKFLOW", type="primary")

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
        # B. CHUNK DATA
        chunks = chunk_data(st.session_state['news_data'], max_tokens=220000)
        
        if len(chunks) > 1:
            st.toast(f"Data too large for one prompt. Split into {len(chunks)} parts.")
        
        # C. EXECUTE MODE
        if "Dry Run" in mode:
            prompts = []
            for i, chunk in enumerate(chunks):
                # Use the shared helper to get exact prompt
                p = build_chunk_prompt(chunk, i, len(chunks), system_instruction)
                prompts.append(p)
            st.session_state['dry_run_prompts'] = prompts
            st.toast(f"Dry Run Complete: {len(prompts)} prompts built.")
            
        else:
            # Run AI
            if not ai_client:
                st.error("AI Client unavailable.")
            else:
                final_output = ""
                
                # CASE 1: Single Chunk (Direct)
                if len(chunks) == 1:
                    full_prompt = build_chunk_prompt(chunks[0], 0, 1, system_instruction)
                    with st.spinner(f"2/3 Analyzing {len(chunks[0])} items with {selected_model}..."):
                        result = ai_client.generate_content(full_prompt, config_id=selected_model)
                        if result['success']:
                            final_output = result['content']
                        else:
                            st.error(f"Analysis Failed: {result['content']}")
                            
                # CASE 2: Multiple Chunks (Map-Reduce)
                else:
                    summaries = []
                    progress_bar = st.progress(0)
                    status_text = st.empty()
                    
                    # MAP PHASE
                    for i, chunk in enumerate(chunks):
                        # RATE LIMIT ENFORCEMENT
                        if i > 0:
                            for s in range(60, 0, -1):
                                status_text.info(f"⏳ Rate Limit Cooldown: Resuming in {s} seconds...")
                                time.sleep(1)
                        
                        status_text.text(f"Processing Part {i+1}/{len(chunks)}...")
                        progress_bar.progress((i) / len(chunks))
                        
                        with st.spinner(f"2/3 Processing Part {i+1}/{len(chunks)} ({len(chunk)} items)..."):
                             # Use the shared helper
                             p = build_chunk_prompt(chunk, i, len(chunks), system_instruction)
                             
                             res = ai_client.generate_content(p, config_id=selected_model)
                             if res['success']:
                                 summaries.append(f"--- PART {i+1} SUMMARY ---\n{res['content']}")
                             else:
                                 st.error(f"Part {i+1} Failed: {res['content']}")
                        
                    progress_bar.progress(1.0)
                    status_text.empty()
                    
                    # REDUCE PHASE
                    if summaries:
                        if len(chunks) > 1:
                             for s in range(60, 0, -1):
                                st.info(f"⏳ Final Cooldown: Synthesis in {s} seconds...")
                                time.sleep(1)
                                
                        with st.spinner("3/3 Synthesizing Final Report..."):
                            combined_summaries = "\n\n".join(summaries)
                            reduce_prompt = (
                                f"{system_instruction}\n\n"
                                "Here are the intermediate summaries from analyzing the data in parts. "
                                "Synthesize them into one final, cohesive intelligence report.\n\n"
                                f"=== INTERMEDIATE DATA ===\n{combined_summaries}"
                            )
                            
                            final_res = ai_client.generate_content(reduce_prompt, config_id=selected_model)
                            if final_res['success']:
                                final_output = final_res['content']
                            else:
                                st.error(f"Final Synthesis Failed: {final_res['content']}")
                
                if final_output:
                    st.session_state['ai_report'] = final_output
                    st.balloons()


# 4. RESULTS DISPLAY
if st.session_state['data_loaded']:
    st.divider()
    
    # Ticker
    items = st.session_state['news_data']
    headlines = [f"💥 {n.get('title', 'Unknown').upper()}" for n in items[:15]]
    ticker_text = "   +++   ".join(headlines)
    st.markdown(f"""
    <div class="ticker-wrap">
    <div class="ticker">{ticker_text}</div>
    </div>
    """, unsafe_allow_html=True)
    
    with st.expander(f"📊 Market Data Preview ({len(items)} items found)"):
        st.json(items)
        
    # Report Section
    if st.session_state['ai_report']:
        st.subheader("📝 Intelligence Report")
        st.markdown(st.session_state['ai_report'])
        st.download_button("📥 Download Report", st.session_state['ai_report'], file_name="ai_analysis.md")
        
    elif st.session_state['dry_run_prompts']:
        st.subheader(f"🧪 Dry Run Result ({len(st.session_state['dry_run_prompts'])} Parts)")
        
        tabs = st.tabs([f"Part {i+1}" for i in range(len(st.session_state['dry_run_prompts']))])
        
        for i, tab in enumerate(tabs):
            prompt = st.session_state['dry_run_prompts'][i]
            with tab:
                st.info(f"Part {i+1} Prompt - This would be sent as a separate request.")
                st.caption(f"Estimated Tokens: {km.estimate_tokens(prompt) if km else 'N/A'}")
                st.code(prompt, language="text")
    
    pass
