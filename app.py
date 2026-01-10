import streamlit as st
import time
import datetime
from modules.db_client import NewsDatabase

# --- CONFIG ---
st.set_page_config(page_title="Global News Network", page_icon="🌐", layout="wide")

# Custom CSS for "News Network" Feel
st.markdown("""
<style>
    .big-headline {
        font-size: 2.5em;
        font-weight: 800;
        color: #ff4b4b;
        margin-bottom: 0px;
    }
    .sub-headline {
        font-size: 1.2em;
        color: #fafafa;
        margin-bottom: 20px;
    }
    .ticker-wrap {
        width: 100%;
        overflow: hidden;
        background-color: #0e1117;
        color: #00ff41;
        font-family: 'Courier New', Courier, monospace;
        padding: 10px 0;
        border-top: 2px solid #00ff41;
        border-bottom: 2px solid #00ff41;
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
    .news-card {
        background-color: #262730;
        padding: 15px;
        border-radius: 5px;
        margin-bottom: 10px;
        border-left: 5px solid #ff4b4b;
    }
    .stock-card {
        border-left: 5px solid #00ff41 !important;
    }
    .meta-tag {
        font-size: 0.8em;
        color: #aaa;
    }
</style>
""", unsafe_allow_html=True)

# --- INIT DB ---
db = None

# Prioritize the specific News DB, fallback to generic
if "turso_news" in st.secrets:
    try:
        db_url = st.secrets["turso_news"]["db_url"].replace("libsql://", "https://")
        db_token = st.secrets["turso_news"]["auth_token"]
        db = NewsDatabase(db_url, db_token)
    except:
        st.error("News Database Connection Failed")
elif "turso" in st.secrets:
    try:
        db_url = st.secrets["turso"]["db_url"].replace("libsql://", "https://")
        db_token = st.secrets["turso"]["auth_token"]
        db = NewsDatabase(db_url, db_token)
    except:
        st.error("Legacy Database Connection Failed")

# --- HEADER ---
st.title("🌐 GRANDMASTER NEWS NETWORK")
st.caption("LIVE INTELLIGENCE FEED")

# --- HELPERS ---
import re

def clean_content(content_list):
    """
    Cleans raw text validation issues, stripping HTML tags and fixing spacing.
    Returns a list of clean paragraphs.
    """
    if not content_list: return []
    
    # 1. Join to handle fragmented spans
    full_text = " ".join(content_list)
    
    # 2. Strip HTML Tags (e.g. <span>, <div>)
    clean_text = re.sub(r'<[^>]+>', '', full_text)
    
    # 3. Fix "StockStory" clutter
    
    # 4. Split back into paragraphs based on sentence endings or existing newlines
    paragraphs = []
    
    # If the text was originally just one big block
    if len(content_list) <= 1:
        # Split by semantic boundaries for readability
        parts = clean_text.split(". ")
        current_p = ""
        for p in parts:
            current_p += p + ". "
            if len(current_p) > 200: # chunk size
                paragraphs.append(current_p.strip())
                current_p = ""
        if current_p: paragraphs.append(current_p.strip())
    else:
        # It was already a list, just cleaned the HTML from items
        # Re-split by newline in case 
        paragraphs = [re.sub(r'<[^>]+>', '', c).strip() for c in content_list if c.strip()]

    return paragraphs


# --- SIDEBAR & FILTER ---
# Place Sidebar UI setup first, but we need date/time inputs to do the fetch.
# Strategy: Render Time inputs first -> Fetch Data -> Render Export & Rest of Sidebar

with st.sidebar:
    st.header("🕰️ Time Machine")
    use_time_travel = st.checkbox("Enable Time Filter", value=False)
    
    if use_time_travel:
        col_start_d, col_start_t = st.columns(2)
        with col_start_d:
            start_date = st.date_input("From Date", value=None)
        with col_start_t:
            start_time = st.time_input("From Time", value=datetime.time(0, 0))
            
        col_end_d, col_end_t = st.columns(2)
        with col_end_d:
            end_date = st.date_input("To Date", value=datetime.date.today())
        with col_end_t:
            end_time = st.time_input("To Time", value=datetime.time(23, 59))
        
        st.info("Displaying news strictly within this window.")

# --- FETCH DATA ---
# (Executed BEFORE Export section logic)
if db:
    if use_time_travel and start_date and end_date:
        # Construct ISO Strings
        dt_start = datetime.datetime.combine(start_date, start_time)
        dt_end = datetime.datetime.combine(end_date, end_time)
        iso_start = dt_start.isoformat()
        iso_end = dt_end.isoformat()
        
        news_items = db.fetch_news_range(iso_start, iso_end)
        st.caption(f"Found {len(news_items)} reports between {iso_start} and {iso_end}")
    else:
        # Default: Fetch EVERYTHING from Today (or last active day)
        # This ensures export includes Macro + Stocks + Company, not just last 50 items.
        today = datetime.date.today()
        news_items = db.fetch_news_by_date(today)
        st.caption(f"Showing all events for Today ({today})")

else:
    news_items = []

if not news_items:
    st.info("No news found for today. (Try running the Hunter in 'app' or check the Time Machine)")
    st.stop()

# --- EXPORT SECTION ---
with st.sidebar:


    st.divider()
    
    # --- SOURCE FILTER ---
    st.header("🔍 Source Filter")
    if 'news_items' in locals() and news_items:
        # distinct publishers
        all_pubs = sorted(list(set([n.get('publisher', 'Unknown') for n in news_items])))
        
        # Default: Select All
        selected_pubs = st.multiselect(
            "Select Publishers",
            options=all_pubs,
            default=all_pubs
        )
        
        # Apply Filter
        news_items = [n for n in news_items if n.get('publisher', 'Unknown') in selected_pubs]
        st.caption(f"Showing {len(news_items)} articles from {len(selected_pubs)} publishers.")
    else:
        st.info("Load data to see filters.")

    st.divider()
    
    # --- EXPORT SETTINGS ---
    st.header("📋 Export Options")
    
    # Define Category Groups
    macro_cats = ['MACRO', 'FED', 'INDICATORS', 'TREASURY', 'ECONOMY_GROWTH', 'ENERGY', 'COMMODITIES', 'GEO_POLITICS', 'FX', 'ECONOMY', 'GEO', 'MARKETS', 'GLOBAL']
    stock_cats = ['STOCKS', 'EQUITIES', 'EARNINGS', 'IPO', 'ANALYST_RATINGS', 'MERGERS', 'MERGERS_ACQUISITIONS', 'DIVIDENDS', 'BUYBACKS', 'INSIDER_MOVES', 'GUIDANCE', 'CONTRACTS', 'FDA', 'LEGAL', 'MANAGEMENT', 'SECTOR_NEWS']
    # Company is anything else
    
    export_options = ["MACRO", "STOCKS", "COMPANY"]
    selected_export_cats = st.multiselect(
        "Select Categories to Export",
        options=export_options,
        default=export_options
    )
    
    with st.expander("📥 Generate Report", expanded=True):
        if 'news_items' in locals() and news_items:
            # Filter Items based on Selection
            final_export_items = []
            
            for item in news_items:
                cat = item.get('category', 'GENERAL')
                group = "COMPANY" # Default
                
                if cat in macro_cats: group = "MACRO"
                elif cat in stock_cats: group = "STOCKS"
                
                if group in selected_export_cats:
                    final_export_items.append(item)
            
            if not final_export_items:
                st.warning("No items match your selected categories.")
            else:
                # Generate Text Blob
                export_lines = []
                export_lines.append(f"# Market Intelligence Report ({len(final_export_items)} items)")
                export_lines.append(f"Categories: {', '.join(selected_export_cats)}")
                if use_time_travel:
                    export_lines.append(f"Time Window: {start_time} to {end_time}")
                export_lines.append("---")
                
                for item in final_export_items:
                    cat = item.get('category', 'GENERAL')
                    time_str = item.get('time', 'N/A')
                    title = item.get('title', 'No Title')
                    pub = item.get('publisher', 'Unknown')
                    # Full content for AI context
                    full_body = "\n".join(clean_content(item.get('content', [])))
                    
                    export_lines.append(f"[{time_str}] ({cat}) {title} [{pub}]")
                    export_lines.append(f"{full_body}\n")
                
                final_text = "\n".join(export_lines)
                
                # Button for Download (Better for large files)
                st.download_button(
                    label=f"📥 Download Report ({len(final_export_items)} items)",
                    data=final_text,
                    file_name=f"market_intel_{'_'.join(selected_export_cats)}_{datetime.date.today()}.txt",
                    mime="text/plain"
                )
                
                # Preview / Copy Block
                st.caption("👇 Click the Copy icon in the top-right of this block:")
                st.code(final_text, language="markdown")
        else:
            st.warning("No news loaded to export.")


# --- TICKER ---
# Create a string for the marquee
headlines = [f"💥 {n['title'].upper()}" for n in news_items[:10]]
ticker_text = "   +++   ".join(headlines)

st.markdown(f"""
<div class="ticker-wrap">
<div class="ticker">{ticker_text}</div>
</div>
""", unsafe_allow_html=True)

st.markdown("##") # Spacer

# --- HELPERS ---

# Tabs for clearer separation
# Tabs for clearer separation
tab1, tab2, tab3 = st.tabs(["🌍 GLOBAL HEADLINES", "📈 STOCKS NEWS", "🏢 COMPANY NEWS"])

with tab1:
    # Filter for MACRO or Main news
    # New Categories: FED, INDICATORS, TREASURY, ECONOMY_GROWTH, ENERGY, GEO_POLITICS, FX
    macro_cats = ['MACRO', 'FED', 'INDICATORS', 'TREASURY', 'ECONOMY_GROWTH', 'ENERGY', 'COMMODITIES', 'GEO_POLITICS', 'FX', 'ECONOMY', 'GEO', 'MARKETS', 'GLOBAL']
    macro_news = [n for n in news_items if n.get('category') in macro_cats]
    
    if not macro_news:
         st.markdown("*No Macro reports available.*")
    
    for item in macro_news:
        clean_paragraphs = clean_content(item['content'])
        preview = clean_paragraphs[0] if clean_paragraphs else 'Updates coming in...'
        
        # Add visual tag for granular category
        cat_tag = item.get('category', 'MACRO')
        pub = item.get('publisher', 'Unknown')
        
        with st.container():
            st.markdown(f"""
            <div class="news-card">
                <h4>{item['title']} <span style="font-size:0.6em; background-color:#444; padding:2px 5px; border-radius:3px;">{cat_tag}</span></h4>
                <p class="meta-tag">🕒 {item.get('time', 'N/A')} | 🏢 <b>{pub}</b> | 📡 {item['source_domain']}</p>
                <p>{preview}</p>
            </div>
            """, unsafe_allow_html=True)
            with st.expander("Reading Full Report"):
                for p in clean_paragraphs:
                    st.write(p)
                st.caption(f"[Source Link]({item['url']})")

with tab2:
    # Filter for STOCKS
    # New Categories: EARNINGS, ANALYST_RATINGS, MERGERS_ACQUISITIONS, IPO, INSIDER_MOVES, SECTOR_NEWS, EQUITIES
    stock_cats = ['STOCKS', 'TEST', 'EARNINGS', 'ANALYST_RATINGS', 'MERGERS', 'MERGERS_ACQUISITIONS', 'IPO', 'INSIDER_MOVES', 'SECTOR_NEWS', 'EQUITIES', 'DIVIDENDS', 'BUYBACKS', 'GUIDANCE', 'CONTRACTS', 'FDA', 'LEGAL', 'MANAGEMENT']
    stock_news = [n for n in news_items if n.get('category') in stock_cats]
    
    if not stock_news:
        st.markdown("*No Stock reports available.*")

    for item in stock_news:
        clean_paragraphs = clean_content(item['content'])
        preview = clean_paragraphs[0] if clean_paragraphs else 'Updates coming in...'
        
        cat_tag = item.get('category', 'STOCKS')
        pub = item.get('publisher', 'Unknown')

        # Use the SAME Card Style, but with green accent (stock-card class)
        with st.container():
            st.markdown(f"""
            <div class="news-card stock-card">
                 <h4>{item['title']} <span style="font-size:0.6em; background-color:#444; padding:2px 5px; border-radius:3px;">{cat_tag}</span></h4>
                <p class="meta-tag">🕒 {item.get('time', 'N/A')} | 🏢 <b>{pub}</b> | 📡 {item['source_domain']}</p>
                <p>{preview}</p>
            </div>
            """, unsafe_allow_html=True)
            
        with st.expander("Details"):
             if clean_paragraphs:
                 for p in clean_paragraphs:
                     st.write(p)
             else:
                 st.caption("No content available.")
        st.divider()

with tab3:
    # Filter for COMPANY SPECIFIC (Anything not in the previous cats)
    # We explicitly exclude the known macro/stock categories to find the "tickers"
    # Actually, simpler logic: verify if category is likely a Ticker (all caps, short)
    # OR just subtract the sets.
    
    known_cats = set(macro_cats + stock_cats)
    company_news = [n for n in news_items if n.get('category') not in known_cats]
    
    if not company_news:
        st.markdown("*No Company-Specific reports available.*")
        
    for item in company_news:
        clean_paragraphs = clean_content(item['content'])
        preview = clean_paragraphs[0] if clean_paragraphs else 'Updates coming in...'
        
        cat_tag = item.get('category', 'COMPANY')
        pub = item.get('publisher', 'Unknown')
        
        # Blue accent for Companies
        with st.container():
            st.markdown(f"""
            <div class="news-card" style="border-left: 5px solid #1f77b4 !important;">
                 <h4>{item['title']} <span style="font-size:0.6em; background-color:#1f77b4; padding:2px 5px; border-radius:3px;">{cat_tag}</span></h4>
                <p class="meta-tag">🕒 {item.get('time', 'N/A')} | 🏢 <b>{pub}</b> | 📡 {item['source_domain']}</p>
                <p>{preview}</p>
            </div>
            """, unsafe_allow_html=True)
            
        with st.expander("Reading Full Report"):
             if clean_paragraphs:
                 for p in clean_paragraphs:
                     st.write(p)
             else:
                 st.caption("No content available.")
        st.divider()

# --- REFRESH BUTTON ---
if st.button("🔄 REFRESH FEED"):
    st.rerun()
