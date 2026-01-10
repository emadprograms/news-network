import libsql_client
from datetime import datetime
from dateutil import parser as dt_parser
import modules.market_utils as market_utils

class NewsDatabase:
    def __init__(self, db_url, db_token, init_schema=True):
        # Force HTTPS instead of WSS/LibSQL for stability
        self.url = db_url.replace("wss://", "https://").replace("libsql://", "https://")
        self.token = db_token
        try:
            self.client = libsql_client.create_client_sync(url=self.url, auth_token=db_token)
            if init_schema:
                self._initialize_db()
        except Exception as e:
            print(f"❌ DB Connect Error: {e}")
            self.client = None

    def _initialize_db(self):
        """ Creates tables if they don't exist. Handles Schema Updates. """
        if not self.client: return

        # 1. Check if we need to migrate/rebuild (Add Publisher Column)
        try:
            # Check if table exists and has publisher column
            check_sql = "PRAGMA table_info(market_news)"
            columns = self.client.execute(check_sql).rows
            col_names = [c[1] for c in columns]
            
            if columns and "publisher" not in col_names:
                print("⚠️ Schema Mismatch: Dropping old table to add 'publisher' column...")
                self.client.execute("DROP TABLE IF EXISTS market_news")
        except Exception as e:
            print(f"⚠️ Schema Check Error: {e}")

        # Check if 'country' column exists (Migration)
        try:
            self.client.execute("SELECT country FROM market_calendar LIMIT 1")
        except:
            print("📦 Migrating Schema: Adding 'country' to market_calendar...")
            try:
                self.client.execute("ALTER TABLE market_calendar ADD COLUMN country TEXT")
            except Exception as e:
                print(f"⚠️ Schema Migration (Country) Error: {e}")

        # Check if 'event_time' column exists (Migration)
        try:
            self.client.execute("SELECT event_time FROM market_calendar LIMIT 1")
        except:
            print("📦 Migrating Schema: Adding 'event_time' to market_calendar...")
            try:
                self.client.execute("ALTER TABLE market_calendar ADD COLUMN event_time TEXT")
            except Exception as e:
                print(f"⚠️ Schema Migration (Time) Error: {e}")

        # 2. Create Table (New Schema)
        sql_create = """
        CREATE TABLE IF NOT EXISTS market_news (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            published_at TEXT,
            title TEXT,
            url TEXT UNIQUE,
            source_domain TEXT,
            publisher TEXT,
            category TEXT,
            content TEXT,
            eps_estimate TEXT,
            eps_reported TEXT,
            eps_surprise TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );
        """
        try:
            self.client.execute(sql_create)
            # Index on Category/Date for speed
            self.client.execute("CREATE INDEX IF NOT EXISTS idx_cat_date ON market_news(category, published_at);")
        except Exception as e:
            print(f"❌ Schema Init Error: {e}")

        # 3. Create Calendar Table
        sql_create_cal = """
        CREATE TABLE IF NOT EXISTS market_calendar (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_name TEXT,
            ticker TEXT,
            event_type TEXT,
            event_date TEXT, -- Stored as ISO/YYYY-MM-DD
            importance TEXT,
            status TEXT DEFAULT 'UPCOMING',
            country TEXT,
            event_time TEXT,
            eps_estimate TEXT,
            eps_reported TEXT,
            eps_surprise TEXT
        );
        """
        try:
             self.client.execute(sql_create_cal)
             self.client.execute("CREATE INDEX IF NOT EXISTS idx_cal_date ON market_calendar(event_date);")
        except Exception as e:
            print(f"❌ Calendar Init Error: {e}")

    def fetch_monitored_tickers(self):
        """
        Fetches the list of tickers from the 'stocks' table (Analyst DB).
        Expected table schema: stocks(ticker, ...)
        """
        if not self.client: return []
        
        sql = "SELECT ticker FROM stocks"
        try:
            rs = self.client.execute(sql)
            # Flatten list of tuples: [('AAPL',), ('TSLA',)] -> ['AAPL', 'TSLA']
            tickers = [row[0] for row in rs.rows if row[0]]
            return sorted(tickers)
        except Exception as e:
            print(f"⚠️ Fetch Tickers Error: {e}")
            return []

    def insert_news(self, news_list, category):
        """
        Inserts a list of news dictionaries into the DB.
        Returns (inserted_count, duplicate_count)
        """
        if not self.client or not news_list:
            return 0, 0
        
        inserted = 0
        duplicates = 0
        
        sql = """
        INSERT OR IGNORE INTO market_news 
        (published_at, title, url, source_domain, publisher, category, content) 
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """
        
        for item in news_list:
            try:
                # Prepare values
                pub_at = item.get('published_at')
                title = item.get('title')
                url = item.get('url')
                domain = item.get('source_domain', 'Unknown')
                publisher = item.get('publisher', 'Unknown')
                # PREFER ITEM CATEGORY, FALLBACK TO ARG
                item_cat = item.get('category', category)
                content_list = item.get('content', [])
                
                # Convert list content to string if needed
                if isinstance(content_list, list):
                    content_str = "\n".join(content_list)
                else:
                    content_str = str(content_list)

                # Execute
                rs = self.client.execute(sql, [pub_at, title, url, domain, publisher, item_cat, content_str])
                
                # Check if inserted (rows_affected)
                # Libsql might return rowcount differently, but OR IGNORE usually means 0 rows if dupe
                if rs.rows_affected > 0:
                    inserted += 1
                else:
                    duplicates += 1
                    
            except Exception as e:
                print(f"⚠️ Insert Error for {item.get('title')}: {e}")
        
        # 💾 EXPLICIT COMMIT (Crucial for Persistence)
        try:
            self.client.commit()
        except:
            pass # Sync client might auto-commit or not have method, but harmless to try
            
        return inserted, duplicates

    def fetch_news_by_date(self, date_obj, category=None):
        """
        Retrieves news from DB for a specific date.
        If category is None, returns ALL categories.
        """
        if not self.client: return []
        
        target_date_str = date_obj.strftime("%Y-%m-%d")
        
        if category:
            sql = """
            SELECT title, url, content, published_at, source_domain, category, publisher
            FROM market_news 
            WHERE category = ? 
            AND date(published_at) = ?
            AND category != 'HIDDEN'
            AND publisher != 'BLOCKED'
            ORDER BY published_at DESC
            """
            params = [category, target_date_str]
        else:
            sql = """
            SELECT title, url, content, published_at, source_domain, category, publisher
            FROM market_news 
            WHERE date(published_at) = ?
            AND category != 'HIDDEN'
            AND publisher != 'BLOCKED'
            ORDER BY published_at DESC
            """
            params = [target_date_str]
        
        try:
            rs = self.client.execute(sql, params)
            results = []
            for row in rs.rows:
                # Reconstruct dict to match engine output
                # row structure: (title, url, content, published_at, source_domain, cat, publisher)
                # content is stored as string in DB, engine returns list. Splitting by newline for compatibility.
                content_str = row[2]
                content_list = content_str.split("\n") if content_str else []
                
                # Format time string for UI (HH:MM style)
                try:
                    # Robust parsing using dateutil
                    dt = dt_parser.parse(row[3])
                    
                    # Convert to Bahrain Time (UTC+3)
                    bahrain_tz = datetime.timezone(datetime.timedelta(hours=3))
                    dt_local = dt.astimezone(bahrain_tz)
                    
                    time_str = dt_local.strftime("%H:%M %Z%z").strip()
                except:
                    time_str = "??:??"

                # Get publisher (index 6)
                publisher_val = row[6] if len(row) > 6 and row[6] else "Unknown"

                results.append({
                    "title": row[0],
                    "url": row[1],
                    "content": content_list,
                    "time": time_str,
                    "published_at": row[3],
                    "source_domain": row[4],
                    "category": row[5],
                    "publisher": publisher_val
                })
            return results
        except Exception as e:
            print(f"⚠️ Fetch Error: {e}")
            return []

    def fetch_cache_map(self, date_obj, category=None):
        """ Returns a dict of {url: item} for the given date. """
        results = self.fetch_news_by_date(date_obj, category)
        cache_map = {}
        for item in results:
            url = item.get('url')
            if url:
                cache_map[url] = item
        return cache_map

    def fetch_existing_titles(self, date_obj):
        """ Returns a DICT of {normalized_title: id} for the given date for fast deduplication and auditing. """
        if not self.client: return {}
        date_iso = date_obj.strftime("%Y-%m-%d")
        try:

            # We fetch ALL and filter in Python to handle mixed date formats (ISO vs Raw RSS)
            sql = "SELECT id, title, published_at FROM market_news"
            rs = self.client.execute(sql)
            titles_map = {}
            target_iso = date_obj.strftime("%Y-%m-%d")
            
            for row in rs.rows:
                row_id = row[0]
                t = row[1]
                pub_at = row[2]
                
                # DEBUG: Trace Raw Rows
                if "Trump" in t or "trump" in t:
                     print(f"🕵️ RAW DB ROW: '{t[:30]}...' | Date: {pub_at} | Target: {target_iso}")
                
                # Check Date Match (Robust)
                try:
                    # Try ISO string match First (Fast)
                    if pub_at.startswith(target_iso):
                        match = True
                    else:
                        # Fallback Parse
                        dt = dt_parser.parse(pub_at)
                        if dt.date() == date_obj:
                            match = True
                        else:
                            match = False
                except:
                    match = False
                
                if match:
                    # improved normalization
                    norm_t = market_utils.normalize_title(t).lower()
                    titles_map[norm_t] = row_id
                    
                    # DEBUG: Trace DB Loading
                    if "trump" in norm_t:
                        print(f"📂 DB LOAD: '{norm_t}' (ID: {row_id}, DateMatch: {match})")
                
            print(f"🔎 DEBUG: DB has {len(titles_map)} existing (normalized) titles for {target_iso}")
            return titles_map
        except Exception as e:
            print(f"⚠️ Fetch Existing Titles Error: {e}")
            return {}

    def fetch_recent_news(self, limit=50):
        """
        Retrieves the latest news across ALL categories.
        """
        if not self.client: return []
        
        sql = """
        SELECT title, url, content, published_at, source_domain, category, publisher
        FROM market_news 
        ORDER BY published_at DESC
        LIMIT ?
        """
        
        try:
            rs = self.client.execute(sql, [limit])
            results = []
            for row in rs.rows:
                content_str = row[2]
                content_list = content_str.split("\n") if content_str else []
                
                try:
                    # Robust parsing
                    dt = dt_parser.parse(row[3])
                    
                    # Convert to Bahrain Time (UTC+3)
                    bahrain_tz = datetime.timezone(datetime.timedelta(hours=3))
                    dt_local = dt.astimezone(bahrain_tz)
                    
                    time_str = dt_local.strftime("%H:%M %d-%b")
                except:
                    time_str = "Unknown"

                # row indices: 0=title, 1=url, 2=content, 3=pub, 4=src, 5=cat, 6=publisher
                pub_name = row[6] if len(row) > 6 else "Unknown"

                results.append({
                    "title": row[0],
                    "url": row[1],
                    "content": content_list,
                    "time": time_str,
                    "published_at": row[3],
                    "source_domain": row[4],
                    "category": row[5],
                    "publisher": pub_name
                })
            return results
        except Exception as e:
            print(f"⚠️ Fetch Recent Error: {e}")
            return []

    def fetch_news_range(self, start_iso, end_iso):
        """
        Retrieves news strictly between start_iso and end_iso.
        """
        if not self.client: return []
        
        sql = """
        SELECT title, url, content, published_at, source_domain, category, publisher
        FROM market_news 
        WHERE published_at >= ? AND published_at <= ?
        ORDER BY published_at DESC
        """
        
        try:
            rs = self.client.execute(sql, [start_iso, end_iso])
            results = []
            for row in rs.rows:
                content_str = row[2]
                content_list = content_str.split("\n") if content_str else []
                
                try:
                    # Robust Parsing
                    dt = dt_parser.parse(row[3])
                    
                    # Convert to Bahrain Time (UTC+3)
                    bahrain_tz = datetime.timezone(datetime.timedelta(hours=3))
                    dt_local = dt.astimezone(bahrain_tz)
                    
                    time_str = dt_local.strftime("%H:%M %d-%b")
                except:
                    time_str = "Unknown"

                pub_name = row[6] if len(row) > 6 else "Unknown"

                results.append({
                    "title": row[0],
                    "url": row[1],
                    "content": content_list,
                    "time": time_str,
                    "published_at": row[3],
                    "source_domain": row[4],
                    "category": row[5],
                    "publisher": pub_name
                })
            return results
        except Exception as e:
            print(f"⚠️ Fetch Range Error: {e}")
            return []

    # --- CALENDAR METHODS ---
    def clear_calendar(self):
        """ Clears all upcoming events to allow a fresh sync. """
        if not self.client: return
        try:
            self.client.execute("DELETE FROM market_calendar")
        except Exception as e:
            print(f"⚠️ Clear Calendar Error: {e}")

    def insert_calendar_events(self, events_list):
        """
        Inserts a list of event dicts:
        { "name": "CPI", "ticker": None, "type": "MACRO", "date": "...", "importance": "HIGH" }
        """
        if not self.client or not events_list: return 0
        
        sql = "INSERT INTO market_calendar (event_name, ticker, event_type, event_date, importance, country, event_time, eps_estimate, eps_reported, eps_surprise) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        
        stmts = []
        for ev in events_list:
            try:
                params = [
                    ev['name'], 
                    ev.get('ticker'), 
                    ev['type'], 
                    ev['date'], 
                    ev.get('importance', 'MEDIUM'), 
                    ev.get('country', 'US'),
                    ev.get('time', 'TBA'),
                    ev.get('eps_estimate', '-'),
                    ev.get('eps_reported', '-'),
                    ev.get('eps_surprise', '-')
                ]
                stmts.append(libsql_client.Statement(sql, params))
            except Exception as e:
                print(f"⚠️ Prepare Event Error: {e}")

        if not stmts: return 0

        try:
            # 🚀 Batch Execute (Single Transaction)
            rs = self.client.batch(stmts)
            return len(stmts)
        except Exception as e:
            print(f"⚠️ Batch Insert Error: {e}")
            return 0

    def get_upcoming_events(self, start_date_iso, end_date_iso):
        """ Fetches events between two dates (inclusive) """
        if not self.client: return []
        sql = "SELECT event_name, ticker, event_type, event_date, importance, country, event_time, eps_estimate, eps_reported, eps_surprise FROM market_calendar WHERE event_date >= ? AND event_date <= ? ORDER BY event_date ASC"
        try:
            rs = self.client.execute(sql, [start_date_iso, end_date_iso])
            events = []
            for row in rs.rows:
                events.append({
                    "name": row[0],
                    "ticker": row[1],
                    "type": row[2],
                    "date": row[3],
                    "importance": row[4],
                    "country": row[5] if len(row) > 5 else "US",
                    "time": row[6] if len(row) > 6 else "TBA",
                    "eps_estimate": row[7] if len(row) > 7 else "-",
                    "eps_reported": row[8] if len(row) > 8 else "-",
                    "eps_surprise": row[9] if len(row) > 9 else "-"
                })
            return events
        except Exception as e:
            print(f"⚠️ Get Events Error: {e}")
            return []

    def article_exists(self, url, title=None):
        """
        Checks if an article exists by URL (PRIMARY) or Title (FALLBACK).
        Ignores date - checks entire history.
        """
        if not self.client: return False
        
        try:
            # Check URL first (Fast, Indexed)
            sql = "SELECT id FROM market_news WHERE url = ?"
            rs = self.client.execute(sql, [url])
            if rs.rows: 
                print(f"    ✅ MATCH FOUND by URL: {url}")
                return rs.rows[0][0]
            
            # Check Title (slower, but catches URL variations)
            if title:
                # Use simplified normalization check if possible, but exact match for now
                sql_t = "SELECT id FROM market_news WHERE title = ?"
                rs_t = self.client.execute(sql_t, [title])
                if rs_t.rows: 
                    print(f"    ✅ MATCH FOUND by Title: {title}")
                    return rs_t.rows[0][0]
            
            print(f"    ❌ NO MATCH for: {url} OR {title}")
            return False
        except Exception as e:
            print(f"⚠️ Existence Check Error: {e}")
            return False

    def get_last_update_time(self):
        """ Returns the timestamp of the most recently added news item. """
        try:
            rs = self.client.execute("SELECT MAX(created_at) FROM market_news")
            if rs.rows and rs.rows[0][0]:
                return rs.rows[0][0]
            return None
        except Exception as e:
            print(f"⚠️ Failed to fetch last update time: {e}")
            return None
