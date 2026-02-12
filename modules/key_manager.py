from __future__ import annotations
import time
from collections import deque
import logging
import random
import hashlib
import libsql_client
import traceback
import requests
import json

log = logging.getLogger(__name__)

# --- TABLE 1: KEYS ---
CREATE_KEYS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS gemini_api_keys (
    key_name TEXT PRIMARY KEY NOT NULL,
    key_value TEXT NOT NULL,
    priority INTEGER DEFAULT 10,
    tier TEXT DEFAULT 'free', 
    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

# --- TABLE 2: STATUS (V6 - RPM) ---
CREATE_STATUS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS gemini_key_status (
    key_hash TEXT PRIMARY KEY NOT NULL,
    
    -- RPM / TPM TRACKING
    rpm_requests INTEGER NOT NULL DEFAULT 0,
    rpm_window_start REAL NOT NULL DEFAULT 0,
    tpm_tokens INTEGER NOT NULL DEFAULT 0,

    -- HEALTH TRACKING
    strikes INTEGER NOT NULL DEFAULT 0,
    release_time REAL NOT NULL DEFAULT 0
);
"""

# --- TABLE 3: MODEL USAGE (V8 - ISOLATED BUCKETS) ---
# Added this based on KeyManager code references
CREATE_MODEL_USAGE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS gemini_model_usage (
    key_hash TEXT NOT NULL,
    model_id TEXT NOT NULL,
    
    rpm_requests INTEGER DEFAULT 0,
    rpm_window_start REAL DEFAULT 0,
    tpm_tokens INTEGER DEFAULT 0,
    
    rpd_requests INTEGER DEFAULT 0,
    last_used_day TEXT DEFAULT '',
    
    strikes INTEGER DEFAULT 0,
    
    PRIMARY KEY (key_hash, model_id)
);
"""

class KeyManager:
    """
    GEMINI KEY MANAGER V8 - MODEL INDEPENDENCE & TOKEN GUARD
    
    ARCHITECTURE OVERVIEW:
    1. Model Isolation: Usage for 'gemini-3-pro' does not affect 'gemini-3-flash' limits, 
       even on the same physical API key. This uses the 'gemini_model_usage' table.
    2. Strict Tiering: Keys are marked as 'paid' or 'free'. Paid model configurations 
       will ONLY use 'paid' keys. Free configs will ONLY use 'free' keys.
    3. Token Guard: Pre-calculates estimated request size. Rejects requests that 
       exceed absolute model capacity (-1.0 wait) or asks to wait if usage > minute limit.
    4. LibSQL Bug Workaround: Uses Raw HTTP Pipeline (requests) for all DB writes 
       to bypass client parsing issues.
    """
    
    # --- CONFIGURATION (V8: SPLIT CONFIGS) ---
    TIER_FREE = 'free'
    TIER_PAID = 'paid'

    # MODELS_CONFIG: Maps 'Display Key' (used in app) -> {internal_model_id, tier, limits}
    # IMPORTANT: The same physical model (e.g. Gemini 3 Pro) exists in two tiers:
    # - 'paid' tier: Higher RPM/TPM limits (requires paid API keys)
    # - 'free' tier: Default Gemini limits (requires free API keys)
    MODELS_CONFIG = {
        # --- PAID TIER (Higher Limits) ---
        'gemini-3-pro-paid': {
            'model_id': 'gemini-3-pro-preview',
            'tier': 'paid',
            'display': 'Gemini 3 Pro (Paid)',
            'limits': {'rpm': 25, 'tpm': 1000000, 'rpd': 250} # RPD ADDED
        },
        'gemini-3-flash-paid': {
            'model_id': 'gemini-3-flash-preview', 
            'tier': 'paid',
            'display': 'Gemini 3 Flash (Paid)',
            'limits': {'rpm': 1000, 'tpm': 4000000, 'rpd': 10000}
        },
        'gemini-2.5-pro-paid': {
            'model_id': 'gemini-2.5-pro',
            'tier': 'paid',
            'display': 'Gemini 2.5 Pro (Paid)',
            'limits': {'rpm': 150, 'tpm': 2000000, 'rpd': 10000}
        },
        'gemini-2.5-flash-paid': {
            'model_id': 'gemini-2.5-flash',
            'tier': 'paid',
            'display': 'Gemini 2.5 Flash (Paid)',
            'limits': {'rpm': 1000, 'tpm': 4000000, 'rpd': 10000}
        },
        'gemini-2.5-flash-lite-paid': {
            'model_id': 'gemini-2.5-flash-lite',
            'tier': 'paid',
            'display': 'Gemini 2.5 Flash Lite (Paid)',
            'limits': {'rpm': 4000, 'tpm': 4000000, 'rpd': 1000000} # Unlimited effectively
        },
        'gemini-2.0-flash-paid': {
            'model_id': 'gemini-2.0-flash',
            'tier': 'paid',
            'display': 'Gemini 2.0 Flash (Paid)',
            'limits': {'rpm': 1000, 'tpm': 4000000, 'rpd': 10000} 
        },

        # --- FREE TIER (Standard Limits) ---
        'gemini-3-flash-free': {
             'model_id': 'gemini-3-flash-preview',
             'tier': 'free',
             'display': 'Gemini 3 Flash (Free)',
             'limits': {'rpm': 5, 'tpm': 250000, 'rpd': 10000}
        },
        'gemini-3-pro-free': {
             'model_id': 'gemini-3-pro-preview',
             'tier': 'free',
             'display': 'Gemini 3 Pro (Free)',
             'limits': {'rpm': 2, 'tpm': 32000, 'rpd': 50}
        },
        'gemini-2.5-flash-free': {
             'model_id': 'gemini-2.5-flash',
             'tier': 'free',
             'display': 'Gemini 2.5 Flash (Free)',
             'limits': {'rpm': 5, 'tpm': 250000, 'rpd': 10000} 
        },
        'gemini-2.5-pro-free': {
             'model_id': 'gemini-2.5-pro',
             'tier': 'free',
             'display': 'Gemini 2.5 Pro (Free)',
             'limits': {'rpm': 2, 'tpm': 32000, 'rpd': 50}
        },
        'gemini-2.5-flash-lite-free': {
             'model_id': 'gemini-2.5-flash-lite',
             'tier': 'free',
             'display': 'Gemini 2.5 Flash Lite (Free)',
             'limits': {'rpm': 10, 'tpm': 250000, 'rpd': 10000}
        },
        'gemini-2.0-flash-free': {
             'model_id': 'gemini-2.0-flash',
             'tier': 'free',
             'display': 'Gemini 2.0 Flash (Free)',
             'limits': {'rpm': 10, 'tpm': 1000000, 'rpd': 1500}
        },
        'gemini-2.0-flash-lite-free': {
             'model_id': 'gemini-2.0-flash-lite',
             'tier': 'free',
             'display': 'Gemini 2.0 Flash Lite (Free)',
             'limits': {'rpm': 15, 'tpm': 1000000, 'rpd': 1500}
        },
        
        # --- GEMMA FAMILY ---
        'gemma-3-27b': {
            'model_id': 'gemma-3-27b-it',
            'tier': 'free', 
            'display': 'Gemma 3 27B',
            'limits': {'rpm': 30, 'tpm': 15000, 'rpd': 10000}
        },
         'gemma-3-12b': {
            'model_id': 'gemma-3-12b-it',
            'tier': 'free',
            'display': 'Gemma 3 12B',
            'limits': {'rpm': 30, 'tpm': 15000, 'rpd': 10000}
        }
    }

    COOLDOWN_PERIODS = {1: 10, 2: 60, 3: 300, 4: 3600} 
    MAX_STRIKES = 5
    FATAL_STRIKE_COUNT = 999

    def __init__(self, db_url: str, auth_token: str):
        self.db_url = db_url.replace("libsql://", "https://") 
        self.auth_token = auth_token
        
        try:
            self.db_client = libsql_client.create_client_sync(url=self.db_url, auth_token=auth_token)
            self.db_client.execute(CREATE_KEYS_TABLE_SQL)
            self.db_client.execute(CREATE_STATUS_TABLE_SQL)
            self.db_client.execute(CREATE_MODEL_USAGE_TABLE_SQL) # V8 addition
            self._validate_schema_or_die()
        except Exception as e:
            log.critical(f"DB Connection failed: {e}")
            raise

        self.name_to_key = {}
        self.key_to_name = {}
        self.key_to_hash = {}
        self.key_metadata = {} 
        
        self.available_keys = deque()
        self.cooldown_keys = {}
        self.key_failure_strikes = {}
        self.dead_keys = set()
        
        self._refresh_keys_from_db()

    def _validate_schema_or_die(self):
        # V8 Validation: Check ONLY for model_usage table availability
        try:
            rs = self.db_client.execute("SELECT * FROM gemini_model_usage LIMIT 0")
        except Exception as e:
            if "no such table" in str(e):
                msg = f"CRITICAL: DB missing V8 table 'gemini_model_usage'. Run modules/apply_schema.py."
                log.critical(msg)
                raise Exception(msg)
            pass
            
    # ... (hash/crud omitted, same as before) ...
    def _hash_key(self, key: str) -> str:
        return hashlib.sha256(key.encode('utf-8')).hexdigest()
    
    def _row_to_dict(self, columns, row):
        return dict(zip(columns, row))
        
    def add_key(self, name: str, value: str, tier: str = 'free', display_order: int = 10):
        try:
            self.db_client.execute(
                "INSERT INTO gemini_api_keys (key_name, key_value, priority, tier) VALUES (?, ?, ?, ?)", 
                [name, value, display_order, tier]
            )
            self._refresh_keys_from_db()
            return True, "Key added."
        except Exception as e: return False, str(e)

    def update_key_tier(self, name: str, new_tier: str):
        try:
            self.db_client.execute("UPDATE gemini_api_keys SET tier = ? WHERE key_name = ?", [new_tier, name])
            self._refresh_keys_from_db()
            return True, "Updated Tier."
        except Exception as e: return False, str(e)

    def delete_key(self, name: str):
        try:
            self.db_client.execute("DELETE FROM gemini_api_keys WHERE key_name = ?", [name])
            self._refresh_keys_from_db()
            return True, "Deleted."
        except Exception as e: return False, str(e)

    def get_all_managed_keys(self):
        rs = self.db_client.execute("SELECT key_name, key_value, priority, tier, added_at FROM gemini_api_keys ORDER BY priority ASC, key_name ASC")
        if not rs.rows: return []
        return [self._row_to_dict(rs.columns, row) for row in rs.rows]

    def _refresh_keys_from_db(self):
        keys_rs = self.db_client.execute("SELECT key_name, key_value, tier FROM gemini_api_keys")
        self.name_to_key = {}
        self.key_metadata = {}
        
        if keys_rs.rows:
            for row in keys_rs.rows:
                d = self._row_to_dict(keys_rs.columns, row)
                self.name_to_key[d["key_name"]] = d["key_value"]
                self.key_metadata[d["key_value"]] = {'tier': d.get('tier', 'free')}

        self.key_to_name = {v: k for k, v in self.name_to_key.items()}
        all_real_keys = list(self.name_to_key.values())
        self.key_to_hash = {k: self._hash_key(k) for k in all_real_keys}

        self.available_keys = deque()
        self.cooldown_keys = {}
        self.key_failure_strikes = {}
        self.dead_keys = set()
        
        for key in all_real_keys:
            self.available_keys.append(key)
            self.key_failure_strikes[key] = 0

        random.shuffle(self.available_keys)

    @staticmethod
    def estimate_tokens(text: str) -> int:
        """
        Rough Token Estimation (1 token ~= 4 chars).
        Use this BEFORE calling get_key to ensure the request fits in the chosen bucket.
        """
        if not text: return 0
        return int(len(text) / 2.5) + 1

    def get_key(self, config_id: str, estimated_tokens: int = 0) -> tuple[str | None, str | None, float, str | None]:
        """
        V8: Retrieves an available key for the given config_id.
        
        ARGS:
            config_id: The key from MODELS_CONFIG (e.g., 'gemini-3-pro-paid').
            estimated_tokens: The rough size of the request.
            
        RETURNS:
            (key_name, key_value, wait_time, model_id)
            - wait_time == 0.0: Success! Use the key.
            - wait_time == -1.0: FATAL. Request exceeds absolute model capacity.
            - wait_time > 0.0: COMPACITY REACHED. Seconds to wait for next minute window.
            - model_id: The internal string Google expects (e.g. 'gemini-3-pro-preview').
        """
        self._reclaim_keys()
        
        config = self.MODELS_CONFIG.get(config_id)
        if not config:
            log.warning(f"Unknown config_id '{config_id}', using safe defaults.")
            rpm_limit = 5; tpm_limit = 32000; rpd_limit = 10000; required_tier = 'free'
            target_model_id = 'unknown'
        else:
            rpm_limit = config['limits']['rpm']
            tpm_limit = config['limits']['tpm']
            rpd_limit = config['limits'].get('rpd', 10000)
            required_tier = config.get('tier') 
            target_model_id = config.get('model_id')
            if required_tier not in ['paid', 'free']: required_tier = 'free' 

        # NEW: FATAL CHECK (Request > Model Limit)
        if estimated_tokens > tpm_limit:
            # Special signal: -1.0 means "Impossible"
            return None, None, -1.0, target_model_id
        
        rotation = deque()
        best_wait = float('inf')
        
        checked_count = 0
        limit_checked_count = len(self.available_keys) 
        
        while self.available_keys and checked_count < limit_checked_count:
            key_val = self.available_keys.popleft()
            checked_count += 1
            
            # 1. STRICT TIER 
            key_tier = self.key_metadata[key_val].get('tier', 'free')
            if required_tier == 'paid' and key_tier != 'paid':
                rotation.append(key_val)
                continue
            if required_tier == 'free' and key_tier != 'free':
                rotation.append(key_val)
                continue
            
            # 2. HEALTH
            if key_val in self.dead_keys:
                rotation.append(key_val)
                continue

            # NEW: Respect the Cooldown Penalty Box
            if key_val in self.cooldown_keys:
                release_time = self.cooldown_keys[key_val]
                if time.time() < release_time:
                    # Key is still in penalty box
                    wait_for_this_key = release_time - time.time()
                    best_wait = min(best_wait, wait_for_this_key)
                    rotation.append(key_val)
                    continue
                else:
                    # Time served. Release it.
                    del self.cooldown_keys[key_val]
                
            # 3. LIMITS (V8: Model Specific)
            wait = self._check_key_limits(key_val, target_model_id, rpm_limit, tpm_limit, rpd_limit, estimated_tokens)
            
            if wait == 0:
                self.available_keys.extendleft(reversed(rotation)) 
                # Key is NOT added back to available_keys here. 
                # It will be added back by report_usage() or report_failure() after serve.
                return self.key_to_name[key_val], key_val, 0.0, target_model_id
            
            best_wait = min(best_wait, wait)
            rotation.append(key_val)
            
        self.available_keys.extend(rotation)
        if best_wait == float('inf'):
            # --- NEW: EMPTY POOL HANDLING ---
            # If we reach here, it means we scanned ALL available_keys and none were ready.
            # OR available_keys was empty to begin with (all keys in cooldown or checked out).
            
            # 1. Check if ANY keys exist for this tier
            matching_keys = [k for k, meta in self.key_metadata.items() if meta.get('tier') == required_tier and k not in self.dead_keys]
            if not matching_keys:
                 return None, None, 0.0, target_model_id # Truly no keys
            
            # 2. Check Cooldowns for the matching keys
            cooldown_waits = [t - time.time() for k, t in self.cooldown_keys.items() if k in matching_keys and time.time() < t]
            if cooldown_waits:
                return None, None, min(cooldown_waits), target_model_id
            
            # 3. Busy Wait (all keys currently checked out by other threads)
            return None, None, 5.0, target_model_id 

        return None, None, best_wait, target_model_id

    def _check_key_limits(self, key_val: str, model_id: str, rpm_limit: int, tpm_limit: int, rpd_limit: int, estimated_tokens: int = 0) -> float:
        """Returns seconds to wait. 0.0 if ready. Uses V8 model_usage table."""
        key_hash = self.key_to_hash.get(key_val)
        if not key_hash: return 0.0
        
        try:
            # V8: Query gemini_model_usage
            rs = self.db_client.execute(
                "SELECT rpm_requests, rpm_window_start, tpm_tokens, strikes, rpd_requests, last_used_day FROM gemini_model_usage WHERE key_hash = ? AND model_id = ?", 
                [key_hash, model_id]
            )
            if not rs.rows: return 0.0
            
            # row = self._row_to_dict(rs.columns, rs.rows[0])
            # if row['strikes'] >= self.MAX_STRIKES: return 86400.0
            
            row = self._row_to_dict(rs.columns, rs.rows[0])
            
            now = time.time()
            today_str = time.strftime('%Y-%m-%d', time.gmtime(now))
            
            # CHECK RPD
            last_day = row.get('last_used_day', '')
            daily_req = row.get('rpd_requests', 0)
            
            if last_day == today_str and daily_req >= rpd_limit:
                 return 3600.0 
            
            # CHECK RPM/TPM
            start = row['rpm_window_start']
            count_req = row['rpm_requests']
            count_tok = row['tpm_tokens']
            
            if now - start >= 60: return 0.0
            
            if count_req >= rpm_limit: return max(1.0, 60 - (now - start))
            
            # PRE_CHECK TOKEN CAPACITY (TPM)
            if (count_tok + estimated_tokens) > tpm_limit:
                # Token limit exceeded for this minute
                return max(1.0, 60 - (now - start))
                
            return 0.0
        except Exception:
            return 0.0

    def get_key_stats(self, key_value: str, model_id: str = None):
        """V8: Returns stats for a key. If model_id provided, returns specific model stats."""
        key_hash = self.key_to_hash.get(key_value)
        if not key_hash: return {}
        try:
             if model_id:
                 rs = self.db_client.execute("SELECT * FROM gemini_model_usage WHERE key_hash = ? AND model_id = ?", [key_hash, model_id])
                 return self._row_to_dict(rs.columns, rs.rows[0]) if rs.rows else {}
             else:
                 # Just return generic health from key_status (legacy) or summary
                 # For V8 migration, we focus on model usage.
                 return {}
        except: return {}

    def _reclaim_keys(self):
        current_time = time.time()
        released = [k for k, t in self.cooldown_keys.items() if current_time >= t]
        if not released: return
        for key in released:
            del self.cooldown_keys[key]
            self.available_keys.append(key)
    
    # --- RAW HTTP HELPER (Bypassing buggy client) ---
    def _raw_http_execute(self, sql: str, args: list):
        """Standard LibSQL HTTP Pipeline execution to avoid client parsing bugs."""
        url = f"{self.db_url}/v2/pipeline"
        headers = {"Authorization": f"Bearer {self.auth_token}", "Content-Type": "application/json"}
        
        encoded_args = []
        for a in args:
            if isinstance(a, int): encoded_args.append({"type": "integer", "value": str(a)})
            elif isinstance(a, float): encoded_args.append({"type": "float", "value": a})
            elif isinstance(a, str): encoded_args.append({"type": "text", "value": a})
            elif a is None: encoded_args.append({"type": "null"})
            else: encoded_args.append({"type": "text", "value": str(a)})
            
        payload = {"requests": [{"type": "execute", "stmt": {"sql": sql, "args": encoded_args}}]}
        
        try:
            resp = requests.post(url, json=payload, headers=headers, timeout=5)
            if resp.status_code != 200:
                msg = f"Raw DB Exec Failed: {resp.status_code} {resp.text}"
                log.error(msg)
                raise Exception(msg)
        except Exception as e:
            log.error(f"Raw DB Conn Failed: {e}")
            raise e

    def report_usage(self, key: str, tokens: int = 0, model_id: str = 'unknown'):
        """
        V8: Reports usage to isolated model buckets.
        
        IMPORTANT: model_id MUST be the internal ID (e.g., 'gemini-3-pro-preview'), 
        NOT the config_id. This ensures that usage is tracked correctly even 
        if multiple configs share the same internal model.
        """
        key_hash = self.key_to_hash[key]
        now = time.time()
        today_str = time.strftime('%Y-%m-%d', time.gmtime(now))
        
        try:
            # 1. Get current state from MODEL usage table
            rs = self.db_client.execute(
                "SELECT rpm_requests, rpm_window_start, tpm_tokens, rpd_requests, last_used_day FROM gemini_model_usage WHERE key_hash = ? AND model_id = ?", 
                [key_hash, model_id]
            )
            
            if rs.rows:
                # UPDATE
                row = self._row_to_dict(rs.columns, rs.rows[0])
                current_start = row['rpm_window_start']
                current_count = row['rpm_requests']
                current_tokens = row['tpm_tokens']
                
                # Update Minutes
                if now - current_start >= 60:
                    new_count = 1
                    new_start = now
                    new_tokens = tokens
                else:
                    new_count = current_count + 1
                    new_start = current_start
                    new_tokens = current_tokens + tokens
                
                # Update Days
                last_day = row.get('last_used_day', '')
                current_rpd = row.get('rpd_requests', 0)
                
                if last_day != today_str:
                    new_rpd = 1
                else:
                    new_rpd = current_rpd + 1
                
                # USE RAW REQUEST
                self._raw_http_execute(
                    """UPDATE gemini_model_usage SET 
                       rpm_requests = ?, rpm_window_start = ?, tpm_tokens = ?, 
                       rpd_requests = ?, last_used_day = ?,
                       strikes = 0 
                       WHERE key_hash = ? AND model_id = ?""",
                    [new_count, new_start, new_tokens, new_rpd, today_str, key_hash, model_id]
                )
            else:
                # INSERT
                self._raw_http_execute(
                    """INSERT INTO gemini_model_usage 
                       (key_hash, model_id, rpm_requests, rpm_window_start, tpm_tokens, 
                        rpd_requests, last_used_day, strikes) 
                       VALUES (?, ?, 1, ?, ?, 1, ?, 0)""",
                    [key_hash, model_id, now, tokens, today_str]
                )
                
            self.available_keys.append(key)
        except Exception as e:
            import traceback
            log.error(f"Report Usage Failed: {e}\n{traceback.format_exc()}")

    def report_failure(self, key: str, is_info_error=False):
        if is_info_error:
            self.available_keys.append(key)
            return
            
        # V8 FIX: No more strikes. Just a temporary cooldown/penalty.
        penalty = 60 
        self.cooldown_keys[key] = time.time() + penalty
        
        try:
            key_hash = self.key_to_hash[key]
            # We no longer track 'strikes' in the DB to avoid permanent bans
            self.db_client.execute(
                "UPDATE gemini_key_status SET release_time = ? WHERE key_hash = ?", 
                [time.time() + penalty, key_hash]
            )
        except: pass

    def report_fatal_error(self, key: str):
        self.dead_keys.add(key)
        try:
            self.db_client.execute("UPDATE gemini_key_status SET strikes = 999 WHERE key_hash = ?", [self.key_to_hash[key]])
        except: pass
