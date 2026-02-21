import datetime

def normalize_title(title):
    """
    Standardizes titles for deduplication.
    1. Trims whitespace.
    2. Removes common Source Suffixes (e.g. ' - Yahoo Finance').
    """
    if not title: return ""
    t = title.strip()
    # Suffixes to remove
    suffixes = [" - Yahoo Finance", " - Bloomberg", " - Reuters", " - CNBC", " - MarketWatch", " - The Wall Street Journal"]
    for s in suffixes:
        if t.endswith(s):
            t = t.rsplit(s, 1)[0].strip()
    return t

class MarketCalendar:
    """
    Utility for NYSE Trading Days, Market Sessions, and DST-aware switchovers.
    """
    # NYSE Full-Day Holidays for 2026
    HOLIDAYS_2026 = {
        datetime.date(2026, 1, 1),   # New Year's Day
        datetime.date(2026, 1, 19),  # MLK Jr. Day
        datetime.date(2026, 2, 16),  # Presidents Day
        datetime.date(2026, 4, 3),   # Good Friday
        datetime.date(2026, 5, 25),  # Memorial Day
        datetime.date(2026, 6, 19),  # Juneteenth
        datetime.date(2026, 7, 3),   # Independence Day (Observed)
        datetime.date(2026, 9, 7),   # Labor Day
        datetime.date(2026, 11, 26), # Thanksgiving
        datetime.date(2026, 12, 25), # Christmas
    }

    # NYSE Early Close Days (1 PM EST)
    EARLY_CLOSE_2026 = {
        datetime.date(2026, 7, 2),   # Day Before Independence Day
        datetime.date(2026, 11, 27), # Day After Thanksgiving
        datetime.date(2026, 12, 24), # Christmas Eve
    }

    # --- DST BOUNDARIES (US Eastern) ---
    # 2026: DST starts Mar 8, ends Nov 1
    DST_START_2026 = datetime.date(2026, 3, 8)
    DST_END_2026 = datetime.date(2026, 11, 1)

    @staticmethod
    def is_us_dst(dt):
        """ Returns True if the given date falls within US Daylight Saving Time. """
        if isinstance(dt, datetime.datetime):
            dt = dt.date()
        return MarketCalendar.DST_START_2026 <= dt < MarketCalendar.DST_END_2026

    @staticmethod
    def get_premarket_switch_hour_utc(dt):
        """
        Returns the UTC hour at which pre-market opens (focus switch).
        Standard Time: 9 AM UTC (4 AM EST)
        Daylight Time: 8 AM UTC (4 AM EDT)
        """
        return 8 if MarketCalendar.is_us_dst(dt) else 9

    @staticmethod
    def is_trading_day(dt):
        """ Checks if a given date is a NYSE trading day. """
        if isinstance(dt, datetime.datetime):
            dt = dt.date()
        if dt.weekday() >= 5: # Saturday/Sunday
            return False
        if dt in MarketCalendar.HOLIDAYS_2026:
            return False
        return True

    @staticmethod
    def is_early_close(dt):
        """ Returns True if the given date is an NYSE early close day. """
        if isinstance(dt, datetime.datetime):
            dt = dt.date()
        return dt in MarketCalendar.EARLY_CLOSE_2026

    @staticmethod
    def get_prev_trading_day(dt):
        """ Returns the most recent trading day before the given date. """
        if isinstance(dt, datetime.datetime):
            dt = dt.date()
        curr = dt - datetime.timedelta(days=1)
        while not MarketCalendar.is_trading_day(curr):
            curr -= datetime.timedelta(days=1)
        return curr

    @staticmethod
    def get_next_trading_day(dt):
        """ Returns the next trading day after the given date. """
        if isinstance(dt, datetime.datetime):
            dt = dt.date()
        curr = dt + datetime.timedelta(days=1)
        while not MarketCalendar.is_trading_day(curr):
            curr += datetime.timedelta(days=1)
        return curr

    @staticmethod
    def get_current_or_prev_trading_day(dt):
        """ If today is a trading day, returns today. Otherwise, returns the last trading day. """
        if isinstance(dt, datetime.datetime):
            dt = dt.date()
        curr = dt
        while not MarketCalendar.is_trading_day(curr):
            curr -= datetime.timedelta(days=1)
        return curr
    @staticmethod
    def get_market_close_hour_utc(dt):
        """
        Returns the UTC hour of NYSE market close, accounting for early close days.
        Regular Day:    4 PM EST = 9 PM UTC (21:00) / 8 PM UTC (20:00) DST
        Early Close:    1 PM EST = 6 PM UTC (18:00) / 5 PM UTC (17:00) DST
        Non-Trading:    Returns the regular close hour (caller should check is_trading_day).
        """
        if isinstance(dt, datetime.datetime):
            dt = dt.date()
        if dt in MarketCalendar.EARLY_CLOSE_2026:
            return 17 if MarketCalendar.is_us_dst(dt) else 18
        return 20 if MarketCalendar.is_us_dst(dt) else 21

    @staticmethod
    def get_session_label(dt):
        """
        Returns a human-readable label for the session type.
        """
        if isinstance(dt, datetime.datetime):
            dt = dt.date()
        if not MarketCalendar.is_trading_day(dt):
            if dt.weekday() >= 5:
                return "Weekend"
            return "Holiday"
        if dt in MarketCalendar.EARLY_CLOSE_2026:
            return "Early Close"
        return "Regular"

    @staticmethod
    def get_session_window(session_date):
        """
        Returns (session_start, session_end) as naive UTC datetimes for a given trading session.
        - session_start: 1 AM UTC of the day following the previous trading day (post-market close)
        - session_end:   1 AM UTC of the day following the given session date
        """
        if isinstance(session_date, datetime.datetime):
            session_date = session_date.date()

        # Start: previous trading day + 1 day at 1 AM UTC
        prev_day = MarketCalendar.get_prev_trading_day(session_date)
        start = datetime.datetime(prev_day.year, prev_day.month, prev_day.day, 1, 0, 0) + datetime.timedelta(days=1)

        # End: this day + 1 day at 1 AM UTC
        end = datetime.datetime(session_date.year, session_date.month, session_date.day, 1, 0, 0) + datetime.timedelta(days=1)

        return start, end

    @staticmethod
    def get_trading_session_date(dt):
        """
        Maps a UTC datetime to its logical NYSE trading session date.
        The trading session wraps around 1 AM UTC (post-market cutoff).
        """
        if not isinstance(dt, datetime.datetime):
            return dt # Fallback for pure date objects
            
        # Ensure UTC
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=datetime.timezone.utc)
        else:
            dt = dt.astimezone(datetime.timezone.utc)
            
        # Shift back by 1 hour so 1:XX AM falls into the correct logical bucket.
        # e.g. Tuesday 00:30 AM -> Monday date. Tuesday 01:30 AM -> Tuesday date.
        logical_date = (dt - datetime.timedelta(hours=1)).date()
        
        # If the logical date rests on a non-trading day (weekend/holiday),
        # it rolls forward to the NEXT trading day's session.
        if MarketCalendar.is_trading_day(logical_date):
            return logical_date
        else:
            return MarketCalendar.get_next_trading_day(logical_date)
