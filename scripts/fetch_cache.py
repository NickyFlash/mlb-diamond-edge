"""
MLB Diamond Edge — Cache Fetcher
Runs on Mac or GitHub Actions to pre-fetch FanGraphs data
and save to Google Drive cache. Colab reads from this cache.

Uses a residential/GitHub IP so FanGraphs doesn't block requests.
"""

import os
import sys
import glob
import time
import json
import random
import requests
import pandas as pd
from datetime import datetime, timedelta

# ── Config ───────────────────────────────────────────────────
CURRENT_YEAR = datetime.now().year
LAST_YEAR    = CURRENT_YEAR - 1
TODAY        = datetime.now().strftime('%Y-%m-%d')

# ── Cache directory detection ────────────────────────────────
def get_cache_dir():
    # Environment variable override (used by GitHub Actions)
    env = os.environ.get('MLB_CACHE_DIR')
    if env:
        return env
    # Mac — Google Drive desktop app
    for pattern in [
        os.path.expanduser('~/Library/CloudStorage/GoogleDrive-*/My Drive/MLB_Diamond_Edge_Cache'),
        os.path.expanduser('~/Google Drive/My Drive/MLB_Diamond_Edge_Cache'),
        os.path.expanduser('~/Google Drive/MLB_Diamond_Edge_Cache'),
    ]:
        matches = glob.glob(pattern)
        if matches:
            return matches[0]
    # Fallback — local temp (GitHub Actions uses this, then uploads)
    return '/tmp/mlb_cache'

CACHE_DIR    = get_cache_dir()
CACHE_DAILY  = os.path.join(CACHE_DIR, 'daily')
CACHE_WEEKLY = os.path.join(CACHE_DIR, 'weekly')

for d in [CACHE_DAILY, CACHE_WEEKLY]:
    os.makedirs(d, exist_ok=True)

print(f"📁 Cache dir: {CACHE_DIR}")

# ── Request headers (rotating) ───────────────────────────────
_USER_AGENTS = [
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
]

def _headers():
    return {
        'User-Agent': random.choice(_USER_AGENTS),
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': 'https://www.fangraphs.com/',
        'Origin': 'https://www.fangraphs.com',
    }

# ── Cache save/load ──────────────────────────────────────────
def save(df, key, weekly=False):
    """Save DataFrame to cache (parquet + csv backup)."""
    if df is None or df.empty:
        return
    base = CACHE_WEEKLY if weekly else CACHE_DAILY
    suffix = '_weekly' if weekly else f'_{TODAY}'
    try:
        path = os.path.join(base, f'{key}{suffix}.parquet')
        df.to_parquet(path, index=False)
        path_csv = os.path.join(base, f'{key}{suffix}.csv')
        df.to_csv(path_csv, index=False)
        rows = len(df)
        print(f"    💾 Saved {key}: {rows} rows → {os.path.basename(path)}")
    except Exception as e:
        print(f"    ⚠️  Save failed for {key}: {e}")

# ── FanGraphs fetchers ───────────────────────────────────────
def fg_url(stat_type, qual, year, start=None, end=None):
    url = (f"https://www.fangraphs.com/api/leaders/major-league/data"
           f"?age=&pos=all&stats={stat_type}&lg=all&qual={qual}"
           f"&season={year}&season1={year}&ind=0&team=0"
           f"&pageitems=2000&pagenum=1&sortdir=default&sortstat=WAR")
    if start:
        url += f"&startdate={start}&enddate={end}"
    elif year < CURRENT_YEAR:
        url += f"&startdate={year}-01-01&enddate={year}-12-31"
    return url


def fetch_fg(stat_type, year, qual=1, start=None, end=None, label=''):
    """Fetch a single FanGraphs leaderboard with retry and longer delays."""
    url = fg_url(stat_type, qual, year, start, end)
    for attempt in range(5):
        try:
            # Longer delays between attempts — FG rate limits aggressively
            wait = [3, 8, 15, 30, 60][attempt] + random.uniform(1, 3)
            if attempt > 0:
                print(f"    Retry {attempt}/4 — waiting {wait:.0f}s")
            time.sleep(wait)
            r = requests.get(url, headers=_headers(), timeout=30)
            if r.status_code == 200:
                rows = r.json().get('data', [])
                if rows:
                    df = pd.DataFrame(rows)
                    for col in ['K%', 'BB%', 'SwStr%']:
                        if col in df.columns:
                            df[col] = pd.to_numeric(df[col], errors='coerce')
                            if df[col].dropna().mean() > 1.0:
                                df[col] = df[col] / 100.0
                    print(f"    ✅ FG {stat_type} {year} {label}: {len(df)} rows")
                    return df
                else:
                    print(f"    ⚠️  FG {stat_type} {year} {label}: empty response")
                    return pd.DataFrame()
            elif r.status_code == 429:
                wait = float(r.headers.get('Retry-After', 30)) + random.uniform(5, 10)
                print(f"    Rate limited — waiting {wait:.0f}s")
                time.sleep(wait)
            elif r.status_code == 403:
                print(f"    ⚠️  FG {stat_type} {year} {label}: HTTP 403 (blocked)")
                if attempt < 4:
                    continue
                return pd.DataFrame()
            else:
                print(f"    ⚠️  FG {stat_type} {year} {label}: HTTP {r.status_code}")
                return pd.DataFrame()
        except Exception as e:
            print(f"    ⚠️  FG {stat_type} {year} {label}: {str(e)[:60]}")
            if attempt < 4:
                time.sleep(10)
    return pd.DataFrame()


def fetch_fg_team_relief(year, period_days=None):
    """Team-level relief stats via FG team=0,ts endpoint."""
    today  = datetime.now().strftime('%Y-%m-%d')
    start  = (datetime.now()-timedelta(days=period_days)).strftime('%Y-%m-%d') if period_days else None
    label  = f'L{period_days}' if period_days else 'YTD'
    url = (f"https://www.fangraphs.com/api/leaders/major-league/data"
           f"?age=&pos=all&stats=rel&lg=all&qual=1"
           f"&season={year}&season1={year}&ind=0&team=0,ts"
           f"&pageitems=30&pagenum=1&sortdir=default&sortstat=ERA"
           f"&startdate={start or f'{year}-01-01'}&enddate={today}")
    try:
        time.sleep(1.5 + random.uniform(0, 1))
        r = requests.get(url, headers=_headers(), timeout=25)
        if r.status_code == 200:
            rows = r.json().get('data', [])
            if rows and len(rows) >= 10:
                df = pd.DataFrame(rows)
                for col in ['K%', 'BB%', 'HR/FB']:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                        if df[col].dropna().mean() > 1.0:
                            df[col] = df[col] / 100.0
                if 'LOB%' in df.columns:
                    df['LOB%'] = pd.to_numeric(
                        df['LOB%'].astype(str).str.replace('%',''), errors='coerce') / 100.0
                print(f"    ✅ FG team relief {year} {label}: {len(df)} teams")
                return df
    except Exception as e:
        print(f"    ⚠️  FG team relief {year} {label}: {str(e)[:60]}")
    return pd.DataFrame()


def fetch_fg_platoon(year, split, qual=1):
    """Platoon splits from FanGraphs."""
    today = datetime.now().strftime('%Y-%m-%d')
    url = (f"https://www.fangraphs.com/api/leaders/major-league/data"
           f"?age=&pos=all&stats=bat&lg=all&qual={qual}"
           f"&season={year}&season1={year}&ind=0&team=0"
           f"&pageitems=2000&pagenum=1&sortdir=default&sortstat=wRC"
           f"&split={split}&startdate={year}-01-01&enddate={today}")
    try:
        time.sleep(1.5 + random.uniform(0, 1))
        r = requests.get(url, headers=_headers(), timeout=25)
        if r.status_code == 200:
            rows = r.json().get('data', [])
            if rows:
                df = pd.DataFrame(rows)
                print(f"    ✅ FG platoon {split} {year}: {len(df)} rows")
                return df
    except Exception as e:
        print(f"    ⚠️  FG platoon {split} {year}: {str(e)[:60]}")
    return pd.DataFrame()


def fetch_savant(stat_type, year):
    """Baseball Savant statcast leaderboard via pybaseball."""
    try:
        if stat_type == 'pit':
            from pybaseball import statcast_pitcher_exitvelo_barrels
            df = statcast_pitcher_exitvelo_barrels(year, minBBE=25)
        else:
            from pybaseball import statcast_batter_exitvelo_barrels
            df = statcast_batter_exitvelo_barrels(year, minBBE=25)
        if df is not None and not df.empty:
            print(f"    ✅ Savant {stat_type} {year}: {len(df)} rows")
            return df
    except Exception as e:
        # Try alternative pybaseball endpoint
        try:
            from pybaseball import pitching_stats, batting_stats
            if stat_type == 'pit':
                df = pitching_stats(year, qual=10)
            else:
                df = batting_stats(year, qual=30)
            if df is not None and not df.empty:
                print(f"    ✅ pybaseball {stat_type} {year}: {len(df)} rows (fallback)")
                return df
        except Exception as e2:
            print(f"    ⚠️  Savant {stat_type} {year}: {str(e)[:50]} | {str(e2)[:50]}")
    return pd.DataFrame()


def fetch_fg_team_bat(year):
    """Fetch team-level batting stats from FanGraphs team=0,ts endpoint."""
    today = datetime.now().strftime('%Y-%m-%d')
    url = (f"https://www.fangraphs.com/api/leaders/major-league/data"
           f"?age=&pos=all&stats=bat&lg=all&qual=0"
           f"&season={year}&season1={year}&ind=0&team=0,ts"
           f"&pageitems=30&pagenum=1&sortdir=default&sortstat=wRC"
           f"&startdate={year}-01-01&enddate={today}")
    try:
        time.sleep(random.uniform(3, 6))
        r = requests.get(url, headers=_headers(), timeout=25)
        if r.status_code == 200:
            rows = r.json().get('data', [])
            if rows and len(rows) >= 10:
                df = pd.DataFrame(rows)
                for col in ['K%', 'BB%']:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                        if df[col].dropna().mean() > 1.0:
                            df[col] = df[col] / 100.0
                print(f"    ✅ FG team bat {year}: {len(df)} teams")
                return df
        print(f"    ⚠️  FG team bat {year}: HTTP {r.status_code}")
    except Exception as e:
        print(f"    ⚠️  FG team bat {year}: {str(e)[:60]}")
    return pd.DataFrame()


# ── Main fetch sequence ──────────────────────────────────────
def run():
    print(f"\n{'='*55}")
    print(f"🔄 MLB Diamond Edge Cache Fetcher")
    print(f"   {TODAY}  |  Current: {CURRENT_YEAR}  |  LY: {LAST_YEAR}")
    print(f"{'='*55}\n")

    # ── Current year (skip if pre-season) ────────────────────
    opening_day_approx = datetime(CURRENT_YEAR, 3, 20)
    is_preseason = datetime.now() < opening_day_approx

    if is_preseason:
        print("ℹ️  Pre-season — skipping current year fetches\n")
    else:
        print(f"📡 FanGraphs {CURRENT_YEAR} — Pitchers (YTD):")
        df = fetch_fg('pit', CURRENT_YEAR, qual=1, label='YTD')
        save(df, f'pit_ytd_{CURRENT_YEAR}', weekly=True)
        save(df, f'pit_ytd_{CURRENT_YEAR}')

        print(f"\n📡 FanGraphs {CURRENT_YEAR} — Hitters (YTD):")
        df = fetch_fg('bat', CURRENT_YEAR, qual=1, label='YTD')
        save(df, f'bat_ytd_{CURRENT_YEAR}', weekly=True)
        save(df, f'bat_ytd_{CURRENT_YEAR}')

        print(f"\n📡 FanGraphs {CURRENT_YEAR} — Bullpen Relief:")
        df = fetch_fg_team_relief(CURRENT_YEAR)
        save(df, f'bp_fg_ytd_{CURRENT_YEAR}', weekly=True)
        save(df, f'bp_fg_ytd_{CURRENT_YEAR}')

        print(f"\n📡 FanGraphs {CURRENT_YEAR} — Platoon splits:")
        df_l = fetch_fg_platoon(CURRENT_YEAR, 'vsLeft',  qual=1)
        save(df_l, f'plat_l_{CURRENT_YEAR}', weekly=True)
        save(df_l, f'plat_l_{CURRENT_YEAR}')
        df_r = fetch_fg_platoon(CURRENT_YEAR, 'vsRight', qual=1)
        save(df_r, f'plat_r_{CURRENT_YEAR}', weekly=True)
        save(df_r, f'plat_r_{CURRENT_YEAR}')

        print(f"\n📡 Savant {CURRENT_YEAR} — Pitchers:")
        df = fetch_savant('pit', CURRENT_YEAR)
        save(df, f'sv_pit_{CURRENT_YEAR}', weekly=True)

        print(f"\n📡 Savant {CURRENT_YEAR} — Hitters:")
        df = fetch_savant('bat', CURRENT_YEAR)
        save(df, f'sv_bat_{CURRENT_YEAR}', weekly=True)

    # ── Last year (always needed for Phase 1 fallback) ───────
    print(f"\n{'─'*55}")
    print(f"📡 FanGraphs {LAST_YEAR} — Pitchers (LY baseline):")
    df = fetch_fg('pit', LAST_YEAR, qual=0, label='YTD')
    save(df, f'pit_ytd_{LAST_YEAR}', weekly=True)
    save(df, f'pit_ytd_{LAST_YEAR}')

    print(f"\n📡 FanGraphs {LAST_YEAR} — Hitters (LY baseline):")
    time.sleep(random.uniform(8, 12))
    df = fetch_fg('bat', LAST_YEAR, qual=0, label='YTD')
    save(df, f'bat_ytd_{LAST_YEAR}', weekly=True)
    save(df, f'bat_ytd_{LAST_YEAR}')

    print(f"\n📡 FanGraphs {LAST_YEAR} — Bullpen Relief (LY baseline):")
    df = fetch_fg_team_relief(LAST_YEAR)
    save(df, f'bp_fg_ytd_{LAST_YEAR}', weekly=True)
    save(df, f'bp_fg_ytd_{LAST_YEAR}')

    print(f"\n📡 FanGraphs {LAST_YEAR} — Platoon splits (LY baseline):")
    df_l = fetch_fg_platoon(LAST_YEAR, 'vsLeft',  qual=50)
    save(df_l, f'plat_l_{LAST_YEAR}', weekly=True)
    save(df_l, f'plat_l_{LAST_YEAR}')
    df_r = fetch_fg_platoon(LAST_YEAR, 'vsRight', qual=50)
    save(df_r, f'plat_r_{LAST_YEAR}', weekly=True)
    save(df_r, f'plat_r_{LAST_YEAR}')

    print(f"\n📡 Savant {LAST_YEAR} — Pitchers (LY baseline):")
    df = fetch_savant('pit', LAST_YEAR)
    save(df, f'sv_pit_{LAST_YEAR}', weekly=True)

    print(f"\n📡 Savant {LAST_YEAR} — Hitters (LY baseline):")
    df = fetch_savant('bat', LAST_YEAR)
    save(df, f'sv_bat_{LAST_YEAR}', weekly=True)

    print(f"\n📡 FanGraphs {LAST_YEAR} — Team Batting (LY baseline):")
    df = fetch_fg_team_bat(LAST_YEAR)
    if not df.empty:
        save(df, f'bat_team_{LAST_YEAR}', weekly=True)
        save(df, f'bat_team_{LAST_YEAR}')

    print(f"\n{'='*55}")
    print(f"✅ Cache refresh complete — {datetime.now().strftime('%H:%M:%S')}")
    print(f"   Colab will use cached data on next run")
    print(f"{'='*55}\n")


if __name__ == '__main__':
    run()
