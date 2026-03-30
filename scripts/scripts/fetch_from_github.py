"""
MLB Diamond Edge — GitHub Cache Fetcher for Colab

Add this to Cell 5 in Colab as a fallback:
  If Drive cache is missing or stale, pull latest from GitHub.

Usage in Colab:
  GITHUB_REPO = 'YOUR_USERNAME/mlb-diamond-edge'  # set this once in Cell 2
  
  # At top of Cell 5:
  from scripts.fetch_from_github import fetch_github_cache
  fetch_github_cache(GITHUB_REPO, CACHE_WEEKLY)
"""

import os
import requests

def fetch_github_cache(repo, cache_weekly_dir, branch='main'):
    """
    Download latest cache files from GitHub repo to local Drive cache.
    Only downloads files that are missing or older than 20 hours.
    
    Args:
        repo:              'username/repo-name'
        cache_weekly_dir:  path to CACHE_WEEKLY directory
        branch:            git branch (default 'main')
    """
    base_url = f"https://raw.githubusercontent.com/{repo}/{branch}/cache/weekly"
    
    # Files we want from GitHub
    files_to_fetch = [
        f'pit_ytd_{2025}_weekly.parquet',
        f'bat_ytd_{2025}_weekly.parquet',
        f'bp_fg_ytd_{2025}_weekly.parquet',
        f'plat_l_{2025}_weekly.parquet',
        f'plat_r_{2025}_weekly.parquet',
        f'sv_pit_{2025}_weekly.parquet',
        f'sv_bat_{2025}_weekly.parquet',
        f'pit_ytd_{2026}_weekly.parquet',
        f'bat_ytd_{2026}_weekly.parquet',
        f'bp_fg_ytd_{2026}_weekly.parquet',
        f'plat_l_{2026}_weekly.parquet',
        f'plat_r_{2026}_weekly.parquet',
    ]
    
    from datetime import datetime
    now = datetime.now().timestamp()
    stale_threshold = 20 * 3600  # 20 hours
    
    os.makedirs(cache_weekly_dir, exist_ok=True)
    downloaded = 0
    skipped    = 0
    
    print(f"📥 Checking GitHub cache ({repo})...")
    
    for fname in files_to_fetch:
        local_path = os.path.join(cache_weekly_dir, fname)
        
        # Skip if file exists and is fresh
        if os.path.exists(local_path):
            age = now - os.path.getmtime(local_path)
            if age < stale_threshold:
                skipped += 1
                continue
        
        url = f"{base_url}/{fname}"
        try:
            r = requests.get(url, timeout=30)
            if r.status_code == 200:
                with open(local_path, 'wb') as f:
                    f.write(r.content)
                size_kb = len(r.content) / 1024
                print(f"   ✅ {fname} ({size_kb:.0f} KB)")
                downloaded += 1
            elif r.status_code == 404:
                pass  # File doesn't exist in GitHub yet (e.g. 2026 pre-season)
            else:
                print(f"   ⚠️  {fname}: HTTP {r.status_code}")
        except Exception as e:
            print(f"   ⚠️  {fname}: {str(e)[:50]}")
    
    if downloaded:
        print(f"   Downloaded {downloaded} files from GitHub")
    elif skipped:
        print(f"   All {skipped} cache files already fresh — skipped")
    else:
        print(f"   No files downloaded")
    
    return downloaded
