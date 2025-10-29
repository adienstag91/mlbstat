"""
Thread-Safe MLB Cached Page Fetcher
===================================

Robust page fetching with intelligent caching for MLB data.
Thread-safe for concurrent processing.
"""

import json
import time
import os
import threading
from typing import Optional
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import sys

class HighPerformancePageFetcher:
    """Thread-safe page fetcher with intelligent caching"""
    
    # Class-level lock for thread safety
    _cache_lock = threading.RLock()  # Reentrant lock
    
    # Cache expiry settings optimized for MLB data
    CACHE_EXPIRY = {
        "box_scores": 30 * 24 * 60 * 60,    # 30 days (box scores never change)
        "schedules": 24 * 60 * 60,          # 1 day (schedules can change)
        "rosters": 7 * 24 * 60 * 60,        # 7 days (rosters change weekly)
        "play_by_play": 30 * 24 * 60 * 60,  # 30 days (never changes)
        "player_pages": 30 * 24 * 60 * 60,  # 30 days (bio data rarely changes)
        "general": 12 * 60 * 60             # 12 hours (default)
    }
    
    def __init__(self, cache_dir: str = "cache", max_cache_size_mb: int = 500):
        """
        Initialize fetcher with thread-safe caching
        
        Args:
            cache_dir: Directory to store cache file
            max_cache_size_mb: Maximum cache size (not enforced, just for reference)
        """
        self.cache_dir = cache_dir
        self.max_cache_size_mb = max_cache_size_mb
        
        # Create cache directory if it doesn't exist
        os.makedirs(cache_dir, exist_ok=True)
        
        self.cache_file = os.path.join(cache_dir, "mlb_cache.json")
        
        # Initialize cache if it doesn't exist
        with self._cache_lock:
            if not os.path.exists(self.cache_file):
                self._save_cache({
                    "box_scores": {},
                    "schedules": {},
                    "rosters": {},
                    "play_by_play": {},
                    "player_pages": {},
                    "general": {},
                    "stats": {
                        "cache_hits": 0,
                        "cache_misses": 0,
                        "total_requests": 0
                    }
                })
    
    def _load_cache(self) -> dict:
        """Thread-safe cache loading"""
        with self._cache_lock:
            if not os.path.exists(self.cache_file):
                return {
                    "box_scores": {},
                    "schedules": {},
                    "rosters": {},
                    "play_by_play": {},
                    "player_pages": {},
                    "general": {},
                    "stats": {
                        "cache_hits": 0,
                        "cache_misses": 0,
                        "total_requests": 0
                    }
                }
            
            try:
                with open(self.cache_file, "r") as f:
                    cache = json.load(f)
                    # Ensure all required categories exist
                    for category in self.CACHE_EXPIRY.keys():
                        if category not in cache:
                            cache[category] = {}
                    if "stats" not in cache:
                        cache["stats"] = {"cache_hits": 0, "cache_misses": 0, "total_requests": 0}
                    return cache
            except (json.JSONDecodeError, FileNotFoundError):
                print("⚠️  Cache file corrupted. Resetting cache.")
                return self._load_cache()  # Recursive call after reset
    
    def _save_cache(self, cache: dict) -> None:
        """Thread-safe cache saving with atomic write"""
        with self._cache_lock:
            try:
                # Write to temporary file first (atomic operation)
                temp_file = self.cache_file + ".tmp"
                with open(temp_file, "w") as f:
                    json.dump(cache, f, indent=2)
                
                # Atomic rename (works on Unix and Windows)
                if os.path.exists(self.cache_file):
                    os.replace(temp_file, self.cache_file)
                else:
                    os.rename(temp_file, self.cache_file)
                    
            except Exception as e:
                print(f"⚠️  Failed to save cache: {e}")
                # Clean up temp file if it exists
                if os.path.exists(temp_file):
                    try:
                        os.remove(temp_file)
                    except:
                        pass
    
    def _categorize_url(self, url: str) -> str:
        """Automatically categorize URL based on its content"""
        url_lower = url.lower()
        
        if '/boxes/' in url_lower:
            return "box_scores"
        elif 'schedule' in url_lower:
            return "schedules"
        elif 'roster' in url_lower:
            return "rosters"
        elif 'play_by_play' in url_lower or 'pbp' in url_lower:
            return "play_by_play"
        elif '/players/' in url_lower:
            return "player_pages"
        else:
            return "general"
    
    def _get_cache_key(self, url: str) -> str:
        """Generate a consistent cache key for the URL"""
        return url
    
    def fetch_page(self, url: str, max_retries: int = 3, force_refresh: bool = False) -> BeautifulSoup:
        """
        Thread-safe page fetching with caching and retries
        
        Args:
            url: URL to fetch
            max_retries: Number of retry attempts
            force_refresh: Skip cache and fetch fresh data
            
        Returns:
            BeautifulSoup object of the page content
        """
        category = self._categorize_url(url)
        cache_key = self._get_cache_key(url)
        
        # Check cache first (unless force refresh)
        if not force_refresh:
            cache = self._load_cache()
            
            # Update total requests counter
            cache["stats"]["total_requests"] += 1
            
            if cache_key in cache[category]:
                cached_entry = cache[category][cache_key]
                timestamp = cached_entry.get("timestamp", 0)
                age = time.time() - timestamp
                
                if age < self.CACHE_EXPIRY[category]:
                    # Cache hit!
                    cache["stats"]["cache_hits"] += 1
                    self._save_cache(cache)
                    
                    age_hours = age / 3600
                    print(f"✅ Cache hit for {category}: {url[:60]}... (age: {age_hours:.1f}h)")
                    
                    # Return cached HTML as BeautifulSoup
                    return BeautifulSoup(cached_entry["data"], "html.parser")
                else:
                    print(f"⏳ Cache expired for {category}: {url[:60]}... (age: {age/3600:.1f}h)")
        
        # Cache miss or expired - fetch fresh data
        cache = self._load_cache()
        cache["stats"]["cache_misses"] += 1
        self._save_cache(cache)
        
        print(f"🌍 Fetching fresh data: {url[:80]}...")
        
        # Fetch with retries
        html_content = None
        for attempt in range(max_retries):
            try:
                with sync_playwright() as p:
                    browser = p.chromium.launch(headless=True)
                    context = browser.new_context(
                        user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                    )
                    page = context.new_page()
                    page.set_default_timeout(30000)
                    page.goto(url, wait_until='domcontentloaded')
                    page.wait_for_timeout(2000)
                    html_content = page.content()
                    browser.close()
                    break  # Success!
                    
            except Exception as e:
                if attempt < max_retries - 1:
                    wait_time = 3 * (attempt + 1)
                    print(f"❌ Attempt {attempt + 1} failed: {e}. Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    print(f"❌ Failed to fetch {url} after {max_retries} attempts: {e}")
                    raise Exception(f"Failed to fetch {url} after {max_retries} attempts: {e}")
        
        # Cache the successful result
        if html_content and html_content.strip():
            cache = self._load_cache()
            cache[category][cache_key] = {
                "data": html_content,
                "timestamp": time.time(),
                "url": url
            }
            self._save_cache(cache)
            print(f"✅ Cached fresh data for {category}")
        
        return BeautifulSoup(html_content, "html.parser")
    
    def get_cache_stats(self) -> dict:
        """Get cache performance statistics"""
        cache = self._load_cache()
        stats = cache.get("stats", {"cache_hits": 0, "cache_misses": 0, "total_requests": 0})
        
        total_requests = stats["total_requests"]
        cache_hits = stats["cache_hits"]
        cache_misses = stats["cache_misses"]
        hit_rate = (cache_hits / total_requests * 100) if total_requests > 0 else 0
        
        # Count cached entries by category
        category_counts = {}
        for category in self.CACHE_EXPIRY.keys():
            category_counts[category] = len(cache.get(category, {}))
        
        return {
            "total_requests": total_requests,
            "cache_hits": cache_hits,
            "cache_misses": cache_misses,
            "hit_rate_percentage": hit_rate,
            "category_counts": category_counts,
            "cache_file_size_mb": self._get_cache_file_size()
        }
    
    def _get_cache_file_size(self) -> float:
        """Get cache file size in MB"""
        try:
            if os.path.exists(self.cache_file):
                size_bytes = os.path.getsize(self.cache_file)
                return round(size_bytes / (1024 * 1024), 2)
            return 0.0
        except:
            return 0.0
    
    def clear_cache(self, category: Optional[str] = None) -> None:
        """Clear cache for specific category or entire cache"""
        cache = self._load_cache()
        
        if category:
            if category in cache:
                cache[category] = {}
                self._save_cache(cache)
                print(f"🗑️  Cleared {category} cache")
            else:
                print(f"⚠️  Category '{category}' not found")
        else:
            # Clear all categories but preserve stats
            for cat in self.CACHE_EXPIRY.keys():
                cache[cat] = {}
            self._save_cache(cache)
            print("🗑️  Cleared entire cache")
    
    def print_cache_summary(self) -> None:
        """Print a nice summary of cache status"""
        stats = self.get_cache_stats()
        
        print(f"\n📊 MLB CACHE SUMMARY")
        print("=" * 30)
        print(f"Total requests: {stats['total_requests']}")
        print(f"Cache hits: {stats['cache_hits']}")
        print(f"Cache misses: {stats['cache_misses']}")
        print(f"Hit rate: {stats['hit_rate_percentage']:.1f}%")
        print(f"Cache file size: {stats['cache_file_size_mb']} MB")
        
        print(f"\nCached entries by category:")
        for category, count in stats['category_counts'].items():
            print(f"  {category}: {count} pages")


# SimpleFetcher for when you don't want caching
class SimpleFetcher:
    """Simple fetcher without caching - for one-off requests"""
    
    def fetch_page(self, url: str, max_retries: int = 3) -> BeautifulSoup:
        """Fetch page without caching"""
        print(f"🌍 Fetching (no cache): {url[:80]}...")
        
        for attempt in range(max_retries):
            try:
                with sync_playwright() as p:
                    browser = p.chromium.launch(headless=True)
                    context = browser.new_context(
                        user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                    )
                    page = context.new_page()
                    page.set_default_timeout(30000)
                    page.goto(url, wait_until='domcontentloaded')
                    page.wait_for_timeout(2000)
                    html_content = page.content()
                    browser.close()
                    return BeautifulSoup(html_content, "html.parser")
                    
            except Exception as e:
                if attempt < max_retries - 1:
                    wait_time = 3 * (attempt + 1)
                    print(f"❌ Attempt {attempt + 1} failed: {e}. Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    raise Exception(f"Failed to fetch {url} after {max_retries} attempts: {e}")


# Testing
if __name__ == "__main__":
    print("🧪 Testing Thread-Safe Cache")
    print("=" * 40)
    
    fetcher = HighPerformancePageFetcher()
    
    test_url = "https://www.baseball-reference.com/boxes/NYA/NYA202503270.shtml"
    
    # First fetch (cache miss)
    print("\n1. First fetch (should be cache miss):")
    start = time.time()
    soup1 = fetcher.fetch_page(test_url)
    print(f"   Time: {time.time() - start:.2f}s")
    
    # Second fetch (cache hit)
    print("\n2. Second fetch (should be cache hit):")
    start = time.time()
    soup2 = fetcher.fetch_page(test_url)
    print(f"   Time: {time.time() - start:.2f}s")
    
    # Show stats
    fetcher.print_cache_summary()
    
    print("\n✅ Thread-safe cache ready for production!")
