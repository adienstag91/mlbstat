"""
MLB Cached Page Fetcher
=======================

Robust page fetching with intelligent caching for MLB data.
Designed to minimize web requests and avoid rate limiting.
"""

import json
import time
import os
import hashlib
import sys
from typing import Optional
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup

class SafePageFetcher:
    """Enhanced SafePageFetcher with intelligent caching"""
    
    CACHE_FILE = "mlb_cache.json"
    
    # Cache expiry settings optimized for MLB data
    CACHE_EXPIRY = {
        "box_scores": 30 * 24 * 60 * 60,    # 30 days (box scores never change once game is final)
        "schedules": 24 * 60 * 60,          # 1 day (schedules can change)
        "rosters": 7 * 24 * 60 * 60,        # 7 days (rosters change weekly)
        "play_by_play": 30 * 24 * 60 * 60,  # 30 days (play-by-play never changes)
        "general": 12 * 60 * 60             # 12 hours (default for other pages)
    }
    
    @classmethod
    def load_cache(cls) -> dict:
        """Load the cache from file or return an empty structured cache"""
        if not os.path.exists(cls.CACHE_FILE):
            return {
                "box_scores": {},
                "schedules": {},
                "rosters": {},
                "play_by_play": {},
                "general": {},
                "stats": {
                    "cache_hits": 0,
                    "cache_misses": 0,
                    "total_requests": 0
                }
            }
        
        try:
            with open(cls.CACHE_FILE, "r") as f:
                cache = json.load(f)
                # Ensure all required categories exist
                for category in cls.CACHE_EXPIRY.keys():
                    if category not in cache:
                        cache[category] = {}
                if "stats" not in cache:
                    cache["stats"] = {"cache_hits": 0, "cache_misses": 0, "total_requests": 0}
                return cache
        except json.JSONDecodeError:
            print("⚠️  MLB cache file is corrupted. Resetting cache.")
            return cls.load_cache()  # Recursive call to create fresh cache
    
    @classmethod
    def save_cache(cls, cache: dict) -> None:
        """Save the cache to file"""
        try:
            with open(cls.CACHE_FILE, "w") as f:
                json.dump(cache, f, indent=2)
        except Exception as e:
            print(f"⚠️  Failed to save cache: {e}")
    
    @classmethod
    def _categorize_url(cls, url: str) -> str:
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
        else:
            return "general"
    
    @classmethod
    def _get_cache_key(cls, url: str) -> str:
        """Generate a consistent cache key for the URL"""
        # Use URL as-is for cache key (readable and debuggable)
        return url
    
    @classmethod
    def fetch_page(cls, url: str, max_retries: int = 3, force_refresh: bool = False) -> BeautifulSoup:
        """
        Safely fetch page with caching and retries
        
        Args:
            url: URL to fetch
            max_retries: Number of retry attempts
            force_refresh: Skip cache and fetch fresh data
            
        Returns:
            BeautifulSoup object of the page content
        """
        cache = cls.load_cache()
        category = cls._categorize_url(url)
        cache_key = cls._get_cache_key(url)
        
        # Update total requests counter
        cache["stats"]["total_requests"] += 1
        
        # Check cache first (unless force refresh)
        if not force_refresh and cache_key in cache[category]:
            cached_entry = cache[category][cache_key]
            timestamp = cached_entry.get("timestamp", 0)
            age = time.time() - timestamp
            
            if age < cls.CACHE_EXPIRY[category]:
                # Cache hit!
                cache["stats"]["cache_hits"] += 1
                cls.save_cache(cache)
                
                print(f"✅ Cache hit for {category}: {url} (age: {age/3600:.1f}h)")
                
                # Return cached HTML as BeautifulSoup
                return BeautifulSoup(cached_entry["data"], "html.parser")
            else:
                print(f"⏳ Cache expired for {category}: {url} (age: {age/3600:.1f}h)")
        
        # Cache miss - fetch fresh data
        cache["stats"]["cache_misses"] += 1
        print(f"🌍 Fetching fresh data: {url}")
        
        # Fetch with retries (your original logic)
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
                    cls.save_cache(cache)  # Save stats even on failure
                    raise Exception(f"Failed to fetch {url} after {max_retries} attempts: {e}")
        
        # Cache the successful result
        if html_content and html_content.strip():
            cache[category][cache_key] = {
                "data": html_content,
                "timestamp": time.time(),
                "url": url  # Store original URL for debugging
            }
            cls.save_cache(cache)
            print(f"✅ Cached fresh data for {category}: {url}")
        
        return BeautifulSoup(html_content, "html.parser")
    
    @classmethod
    def get_cache_stats(cls) -> dict:
        """Get cache performance statistics"""
        cache = cls.load_cache()
        stats = cache.get("stats", {"cache_hits": 0, "cache_misses": 0, "total_requests": 0})
        
        total_requests = stats["total_requests"]
        cache_hits = stats["cache_hits"]
        cache_misses = stats["cache_misses"]
        hit_rate = (cache_hits / total_requests * 100) if total_requests > 0 else 0
        
        # Count cached entries by category
        category_counts = {}
        for category in cls.CACHE_EXPIRY.keys():
            category_counts[category] = len(cache.get(category, {}))
        
        return {
            "total_requests": total_requests,
            "cache_hits": cache_hits,
            "cache_misses": cache_misses,
            "hit_rate_percentage": hit_rate,
            "category_counts": category_counts,
            "cache_file_size_mb": cls._get_cache_file_size()
        }
    
    @classmethod
    def _get_cache_file_size(cls) -> float:
        """Get cache file size in MB"""
        try:
            if os.path.exists(cls.CACHE_FILE):
                size_bytes = os.path.getsize(cls.CACHE_FILE)
                return round(size_bytes / (1024 * 1024), 2)
            return 0.0
        except:
            return 0.0
    
    @classmethod
    def clear_cache(cls, category: Optional[str] = None) -> None:
        """Clear cache for specific category or entire cache"""
        cache = cls.load_cache()
        
        if category:
            if category in cache:
                cache[category] = {}
                cls.save_cache(cache)
                print(f"🗑️  Cleared {category} cache")
            else:
                print(f"⚠️  Category '{category}' not found")
        else:
            # Clear all categories but preserve stats
            for cat in cls.CACHE_EXPIRY.keys():
                cache[cat] = {}
            cls.save_cache(cache)
            print("🗑️  Cleared entire cache")
    
    @classmethod
    def print_cache_summary(cls) -> None:
        """Print a nice summary of cache status"""
        stats = cls.get_cache_stats()
        
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
    
    @classmethod
    def cleanup_expired_cache(cls) -> None:
        """Remove expired entries from cache"""
        cache = cls.load_cache()
        current_time = time.time()
        total_removed = 0
        
        for category, expiry_time in cls.CACHE_EXPIRY.items():
            if category in cache:
                expired_keys = []
                for key, entry in cache[category].items():
                    age = current_time - entry.get("timestamp", 0)
                    if age > expiry_time:
                        expired_keys.append(key)
                
                for key in expired_keys:
                    del cache[category][key]
                    total_removed += 1
        
        if total_removed > 0:
            cls.save_cache(cache)
            print(f"🧹 Removed {total_removed} expired cache entries")
        else:
            print("✅ No expired cache entries found")

# Testing and utility functions
def test_caching_system():
    """Test the caching system with sample MLB URLs"""
    
    print("🧪 TESTING MLB CACHING SYSTEM")
    print("=" * 40)
    
    test_urls = [
        "https://www.baseball-reference.com/boxes/NYA/NYA202503270.shtml",  # Box score
        "https://www.baseball-reference.com/teams/NYY/2025-schedule-scores.shtml",  # Schedule
    ]
    
    fetcher = SafePageFetcher()
    
    for i, url in enumerate(test_urls, 1):
        print(f"\n{i}. Testing {url}")
        
        # First fetch (should be cache miss)
        start_time = time.time()
        soup1 = fetcher.fetch_page(url)
        time1 = time.time() - start_time
        print(f"   First fetch: {time1:.2f}s")
        
        # Second fetch (should be cache hit)
        start_time = time.time()
        soup2 = fetcher.fetch_page(url)
        time2 = time.time() - start_time
        print(f"   Second fetch: {time2:.2f}s (should be much faster!)")
        
        # Verify content is the same
        content_match = str(soup1) == str(soup2)
        print(f"   Content matches: {content_match}")
    
    # Show cache stats
    fetcher.print_cache_summary()

if __name__ == "__main__":
    fetcher = SafePageFetcher()
    if len(sys.argv) > 1 and sys.argv[1] == "clear":
        fetcher.clear_cache()
    else:
        test_caching_system()
