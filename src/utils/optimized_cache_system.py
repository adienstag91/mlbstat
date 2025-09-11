"""
MLB Optimized Cache System
==========================

High-performance caching with size limits, lazy loading, and compression.
Fixes the performance crisis from the existing JSON-based system.
"""

import pickle
import gzip
import time
import os
import hashlib
import sqlite3
from typing import Optional, Dict, Any
from pathlib import Path
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import logging

class OptimizedMLBCache:
    """
    High-performance MLB cache with:
    - SQLite backend for fast lookups
    - Gzip compression for HTML content  
    - Size limits with LRU eviction
    - Lazy loading - no full cache loads
    - Category-based expiry
    """
    
    def __init__(self, cache_dir: str = "cache", max_size_mb: int = 500):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(exist_ok=True)
        self.db_path = self.cache_dir / "cache_index.db"
        self.max_size_mb = max_size_mb
        self.logger = logging.getLogger(__name__)
        
        # Cache expiry settings (in seconds)
        self.CACHE_EXPIRY = {
            "box_scores": 30 * 24 * 60 * 60,    # 30 days (final games never change)
            "schedules": 24 * 60 * 60,          # 1 day (schedules can change)
            "rosters": 7 * 24 * 60 * 60,        # 7 days (roster changes)
            "play_by_play": 30 * 24 * 60 * 60,  # 30 days (final games never change)
            "general": 12 * 60 * 60             # 12 hours (default)
        }
        
        self._init_database()
    
    def _init_database(self):
        """Initialize SQLite cache index database"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS cache_entries (
                    cache_key TEXT PRIMARY KEY,
                    category TEXT NOT NULL,
                    url TEXT NOT NULL,
                    timestamp REAL NOT NULL,
                    file_path TEXT NOT NULL,
                    size_bytes INTEGER NOT NULL,
                    last_accessed REAL NOT NULL
                )
            """)
            
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_category ON cache_entries(category)
            """)
            
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_timestamp ON cache_entries(timestamp)
            """)
            
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_last_accessed ON cache_entries(last_accessed)
            """)
            
            # Stats table for performance tracking
            conn.execute("""
                CREATE TABLE IF NOT EXISTS cache_stats (
                    stat_name TEXT PRIMARY KEY,
                    stat_value INTEGER DEFAULT 0
                )
            """)
            
            # Initialize stats if they don't exist
            conn.execute("""
                INSERT OR IGNORE INTO cache_stats (stat_name, stat_value) 
                VALUES ('cache_hits', 0), ('cache_misses', 0), ('total_requests', 0)
            """)
    
    def _categorize_url(self, url: str) -> str:
        """Categorize URL for appropriate caching strategy"""
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
    
    def _get_cache_key(self, url: str) -> str:
        """Generate consistent cache key"""
        return hashlib.md5(url.encode('utf-8')).hexdigest()
    
    def _get_cache_file_path(self, cache_key: str) -> Path:
        """Get file path for cached content"""
        return self.cache_dir / f"{cache_key}.gz"
    
    def _save_content(self, content: str, file_path: Path) -> int:
        """Save compressed content to file, return size in bytes"""
        with gzip.open(file_path, 'wt', encoding='utf-8') as f:
            f.write(content)
        return file_path.stat().st_size
    
    def _load_content(self, file_path: Path) -> str:
        """Load and decompress content from file"""
        with gzip.open(file_path, 'rt', encoding='utf-8') as f:
            return f.read()
    
    def get_cached_content(self, url: str) -> Optional[BeautifulSoup]:
        """
        Get cached content if available and not expired
        Returns None if cache miss or expired
        """
        cache_key = self._get_cache_key(url)
        category = self._categorize_url(url)
        current_time = time.time()
        
        with sqlite3.connect(self.db_path) as conn:
            # Update total requests
            conn.execute("UPDATE cache_stats SET stat_value = stat_value + 1 WHERE stat_name = 'total_requests'")
            
            # Check if cached entry exists and is valid
            cursor = conn.execute("""
                SELECT file_path, timestamp, size_bytes 
                FROM cache_entries 
                WHERE cache_key = ? AND category = ?
            """, (cache_key, category))
            
            row = cursor.fetchone()
            
            if row is None:
                return None  # Cache miss
            
            file_path, timestamp, size_bytes = row
            age = current_time - timestamp
            max_age = self.CACHE_EXPIRY[category]
            
            if age > max_age:
                # Expired - remove from cache
                self._remove_entry(cache_key, Path(file_path))
                return None
            
            # Cache hit! Update last accessed time
            conn.execute("""
                UPDATE cache_entries 
                SET last_accessed = ? 
                WHERE cache_key = ?
            """, (current_time, cache_key))
            
            conn.execute("UPDATE cache_stats SET stat_value = stat_value + 1 WHERE stat_name = 'cache_hits'")
            
            try:
                content = self._load_content(Path(file_path))
                age_hours = age / 3600
                self.logger.info(f"✅ Cache hit for {category}: {url[:60]}... (age: {age_hours:.1f}h)")
                return BeautifulSoup(content, "html.parser")
            except Exception as e:
                self.logger.warning(f"Failed to load cached content: {e}")
                self._remove_entry(cache_key, Path(file_path))
                return None
    
    def save_content(self, url: str, content: str) -> None:
        """Save content to cache"""
        cache_key = self._get_cache_key(url)
        category = self._categorize_url(url)
        file_path = self._get_cache_file_path(cache_key)
        current_time = time.time()
        
        try:
            # Save compressed content
            size_bytes = self._save_content(content, file_path)
            
            with sqlite3.connect(self.db_path) as conn:
                # Update cache miss counter
                conn.execute("UPDATE cache_stats SET stat_value = stat_value + 1 WHERE stat_name = 'cache_misses'")
                
                # Insert or replace cache entry
                conn.execute("""
                    INSERT OR REPLACE INTO cache_entries 
                    (cache_key, category, url, timestamp, file_path, size_bytes, last_accessed)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (cache_key, category, url, current_time, str(file_path), size_bytes, current_time))
            
            self.logger.info(f"✅ Cached fresh data for {category}: {url[:60]}... ({size_bytes/1024:.1f} KB)")
            
            # Check if we need to clean up space
            self._enforce_size_limit()
            
        except Exception as e:
            self.logger.error(f"Failed to save cache content: {e}")
    
    def _remove_entry(self, cache_key: str, file_path: Path) -> None:
        """Remove entry from cache"""
        try:
            if file_path.exists():
                file_path.unlink()
        except Exception:
            pass
        
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("DELETE FROM cache_entries WHERE cache_key = ?", (cache_key,))
    
    def _enforce_size_limit(self):
        """Remove oldest entries if cache exceeds size limit"""
        max_size_bytes = self.max_size_mb * 1024 * 1024
        
        with sqlite3.connect(self.db_path) as conn:
            # Get current total size
            cursor = conn.execute("SELECT SUM(size_bytes) FROM cache_entries")
            total_size = cursor.fetchone()[0] or 0
            
            if total_size <= max_size_bytes:
                return  # Under limit
            
            self.logger.info(f"Cache size {total_size/1024/1024:.1f} MB exceeds limit {self.max_size_mb} MB. Cleaning up...")
            
            # Remove oldest entries (LRU style)
            cursor = conn.execute("""
                SELECT cache_key, file_path, size_bytes 
                FROM cache_entries 
                ORDER BY last_accessed ASC
            """)
            
            removed_count = 0
            for cache_key, file_path, size_bytes in cursor:
                self._remove_entry(cache_key, Path(file_path))
                total_size -= size_bytes
                removed_count += 1
                
                if total_size <= max_size_bytes * 0.8:  # Clean to 80% of limit
                    break
            
            self.logger.info(f"🧹 Removed {removed_count} old cache entries")
    
    def cleanup_expired(self) -> int:
        """Remove all expired entries, return count removed"""
        current_time = time.time()
        removed_count = 0
        
        with sqlite3.connect(self.db_path) as conn:
            for category, max_age in self.CACHE_EXPIRY.items():
                cutoff_time = current_time - max_age
                
                cursor = conn.execute("""
                    SELECT cache_key, file_path 
                    FROM cache_entries 
                    WHERE category = ? AND timestamp < ?
                """, (category, cutoff_time))
                
                for cache_key, file_path in cursor:
                    self._remove_entry(cache_key, Path(file_path))
                    removed_count += 1
        
        if removed_count > 0:
            self.logger.info(f"🧹 Removed {removed_count} expired cache entries")
        
        return removed_count
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache performance statistics"""
        with sqlite3.connect(self.db_path) as conn:
            # Get basic stats
            cursor = conn.execute("SELECT stat_name, stat_value FROM cache_stats")
            stats = dict(cursor.fetchall())
            
            # Get category counts and sizes
            cursor = conn.execute("""
                SELECT category, COUNT(*) as count, SUM(size_bytes) as size_bytes
                FROM cache_entries 
                GROUP BY category
            """)
            
            category_info = {}
            total_size = 0
            total_entries = 0
            
            for category, count, size_bytes in cursor:
                size_mb = (size_bytes or 0) / 1024 / 1024
                category_info[category] = {
                    'entries': count,
                    'size_mb': round(size_mb, 2)
                }
                total_size += size_bytes or 0
                total_entries += count
            
            # Calculate hit rate
            total_requests = stats.get('total_requests', 0)
            cache_hits = stats.get('cache_hits', 0)
            hit_rate = (cache_hits / total_requests * 100) if total_requests > 0 else 0
            
            return {
                'total_requests': total_requests,
                'cache_hits': cache_hits,
                'cache_misses': stats.get('cache_misses', 0),
                'hit_rate_percentage': round(hit_rate, 1),
                'total_entries': total_entries,
                'total_size_mb': round(total_size / 1024 / 1024, 2),
                'max_size_mb': self.max_size_mb,
                'categories': category_info
            }
    
    def clear_cache(self, category: Optional[str] = None) -> int:
        """Clear cache for specific category or entire cache"""
        removed_count = 0
        
        with sqlite3.connect(self.db_path) as conn:
            if category:
                cursor = conn.execute("""
                    SELECT cache_key, file_path 
                    FROM cache_entries 
                    WHERE category = ?
                """, (category,))
            else:
                cursor = conn.execute("SELECT cache_key, file_path FROM cache_entries")
            
            for cache_key, file_path in cursor:
                self._remove_entry(cache_key, Path(file_path))
                removed_count += 1
        
        target = f"{category} cache" if category else "entire cache"
        self.logger.info(f"🗑️ Cleared {target} ({removed_count} entries)")
        return removed_count


class HighPerformancePageFetcher:
    """
    MLB page fetcher with optimized caching
    Drop-in replacement for SafePageFetcher with massive performance gains
    """
    
    def __init__(self, cache_dir: str = None, max_cache_size_mb: int = 500):
        if cache_dir is None:
            import os
            # Expand ~ to home directory, then add your project path
            cache_dir = os.path.expanduser("~/mlbstat/cache")
            # Or absolute path:
            # cache_dir = "/Users/yourname/mlbstat/cache"
        
        self.cache = OptimizedMLBCache(cache_dir, max_cache_size_mb)
        self.logger = logging.getLogger(__name__)
    
    def fetch_page(self, url: str, max_retries: int = 3, force_refresh: bool = False) -> BeautifulSoup:
        """
        High-performance page fetching with optimized caching
        
        Args:
            url: URL to fetch
            max_retries: Number of retry attempts
            force_refresh: Skip cache and fetch fresh data
            
        Returns:
            BeautifulSoup object of the page content
        """
        
        # Check cache first (unless force refresh)
        if not force_refresh:
            cached_soup = self.cache.get_cached_content(url)
            if cached_soup is not None:
                return cached_soup
        
        # Cache miss - fetch fresh data
        self.logger.info(f"🌍 Fetching fresh data: {url}")
        
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
                    self.logger.warning(f"❌ Attempt {attempt + 1} failed: {e}. Retrying in {wait_time}s...")
                    time.sleep(wait_time)
                else:
                    raise Exception(f"Failed to fetch {url} after {max_retries} attempts: {e}")
        
        # Cache the successful result
        if html_content and html_content.strip():
            self.cache.save_content(url, html_content)
        
        return BeautifulSoup(html_content, "html.parser")
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache performance statistics"""
        return self.cache.get_stats()
    
    def print_cache_summary(self) -> None:
        """Print detailed cache status"""
        stats = self.get_cache_stats()
        
        print(f"\n📊 OPTIMIZED MLB CACHE SUMMARY")
        print("=" * 40)
        print(f"Total requests: {stats['total_requests']}")
        print(f"Cache hits: {stats['cache_hits']}")
        print(f"Cache misses: {stats['cache_misses']}")
        print(f"Hit rate: {stats['hit_rate_percentage']:.1f}%")
        print(f"Total entries: {stats['total_entries']}")
        print(f"Cache size: {stats['total_size_mb']} MB / {stats['max_size_mb']} MB")
        
        print(f"\nEntries by category:")
        for category, info in stats['categories'].items():
            print(f"  {category}: {info['entries']} entries ({info['size_mb']} MB)")
    
    def cleanup_expired_cache(self) -> None:
        """Remove expired entries from cache"""
        removed_count = self.cache.cleanup_expired()
        if removed_count == 0:
            print("✅ No expired cache entries found")
    
    def clear_cache(self, category: Optional[str] = None) -> None:
        """Clear cache for specific category or entire cache"""
        self.cache.clear_cache(category)


# Testing and demonstration
def test_optimized_caching_system():
    """Test the optimized caching system performance"""
    print("🧪 TESTING OPTIMIZED MLB CACHING SYSTEM")
    print("=" * 50)
    
    # Test URLs
    test_urls = [
        "https://www.baseball-reference.com/boxes/NYA/NYA202503270.shtml",  # Box score
        "https://www.baseball-reference.com/teams/NYY/2025-schedule-scores.shtml",  # Schedule
    ]
    
    fetcher = HighPerformancePageFetcher(max_cache_size_mb=100)
    
    for i, url in enumerate(test_urls, 1):
        print(f"\n{i}. Testing {url}")
        
        # First fetch (cache miss)
        start_time = time.time()
        soup1 = fetcher.fetch_page(url)
        time1 = time.time() - start_time
        print(f"   First fetch: {time1:.2f}s")
        
        # Second fetch (cache hit)
        start_time = time.time()
        soup2 = fetcher.fetch_page(url)
        time2 = time.time() - start_time
        print(f"   Second fetch: {time2:.2f}s (should be much faster!)")
        
        # Performance improvement
        if time1 > 0:
            speedup = time1 / time2 if time2 > 0 else float('inf')
            print(f"   Speedup: {speedup:.1f}x faster")
        
        # Verify content integrity
        content_match = len(str(soup1)) == len(str(soup2))
        print(f"   Content length matches: {content_match}")
    
    # Show performance summary
    fetcher.print_cache_summary()
    
    print(f"\n🎯 KEY IMPROVEMENTS:")
    print(f"   ✅ No more full cache file loading")
    print(f"   ✅ SQLite index for instant lookups")
    print(f"   ✅ Gzip compression saves 60-80% space")
    print(f"   ✅ Automatic size limits with LRU eviction")
    print(f"   ✅ Category-based expiry policies")


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        if sys.argv[1] == "test":
            test_optimized_caching_system()
        elif sys.argv[1] == "clear":
            fetcher = HighPerformancePageFetcher()
            fetcher.clear_cache()
            print("🗑️ Cache cleared")
        elif sys.argv[1] == "stats":
            fetcher = HighPerformancePageFetcher()
            fetcher.print_cache_summary()
    else:
        test_optimized_caching_system()
