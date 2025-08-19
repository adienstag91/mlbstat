"""
Game URL Fetcher
================

Dynamically fetch game URLs from Baseball Reference by date or team.
Filters for completed games only and provides flexible selection options.
"""

import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from typing import List, Optional
import re
import time
from utils.mlb_cached_fetcher import SafePageFetcher
import sys

def get_games_by_date(date: str, completed_only: bool = True) -> List[str]:
    """
    Get all games from a specific date
    
    Args:
        date: Date in YYYY-MM-DD format (e.g., "2025-03-27")
        completed_only: Only return completed games (default: True)
        
    Returns:
        List of game URLs
    """
    try:
        year, month, day = date.split('-')
        url = f"https://www.baseball-reference.com/boxes/index.fcgi?year={year}&month={int(month)}&day={int(day)}"
        print(url)
        url_date = year + month + day

        soup = SafePageFetcher.fetch_page(url)
        
        game_urls = []
        
        if completed_only:
            # Check if date is before today (simple optimization)
            game_date = datetime.strptime(date, '%Y-%m-%d').date()
            today = datetime.now().date()
            
            if game_date >= today:
                print("too early!")
                # Future date - no completed games
                return []
        
        # Find all game links on the day's schedule
        game_links = soup.find_all('a', href=re.compile(rf'/boxes/[A-Z]{{3}}/[A-Z]{{3}}{url_date}\d\.shtml'))
        
        for link in game_links:
            game_url = "https://www.baseball-reference.com/" + link['href']
            game_urls.append(game_url)
        
        return game_urls
        
    except Exception as e:
        print(f"Error fetching games for {date}: {e}")
        return []

def get_games_by_team(team_code: str, year: int = 2025, completed_only: bool = True, first_n: Optional[int] = None, last_n: Optional[int] = None) -> List[str]:
    """
    Get games for a specific team
    
    Args:
        team_code: 3-letter team code (e.g., "NYY", "LAD", "HOU")
        year: Season year (default: 2025)
        completed_only: Only return completed games (default: True)
        first_n: Return first N games (default: None = all)
        last_n: Return last N games (default: None = all)
        
    Returns:
        List of game URLs
    """
    try:
        url = f"https://www.baseball-reference.com/teams/{team_code}/{year}-schedule-scores.shtml"
        
        soup = SafePageFetcher.fetch_page(url)
        
        # Find the schedule table
        schedule_table = soup.find('table', {'id': 'team_schedule'})
        #print(schedule_table)
        if not schedule_table:
            print(f"Could not find schedule table for {team_code} {year}")
            return []
        
        game_urls = []
        rows = schedule_table.find_all('tr')[1:]  # Skip header
        
        for row in rows: 
            # Check if boxscore link exists (completed games have links)
            boxscore_cell = row.find('td', {'data-stat': 'boxscore'})
            if not boxscore_cell:
                continue
            
            boxscore_link = boxscore_cell.find('a')
            if not boxscore_link:
                continue  # No boxscore link = game not completed
            
            if completed_only:
                # Check if boxscore cell contains "preview" text
                boxscore_text = boxscore_cell.get_text(strip=True).lower()
                if 'preview' in boxscore_text:
                    continue  # Preview = not completed
            
            game_url = "https://www.baseball-reference.com/" + boxscore_link['href']
            game_urls.append(game_url)
        
        # Apply first_n or last_n filtering
        if first_n is not None:
            game_urls = game_urls[:first_n]
        elif last_n is not None:
            game_urls = game_urls[-last_n:]
        
        return game_urls
        
    except Exception as e:
        print(f"Error fetching games for team {team_code}: {e}")
        return []

def get_games_last_n_days(n_days: int, end_date: Optional[str] = None) -> List[str]:
    """
    Get all completed games from the last N days
    
    Args:
        n_days: Number of days to look back
        end_date: End date in YYYY-MM-DD format (default: yesterday)
        
    Returns:
        List of game URLs from all games in the date range
    """
    if end_date:
        end = datetime.strptime(end_date, '%Y-%m-%d')
    else:
        # Default to yesterday to avoid in-progress games
        end = datetime.now() - timedelta(days=1)
    
    all_game_urls = []
    
    for i in range(n_days):
        date = end - timedelta(days=i)
        # Skip future dates entirely
        if date.date() >= datetime.now().date():
            continue
            
        date_str = date.strftime('%Y-%m-%d')
        
        print(f"Fetching games for {date_str}...")
        games = get_games_by_date(date_str, completed_only=True)
        all_game_urls.extend(games)
        
        # Be respectful with requests
        time.sleep(1)
    
    return all_game_urls

def get_games_full_season(year: str = "2025"):
    start_date = f"{year}-03-01"
    end_date = f"{year}-11-30"
    start_dt = datetime.strptime(start_date, '%Y-%m-%d')
    end_dt = datetime.strptime(end_date, '%Y-%m-%d')
    total_days = (end_dt - start_dt).days + 1

    all_game_urls = []
    failed_dates = []

    print(f"\n📊 Scanning {total_days} days for completed games...")
    current_date = start_dt
    day_count = 0

    while current_date <= end_dt:
        date_str = current_date.strftime('%Y-%m-%d')
        day_count += 1
        
        try:
            print(f"  Day {day_count:3d}/{total_days}: {date_str}...", end=" ")
            
            # Use existing fetcher to get games for this date
            games = get_games_by_date(date_str, completed_only=True)
            
            if games:
                all_game_urls.extend(games)
                print(f"✅ {len(games)} games")
            else:
                print("📭 No games")
            
            # Be respectful with requests
            time.sleep(1)
            
        except Exception as e:
            print(f"❌ Error: {str(e)[:50]}...")
            failed_dates.append(date_str)
        
        current_date += timedelta(days=1)

    # Summary of collection phase
    print(f"\n📋 GAME COLLECTION COMPLETE:")
    print(f"   Total games found: {len(all_game_urls)}")
    print(f"   Failed dates: {len(failed_dates)}")
    if failed_dates:
        print(f"   Failed dates: {', '.join(failed_dates[:5])}{'...' if len(failed_dates) > 5 else ''}")
    
    # Remove any potential duplicates (shouldn't happen with date-based, but safety first)
    unique_games = list(dict.fromkeys(all_game_urls))
    duplicates_removed = len(all_game_urls) - len(unique_games)
    
    if duplicates_removed > 0:
        print(f"   🔄 Removed {duplicates_removed} duplicate games")
        all_game_urls = unique_games
    
    return all_game_urls


# Example usage and testing

if __name__ == "__main__":
    if len(sys.argv) > 2 and sys.argv[1] == "team":
        team = sys.argv[2]
        games_by_team= get_games_by_team(team)
        print(f"Found {len(games_by_team)} games")
        for i, url in enumerate(games_by_team, 1):
            print(f"   {i}. {url}")
    elif len(sys.argv) > 1 and sys.argv[1] == "full":
        if len(sys.argv) > 2:
            year = sys.argv[2]
            get_games_full_season(year)
        else:
            get_games_full_season()
    else:
        get_games_by_date("2025-03-27")

    #print("🧪 TESTING URL FETCHER")
    #print("=" * 40)
    
    # Test 1: Get games by date
    #print("📅 Test 1: Games on 2025-03-18")
    #games_by_date = get_games_by_date("2025-03-18")
    #print(f"Found {len(games_by_date)} games")
    #for i, url in enumerate(games_by_date, 1):  
        #print(f"   {i}. {url}")
    
    # Test 2: Get Yankees last 5 games
    #print("\n⚾ Test 2: Yankees last 5 games")
    #yankees_games = get_games_by_team("NYY", last_n=5)
    #print(f"Found {len(yankees_games)} games")
    #for i, url in enumerate(yankees_games, 1):
        #print(f"   {i}. {url}")

    #test 3
    #print("full season test")
    #this_season = get_games_full_season()
    
    #print(f"\n✅ All tests completed!")