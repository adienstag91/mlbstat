#!/usr/bin/env python3
"""
Player Performance Analytics
============================

Analyze player performance across various splits for predictive modeling.
Useful for player props, DFS, and general analysis.
"""

import sqlite3
import pandas as pd
from typing import Dict, List, Optional
from datetime import datetime, timedelta

class PlayerAnalytics:
    """Analytics engine for player performance analysis"""
    
    def __init__(self, db_path: str = "mlb_games.db"):
        self.db_path = db_path
    
    def get_player_game_context(self, game_id: str, player_id: str) -> Dict:
        """
        Get complete analytical context for a player in a specific game
        
        Returns all relevant metrics for analysis:
        - Season stats
        - Recent form (L5, L10, L15 games)
        - vs pitcher matchup history
        - vs handedness (RHP/LHP)
        - At venue performance
        - Home/away splits
        """
        
        with sqlite3.connect(self.db_path) as conn:
            # Get game context
            game_info = self._get_game_info(game_id, conn)
            player_info = self._get_player_info(player_id, conn)
            
            # Get opponent pitcher and their handedness
            opponent_pitcher = self._get_opponent_pitcher(game_id, player_id, conn)
            
            # Calculate all splits
            season_stats = self.get_season_stats(player_id, game_info['season'], conn)
            recent_form = self.get_recent_form(player_id, game_id, conn)
            vs_pitcher = self.get_vs_pitcher_stats(player_id, opponent_pitcher['pitcher_id'], conn)
            vs_handedness = self.get_vs_handedness_stats(
                player_id, 
                opponent_pitcher['throws'], 
                game_info['season'], 
                conn
            )
            venue_stats = self.get_venue_stats(player_id, game_info['venue'], conn)
            home_away = self.get_home_away_splits(player_id, game_info['season'], conn)
            
            return {
                'game_info': game_info,
                'player_info': player_info,
                'opponent_pitcher': opponent_pitcher,
                'season_stats': season_stats,
                'recent_form': recent_form,
                'vs_pitcher': vs_pitcher,
                'vs_handedness': vs_handedness,
                'venue_stats': venue_stats,
                'home_away_splits': home_away
            }
    
    def get_season_stats(self, player_id: str, season: int, conn) -> Dict:
        """Season-to-date batting statistics"""
        
        query = """
        SELECT 
            COUNT(DISTINCT ba.game_id) as games,
            SUM(ba.PA) as PA,
            SUM(ba.AB) as AB,
            SUM(ba.H) as H,
            SUM(ba.doubles) as doubles,
            SUM(ba.triples) as triples,
            SUM(ba.HR) as HR,
            SUM(ba.RBI) as RBI,
            SUM(ba.BB) as BB,
            SUM(ba.SO) as SO,
            SUM(ba.HBP) as HBP,
            SUM(ba.SF) as SF,
            
            -- Calculated stats
            ROUND(CAST(SUM(ba.H) AS FLOAT) / NULLIF(SUM(ba.AB), 0), 3) as AVG,
            ROUND(CAST(SUM(ba.H + ba.BB + ba.HBP) AS FLOAT) / NULLIF(SUM(ba.PA), 0), 3) as OBP,
            ROUND(CAST(SUM(ba.H + ba.doubles + 2*ba.triples + 3*ba.HR) AS FLOAT) / NULLIF(SUM(ba.AB), 0), 3) as SLG,
            ROUND(
                CAST(SUM(ba.H + ba.BB + ba.HBP) AS FLOAT) / NULLIF(SUM(ba.PA), 0) +
                CAST(SUM(ba.H + ba.doubles + 2*ba.triples + 3*ba.HR) AS FLOAT) / NULLIF(SUM(ba.AB), 0),
                3
            ) as OPS,
            ROUND(CAST(SUM(ba.H + ba.doubles + 2*ba.triples + 3*ba.HR) AS FLOAT) / NULLIF(SUM(ba.H), 0), 3) as TB_per_H
            
        FROM batting_appearances ba
        JOIN games g ON ba.game_id = g.game_id
        WHERE ba.player_id = ?
          AND strftime('%Y', g.game_date) = ?
        """
        
        df = pd.read_sql(query, conn, params=(player_id, str(season)))
        return df.to_dict('records')[0] if not df.empty else {}
    
    def get_recent_form(self, player_id: str, current_game_id: str, conn) -> Dict:
        """Recent performance trends (last 5, 10, 15 games)"""
        
        # Get current game date to filter games before it
        game_date_query = "SELECT game_date FROM games WHERE game_id = ?"
        current_date = pd.read_sql(game_date_query, conn, params=(current_game_id,))
        
        if current_date.empty:
            return {}
        
        current_date = current_date.iloc[0]['game_date']
        
        query = """
        WITH recent_games AS (
            SELECT 
                ba.game_id,
                g.game_date,
                ba.PA, ba.AB, ba.H, ba.doubles, ba.triples, ba.HR, ba.RBI, ba.BB, ba.SO,
                ROW_NUMBER() OVER (ORDER BY g.game_date DESC) as game_num
            FROM batting_appearances ba
            JOIN games g ON ba.game_id = g.game_id
            WHERE ba.player_id = ?
              AND g.game_date < ?
            ORDER BY g.game_date DESC
        )
        SELECT 
            -- Last 5 games
            SUM(CASE WHEN game_num <= 5 THEN PA END) as L5_PA,
            SUM(CASE WHEN game_num <= 5 THEN AB END) as L5_AB,
            SUM(CASE WHEN game_num <= 5 THEN H END) as L5_H,
            SUM(CASE WHEN game_num <= 5 THEN HR END) as L5_HR,
            ROUND(CAST(SUM(CASE WHEN game_num <= 5 THEN H END) AS FLOAT) / 
                  NULLIF(SUM(CASE WHEN game_num <= 5 THEN AB END), 0), 3) as L5_AVG,
            
            -- Last 10 games
            SUM(CASE WHEN game_num <= 10 THEN PA END) as L10_PA,
            SUM(CASE WHEN game_num <= 10 THEN AB END) as L10_AB,
            SUM(CASE WHEN game_num <= 10 THEN H END) as L10_H,
            SUM(CASE WHEN game_num <= 10 THEN HR END) as L10_HR,
            ROUND(CAST(SUM(CASE WHEN game_num <= 10 THEN H END) AS FLOAT) / 
                  NULLIF(SUM(CASE WHEN game_num <= 10 THEN AB END), 0), 3) as L10_AVG,
            
            -- Last 15 games
            SUM(CASE WHEN game_num <= 15 THEN PA END) as L15_PA,
            SUM(CASE WHEN game_num <= 15 THEN AB END) as L15_AB,
            SUM(CASE WHEN game_num <= 15 THEN H END) as L15_H,
            SUM(CASE WHEN game_num <= 15 THEN HR END) as L15_HR,
            ROUND(CAST(SUM(CASE WHEN game_num <= 15 THEN H END) AS FLOAT) / 
                  NULLIF(SUM(CASE WHEN game_num <= 15 THEN AB END), 0), 3) as L15_AVG
                  
        FROM recent_games
        """
        
        df = pd.read_sql(query, conn, params=(player_id, current_date))
        return df.to_dict('records')[0] if not df.empty else {}
    
    def get_vs_pitcher_stats(self, player_id: str, pitcher_id: str, conn) -> Dict:
        """Career stats against specific pitcher"""
        
        query = """
        SELECT 
            COUNT(DISTINCT e.game_id) as matchups,
            COUNT(*) as PA,
            SUM(e.is_at_bat) as AB,
            SUM(e.is_hit) as H,
            SUM(CASE WHEN e.hit_type LIKE '%double%' THEN 1 ELSE 0 END) as doubles,
            SUM(CASE WHEN e.hit_type LIKE '%triple%' THEN 1 ELSE 0 END) as triples,
            SUM(CASE WHEN e.hit_type LIKE '%home run%' THEN 1 ELSE 0 END) as home_runs,
            SUM(e.is_walk) as BB,
            SUM(e.is_strikeout) as SO,
            
            -- Calculated
            ROUND(CAST(SUM(e.is_hit) AS FLOAT) / NULLIF(SUM(e.is_at_bat), 0), 3) as AVG,
            ROUND(CAST(
                SUM(e.is_hit) + 
                SUM(CASE WHEN e.hit_type LIKE '%double%' THEN 1 ELSE 0 END) +
                2 * SUM(CASE WHEN e.hit_type LIKE '%triple%' THEN 1 ELSE 0 END) +
                3 * SUM(CASE WHEN e.hit_type LIKE '%home run%' THEN 1 ELSE 0 END)
            AS FLOAT) / NULLIF(SUM(e.is_at_bat), 0), 3) as SLG
            
        FROM at_bats e
        WHERE e.batter_id = ?
          AND e.pitcher_id = ?
        """
        
        df = pd.read_sql(query, conn, params=(player_id, pitcher_id))
        return df.to_dict('records')[0] if not df.empty else {}
    
    def get_vs_handedness_stats(self, player_id: str, pitcher_handedness: str, 
                                season: int, conn) -> Dict:
        """Stats vs RHP or LHP for current season"""
        
        query = """
        SELECT 
            COUNT(DISTINCT e.game_id) as games,
            COUNT(*) as PA,
            SUM(e.is_at_bat) as AB,
            SUM(e.is_hit) as H,
            SUM(CASE WHEN e.hit_type LIKE '%home run%' THEN 1 ELSE 0 END) as home_runs,
            SUM(e.is_walk) as BB,
            SUM(e.is_strikeout) as SO,
            
            -- Calculated
            ROUND(CAST(SUM(e.is_hit) AS FLOAT) / NULLIF(SUM(e.is_at_bat), 0), 3) as AVG,
            ROUND(CAST(
                SUM(e.is_hit) + 
                SUM(CASE WHEN e.hit_type LIKE '%double%' THEN 1 ELSE 0 END) +
                2 * SUM(CASE WHEN e.hit_type LIKE '%triple%' THEN 1 ELSE 0 END) +
                3 * SUM(CASE WHEN e.hit_type LIKE '%home run%' THEN 1 ELSE 0 END)
            AS FLOAT) / NULLIF(SUM(e.is_at_bat), 0), 3) as SLG
            
        FROM at_bats e
        JOIN games g ON e.game_id = g.game_id
        JOIN players p ON e.pitcher_id = p.player_id
        WHERE e.batter_id = ?
          AND p.throws = ?
          AND strftime('%Y', g.game_date) = ?
        """
        
        df = pd.read_sql(query, conn, params=(player_id, pitcher_handedness, str(season)))
        result = df.to_dict('records')[0] if not df.empty else {}
        result['handedness'] = 'RHP' if pitcher_handedness == 'R' else 'LHP'
        return result
    
    def get_venue_stats(self, player_id: str, venue: str, conn) -> Dict:
        """Career stats at specific venue"""
        
        query = """
        SELECT 
            COUNT(DISTINCT ba.game_id) as games,
            SUM(ba.PA) as PA,
            SUM(ba.AB) as AB,
            SUM(ba.H) as H,
            SUM(ba.HR) as HR,
            SUM(ba.RBI) as RBI,
            
            ROUND(CAST(SUM(ba.H) AS FLOAT) / NULLIF(SUM(ba.AB), 0), 3) as AVG,
            ROUND(CAST(SUM(ba.H + ba.doubles + 2*ba.triples + 3*ba.HR) AS FLOAT) / NULLIF(SUM(ba.AB), 0), 3) as SLG
            
        FROM batting_appearances ba
        JOIN games g ON ba.game_id = g.game_id
        WHERE ba.player_id = ?
          AND g.venue = ?
        """
        
        df = pd.read_sql(query, conn, params=(player_id, venue))
        return df.to_dict('records')[0] if not df.empty else {}
    
    def get_home_away_splits(self, player_id: str, season: int, conn) -> Dict:
        """Home vs away splits for season"""
        
        query = """
        SELECT 
            CASE WHEN ba.team = g.home_team THEN 'Home' ELSE 'Away' END as location,
            COUNT(DISTINCT ba.game_id) as games,
            SUM(ba.PA) as PA,
            SUM(ba.AB) as AB,
            SUM(ba.H) as H,
            SUM(ba.HR) as HR,
            
            ROUND(CAST(SUM(ba.H) AS FLOAT) / NULLIF(SUM(ba.AB), 0), 3) as AVG,
            ROUND(CAST(SUM(ba.H + ba.doubles + 2*ba.triples + 3*ba.HR) AS FLOAT) / NULLIF(SUM(ba.AB), 0), 3) as SLG
            
        FROM batting_appearances ba
        JOIN games g ON ba.game_id = g.game_id
        WHERE ba.player_id = ?
          AND strftime('%Y', g.game_date) = ?
        GROUP BY location
        """
        
        df = pd.read_sql(query, conn, params=(player_id, str(season)))
        
        splits = {}
        for _, row in df.iterrows():
            splits[row['location'].lower()] = row.to_dict()
        
        return splits
    
    def _get_game_info(self, game_id: str, conn) -> Dict:
        """Get game context"""
        query = """
        SELECT game_id, game_date, home_team, away_team, venue,
               strftime('%Y', game_date) as season
        FROM games WHERE game_id = ?
        """
        df = pd.read_sql(query, conn, params=(game_id,))
        return df.to_dict('records')[0] if not df.empty else {}
    
    def _get_player_info(self, player_id: str, conn) -> Dict:
        """Get player biographical info"""
        query = """
        SELECT player_id, full_name, bats, throws, birth_date
        FROM players WHERE player_id = ?
        """
        df = pd.read_sql(query, conn, params=(player_id,))
        return df.to_dict('records')[0] if not df.empty else {}
    
    def _get_opponent_pitcher(self, game_id: str, batter_id: str, conn) -> Dict:
        """Determine opponent starting pitcher"""
        # Get batter's team
        batter_team_query = """
        SELECT team FROM batting_appearances 
        WHERE game_id = ? AND player_id = ?
        """
        batter_team = pd.read_sql(batter_team_query, conn, params=(game_id, batter_id))
        
        if batter_team.empty:
            return {}
        
        batter_team = batter_team.iloc[0]['team']
        
        # Get opponent starting pitcher
        pitcher_query = """
        SELECT pa.player_id, pa.player_name, p.throws
        FROM pitching_appearances pa
        JOIN players p ON pa.player_id = p.player_id
        WHERE pa.game_id = ?
          AND pa.team != ?
          AND pa.is_starter = 1
        LIMIT 1
        """
        
        df = pd.read_sql(pitcher_query, conn, params=(game_id, batter_team))
        
        if not df.empty:
            result = df.to_dict('records')[0]
            result['pitcher_id'] = result.pop('player_id')
            return result
        
        return {}
    
    def print_player_analysis(self, game_id: str, player_id: str):
        """Print comprehensive player analysis"""
        
        analysis = self.get_player_game_context(game_id, player_id)
        
        print(f"\n{'='*60}")
        print(f"PLAYER ANALYSIS: {analysis['player_info']['full_name']}")
        print(f"Game: {game_id} | {analysis['game_info']['game_date']}")
        print(f"{'='*60}")
        
        # Season stats
        season = analysis['season_stats']
        if season:
            print(f"\nSEASON STATS ({season.get('games')} games):")
            print(f"  PA: {season.get('PA')} | AB: {season.get('AB')}")
            print(f"  AVG: {season.get('AVG')} | OBP: {season.get('OBP')} | SLG: {season.get('SLG')} | OPS: {season.get('OPS')}")
            print(f"  H: {season.get('H')} | HR: {season.get('HR')} | RBI: {season.get('RBI')}")
        
        # Recent form
        recent = analysis['recent_form']
        if recent:
            print(f"\nRECENT FORM:")
            print(f"  L5:  {recent.get('L5_AVG')} AVG ({recent.get('L5_AB')} AB, {recent.get('L5_H')} H, {recent.get('L5_HR')} HR)")
            print(f"  L10: {recent.get('L10_AVG')} AVG ({recent.get('L10_AB')} AB, {recent.get('L10_H')} H, {recent.get('L10_HR')} HR)")
            print(f"  L15: {recent.get('L15_AVG')} AVG ({recent.get('L15_AB')} AB, {recent.get('L15_H')} H, {recent.get('L15_HR')} HR)")
        
        # vs Pitcher
        vs_p = analysis['vs_pitcher']
        pitcher = analysis['opponent_pitcher']
        if vs_p and vs_p.get('PA'):
            print(f"\nvs {pitcher.get('player_name')} ({pitcher.get('throws')}HP):")
            print(f"  {vs_p.get('matchups')} matchups | {vs_p.get('PA')} PA | {vs_p.get('AB')} AB")
            print(f"  AVG: {vs_p.get('AVG')} | SLG: {vs_p.get('SLG')}")
            print(f"  H: {vs_p.get('H')} | HR: {vs_p.get('HR')} | BB: {vs_p.get('BB')} | SO: {vs_p.get('SO')}")
        else:
            print(f"\nvs {pitcher.get('player_name')}: No prior matchups")
        
        # vs Handedness
        vs_hand = analysis['vs_handedness']
        if vs_hand and vs_hand.get('PA'):
            print(f"\nvs {vs_hand.get('handedness')} (Season):")
            print(f"  {vs_hand.get('games')} games | {vs_hand.get('PA')} PA | {vs_hand.get('AB')} AB")
            print(f"  AVG: {vs_hand.get('AVG')} | SLG: {vs_hand.get('SLG')}")
            print(f"  H: {vs_hand.get('H')} | HR: {vs_hand.get('HR')}")
        
        # Venue stats
        venue = analysis['venue_stats']
        venue_name = analysis['game_info'].get('venue')
        if venue and venue.get('PA'):
            print(f"\nAt {venue_name}:")
            print(f"  {venue.get('games')} games | {venue.get('PA')} PA | {venue.get('AB')} AB")
            print(f"  AVG: {venue.get('AVG')} | SLG: {venue.get('SLG')}")
            print(f"  H: {venue.get('H')} | HR: {venue.get('HR')}")
        
        # Home/Away
        splits = analysis['home_away_splits']
        if splits:
            print(f"\nHOME/AWAY SPLITS:")
            for location, data in splits.items():
                print(f"  {location.title()}: {data.get('AVG')} AVG, {data.get('SLG')} SLG ({data.get('games')} games)")

# Example usage
if __name__ == "__main__":
    analytics = PlayerAnalytics("debug_yankees_season.db")
    
    # Example: Analyze Aaron Judge in a specific game
    analytics.print_player_analysis("NYA202509280", "judgeaa01")
    
    print("Player Analytics Engine Ready")
    print("\nUsage:")
    print("  analytics = PlayerAnalytics('demo_season.db')")
    print("  analytics.print_player_analysis(game_id, player_id)")
