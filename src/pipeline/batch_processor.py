"""
Batch Game Processor
===================
Process multiple games or entire seasons using the unified game processor.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
from typing import List, Dict, Optional
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import pandas as pd

from pipeline.game_processor import process_game
from pipeline.game_url_fetcher import get_games_full_season

logger = logging.getLogger(__name__)


def process_batch(
    game_urls: List[str],
    validate: bool = True,
    store: bool = True,
    min_accuracy: float = 100.0,
    skip_if_exists: bool = True,
    max_workers: int = 1,
    halt_on_failure: bool = False,
    save_csv_report: bool = False,
    output_dir: str = "batch_results"
) -> Dict:
    """
    Process multiple games in batch.
    
    Args:
        game_urls: List of Baseball Reference game URLs
        validate: Whether to validate stats
        store: Whether to store to database
        min_accuracy: Minimum validation accuracy required (0-100)
        skip_if_exists: Skip games already in database
        max_workers: Number of parallel workers (1 = sequential)
        halt_on_failure: Stop processing on first failure
        
    Returns:
        Dict with batch processing results and statistics
    """
    start_time = time.time()
    
    results = {
        "total_games": len(game_urls),
        "processed": 0,
        "skipped": 0,
        "failed": 0,
        "stored": 0,
        "validation_failures": 0,
        "games": [],
        "errors": [],
        # Accuracy tracking
        "batting_accuracies": [],
        "pitching_accuracies": [],
        "total_batting_diffs": 0,
        "total_pitching_diffs": 0,
        "games_with_batting_diffs": 0,
        "games_with_pitching_diffs": 0
    }
    
    logger.info(f"Starting batch processing: {len(game_urls)} games")
    logger.info(f"Settings: validate={validate}, store={store}, min_accuracy={min_accuracy}%, skip_if_exists={skip_if_exists}")
    
    if max_workers == 1:
        # Sequential processing
        for i, game_url in enumerate(game_urls, 1):
            logger.info(f"Processing game {i}/{len(game_urls)}: {game_url}")
            
            try:
                result = process_game(
                    game_url=game_url,
                    validate=validate,
                    store=store,
                    min_accuracy=min_accuracy,
                    halt_on_validation_failure=halt_on_failure,
                    skip_if_exists=skip_if_exists
                )
                
                # Track results
                if result["processing_status"] == "skipped":
                    results["skipped"] += 1
                elif result["processing_status"] == "success":
                    results["processed"] += 1
                    if result.get("stored", False):
                        results["stored"] += 1
                    
                    # Track validation accuracy
                    validation = result.get("validation_results", {})
                    batting = validation.get("batting", {})
                    pitching = validation.get("pitching", {})
                    
                    if batting:
                        batting_acc = batting.get("accuracy", 0)
                        batting_diffs = batting.get("total_differences", 0)
                        results["batting_accuracies"].append(batting_acc)
                        results["total_batting_diffs"] += batting_diffs
                        if batting_diffs > 0:
                            results["games_with_batting_diffs"] += 1
                    
                    if pitching:
                        pitching_acc = pitching.get("accuracy", 0)
                        pitching_diffs = pitching.get("total_differences", 0)
                        results["pitching_accuracies"].append(pitching_acc)
                        results["total_pitching_diffs"] += pitching_diffs
                        if pitching_diffs > 0:
                            results["games_with_pitching_diffs"] += 1
                    
                elif result["processing_status"] == "validation_failed":
                    results["validation_failures"] += 1
                else:
                    results["failed"] += 1
                
                results["games"].append({
                    "game_url": game_url,
                    "game_id": result.get("game_id"),
                    "status": result["processing_status"],
                    "stored": result.get("stored", False),
                    "validation": result.get("validation_results", {})
                })
                
            except Exception as e:
                logger.error(f"Error processing {game_url}: {e}")
                results["failed"] += 1
                results["errors"].append({
                    "game_url": game_url,
                    "error": str(e)
                })
                
                if halt_on_failure:
                    logger.error("Halting batch due to error")
                    break
    else:
        # Parallel processing
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_url = {
                executor.submit(
                    process_game,
                    game_url=url,
                    validate=validate,
                    store=store,
                    min_accuracy=min_accuracy,
                    halt_on_validation_failure=False,  # Don't halt in parallel mode
                    skip_if_exists=skip_if_exists
                ): url for url in game_urls
            }
            
            for future in as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    result = future.result()
                    
                    # Track results (same logic as sequential)
                    if result["processing_status"] == "skipped":
                        results["skipped"] += 1
                    elif result["processing_status"] == "success":
                        results["processed"] += 1
                        if result.get("stored", False):
                            results["stored"] += 1
                    elif result["processing_status"] == "validation_failed":
                        results["validation_failures"] += 1
                    else:
                        results["failed"] += 1
                    
                    results["games"].append({
                        "game_url": url,
                        "game_id": result.get("game_id"),
                        "status": result["processing_status"],
                        "stored": result.get("stored", False)
                    })
                    
                except Exception as e:
                    logger.error(f"Error processing {url}: {e}")
                    results["failed"] += 1
                    results["errors"].append({
                        "game_url": url,
                        "error": str(e)
                    })
    
    # Summary
    elapsed = time.time() - start_time
    results["elapsed_time"] = elapsed
    results["avg_time_per_game"] = elapsed / len(game_urls) if game_urls else 0
    
    # Calculate average accuracies
    results["avg_batting_accuracy"] = (
        sum(results["batting_accuracies"]) / len(results["batting_accuracies"])
        if results["batting_accuracies"] else 0.0
    )
    results["avg_pitching_accuracy"] = (
        sum(results["pitching_accuracies"]) / len(results["pitching_accuracies"])
        if results["pitching_accuracies"] else 0.0
    )
    results["overall_accuracy"] = (
        (results["avg_batting_accuracy"] + results["avg_pitching_accuracy"]) / 2
        if (results["batting_accuracies"] or results["pitching_accuracies"]) else 0.0
    )
    
    # Save CSV report if requested
    if save_csv_report and results["games"]:
        csv_files = _save_csv_reports(results, output_dir)
        results["csv_files"] = csv_files
    
    logger.info("=" * 80)
    logger.info("BATCH PROCESSING COMPLETE")
    logger.info(f"Total: {results['total_games']} games")
    logger.info(f"Processed: {results['processed']}")
    logger.info(f"Stored: {results['stored']}")
    logger.info(f"Skipped: {results['skipped']}")
    logger.info(f"Failed: {results['failed']}")
    logger.info(f"Validation failures: {results['validation_failures']}")
    logger.info(f"Avg Batting Accuracy: {results['avg_batting_accuracy']:.2f}%")
    logger.info(f"Avg Pitching Accuracy: {results['avg_pitching_accuracy']:.2f}%")
    logger.info(f"Overall Accuracy: {results['overall_accuracy']:.2f}%")
    logger.info(f"Time: {elapsed:.1f}s ({results['avg_time_per_game']:.1f}s/game)")
    logger.info("=" * 80)
    
    return results


def _save_csv_reports(results: Dict, output_dir: str) -> Dict[str, str]:
    """Save detailed CSV reports from batch results"""
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Detailed game-by-game report
    report_data = []
    for game in results["games"]:
        validation = game.get("validation", {})
        batting = validation.get("batting", {})
        pitching = validation.get("pitching", {})
        
        report_data.append({
            "game_id": game.get("game_id", "unknown"),
            "game_url": game.get("game_url", ""),
            "status": game.get("status", "unknown"),
            "stored": game.get("stored", False),
            "batting_accuracy": batting.get("accuracy", 0.0),
            "batting_players": batting.get("players_compared", 0),
            "batting_differences": batting.get("total_differences", 0),
            "pitching_accuracy": pitching.get("accuracy", 0.0),
            "pitching_players": pitching.get("players_compared", 0),
            "pitching_differences": pitching.get("total_differences", 0)
        })
    
    df = pd.DataFrame(report_data)
    detailed_file = os.path.join(output_dir, f"batch_report_{timestamp}.csv")
    df.to_csv(detailed_file, index=False)
    logger.info(f"📄 CSV report saved: {detailed_file}")
    
    # Summary file
    summary_file = os.path.join(output_dir, f"batch_summary_{timestamp}.txt")
    with open(summary_file, 'w') as f:
        f.write(f"MLB Batch Processing Summary - {timestamp}\n")
        f.write("=" * 50 + "\n\n")
        f.write(f"Games Processed: {results['total_games']}\n")
        f.write(f"Successful: {results['processed']}\n")
        f.write(f"Stored: {results['stored']}\n")
        f.write(f"Skipped: {results['skipped']}\n")
        f.write(f"Failed: {results['failed']}\n")
        f.write(f"Validation Failures: {results['validation_failures']}\n\n")
        f.write(f"Time: {results['elapsed_time']:.1f}s\n")
        f.write(f"Avg per game: {results['avg_time_per_game']:.1f}s\n")
    
    logger.info(f"📄 Summary saved: {summary_file}")
    
    return {
        "detailed_report": detailed_file,
        "summary_file": summary_file
    }


def process_season(
    year: int,
    validate: bool = True,
    store: bool = True,
    min_accuracy: float = 100.0,
    skip_if_exists: bool = True,
    max_workers: int = 1,
    save_csv_report: bool = False
) -> Dict:
    """
    Process all games for a season.
    
    Args:
        year: Season year (e.g., 2024)
        validate: Whether to validate stats
        store: Whether to store to database
        min_accuracy: Minimum validation accuracy required
        skip_if_exists: Skip games already in database
        max_workers: Number of parallel workers
        save_csv_report: Whether to save CSV reports
        
    Returns:
        Dict with season processing results
    """
    logger.info(f"Fetching game URLs for {year} season")
    
    try:
        game_urls = get_games_full_season(year)
        logger.info(f"Found {len(game_urls)} games for {year}")
        
        if not game_urls:
            logger.warning(f"No games found for {year}")
            return {
                "year": year,
                "total_games": 0,
                "status": "no_games_found"
            }
        
        # Process the batch
        results = process_batch(
            game_urls=game_urls,
            validate=validate,
            store=store,
            min_accuracy=min_accuracy,
            skip_if_exists=skip_if_exists,
            max_workers=max_workers,
            halt_on_failure=False,  # Don't halt on failure for full season
            save_csv_report=save_csv_report
        )
        
        results["year"] = year
        
        return results
        
    except Exception as e:
        logger.error(f"Failed to process {year} season: {e}")
        return {
            "year": year,
            "status": "error",
            "error": str(e)
        }


def print_batch_summary(results: Dict):
    """Print a formatted summary of batch results"""
    print("\n" + "=" * 80)
    print("BATCH PROCESSING SUMMARY")
    print("=" * 80)
    
    if "year" in results:
        print(f"Season: {results['year']}")
    
    print(f"\nTotal games: {results['total_games']}")
    print(f"✅ Processed: {results['processed']}")
    print(f"💾 Stored: {results['stored']}")
    print(f"⏭️  Skipped: {results['skipped']}")
    print(f"❌ Failed: {results['failed']}")
    print(f"⚠️  Validation failures: {results['validation_failures']}")
    
    # Accuracy statistics
    if results.get('avg_batting_accuracy') is not None:
        print(f"\n📊 Validation Accuracy:")
        print(f"   Batting:  {results['avg_batting_accuracy']:.2f}%")
        print(f"   Pitching: {results['avg_pitching_accuracy']:.2f}%")
        print(f"   Overall:  {results['overall_accuracy']:.2f}%")
        
        print(f"\n🔍 Discrepancies:")
        print(f"   Total batting diffs: {results.get('total_batting_diffs', 0)}")
        print(f"   Total pitching diffs: {results.get('total_pitching_diffs', 0)}")
        print(f"   Games with batting diffs: {results.get('games_with_batting_diffs', 0)}")
        print(f"   Games with pitching diffs: {results.get('games_with_pitching_diffs', 0)}")
    
    if results.get('elapsed_time'):
        print(f"\n⏱️  Time: {results['elapsed_time']:.1f}s")
        print(f"   Avg: {results['avg_time_per_game']:.1f}s per game")
    
    if results.get('errors'):
        print(f"\n❌ Errors ({len(results['errors'])}):")
        for error in results['errors'][:5]:  # Show first 5
            print(f"   - {error['game_url']}: {error['error']}")
        if len(results['errors']) > 5:
            print(f"   ... and {len(results['errors']) - 5} more")
    
    print("=" * 80 + "\n")


if __name__ == "__main__":
    # Example usage
    import sys
    
    # Configure logging only once, and prevent duplicates
    root_logger = logging.getLogger()
    if not root_logger.handlers:
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
    
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python batch_processor.py <year>")
        print("\nExamples:")
        print("  python batch_processor.py 2024")
        print("  python batch_processor.py 2025")
        sys.exit(1)
    
    year = int(sys.argv[1])
    
    # Process season
    results = process_season(
        year=year,
        validate=True,
        store=True,
        min_accuracy=100.0,
        skip_if_exists=True,
        max_workers=1,  # Sequential for now
        save_csv_report=True
    )

    # Print formatted summary
    print_batch_summary(results)
    
