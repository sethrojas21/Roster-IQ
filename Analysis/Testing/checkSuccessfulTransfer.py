import sqlite3
import pandas as pd
import random
import logging
import os
import signal
import sys
from logging.handlers import RotatingFileHandler
from typing import Dict, Tuple
from Analysis.CalculateScores.calcCompositeScore import composite_score
from Analysis.EvaluateMetrics.successful_transfer import successful_transfer
from Analysis.config import Config
from Analysis.Helpers.queries import single_player_query


# Global variables to track results (accessible by signal handler)
STATS = {
    'correct': 0,
    'total': 0,
    'succ_count': 0,
    'unsucc_count': 0,
    'logger': None,
    'TOP_PERCENT': Config.TOP_PERCENT,
    'BOTTOM_PERCENT': Config.BOTTOM_PERCENT,
    'BREAKOUT_NUMBER': Config.BREAKOUT_NUMBER
}

def print_final_results():
    """Print final results - can be called from signal handler or normal completion."""
    if STATS['logger'] is None:
        return
    
    logger = STATS['logger']
    correct = STATS['correct']
    total = STATS['total']
    succ_count = STATS['succ_count']
    unsucc_count = STATS['unsucc_count']
    TOP_PERCENT = STATS['TOP_PERCENT']
    BOTTOM_PERCENT = STATS['BOTTOM_PERCENT']
    
    logger.info("="*50)
    logger.info("PRINTING FINAL RESULTS")
    logger.info("="*50)
    
    # Final results
    accuracy = correct / total if total > 0 else 0
    logger.info(f"FINAL RESULTS: {correct} correct out of {total} total ({accuracy:.3f} accuracy rate)")
    logger.info(f"Successful transfers: {succ_count}")
    logger.info(f"Unsuccessful transfers: {unsucc_count}")
    
    total_succs = succ_count + unsucc_count
    if total_succs > 0:
        pSucc = succ_count / total_succs
        pUnscc = unsucc_count / total_succs
        logger.info(f"Success rate: {pSucc:.3f}")
        logger.info(f"Unsuccessful rate: {pUnscc:.3f}")
        chance_percentage = pSucc * TOP_PERCENT + pUnscc * BOTTOM_PERCENT
        logger.info(f"Chance percentage: {chance_percentage:.3f}")
    
    logger.info("Transfer success analysis completed")

def signal_handler(sig, frame):
    """Handle Ctrl+C gracefully by printing final results before exiting."""
    if STATS['logger']:
        STATS['logger'].warning("Received interrupt signal - printing final results...")
        print_final_results()
    sys.exit(0)


def setup_logging():
    """Setup rotating logs that keep only the last 3 runs."""
    # Create logs directory if it doesn't exist
    os.makedirs('logs', exist_ok=True)
    
    # Rotating file handler - keeps only 3 files (last 3 runs)
    file_handler = RotatingFileHandler(
        'logs/log1.log',
        maxBytes=50*1024*1024,  # 50MB per file
        backupCount=2  # Keep 2 backup files + current = 3 total
    )
    
    # Console handler for immediate feedback
    console_handler = logging.StreamHandler()
    
    # Formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
    )
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    # Setup root logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

def load_team_data(conn: sqlite3.Connection) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Load available teams and barthag ranking data."""
    # Load available transfer teams
    avail_team_df = pd.read_csv('/Users/sethrojas/Documents/CodeProjects/BAResearch/Analysis/Helpers/CSV/availTransferTeams.csv')
    
    # Load team barthag rankings
    all_teams_barthag = pd.read_sql("SELECT team_name, season_year, barthag_rank FROM Team_Seasons GROUP BY team_name, season_year", conn)
    top_teams_barthag = all_teams_barthag[all_teams_barthag['barthag_rank'] <= 90]
    top_teams_df = pd.merge(avail_team_df, top_teams_barthag, on=['team_name', 'season_year'], how='left').dropna()
    top_teams_df = top_teams_df[top_teams_df['barthag_rank'] <= 90]    
    SAMPLE_SIZE = 1000
    sampled_teams = avail_team_df.sample(n=SAMPLE_SIZE, random_state=random.randint(1, 100))
    return avail_team_df, all_teams_barthag, top_teams_df, sampled_teams


def main():
    """Main function to run the transfer success analysis."""
    # Setup logging first
    logger = setup_logging()
    logger.info("Starting transfer success analysis")
    
    # Store logger in global stats and setup signal handler
    STATS['logger'] = logger
    signal.signal(signal.SIGINT, signal_handler)
    
    conn = sqlite3.connect('rosteriq.db')
    
    TOP_PERCENT = Config.TOP_PERCENT
    BOTTOM_PERCENT = Config.BOTTOM_PERCENT
    BREAKOUT_NUMBER = Config.BREAKOUT_NUMBER

    # Update global stats with config values
    STATS['TOP_PERCENT'] = TOP_PERCENT
    STATS['BOTTOM_PERCENT'] = BOTTOM_PERCENT
    STATS['BREAKOUT_NUMBER'] = BREAKOUT_NUMBER

    # Load all data
    avail_team_df, all_teams_barthag, top_teams_df, sampled_teams = load_team_data(conn)

    logger.info("Starting to iterate over transfers")
    
    try:
        for idx, avail_team in top_teams_df.iterrows():
            team_name = avail_team['team_name']

            season_year = avail_team['season_year']
            player_id_to_replace = avail_team['player_id']  
            player_name = conn.execute("SELECT player_name FROM Players WHERE player_id = ?", (int(player_id_to_replace),)).fetchone()[0]
            position = conn.execute("SELECT position FROM Player_Seasons WHERE player_id = ? AND season_year = ?",
                                    (player_id_to_replace, season_year)).fetchone()[0] 
            # if team_name not in ["Kansas"]:
            #     continue
            logger.info(f"Processing: {player_name} ({position}) - {team_name} {season_year} [ID: {player_id_to_replace}]")
            try:
                bmakr_plyr, cs_df = composite_score(conn, team_name, season_year, player_id_to_replace, specific_name=player_name, debug=False)
            except ValueError as e:
                logger.error(e)
                continue
            
            plyr_query = single_player_query(position)

            plyr_stats = pd.read_sql(plyr_query, 
                                     conn, 
                                     params = (season_year, player_id_to_replace)).iloc[0]
            
            try:
                score, is_succ = successful_transfer(bmakr_plyr, plyr_stats=plyr_stats, debug=False)
                ess = bmakr_plyr.ess
                logger.debug(f"ESS Score: {ess}")
            except Exception as e:
                logger.error(f"Error in successful_transfer calculation: {e}", exc_info=True)

            # ESS Cut-off
            if ess < Config.ESS_THRESHOLD:
                logger.warning(f"ESS Sample below {Config.ESS_THRESHOLD} - caution")
                continue

            try:
                rank = cs_df[cs_df['player_name'] == player_name].index[0]
            except:
                logger.warning("Skipping because player was not here last season")
                logger.debug("-" * 10)
                continue

            length = len(cs_df)
            successPercentile = (rank <= length * TOP_PERCENT)
            unsuccessPercentile = (rank >= length * (1 - BOTTOM_PERCENT))
            successCond = successPercentile and is_succ
            unsuccessCond = unsuccessPercentile and not is_succ

            # Log detailed analysis (using DEBUG level for verbose output)
            logger.info(f"Top 5 players:\n{cs_df.head(5)}")
            logger.info(f"Player context:\n{cs_df.iloc[rank - 2 : rank + 2]}")
            
            logger.info(f"""Analysis Results:
Position: {position}
Rank: {rank}/{length}
B-Mark ESS: {ess}
Player Archetype(s): {bmakr_plyr.plyr_labels}
Player Weight(s): {bmakr_plyr.plyr_weights}
Team Archetype(s): {bmakr_plyr.team_labels}
Team Weight(s): {bmakr_plyr.team_weights}
Percentile Rank: {1 - (rank / length):.3f}
Projected Top {TOP_PERCENT*100}%: {successPercentile}
Projected Bottom {BOTTOM_PERCENT*100}%: {unsuccessPercentile}
Considered Success: {is_succ}
Success Score: {score}""")
            
            if successCond:
                STATS['total'] += 1 
                STATS['correct'] += 1
                logger.info("✓ CORRECT - Successful and Ranked High")
                STATS['succ_count'] += 1
            elif unsuccessCond:
                STATS['total'] += 1
                STATS['correct'] += 1
                logger.info("✓ CORRECT - Unsuccessful and Ranked Low")
                STATS['unsucc_count'] += 1
            elif not successPercentile and not is_succ:
                logger.debug("No classification needed")
            else:
                STATS['total'] += 1
                logger.warning("✗ INCORRECT - Prediction mismatch")

            if STATS['total'] == BREAKOUT_NUMBER:
                logger.info(f"Reached {BREAKOUT_NUMBER} samples, stopping...")
                break

            try:
                accuracy = STATS['correct'] / STATS['total'] if STATS['total'] > 0 else 0
                logger.info(f"Running Stats: {STATS['correct']}/{STATS['total']} = {accuracy:.3f} accuracy")
            except ZeroDivisionError as e:
                logger.error(f"Division by zero error: {e}")
            logger.debug("-" * 20)

    except Exception as e:
        logger.error(f"Unexpected error occurred: {e}", exc_info=True)
    finally:
        # Always print final results, regardless of how we got here
        print_final_results()


if __name__ == "__main__":
    main()




