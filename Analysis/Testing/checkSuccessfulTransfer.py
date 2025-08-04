import sqlite3
import pandas as pd
import random
import logging
import os
from logging.handlers import RotatingFileHandler
from typing import Dict, Tuple
from Analysis.CalculateScores.calcCompositeScore import composite_score
from Analysis.EvaluateMetrics.successful_transfer import successful_transfer
from Analysis.config import Config
from Analysis.Helpers.queries import all_players_query, single_player_query


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


def load_player_data_by_year(conn: sqlite3.Connection, years_range: range, positions: list) -> Dict[int, Dict[str, pd.DataFrame]]:
    """Load and merge player data for all years and positions."""
    logger = logging.getLogger(__name__)
    plyr_df_dict = {}
    
    for year in years_range:
        plyr_df_dict[year] = {}
        team_df = pd.read_csv(f'Analysis/Clustering/Teams/{year}/KClustering/labels.csv')
        logger.info(f"Loading data for year {year}")
        
        for pos in positions:
            # Load player-team data from database
            plyr_team_df = pd.read_sql(all_players_query(pos), 
                                       conn,
                                       params=(year - 3, year))
            
            # Load player clustering data from CSV
            plyr_df = pd.read_csv(f'Analysis/Clustering/Players/{year}/KClustering/player_labels_{pos}.csv')
            
            # Merge player data with team data
            merged_plyr_df = pd.merge(plyr_df, plyr_team_df, on=['player_name', 'season_year', 'team_name'])
            merged_df = pd.merge(merged_plyr_df, team_df, on=['team_name', 'season_year'])
            
            plyr_df_dict[year][pos] = merged_df
    
    return plyr_df_dict


def load_team_data(conn: sqlite3.Connection) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Load available teams and barthag ranking data."""
    # Load available transfer teams
    avail_team_df = pd.read_csv('/Users/sethrojas/Documents/CodeProjects/BAResearch/Analysis/Helpers/CSV/availTransferTeams.csv')
    
    # Load team barthag rankings
    all_teams_barthag = pd.read_sql("SELECT team_name, season_year, barthag_rank FROM Team_Seasons GROUP BY team_name, season_year", conn)
    top_teams_barthag = all_teams_barthag[all_teams_barthag['barthag_rank'] <= 90]
    top_teams_df = pd.merge(avail_team_df, top_teams_barthag, on=['team_name', 'season_year'], how='left').dropna()
    sampled_teams = avail_team_df.sample(n=1500, random_state=random.randint(1, 100))
    
    return avail_team_df, all_teams_barthag, top_teams_df, sampled_teams


def main():
    """Main function to run the transfer success analysis."""
    # Setup logging first
    logger = setup_logging()
    logger.info("Starting transfer success analysis")
    
    conn = sqlite3.connect('rosteriq.db')
    positions = Config.POSITIONS
    years_range = range(Config.START_YEAR, Config.END_YEAR_EXCLUDE)
    
    TOP_PERCENT = Config.TOP_PERCENT
    BOTTOM_PERCENT = Config.BOTTOM_PERCENT
    
    # Load all data
    plyr_df_dict = load_player_data_by_year(conn, years_range, positions)
    avail_team_df, all_teams_barthag, top_teams_df, sampled_teams = load_team_data(conn)

    logger.info("Starting to iterate over transfers")
    correct = 0
    succ_count = 0
    unsucc_count = 0
    total = 0
    for idx, avail_team in avail_team_df.iterrows():
        team_name = avail_team['team_name']

        season_year = avail_team['season_year']
        player_id_to_replace = avail_team['player_id']  
        player_name = conn.execute("SELECT player_name FROM Players WHERE player_id = ?", (int(player_id_to_replace),)).fetchone()[0]
        position = conn.execute("SELECT position FROM Player_Seasons WHERE player_id = ? AND season_year = ?",
                                (player_id_to_replace, season_year)).fetchone()[0] 
        if team_name not in ["Arizona"]:
            continue
        logger.info(f"Processing: {player_name} ({position}) - {team_name} {season_year} [ID: {player_id_to_replace}]")
        try:
            bmakr_plyr, cs_df = composite_score(conn, team_name, season_year, player_id_to_replace, specific_name=player_name)
        except ValueError as e:
            logger.error(e)
            continue
        plyr_cluster_id = bmakr_plyr.plyr_ids
        team_cluster_id = bmakr_plyr.team_ids
        
        plyr_query = single_player_query(position)

        plyr_stats = pd.read_sql(plyr_query, 
                                 conn, 
                                 params = (season_year, player_id_to_replace)).iloc[0]
        
        plyr_pos_stats = plyr_df_dict[season_year][position]
        try:
            score, is_succ, ess = successful_transfer(plyr_cluster_id,
                                                 team_cluster_id,
                                                 plyr_stats,
                                                 plyr_pos_stats,
                                                 bmakr_plyr.plyr_weights,
                                                 bmakr_plyr.team_weights,
                                                 debug=True)
            logger.debug(f"ESS Score: {ess}")
        except Exception as e:
            logger.error(f"Error in successful_transfer calculation: {e}", exc_info=True)

        # ESS Cut-off
        if True and ess <= Config.ESS_THRESHOLD:
            logger.warning("ESS Sample below {Config.ESS_THRESHOLD} - caution")
        
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
ESS: {ess}
Position: {position}
Rank: {rank}/{length}
B-Mark Sample Size: {bmakr_plyr.length}
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
            total += 1 
            correct += 1
            logger.info("✓ CORRECT - Successful and Ranked High")
            succ_count += 1
        elif unsuccessCond:
            total += 1
            correct += 1
            logger.info("✓ CORRECT - Unsuccessful and Ranked Low")
            unsucc_count += 1
        elif not successPercentile and not is_succ:
            logger.debug("No classification needed")
        else:
            total += 1
            logger.warning("✗ INCORRECT - Prediction mismatch")

        try:
            accuracy = correct / total if total > 0 else 0
            logger.info(f"Running Stats: {correct}/{total} = {accuracy:.3f} accuracy")
        except ZeroDivisionError as e:
            logger.error(f"Division by zero error: {e}")
        logger.debug("-" * 20)

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


if __name__ == "__main__":
    main()




