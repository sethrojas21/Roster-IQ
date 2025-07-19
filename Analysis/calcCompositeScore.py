import sqlite3
import pandas as pd
from calcFitScore import calculate_fs_teamYear
from calcVOCRP import calculate_VOCRP_teamYear
from calcMetricHelpers import *

def composite_ranking_percentiles(fs_df, vocrp_df):
    # Merge on player_name (or player_id if that’s more reliable)
    try:
        df = fs_df.merge(vocrp_df, on='player_name')

        # Compute percentiles (0–1c) for each metric
        df['fit_pct']   = df[f'archs_sim_score'].rank(pct=True)
        df['value_pct'] = df['vocbp'].rank(pct=True)

        # Build a composite score (sum of percentiles)
        df['composite_score'] = df['fit_pct'] + df['value_pct']

        # Sort by composite descending
        df_sorted = df.sort_values('composite_score', ascending=False)
        df_sorted = df_sorted.reset_index(drop=True)
    except Exception as e:
        print(e)

    return df_sorted


def composite_score(conn, team_name, season_year, player_id_to_replace):
    try:       
        fs_df = calculate_fs_teamYear(conn, team_name, season_year, player_id_to_replace)
        vocrp_df = calculate_VOCRP_teamYear(conn, 
                                            team_name, 
                                            season_year, 
                                            player_id_to_replace)
        cs_df = composite_ranking_percentiles(fs_df, vocrp_df)
    except Exception as e:
        print(e)
    
    return cs_df

def testing():
    conn = sqlite3.connect('rosteriq.db')
    team = "Arizona"
    player_name = "Caleb Love"
    year = 2024
    id = 72413
    cs_df = composite_score(conn, team, year, id)
    print(cs_df.head(25))
    print(cs_df[cs_df['player_name'] == player_name])
    print(cs_df[cs_df['player_name'] == "Steven Ashworth"])

# testing()