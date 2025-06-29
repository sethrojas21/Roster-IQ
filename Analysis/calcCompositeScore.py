import sqlite3
import pandas as pd
from calcFitScore import calculate_fs_teamYear
from calcVOCRP import calculate_VOCRP_teamYear

def composite_ranking_percentiles(fs_df, vocrp_df):
    # Merge on player_name (or player_id if that’s more reliable)
    df = fs_df.merge(vocrp_df, on='player_name')

    # Compute percentiles (0–1c) for each metric
    df['fit_pct']   = df['sim_score'].rank(pct=True)
    df['value_pct'] = df['vocrp'].rank(pct=True)

    # Build a composite score (sum of percentiles)
    df['composite_score'] = df['fit_pct'] + df['value_pct']

    # Sort by composite descending
    df_sorted = df.sort_values('composite_score', ascending=False)
    df_sorted = df_sorted.reset_index(drop=True)

    return df_sorted


def composite_score(conn, team_name, season_year, player_id_to_replace):
    fs_df = calculate_fs_teamYear(conn, team_name, season_year, player_id_to_replace)
    vocrp_df = calculate_VOCRP_teamYear(conn, team_name, season_year, player_id_to_replace)
    cs_df = composite_ranking_percentiles(fs_df, vocrp_df)
    
    return cs_df
