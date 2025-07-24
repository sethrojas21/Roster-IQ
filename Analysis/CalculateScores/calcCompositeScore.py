import sqlite3
import pandas as pd
from Analysis.CalculateScores.calcFitScore import calculate_fit_score_from_transfers
from Analysis.CalculateScores.calcVOCRP import calculate_vocbp_from_transfers
from Analysis.Helpers.init import InitBenchmarkPlayer

def composite_ranking_percentiles(fs_df, vocrp_df, fs_w = 0.6, v_w = 0.4, sortBy = 'composite_score'):
    # Merge on player_name (or player_id if that’s more reliable)
    try:
        df = fs_df.merge(vocrp_df, on='player_name')

        # Compute percentiles (0–1c) for each metric
        df['fit_pct']   = df[f'sim_score'].rank(pct=True)
        df['value_pct'] = df['vocbp'].rank(pct=True)

        # Build a composite score (sum of percentiles)
        df['composite_score'] = df['fit_pct'] * fs_w + df['value_pct'] * v_w

        # Sort by composite descending
        df_sorted = df.sort_values(sortBy, ascending=False)
        df_sorted = df_sorted.reset_index(drop=True)
    except Exception as e:
        print(e)

    return df_sorted


def composite_score(conn, team_name, season_year, player_id_to_replace):
    """
    Returns the benchmark player information and the rankings from the players inputted
    """
    bmark_plyr = InitBenchmarkPlayer(conn, team_name, season_year, player_id_to_replace)

    fs_df = calculate_fit_score_from_transfers(bmark_plyr, sort=False)
    vocbp_df = calculate_vocbp_from_transfers(bmark_plyr, sort=False)
    cs_df = composite_ranking_percentiles(fs_df, vocbp_df)
    
    return bmark_plyr, cs_df

def testing():
    conn = sqlite3.connect('rosteriq.db')
    team = "Arizona"

    player_name = "Caleb Love"
    year = 2024
    id = 72413
    bmark_plyr, cs_df = composite_score(conn, team, year, id)
    print(cs_df.head(25))
    print(cs_df[cs_df['player_name'] == player_name])

    print(bmark_plyr.plyr_labels)
    print(bmark_plyr.team_labels)

# testing()