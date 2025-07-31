from typing import List
import pandas as pd
import numpy as np

def get_prev_new_season_data(prev_year, new_year, conn, transfer = True):
    
    transfer_query = "AND ps1.team_name != ps2.team_name"

    query = f"""SELECT
                       p.player_name,
                       ps1.position,                       
                       CASE
                           WHEN ps2.min_pg < 10 THEN 'bench'
                           WHEN ps2.min_pg < 25 THEN 'rotation'
                           ELSE 'starter'
                       END AS new_role,                       
                       ps1.bpm AS prev_bpm,
                       ps2.bpm AS new_bpm,
                       -- ps1.min_pg AS prev_min_pg,
                       -- ps2.min_pg AS new_min_pg,
                       ps1.ts_percent AS prev_ts,
                       ps2.ts_percent AS new_ts,
                       ((ps1.dreb_pg * ps1.adj_gp * 100) / ps1.POSS) AS prev_dreb_per_100,                                              
                       ((ps2.dreb_pg * ps2.adj_gp * 100) / ps2.POSS) AS new_dreb_per_100,
                       ((ps1.oreb_pg * ps1.adj_gp * 100) / ps1.POSS) AS prev_oreb_per_100,
                       ((ps2.treb_pg * ps2.adj_gp * 100) / ps2.POSS) AS new_oreb_per_100,
                       ps1.ast_tov_r AS prev_ast_tov_r,
                       ps2.ast_tov_r AS new_ast_tov_r,
                       ps1.adrtg AS prev_adjde,
                       ps2.adrtg AS new_adjde
                   FROM Player_Seasons ps1
                   JOIN Player_Seasons ps2 ON ps1.player_id = ps2.player_id
                   JOIN Players p ON p.player_id = ps1.player_id  
                   WHERE ps1.season_year = ? AND ps2.season_year = ? {transfer_query if transfer else ""}              
                 """

    df = pd.read_sql(query, conn, params=(prev_year, new_year))
    
    return df

## TESTING A CHANGE

def is_successful_transfer(prev_year, new_year, conn):
    """
    dataframe should have incoming metrics for year and end of year metrics
    They should be:
        - player name
        - fit
            - bpm (5%)
            - min per game (non decreasing)
        - value/production -> need to be adjusted for team playstyle and opportunity
            - ts% (non decreasing)
            - treb per 100 (non decreasing)
            - ast_tov_r (non decreasing)
            - adjde (non decreasing)
    """
    ps = get_prev_new_season_data(prev_year, new_year, conn)
    BUFFER = 0.08

    def score_row(row):
        score = 0
        metric_weights = []

        # bpm
        if row['new_bpm'] >= row['prev_bpm'] * (1 - BUFFER):
            metric_weights.append(1.1)

        # ts%
        if row['new_ts'] >= row['prev_ts'] * (1 - BUFFER):
            metric_weights.append(1.1)

        # dreb per 100
        dreb_weight = 1.0
        if row['position'] in ('F', 'C'):
            dreb_weight *= 1.2
        if row['new_dreb_per_100'] >= row['prev_dreb_per_100'] * (1 - BUFFER):
            metric_weights.append(dreb_weight)

        # oreb per 100
        oreb_weight = 1.0
        if row['position'] in ('F', 'C'):
            oreb_weight *= 1.2
        if row['new_oreb_per_100'] >= row['prev_oreb_per_100'] * (1 - BUFFER):
            metric_weights.append(oreb_weight)

        # ast/tov ratio
        ast_tov_weight = 1.0
        if row['position'] == 'G':
            ast_tov_weight *= 1.2
        if row['new_ast_tov_r'] >= row['prev_ast_tov_r'] * (1 - BUFFER):
            metric_weights.append(ast_tov_weight)

        # defensive rating (lower is better)
        if row['new_adjde'] <= row['prev_adjde'] * (1 + BUFFER):
            metric_weights.append(1.1)

        # Normalize score
        score = sum(metric_weights) / 6.6  # Normalize so full credit ~1
        return score

    ps['success_score'] = ps.apply(score_row, axis=1)

    # Optional: define binary label for model evaluation
    ps['successful'] = ps['success_score'] >= 0.75

    return ps

def successful_transfer(plyr_clusters: list, team_clusters: list, plyr_stats: pd.Series, all_plyr_stats, plyr_weights: list, cluster_weights: list, debug : bool = False):
    """
    plyr_stats -> should have players name and the players stats from the current year (the year after playing)
    all_plyr_stats -> data frame of all players stats from the previous season of current plyr_stats 
        and the team and player clusters they were mapped to
    """

    META = ["player_name", "team_name", "season_year", "player_id", "position", 'player_id']
    combined_df = pd.DataFrame()
    for p_cluster, t_cluster, p_weight, t_weight in zip(plyr_clusters, team_clusters, plyr_weights, cluster_weights):
        subset = all_plyr_stats[(all_plyr_stats['team_cluster'] == t_cluster) & (all_plyr_stats['Cluster'] == p_cluster)]
        if not subset.empty:
            weighted_subset = subset.copy()
            weighted_subset['weight'] = p_weight * t_weight
            combined_df = pd.concat([combined_df, weighted_subset], ignore_index=True)
    if combined_df.empty:
        return (0.0, False)

    columns = [col for col in combined_df.columns if col not in META + ["team_cluster", "Cluster", "weight"]]
    stats_columns = columns

    mean_vals = pd.Series(index=stats_columns, dtype=float)
    std_vals = pd.Series(index=stats_columns, dtype=float)
    for col in stats_columns:
        vals = combined_df[col].dropna()
        wts = combined_df.loc[vals.index, 'weight']
        if vals.empty:
            mean_vals[col] = 0
            std_vals[col] = 1
        else:
            mean_val = np.average(vals, weights=wts)
            mean_vals[col] = mean_val
            std_vals[col] = np.sqrt(np.average((vals - mean_val)**2, weights=wts))
            if std_vals[col] == 0:
                std_vals[col] = 1

    plyr_vals = plyr_stats[stats_columns]

    # --- Effective Sample Size (ESS) check ---
    def effective_sample_size(weights):
        weights = np.array(weights)
        if weights.sum() == 0:
            return 0
        return (weights.sum()**2) / (weights**2).sum()

    ess = effective_sample_size(combined_df['weight'])

    # Define impact weights for key stats
    impact_weights = {
        'dporpag': 1.2,
        'porpag': 1.2,
        'ts_percent': 1.1
    }

    # Compute weighted z-score sum
    flipped_stats_lst = ['tov_percent', 'adjde', 'dporpag']
    score_sum = 0.0
    weight_sum = 0.0
    for col in stats_columns:
        # raw z-score relative to mean
        z = (plyr_vals[col] - mean_vals[col]) / std_vals[col]
        # invert for metrics where lower is better
        if col in flipped_stats_lst:
            z = -z
        weight = impact_weights.get(col, 1.0)
        score_sum += z * weight
        weight_sum += weight

    # Normalize to final score
    score = score_sum / weight_sum

    # Debug: log large deviations
    if debug:
        for col in stats_columns:
            dev = (plyr_vals[col] - mean_vals[col]) / std_vals[col]
            if col in flipped_stats_lst:
                dev *= -1
            if abs(dev) >= 0.75:
                print(f"{col} deviation: {dev:.2f} SDs from mean")

    THRESHOLD = -0.05
    is_successful = score > THRESHOLD
    return (score, is_successful, ess)
    



def testing():
    import sqlite3

    conn = sqlite3.connect('rosteriq.db')

    df = is_successful_transfer(2021, 2022, conn)

    import matplotlib.pyplot as plt

    df = is_successful_transfer(2021, 2022, conn)

    # Plot histogram of success scores
    plt.hist(df['success_score'], bins=20, edgecolor='black')
    plt.title("Distribution of Success Scores (2021–2022 Transfers)")
    plt.xlabel("Success Score")
    plt.ylabel("Number of Players")
    plt.grid(True)
    plt.show()

# testing()