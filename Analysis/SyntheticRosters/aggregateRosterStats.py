import pandas as pd
import numpy as np

# --- Aggregation function for team stats from player-level DataFrame ---
def aggregate_team_stats_from_players_df(df, roleModifier = False):
    """
    Given a DataFrame of player stats with columns:
    FGA, FGM, FTA, TOV, STL, OREB, DREB, P3M, P3A, adjoe, adjde, and any other stats,
    compute team-level aggregated metrics analogous to clusterTeams.r.
    Returns a dict with the aggregated stats.
    """
    # compute possessions per player
    df = df.copy()
    # apply role_modifier to each player's stats to forecast next-season impact
    stat_cols = ['adjoe','FGM','P3M','FGA','P3A','TOV','OREB','DREB','STL']
    if roleModifier:    
        df[stat_cols] = df[stat_cols].multiply(df['role_modifier'], axis=0)
        # compute inverse modifier, but never below 1
        inv_mod = (1 / df['role_modifier']).clip(lower=1.0)
    else:
        inv_mod = 1
    # apply to adjde
    df['adjde'] = df['adjde'] * inv_mod
    df['poss'] = df['FGA'] + 0.44 * df['FTA'] + df['TOV'] - df['OREB']
    total_poss = df['poss'].sum()    
    # weighted mean of adjoe and adjde by possessions
    team_adjoe = np.average(df['adjoe'], weights=df['poss']) if total_poss else np.nan
    team_adjde = np.average(df['adjde'], weights=df['poss']) if total_poss else np.nan
    # turnover-to-steal ratio
    team_stltov_ratio = df['TOV'].sum() / df['STL'].sum() if df['STL'].sum() else np.nan
    # offensive and defensive rebound rates per 100 possessions
    team_oreb_per100 = df['OREB'].sum() / total_poss * 100 if total_poss else np.nan
    team_dreb_per100 = df['DREB'].sum() / total_poss * 100 if total_poss else np.nan
    # effective field goal percentage
    team_eFG = (df['FGM'].sum() + 0.5 * df['P3M'].sum()) / df['FGA'].sum() if df['FGA'].sum() else np.nan
    # combined 3pt‐FGA metric from R code
    team_3pt_fga = 3 * (df['P3M'].sum() / df['FGA'].sum()) * (df['P3A'].sum() / df['FGA'].sum())    
    
    return {
        'team_adjoe': team_adjoe,
        'team_adjde': team_adjde,
        'team_stltov_ratio': team_stltov_ratio,
        'team_oreb_per100': team_oreb_per100,
        'team_dreb_per100': team_dreb_per100,
        'team_eFG': team_eFG,           
    }