import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.metrics.pairwise import cosine_similarity
from Analysis.Helpers.standardization import scale_player_stats

def get_player_similarity_score(
        player_stats_df: pd.DataFrame,
        scaler: StandardScaler,
        columns: list,
        nPercentile_vals: pd.DataFrame
) -> float:
    """
    Apply an existing fitted StandardScaler to the given player_stats_df
    on the specified rate-stat columns and compute cosine similarity with median_vals.
    """
    player_stats_df_t = player_stats_df.to_frame().T
    if player_stats_df_t.isnull().values.any():
        return -1  
    scaled_player_vec = scale_player_stats(player_stats_df_t, scaler, columns)
    nPercentile_vec = nPercentile_vals[columns].values.reshape(1, -1)
    score = float(cosine_similarity(scaled_player_vec, nPercentile_vec)[0, 0])
    return score