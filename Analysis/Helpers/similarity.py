import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.metrics.pairwise import cosine_similarity
from Analysis.Helpers.standardization import scale_player_stats

def get_player_similarity_score(
        player_stats_df: pd.Series,
        scaler: StandardScaler,
        columns: list,
        benchmark_vals
) -> float:
    """
    Apply an existing fitted StandardScaler to the given player_stats_df
    on the specified rate-stat columns and compute cosine similarity with benchmark_vals.
    
    Args:
        benchmark_vals: Can be either pd.Series or numpy array
    """
    # Convert to DF
    player_stats_df_t = player_stats_df.to_frame().T
    if player_stats_df_t.isnull().values.any():
        return -1  
    # Scale the player stats
    scaled_player_vec = scale_player_stats(player_stats_df_t, scaler, columns)
    
    # Handle benchmark_vals - could be Series or numpy array
    if isinstance(benchmark_vals, pd.Series):
        benchmark_vec = benchmark_vals[columns].values.reshape(1, -1)
    else:
        # If it's already a numpy array, just reshape it
        benchmark_vec = benchmark_vals.reshape(1, -1)
    
    # Compute cosine similarity
    score = float(cosine_similarity(scaled_player_vec, benchmark_vec)[0, 0])
    return score