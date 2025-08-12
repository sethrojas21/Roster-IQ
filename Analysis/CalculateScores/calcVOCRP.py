import sqlite3
import numpy as np
import pandas as pd
from Analysis.Helpers.standardization import scale_player_stats
from Analysis.Helpers.dataLoader import get_transfers
from Analysis.Benchmark.init import InitBenchmarkPlayer
from Analysis.CalculateScores.sosAdjustmentFactor import apply_sos_bonus_to_value_df

# Position-specific stat weights; adjust values as needed
POSITION_STAT_WEIGHTS = {
    'G': {
        'ast_percent': 1.3,
        'oreb_percent': 1,
        'dreb_percent': 1,
        'ft_percent': 1.1,
        'stl_percent': 1.2,
        'blk_percent': 0.8,
        'ts_percent': 1.3,
    },
    'F': {
        'ast_percent': 1.2,
        'oreb_percent': 1.1,
        'dreb_percent': 1.1,
        'ft_percent': 1.0,
        'stl_percent': 1,
        'blk_percent': 1.2,
        'ts_percent': 1.2,
    },
    'C': {
        'ast_percent': 0.8,
        'oreb_percent': 1.4,
        'dreb_percent': 1.5,
        'ft_percent': 1.2,
        'stl_percent': 0.8,
        'blk_percent': 1.3,
        'ts_percent': 1.1,
    },
}

def player_difference(player_stats_df,
                      scaler,
                      columns,
                      benchmark_vals):
    player_stats_df_t = player_stats_df.to_frame().T
    player_vec = scale_player_stats(player_stats_df_t, scaler, columns)
    difference = np.subtract(player_vec, benchmark_vals)
    # Convert to Series to avoid broadcasting issues
    return pd.Series(difference.flatten(), index=columns)

def avg_zScore_deviation(diff_vec):    
    return diff_vec.iloc[0].sum() / len(diff_vec.iloc[0])

def _calculate_vocbp_scores(bmark_plyr : InitBenchmarkPlayer, 
                            iter_players_df,
                            season_year : str, 
                            sort: bool,
                            adjustment_factor : bool = True, 
                            specific_name: str = None):
    df = pd.DataFrame(columns=['player_name', 'prev_team_name', 'vocbp_raw'])

    # pull scalar and benchmark DataFrame (1×N) back out
    scaler     = bmark_plyr.vocbp_scalar()
    bmark_vals = bmark_plyr.vocbp_bmark_values()  # This should return the benchmark values

    print("VOCBP Benchmark Raw")
    print(bmark_plyr.vocbp_benchmark_unscaled())

    for _, plyr in iter_players_df.iterrows():
        name = plyr['player_name']
        prev_team_name = plyr.get('prev_team_name', None)
        indices = bmark_plyr.vocbp_benchmark_indices()

        # Use raw player stats; SOS bonus will be applied at the very end to the VALUE score
        plyr_adjusted = plyr[indices]
            
        # Calculate the vector difference between player stats and benchmark stats (global z-units)
        vec_diff = player_difference(plyr_adjusted, scaler, indices, bmark_vals)

        # Keep unweighted diff for correct aggregation
        raw_vec_diff = vec_diff.astype(float).copy()

        # Build position weights aligned to indices and normalize to mean=1
        pos = plyr.get('position', bmark_plyr.replaced_plyr_pos)
        stat_weights = POSITION_STAT_WEIGHTS.get(pos, {})
        weights_series = pd.Series(
            {stat: stat_weights.get(stat, 1.0) for stat in indices},
            index=indices,
            dtype=float,
        )
        w_mean = float(weights_series.mean()) if len(weights_series) else 1.0
        if w_mean == 0.0:
            w_mean = 1.0
        norm_weights = weights_series / w_mean

        # Print specific player stats if requested
        if name == specific_name:
            print("Specific player:", specific_name)
            print("Player Stats:")
            print(plyr_adjusted)
            # print("Position Weights (normalized, mean=1):")
            # print(norm_weights)
            # print("Vector Diff (position-weighted, for display only):")
            # print(raw_vec_diff * norm_weights)

        # Compute weighted average z-score deviation (apply weights once)
        vocbp = float((raw_vec_diff * norm_weights).sum() / norm_weights.sum())
        df.loc[len(df)] = [name, prev_team_name, vocbp]

    # Apply the SOS additive bump at the end (one-sided, precomputed per team-season)
    if adjustment_factor:
        # season_year here is the prior season for transfers (already passed as such by caller)
        df = apply_sos_bonus_to_value_df(
            df, season_year,
            team_col='prev_team_name',
            in_col='vocbp_raw',
            out_col='vocbp',
            csv_path="Analysis/CalculateScores/CSV/sos_value_adjustment.csv",
        )
    else:
        df['vocbp'] = df['vocbp_raw']
    
    if sort:
        df = df.sort_values('vocbp', ascending=False).reset_index(drop=True)

    return df


def calculate_vocbp_score(conn, team_name, incoming_season_year, player_id_to_replace, sort=True, specific_name=None):
    bmark = InitBenchmarkPlayer(conn, team_name, incoming_season_year, player_id_to_replace)
    transfers = get_transfers(
            conn,
            incoming_season_year,
            bmark.replaced_plyr_pos,
            InitBenchmarkPlayer.vocbp_query()
        )
    return _calculate_vocbp_scores(bmark, transfers, incoming_season_year - 1, sort, specific_name=specific_name)

def calculate_vocbp_from_transfers(bmark_plyr: InitBenchmarkPlayer, sort=True, specific_name=None):
    transfers = get_transfers(
            bmark_plyr.conn,
            bmark_plyr.season_year,
            bmark_plyr.replaced_plyr_pos,
            InitBenchmarkPlayer.vocbp_query()
        )

    return _calculate_vocbp_scores(bmark_plyr, transfers, bmark_plyr.season_year - 1, sort, specific_name=specific_name)


def testing():
    conn = sqlite3.connect('rosteriq.db')
    df = calculate_vocbp_score(conn, "Arizona", 2024, 72413, sort=True, specific_name="Caleb Love")
    print(df.head(20))
    print(df[df['player_name'] == "Caleb Love"])

if __name__ == '__main__':
    testing()