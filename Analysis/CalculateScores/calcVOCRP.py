import sqlite3
import numpy as np
import pandas as pd
from Analysis.Helpers.standardization import scale_player_stats
from Analysis.Helpers.dataLoader import get_transfers
from Analysis.Benchmark.init import InitBenchmarkPlayer
from Analysis.CalculateScores.adjustmentFactor import apply_adjustment_factors

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
        'stl_percent': 0.7,
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
    return difference

def avg_zScore_deviation(diff_vec):    
    return diff_vec.iloc[0].sum() / len(diff_vec.iloc[0])

def _calculate_vocbp_scores(bmark_plyr : InitBenchmarkPlayer, 
                            iter_players_df,
                            season_year : str, 
                            sort: bool,
                            adjustment_factor : bool = True, 
                            specific_name: str = None):
    df = pd.DataFrame(columns=['player_name', 'vocbp'])

    # pull scalar and benchmark DataFrame (1×N) back out
    scaler     = bmark_plyr.vocbp_scalar()
    bmark_df   = bmark_plyr.vocbp_bmark_vals()    # should be a 1-row DataFrame

    # inverse-scale back to original units
    unscaled_arr   = scaler.inverse_transform(bmark_df.values)  # shape (1, N)
    unscaled_bmark = pd.DataFrame(unscaled_arr, columns=bmark_df.columns)

    print("Raw benchmark VALUE stats (unscaled):")
    print(unscaled_bmark.iloc[0])  # get a Series if you like

    # Get the adjustment factor for the season year
    if adjustment_factor:
        adjustment_factor_df = bmark_plyr.adjustment_factor_year()
        
    for _, plyr in iter_players_df.iterrows():
        name = plyr['player_name']
        prev_team_name = plyr.get('prev_team_name', None)
        columns_to_use = bmark_plyr.vocbp_bmark_vals().columns

        # Apply adjustment factors if available
        if adjustment_factor and prev_team_name:
            def_factor = adjustment_factor_df.loc[adjustment_factor_df['team_name'] == prev_team_name, 'def_factor'].values[0]
            off_factor = adjustment_factor_df.loc[adjustment_factor_df['team_name'] == prev_team_name, 'off_factor'].values[0]

            plyr_adjusted = apply_adjustment_factors(plyr[columns_to_use], off_factor, def_factor)
        else:
            plyr_adjusted = plyr[columns_to_use]
            
        # Calculate the vector difference between player stats and benchmark stats
        vec_diff = player_difference(plyr_adjusted,
                                     bmark_plyr.vocbp_scalar(),
                                     columns_to_use,
                                     bmark_plyr.vocbp_bmark_vals())
        # Apply position-specific stat weights
        pos = plyr.get('position', bmark_plyr.replaced_plyr_pos)
        stat_weights = POSITION_STAT_WEIGHTS.get(pos, {})
        weights_series = pd.Series(
            {stat: stat_weights.get(stat, 1.0) for stat in vec_diff.columns},
            index=vec_diff.columns
        )
        vec_diff = vec_diff * weights_series

        # Print specific player stats if requested
        if name == specific_name:
            print("Specific player:", specific_name)
            print("Player Stats:")
            print(plyr_adjusted)
            print("Vector Difference:")
            print(vec_diff)

        # Compute weighted average z-score deviation
        vocbp = (vec_diff.iloc[0] * weights_series).sum() / weights_series.sum()
        df.loc[len(df)] = [name, vocbp]

    if sort:
        df = df.sort_values('vocbp', ascending=False).reset_index(drop = True)

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