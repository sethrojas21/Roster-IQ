import sqlite3
import numpy as np
import pandas as pd
from Analysis.Helpers.standardization import scale_player_stats
from Analysis.Helpers.dataLoader import get_transfers
from Analysis.Benchmark.init import InitBenchmarkPlayer

# Position-specific stat weights; adjust values as needed
POSITION_STAT_WEIGHTS = {
    'G': {
        'ast_percent': 1.5,
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

def _calculate_vocbp_scores(bmark_plyr : InitBenchmarkPlayer, iter_players_df, sort: bool, specific_name: str = None):
    df = pd.DataFrame(columns=['player_name', 'vocbp'])

    # pull scalar and benchmark DataFrame (1×N) back out
    scaler     = bmark_plyr.vocbp_scalar()
    bmark_df   = bmark_plyr.vocbp_bmark_vals()    # should be a 1-row DataFrame

    # inverse-scale back to original units
    unscaled_arr   = scaler.inverse_transform(bmark_df.values)  # shape (1, N)
    unscaled_bmark = pd.DataFrame(unscaled_arr, columns=bmark_df.columns)

    print("Raw benchmark stats (unscaled):")
    print(unscaled_bmark.iloc[0])  # get a Series if you like
    for _, plyr in iter_players_df.iterrows():
        name = plyr['player_name']


        vec_diff = player_difference(plyr,
                                     bmark_plyr.vocbp_scalar(),
                                     bmark_plyr.vocbp_bmark_vals().columns,
                                     bmark_plyr.vocbp_bmark_vals())
        # Apply position-specific stat weights
        pos = plyr.get('position', bmark_plyr.replaced_plyr_pos)
        stat_weights = POSITION_STAT_WEIGHTS.get(pos, {})
        weights_series = pd.Series(
            {stat: stat_weights.get(stat, 1.0) for stat in vec_diff.columns},
            index=vec_diff.columns
        )
        vec_diff = vec_diff * weights_series
        if name == "Caleb Love" or name == "Kmani Doughty":
            print(specific_name)
            print(plyr[bmark_plyr.vocbp_bmark_vals().columns])
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
    return _calculate_vocbp_scores(bmark, transfers, sort, specific_name=specific_name)

def calculate_vocbp_from_transfers(bmark_plyr: InitBenchmarkPlayer, sort=True, specific_name=None):
    transfers = get_transfers(
            bmark_plyr.conn,
            bmark_plyr.season_year,
            bmark_plyr.replaced_plyr_pos,
            InitBenchmarkPlayer.vocbp_query()
        )
    
    return _calculate_vocbp_scores(bmark_plyr, transfers, sort, specific_name=specific_name)


def testing():
    conn = sqlite3.connect('rosteriq.db')
    df = calculate_vocbp_score(conn, "Arizona", 2024, 72413, sort=True, specific_name="Caleb Love")
    print(df)
    print(df[df['player_name'] == "Caleb Love"])

if __name__ == '__main__':
    testing()