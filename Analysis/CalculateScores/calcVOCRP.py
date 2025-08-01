import sqlite3
import numpy as np
import pandas as pd
from Analysis.Helpers.standardization import scale_player_stats
from Analysis.Helpers.dataLoader import get_transfers
from Analysis.Benchmark.init import InitBenchmarkPlayer

def player_difference(player_stats_df,
                      scaler,
                      columns,
                      nPercentile_vals):
    player_stats_df_t = player_stats_df.to_frame().T
    player_vec = scale_player_stats(player_stats_df_t, scaler, columns)
    difference = np.subtract(player_vec, nPercentile_vals)
    return difference

def avg_zScore_deviation(diff_vec):    
    return diff_vec.iloc[0].sum() / len(diff_vec.iloc[0])

def _calculate_vocbp_scores(bmark_plyr : InitBenchmarkPlayer, iter_players_df, sort: bool):
    df = pd.DataFrame(columns=['player_name', 'vocbp'])
    for _, plyr in iter_players_df.iterrows():
        name = plyr['player_name']

        vec_diff = player_difference(plyr,
                                     bmark_plyr.vocbp_scalar(),
                                     bmark_plyr.vocbp_bmark_vals().columns,
                                     bmark_plyr.vocbp_bmark_vals())
        vocbp = avg_zScore_deviation(vec_diff)
        df.loc[len(df)] = [name, vocbp]

    if sort:
        df = df.sort_values('vocbp', ascending=False).reset_index(drop = True)
    return df


def calculate_vocbp_score(conn, team_name, incoming_season_year, player_id_to_replace, sort=True):
    bmark = InitBenchmarkPlayer(conn, team_name, incoming_season_year, player_id_to_replace)
    transfers = get_transfers(
            conn,
            incoming_season_year,
            bmark.replaced_plyr_pos,
            InitBenchmarkPlayer.vocbp_query()
        )
    return _calculate_vocbp_scores(bmark, transfers, sort)

def calculate_vocbp_from_transfers(bmark_plyr: InitBenchmarkPlayer, sort=True):
    transfers = get_transfers(
            bmark_plyr.conn,
            bmark_plyr.season_year,
            bmark_plyr.replaced_plyr_pos,
            InitBenchmarkPlayer.vocbp_query()
        )
    
    return _calculate_vocbp_scores(bmark_plyr, transfers, sort)


def testing():
    conn = sqlite3.connect('rosteriq.db')
    df = calculate_vocbp_score(conn, "Arizona", 2024, 72413)
    print(df)
    print(df[df['player_name'] == "Caleb Love"])

# testing()