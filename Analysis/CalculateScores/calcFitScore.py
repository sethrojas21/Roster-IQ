import sqlite3
import numpy as np
import pandas as pd
from Analysis.Helpers.dataLoader import get_transfers
from Analysis.Helpers.similarity import get_player_similarity_score
from Analysis.Benchmark.init import InitBenchmarkPlayer
from Analysis.config import Config


def _calculate_fit_scores(bmark_plyr, iter_players_df, sort: bool, specific_name: str = None):
    df = pd.DataFrame(columns=['player_name', 'sim_score'])
    # pull scalar and benchmark DataFrame (1×N) back out
    scaler     = bmark_plyr.fs_scalar()
    bmark_df   = bmark_plyr.fs_bmark_vals()    # should be a 1-row DataFrame

    # inverse-scale back to original units
    unscaled_arr   = scaler.inverse_transform(bmark_df.values)  # shape (1, N)
    unscaled_bmark = pd.DataFrame(unscaled_arr, columns=bmark_df.columns)

    print("Raw FIT benchmark stats (unscaled):")
    print(unscaled_bmark.iloc[0])  # get a Series if you like
    for _, plyr in iter_players_df.iterrows():
        name = plyr['player_name']
        if name == specific_name:
            print(specific_name)
            print(plyr[bmark_plyr.fs_bmark_vals().columns])
        score = get_player_similarity_score(
            plyr,
            bmark_plyr.fs_scalar(),
            bmark_plyr.fs_bmark_vals().columns,
            bmark_plyr.fs_bmark_vals()
        )
        df.loc[len(df)] = [name, score]

    if sort:
        df = df.sort_values('sim_score', ascending=False).reset_index(drop=True)
    return df

def calculate_fit_score(conn, team_name, season_year, player_id_to_replace, sort=True, specific_name=None):
    bmark = InitBenchmarkPlayer(conn, team_name, season_year, player_id_to_replace)
    transfers = get_transfers(
        conn,
        season_year,
        bmark.replaced_plyr_pos,
        InitBenchmarkPlayer.fs_query()
    )
    return _calculate_fit_scores(bmark, transfers, sort, specific_name=specific_name)

def calculate_fit_score_from_players(bmark_plyr: InitBenchmarkPlayer, iter_players_df, sort=True, specific_name=None):
    return _calculate_fit_scores(bmark_plyr, iter_players_df, sort, specific_name=specific_name)

def calculate_fit_score_from_transfers(bmark_plyr: InitBenchmarkPlayer, sort=True, specific_name=None):
    transfers = get_transfers(
        bmark_plyr.conn,
        bmark_plyr.season_year,
        bmark_plyr.replaced_plyr_pos,
        InitBenchmarkPlayer.fs_query()
    )

    return _calculate_fit_scores(bmark_plyr, transfers, sort=sort, specific_name=specific_name)

# run
def test():
    conn = sqlite3.connect('rosteriq.db')
    team_name = "Arizona"
    name = "Caleb Love"
    year = 2024
    id = 72413
    df = calculate_fit_score(conn, team_name, year, id, sort=True, specific_name=name)
    print(df.head(20))
    print(df[df['player_name'] == name])

if __name__ == '__main__':
    test()