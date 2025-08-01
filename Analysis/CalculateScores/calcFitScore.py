import sqlite3
import numpy as np
import pandas as pd
from Analysis.Helpers.dataLoader import get_transfers
from Analysis.Helpers.similarity import get_player_similarity_score
from Analysis.Benchmark.init import InitBenchmarkPlayer

def _calculate_fit_scores(bmark_plyr, iter_players_df, sort: bool):
    df = pd.DataFrame(columns=['player_name', 'sim_score'])
    for _, plyr in iter_players_df.iterrows():
        name = plyr['player_name']
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

def calculate_fit_score(conn, team_name, season_year, player_id_to_replace, sort=True):
    bmark = InitBenchmarkPlayer(conn, team_name, season_year, player_id_to_replace)
    transfers = get_transfers(
        conn,
        season_year,
        bmark.replaced_plyr_pos,
        InitBenchmarkPlayer.fs_query()
    )
    return _calculate_fit_scores(bmark, transfers, sort)

def calculate_fit_score_from_players(bmark_plyr: InitBenchmarkPlayer, iter_players_df, sort=True):
    return _calculate_fit_scores(bmark_plyr, iter_players_df, sort)

def calculate_fit_score_from_transfers(bmark_plyr: InitBenchmarkPlayer, sort=True):
    transfers = get_transfers(
        bmark_plyr.conn,
        bmark_plyr.season_year,
        bmark_plyr.replaced_plyr_pos,
        InitBenchmarkPlayer.fs_query()
    )

    return _calculate_fit_scores(bmark_plyr, transfers, sort)

# run
def test():
    conn = sqlite3.connect('rosteriq.db')
    df = calculate_fit_score(conn, "Gonzaga", 2021, 49449, sort=True)
    print(df.head(20))
    print(df[df['player_name'] == "Aaron Cook"])

if __name__ == '__main__':
    test()