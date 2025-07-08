import pandas as pd

def get_prev_new_season_data(prev_year, new_year, conn):

    df = pd.read_sql("""SELECT
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
                   WHERE ps1.season_year = ? AND ps2.season_year = ? AND ps1.team_name != ps2.team_name                 
                 """, conn, params=(prev_year, new_year))
    
    return df

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
    