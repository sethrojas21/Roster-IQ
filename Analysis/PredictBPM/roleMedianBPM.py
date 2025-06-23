import sqlite3
import pandas as pd
import numpy as np

# 1) Pull your full player‐season table
conn = sqlite3.connect('rosteriq.db')
df = pd.read_sql("""
    SELECT 
      p.player_name,
      ps.player_id,
      ps.team_name, 
      ps.season_year,   
      ps.MIN,      
      ps.bpm
    FROM Player_Seasons ps
    JOIN Players p ON ps.player_id = p.player_id
""", conn)

# 2a) Compute each team-season’s average BPM
df['team_avg_bpm'] = df.groupby(['team_name','season_year'])['bpm'].transform('mean')

# 2b) Compute each player’s BPM relative to their team average
df['rel_bpm'] = df['bpm'] - df['team_avg_bpm']

# 2) Compute minutes‐rank within each team‐season
df['rank_within_team'] = (
    df
    .groupby(['team_name','season_year'])['MIN']
    .rank(method='first', ascending=False)
)

# 3) Assign roles: top 5 → starter, next 4 → rotation, rest → bench
df['role'] = np.where(
    df['rank_within_team'] <= 5, 'starter',
    np.where(df['rank_within_team'] <= 9, 'rotation', 'bench')
)

# 4) Now slice out the three DataFrames
starters_df  = df[df['role']=='starter'].copy()
rotation_df  = df[df['role']=='rotation'].copy()
bench_df     = df[df['role']=='bench'].copy()

print(starters_df['rel_bpm'].median())
print(rotation_df['rel_bpm'].median())
print(bench_df['rel_bpm'].median())

bench_cut      = bench_df['rel_bpm'].quantile(0.75)     # likely ≈ –2
rotation_cut   = rotation_df['rel_bpm'].quantile(0.75)  # likely ≈ +2

print(bench_cut, rotation_cut)