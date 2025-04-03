import pandas as pd
import sqlite3
import queries

# Predict a players BPM for the next season using just last years statistics
# Stats to use player stats, current team stats, team going to stats

conn = sqlite3.connect('rosteriq.db')

currYear = 2023
nextYear = 2024

allPlayersDf = pd.read_sql(queries.all_players, conn, params=(currYear))
allTeamsDf = pd.read_sql(queries.all)