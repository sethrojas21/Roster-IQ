import sqlite3
import pandas as pd
import pickle

conn = sqlite3.connect('rosteriq.db')

query = """
        SELECT COUNT(weight_lbs)
        FROM Player_Seasons
        WHERE season_year = 2020
"""