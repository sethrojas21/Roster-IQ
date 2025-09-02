import libsql
import os
from dotenv import load_dotenv

load_dotenv()

import pandas as pd

url = os.getenv("TURSO_URL")
auth_token = os.getenv("TURSO_AUTH_TOKEN")
conn = libsql.connect(url, auth_token=auth_token)

df = pd.read_sql("""SELECT player_id FROM Player_Seasons WHERE season_year = 2024 AND team_name = "Arizona"; """, conn)

print(df)