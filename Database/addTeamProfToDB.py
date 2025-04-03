import sqlite3
import pandas as pd

conn = sqlite3.connect('rosteriq.db')

c = conn.cursor()

batch_size = 100

# c.execute("DELETE FROM Teams;")
# conn.commit()
# conn.close()

with open(f'Torvik-CSVs/Team-Final/2024.csv', 'r') as csvfile:
    teams = pd.read_csv(csvfile)['team']

i = 0

for team in teams:
    i += 1
    # try: 
    #     ind = team.index("St.")

    #     if ind != 0:
    #         teamname = team[:ind] + "State" + team[ind + 4:]
    #     else:
    #         teamname = team
    # except:
    #     teamname = team


    print(team)
    c.execute(""" 
        INSERT INTO Teams (team_name, town, state)
        VALUES (?, ?, ?); 
    """, (team, None, None))  # Values must be in a tuple

    if (i + 1) % batch_size == 0:
        conn.commit()

conn.commit()

conn.close()