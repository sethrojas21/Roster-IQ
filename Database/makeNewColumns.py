import sqlite3
import pandas as pd

conn = sqlite3.connect('rosteriq.db')

c = conn.cursor()

# c.execute("""
# ALTER TABLE Player_Seasons2
# ADD aortg FLOAT;
# """)

# c.execute(""" 
# UPDATE Player_Seasons2
# SET TREB = ROUND(treb_pg * games_played);
# """)


# c.execute(""" 
# UPDATE Player_Seasons
# SET AdjORTG = (ORTG / 
#     (SELECT AVG(ORTG) FROM Player_Seasons WHERE season_year = 2018)) 
#     * (SELECT AVG(DRTG) FROM Player_Seasons WHERE season_year = 2018)
# WHERE season_year = 2018;
# """)

# c.execute("""UPDATE Player_Seasons SET AdjORTG = NULL """)

c.execute("""
ALTER TABLE Players2 RENAME TO Players;
""")

conn.commit()