import sqlite3
import pandas as pd

# COMPLTED YIPEEE

conn = sqlite3.connect('rosteriq.db')

c = conn.cursor()

# c.execute("DELETE FROM Players2;")
# c.execute("DELETE FROM Player_Seasons2;")
# conn.commit()
# conn.close()

batch_size = 100
increment = 1
for year in range(2018, 2025):
    main_path = 'Torvik-CSVs/'
    print(year)
    player_path = main_path + f'Player/{year}.csv'

    with open(player_path, 'r') as csvfile:
        pdf = pd.read_csv(csvfile)


    for index, row in pdf.iterrows():
        player_name = row['player_name']

        try:
            home = row['hometown'].split(',')
            hometown, state_or_country = home[0].strip(), home[1].strip()
            if len(state_or_country) > 2:
                country = state_or_country
            else:
                country = 'USA'
        except:
            hometown, state_or_country, country = None, None, None
        
        # c.execute("""
        # SELECT * FROM Players
        # WHERE player_name = ? AND hometown = ?
        #  """, (player_name, hometown,))

        # results = c.fetchone()

        # if results is None:
        #     c.execute(""" 
        # INSERT INTO Players (player_name, hometown, state_or_country, country)
        # VALUES (?, ?, ?, ?)
        # """, (player_name, hometown, state_or_country, country))
        #     player_id = increment
        #     increment += 1 
        # else:
        #     player_id = results[0]

        player_id = row['pid']

        c.execute("""
        SELECT * FROM Players2
        WHERE player_id = ?
         """, (player_id,))

        results = c.fetchone()

        if results is None:
            c.execute(""" 
            INSERT INTO Players2 (player_id, player_name, hometown, state_or_country, country)
            VALUES (?, ?, ?, ?, ?)
            """, (player_id, player_name, hometown, state_or_country, country))

        team_name = row['team']
        years = {"Fr" : 1, "So": 2, "Jr" : 3, "Sr" : 4, "Gr" : 5}
        try:
            player_year = years[row['yr']]
        except:
            player_year = None
        try:
            height_inches = [int(num) for num in row['ht'].split('-')]
            height_inches = height_inches[0] * 12 + height_inches[1]
        except:
            height_inches = None

        def shortened_position(pos):
            try:
                if "G" in pos:
                    return "G"
                elif "F" in pos or "4" in pos:
                    return "F"
                elif "C" in pos:
                    return "C"
            except:
                return None
        
        shortened_pos = shortened_position(row['pos'])
        pos_description = None if shortened_pos is None else row['pos']
        
        try:
            mid_percent = (row['midmade']/row['midmade+midmiss'])
        except:
            mid_percent = 0

        c.execute(""" 
            INSERT INTO Player_Seasons2 (
            player_id, team_name, season_year, games_played, player_year, height_inches, weight_lbs, 
            position, position_description, min_pg, pts_pg, ast_pg, oreb_pg, dreb_pg, treb_pg, stl_pg, blk_pg, 
            efg_percent, ts_percent, usg_percent, oreb_percent, dreb_percent, ast_percent, tov_percent, 
            FTM, FTA, ft_percent, ftr, twoM, twoA, two_percent, threeM, threeA, three_percent, 
            blk_percent, stl_percent, porpag, adjoe, pfr, ast_tov_r, rimM, rimA, rimshot_percent, 
            midM, midA, midshot_percent, dunksM, dunksA, dunksshot_percent, pick, drtg, adrtg, dporpag, 
            stops, bpm, obpm, dbpm, gbpm, ogbpm, dgbpm)
            VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, 
        (player_id, team_name, year, row['GP'],player_year, height_inches, None, shortened_pos,
        pos_description, row['mp'], row['pts'], row['ast'], row['oreb'], row['dreb'], row['treb'], row['stl'], row['blk'],
        row['eFG'], row['TS_per'], row['usg'], row['ORB_per'], row['DRB_per'], row['AST_per'], row['TO_per'], row['FTM'],
        row['FTA'], row['FT_per'] * 100, row['ftr'], row['twoPM'], row['twoPA'], row['twoP_per'] * 100, row['TPM'], row['TPA'], row['TP_per'] * 100,
        row['blk_per'], row['stl_per'], row['porpag'], row['adjoe'], row['pfr'], row['ast/tov'], row['rimmade'],
        row['rimmade+rimmiss'],row['rimmade/(rimmade+rimmiss)'] * 100, row['midmade'], row['midmade+midmiss'], mid_percent * 100,
        row['dunksmade'], row['dunksmiss+dunkmade'], row['dunksmade/(dunksmade+dunksmiss)'] * 100, row['pick'], row['drtg'],
        row['adrtg'], row['dporpag'], row['stops'], row['bpm'], row['obpm'], row['dbpm'], row['gbpm'], row['ogbpm'], row['dgbpm']
        )
        )

        if (index + 1) % 100 == 0:
            conn.commit()  

    conn.commit()

conn.close()






