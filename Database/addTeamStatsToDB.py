import sqlite3
import pandas as pd

conn = sqlite3.connect('rosteriq.db')

c = conn.cursor()


def team_pg_stats(teams, pdf, trdf):
    headers = ['team_name', 'pts_pg', 'oreb_pg', 'dreb_pg', 'treb_pg', 'ast_pg', 'stl_pg', 'blk_pg']
    pgdf = pd.DataFrame(columns=headers)

    for team in teams:
        players = pdf[pdf['team_name'] == team]
        gp = sum([int(val) for val in trdf[trdf['team_name'] == team]['record'].values[0].split('-')])
        num_stats = [(players[header] * players['games_played']).sum() / gp for header in headers[1:]]
        stats = [team] + num_stats

        pgdf.loc[len(pgdf)] = stats
    
    return pgdf


def main():
    main_path = 'Torvik-CSVs/'
    for year in range(2018, 2025):
        
        teamFinal_path = main_path + f'Team-Final/{year}.csv'
        teamResult_path = main_path + f'Team-Results/{year}.csv'        
        
        query = (f""" 
        SELECT * FROM Player_Seasons
        WHERE season_year = {year};
        """)

        pdf = pd.read_sql_query(query, conn)
        with open(teamFinal_path, 'r') as csvfile:
            tfdf = pd.read_csv(csvfile)
        with open(teamResult_path, 'r') as csvfile:
            trdf = pd.read_csv(csvfile)
            
        teams = list(tfdf['team_name'])

        
        teams_pgdf = team_pg_stats(teams, pdf, trdf)

        year_df = pd.DataFrame(columns=['team_name', 'season_year'])

        for team in teams:
            year_df.loc[len(year_df)] = [team, year]

        merged = year_df.merge(teams_pgdf, on = 'team_name').merge(tfdf, on = "team_name").merge(trdf, on="team_name")

        # print(merged)
        merged.to_sql("Team_Seasons", conn, if_exists='append', index=False)

        conn.commit()
        

        

def query():
    query = """
    SELECT ps.*, p.* 
    FROM Player_Seasons ps
    INNER JOIN Players p
    ON ps.player_id = p.player_id
    WHERE ps.season_year = 2018
    AND ps.team_name = "UTEP";
    """

    c.execute(query)

def drop_table():
    drop = """ 
    DROP TABLE Team_Seasons;
    """

    create = """

    CREATE TABLE Team_Seasons (
            team_name VARCHAR(30),
            season_year INT,
            pts_pg FLOAT,
            oreb_pg FLOAT,
            dreb_pg FLOAT,
            treb_pg FLOAT,
            ast_pg FLOAT,
            stl_pg FLOAT,
            blk_pg FLOAT,
            eFG FLOAT,
            eFG_off_rank INT,
            eFG_def FLOAT,
            eFG_def_rank INT,
            ftr FLOAT,
            ftr_rank INT,
            ftr_def FLOAT,
            ftr_def_rank INT,
            or_percent FLOAT,
            or_percent_rank INT,
            dr_percent FLOAT,
            dr_percent_rank INT,
            to_percent FLOAT,
            to_percent_rank INT,
            to_percent_def FLOAT,
            to_percent_def_rank INT,
            three_percent FLOAT,
            three_percent_rank INT,
            three_def_percent FLOAT,
            three_def_percent_rank INT,
            two_percent FLOAT,
            two_percent_rank INT,
            two_def_percent FLOAT,
            two_def_percent_rank INT,
            ft_percent FLOAT,
            ft_percent_rank INT,
            ft_def_percent FLOAT,
            ft_def_percent_rank INT,
            three_rate FLOAT,
            three_rate_rank INT,
            three_rate_def FLOAT,
            three_rate_def_rank INT,
            arate FLOAT,
            arate_rank INT,
            arate_def FLOAT,
            arate_def_rank INT,
            unk1 FLOAT,
            unk2 FLOAT,
            unk3 FLOAT,
            unk4 FLOAT,
            ov_rank INT,
            conf VARCHAR(10),
            record VARCHAR(10),
            adjoe FLOAT,
            oe_rank INT,
            adjde FLOAT,
            de_rank INT,
            barthag FLOAT,
            barthag_rank INT,
            proj_w FLOAT,
            proj_l FLOAT,
            pro_con_w FLOAT,
            pro_con_l FLOAT,
            con_rec VARCHAR(10),
            sos FLOAT,
            ncsos FLOAT,
            consos FLOAT,
            proj_sos FLOAT,
            proj_noncon_sos FLOAT,
            proj_con_sos FLOAT,
            elite_sos FLOAT,
            elite_noncon_sos FLOAT,
            opp_oe FLOAT,
            opp_de FLOAT,
            opp_proj_oe FLOAT,
            opp_proj_de FLOAT,
            con_adj_oe FLOAT,
            con_adj_de FLOAT,
            qual_o FLOAT,
            qual_d FLOAT,
            qual_barthag FLOAT,
            qual_games INT,
            fun FLOAT,
            conpf FLOAT,
            conpa FLOAT,
            conposs FLOAT,
            cono FLOAT,
            conde FLOAT,
            consosremain FLOAT,
            conf_win_percent FLOAT,
            wab FLOAT,
            wab_rank INT,
            fun_rank INT,
            adjt FLOAT,
            FOREIGN KEY (team_name) REFERENCES Teams(team_name)
            );
        """

    c.execute(drop)
    c.execute(create)
    conn.commit()

def creatable():
    query =  """   
    
    CREATE TABLE Team_Seasons (
            team_name VARCHAR(30),
            season_year INT,
            pts_pg FLOAT,
            oreb_pg FLOAT,
            dreb_pg FLOAT,
            treb_pg FLOAT,
            ast_pg FLOAT,
            stl_pg FLOAT,
            blk_pg FLOAT,
            eFG FLOAT,
            eFG_off_rank INT,
            eFG_def FLOAT,
            eFG_def_rank INT,
            ftr FLOAT,
            ftr_rank INT,
            ftr_def FLOAT,
            ftr_def_rank INT,
            or_percent FLOAT,
            or_percent_rank INT,
            dr_percent FLOAT,
            dr_percent_rank INT,
            to_percent FLOAT,
            to_percent_rank INT,
            to_percent_def FLOAT,
            to_percent_def_rank INT,
            three_percent FLOAT,
            three_percent_rank INT,
            three_def_percent FLOAT,
            three_def_percent_rank INT,
            two_percent FLOAT,
            two_percent_rank INT,
            two_def_percent FLOAT,
            two_def_percent_rank INT,
            ft_percent FLOAT,
            ft_percent_rank INT,
            ft_def_percent FLOAT,
            ft_def_percent_rank INT,
            three_rate FLOAT,
            three_rate_rank INT,
            three_rate_def FLOAT,
            three_rate_def_rank INT,
            arate FLOAT,
            arate_rank INT,
            arate_def FLOAT,
            arate_def_rank INT,
            unk1 FLOAT,
            unk2 FLOAT,
            unk3 FLOAT,
            unk4 FLOAT,
            ov_rank INT,
            conf VARCHAR(10),
            record VARCHAR(10),
            adjoe FLOAT,
            oe_rank INT,
            adjde FLOAT,
            de_rank INT,
            barthag FLOAT,
            barthag_rank INT,
            proj_w FLOAT,
            proj_l FLOAT,
            pro_con_w FLOAT,
            pro_con_l FLOAT,
            con_rec VARCHAR(10),
            sos FLOAT,
            ncsos FLOAT,
            consos FLOAT,
            proj_sos FLOAT,
            proj_noncon_sos FLOAT,
            proj_con_sos FLOAT,
            elite_sos FLOAT,
            elite_noncon_sos FLOAT,
            opp_oe FLOAT,
            opp_de FLOAT,
            opp_proj_oe FLOAT,
            opp_proj_de FLOAT,
            con_adj_oe FLOAT,
            con_adj_de FLOAT,
            qual_o FLOAT,
            qual_d FLOAT,
            qual_barthag FLOAT,
            qual_games INT,
            fun FLOAT,
            conpf FLOAT,
            conpa FLOAT,
            conposs FLOAT,
            cono FLOAT,
            conde FLOAT,
            consosremain FLOAT,
            conf_win_percent FLOAT,
            wab FLOAT,
            wab_rank INT,
            fun_rank INT,
            adjt FLOAT,
            FOREIGN KEY (team_name) REFERENCES Teams(team_name)
            );
        """
# query()
# drop_table()
main()
conn.close()




