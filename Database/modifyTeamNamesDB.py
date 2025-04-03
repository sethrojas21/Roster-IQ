import sqlite3

conn = sqlite3.connect('rosteriq.db')

c = conn.cursor()

stateQuery = lambda tableName: \
    f"""
    UPDATE {tableName}
    SET team_name = REPLACE(team_name, ' St.', ' State')
    WHERE team_name LIKE '% St.%'
    """

saintQuery = lambda tableName: \
    f"""
    UPDATE {tableName}
    SET team_name = REPLACE(team_name, 'St. ', 'Saint ')
    WHERE team_name LIKE '%St. %'
    """

apostS = lambda tableName: \
    f"""
    UPDATE {tableName}
    SET team_name = REPLACE(team_name, "'s", "`s")
    WHERE team_name LIKE "%'s"
    """

# Grambling State, Queens, Miami Ohio

spotTreatment = { # dbName : savedName
                # Many more im missing like
                #nc state, queens, long island, citadel, charleston, sam houston
                "Cal Baptist" : "California Baptist",             
                "Mount St. Mary's": "Mount Saint Mary's",
                "Arkansas Little Rock" : "Arkansas-Little Rock",
                "Arkansas Pine Bluff" : "Arkansas-Pine Bluff",
                "Bethune Cookman" : "Bethune-Cookman",
                "Gardner Webb" : "Gardner-Webb",
                "Illinois Chicago" : "Illinois-Chicago",
                "Louisiana Lafayette" : "Louisiana-Lafayette",
                "Louisiana Monroe" : "Louisiana-Monroe",
                "Maryland Eastern Shore" : "Maryland-Eastern Shore",
                "UMBC" : "Maryland-Baltimore County",
                "UMass Lowell" : "Massachusetts-Lowell",
                "UMKC": "Missouri-Kansas City",
                "Tennessee Martin" : "Tennessee-Martin",
                "Texas A&M Commerce" : "Texas A&M-Commerce",
                "Texas A&M Corpus Christi" : "Texas A&M-Corpus Christi",
                "UT Arlington" : "Texas-Arlington",
                "UT Rio Grande Valley" : "Texas-Rio Grande Valley",
                "N.C State" : "North Carolina State",
                "USC Upstate" :"South Carolina Upstate",
                "Miami OH" : "Miami (Ohio)",
                "Miami FL" : "Miami (Fla.)",
                "IU Indy" : "IUPUI",
                "FIU" : "Florida International",
                "Purdue Fort Wayne" : "Fort Wayne",
                "Charleston" : "College of Charleston",
                "Southern Miss" : "Southern Mississippi",       
                }


tables = ["Player_Seasons", "Team_Seasons", "Teams"]
def specificTeamNameChange():
    for tableName in tables:
        for db_name, saved_name in spotTreatment.items():
            spotTreatmentQuery = \
                f"""
                UPDATE {tableName}
                SET team_name = ?
                WHERE team_name = ?
                """
            c.execute(spotTreatmentQuery, (saved_name, db_name))
            print(f"Updated team name: {db_name} -> {saved_name}")


def run():
    for table in tables:
        c.execute(apostS(table))


def spotTreat():
    for tableName in tables:
        print(tableName)
        query = \
            f"""
            UPDATE {tableName}
            SET team_name = ?
            WHERE team_name = ?;
            """

        c.execute(query, ("Virginia Military Institute", "VMI"))


spotTreat()
# Commit changes and close the connection
conn.commit()
c.close()
conn.close()