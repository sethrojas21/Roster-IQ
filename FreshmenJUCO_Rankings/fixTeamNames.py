import sqlite3
import pandas as pd

conn = sqlite3.connect("rosteriq.db")
cursor = conn.cursor()


def fix_school_committed_names():
    team_name_corrections = {
    "Washington St": "Washington State",
    "W Kentucky": "Western Kentucky",
    "Michigan St": "Michigan State",
    "W Virginia": "West Virginia",
    "St Mary's": "Saint Mary`s",
    "Miami": "Miami (Fla.)",
    "Miss. St": "Mississippi State",
    "S Florida": "South Florida",
    "San Diego St": "San Diego State",
    "Ole Miss": "Mississippi",
    "Oklahoma St": "Oklahoma State",
    "N Carolina": "North Carolina",
    "UConn": "Connecticut",
    "Kansas St": "Kansas State",
    "TCU": "Texas Christian",
    "Oregon St": "Oregon State",
    "St. John's": "Saint John`s",
    "N Iowa": "Northern Iowa",
    "NC State": "North Carolina State",
    "S Carolina": "South Carolina",
    "Arizona St": "Arizona State",
    "Florida St": "Florida State",
    "Nrthwestrn": "Northwestern"
    }

    for old_team_name in team_name_corrections:
        query = """
        UPDATE HS_Rankings
        SET school_committed = ?
        WHERE school_committed = ?
        """
        new_team_name = team_name_corrections[old_team_name]
        cursor.execute(query, (new_team_name, old_team_name))

    conn.commit()

fix_school_committed_names()