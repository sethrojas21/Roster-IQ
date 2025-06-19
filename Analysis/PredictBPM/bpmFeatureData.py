"""
bpmFeatureData.py

Build the feature matrix for training the BPM classifier.
Extracts transfer and returner data from the database, encodes categorical variables,
computes teammate-based aggregate features, and assembles a feature DataFrame.
"""

from Analysis.queries import gptTransferQuery, statsFromPreviousSeason, playerRostersIncomingSeason
import sqlite3
import pandas as pd
from sklearn.preprocessing import LabelEncoder

# Connect to the SQLite database containing historical player and team statistics
conn = sqlite3.connect('rosteriq.db')

feature_rows = []

# Loop through each season year to prepare feature rows for incoming transfers
for year in range(2018, 2024):
    # Load transfer list, previous season stats, and the incoming roster for the next season
    transferPlayersDF = pd.read_sql_query(gptTransferQuery, conn, params=(year, year+1))
    statsFromPreviousSeasonDF = pd.read_sql_query(statsFromPreviousSeason, conn, params=(year,))
    playerRostersIncomingSeasonDF = pd.read_sql_query(playerRostersIncomingSeason, conn, params=(year+1,))

    # Merge transfer data with each player's previous season performance
    statsFromTransferPlayersPrevSeasonDF = pd.merge(
        transferPlayersDF,
        statsFromPreviousSeasonDF,
        on='player_id',
    )

    # Identify players returning or transferring by merging roster and stats tables
    statsFromPlayersWhoHadAPreviousSeasonDF = pd.merge(
        statsFromPreviousSeasonDF,
        playerRostersIncomingSeasonDF,
        on=['player_id']
    )

    transfer_players_ids = statsFromTransferPlayersPrevSeasonDF['player_id']

    # Flag incoming players as transfers if their player_id is in the transfer list
    statsFromPlayersWhoHadAPreviousSeasonDF['is_transfer'] = (
        statsFromPlayersWhoHadAPreviousSeasonDF['player_id']
        .isin(transfer_players_ids)
        .astype(int)
    )

    # Encode categorical variables: player position and old team for model input
    label_encoder = LabelEncoder()
    statsFromPlayersWhoHadAPreviousSeasonDF['position_encoded'] = label_encoder.fit_transform(
        statsFromPlayersWhoHadAPreviousSeasonDF['position']
    )
    statsFromPlayersWhoHadAPreviousSeasonDF['team_encoded'] = label_encoder.fit_transform(
        statsFromPlayersWhoHadAPreviousSeasonDF['prev_team_name']
    )
    
    # Iterate over each transfer player to compute personalized and teammate-based features
    for index, player in statsFromPlayersWhoHadAPreviousSeasonDF.iterrows():
        player_id = player['player_id']
        new_team = player['next_team_name']
        # Retrieve target BPM and team strength metrics for upcoming season
        bpm_to_predict = statsFromPlayersWhoHadAPreviousSeasonDF[
            statsFromPlayersWhoHadAPreviousSeasonDF['player_id'] == player_id
            ]['bpm_to_predict'].values[0]
        next_team_barthag_rank = statsFromPlayersWhoHadAPreviousSeasonDF[
            statsFromPlayersWhoHadAPreviousSeasonDF['player_id'] == player_id
            ]['next_team_barthag_rank'].values[0]
        next_year_usg_rate = statsFromPlayersWhoHadAPreviousSeasonDF[
            statsFromPlayersWhoHadAPreviousSeasonDF['player_id'] == player_id
            ]['next_year_usg_rate'].values[0]
        
        
        if pd.isna(bpm_to_predict):
            print(f"Skipping player {player['player_name_x']} due to missing bpm_to_predict.")
            continue
                
        # Select teammates with previous season data to compute aggregate statistics
        teammates = statsFromPlayersWhoHadAPreviousSeasonDF[
            (statsFromPlayersWhoHadAPreviousSeasonDF['next_team_name'] == new_team) &
            (statsFromPlayersWhoHadAPreviousSeasonDF['player_id'] != player_id)
        ]
        
        if teammates.empty and len(teammates) < 8:
            print("Teammates no")
            continue

        try:
            incoming_team_barthag_from_last_season = teammates[teammates['prev_team_name'] == new_team].iloc[0, :]['prev_team_barthag_rank']
        except:
            continue

        # Compute weighted and simple averages of teammate BPM, usage, efficiency, and assists        
        total_minutes = teammates['total_player_minutes'].sum()
        # Weighted and unweighted averages
        weighted_bpm = (teammates['bpm'] * teammates['total_player_minutes']).sum() / total_minutes
        unweighted_bpm = teammates['bpm'].mean()
        avg_teammate_bpm = 0.7 * weighted_bpm + 0.3 * unweighted_bpm
        weighted_usg = (teammates['usg_percent'] * teammates['total_player_minutes']).sum() / total_minutes
        unweighted_usg = teammates['usg_percent'].mean()
        avg_teammate_usg = 0.7 * weighted_usg + 0.3 * unweighted_usg
        weighted_efg = (teammates['efg_percent'] * teammates['total_player_minutes']).sum() / total_minutes
        unweighted_efg = teammates['efg_percent'].mean()
        avg_teammate_efg = 0.7 * weighted_efg + 0.3 * unweighted_efg
        weighted_ast = (teammates['ast_percent'] * teammates['total_player_minutes']).sum() / total_minutes
        unweighted_ast = teammates['ast_percent'].mean()
        avg_teammate_ast = 0.7 * weighted_ast + 0.3 * unweighted_ast
        avg_teammate_2ptPercent = (teammates['two_percent'] * teammates['total_player_minutes']).sum() / total_minutes
        avg_teammate_3ptPercent = (teammates['three_percent'] * teammates['total_player_minutes']).sum() / total_minutes
        avg_teammate_adjt = (teammates['adjt'] * teammates['total_player_minutes']).sum() / total_minutes

        # Compute relative features as deviation from teammate averages (role-fit metrics)
        rel_bpm = player['bpm'] - avg_teammate_bpm
        rel_usg = player['usg_percent'] - avg_teammate_usg
        rel_efg = player['efg_percent'] - avg_teammate_efg
        rel_ast = player['ast_percent'] - avg_teammate_ast
        rel_adjt = player['adjt'] - avg_teammate_adjt
        rel_two_percent = player['two_percent'] - avg_teammate_2ptPercent
        rel_three_percent = player['three_percent'] - avg_teammate_3ptPercent
        

        diffInBarthagRank = next_team_barthag_rank - player['prev_team_barthag_rank']
        
        # Assemble the feature row for this player, combining personal, team, and relative metrics
        row = {
            'player_id': player['player_id'],   
            'player_position' : player['position_encoded'],
            'prev_year': year,
            # 'prev_team_cluster' : player['prev_team_cluster'],
            # 'team_name': player['team_encoded'],              
            # 'player_bpm_prev': player['bpm'],
            'percentOfTeamMinutes' : player['total_player_minutes'] / player['total_team_minutes'],
            # 'player_usg_percent': player['usg_percent'],
            # 'player_ts_prev': player['ts_percent'],
            'player_ast_prev': player['ast_percent'],
            # 'player_tov_prev': player['tov_percent'],
            'player_adrtg' : player['adrtg'],
            'player_aortg' : player['adjoe'],
            'player_dreb_prev': player['dreb_percent'],
            'player_height': player['height_inches'],
            'prev_team_barthag_rank': player['prev_team_barthag_rank'],
            'next_team_barthag' : incoming_team_barthag_from_last_season,
            'team_eFG': player['team_eFG'],
            'avg_teammate_bpm': avg_teammate_bpm,
            'avg_teammate_usg': avg_teammate_usg,
            'avg_teammate_efg': avg_teammate_efg,
            # 'avg_teammate_two_percent': avg_teammate_2ptPercent,
            # 'avg_teammate_three_percent': avg_teammate_3ptPercent,
            # 'avg_teammate_ast': avg_teammate_ast,
            # 'avg_teammate_dreb': (teammates['dreb_percent'] * teammates['total_player_minutes']).sum() / total_minutes,
            # 'avg_teammate_oreb': (teammates['oreb_percent'] * teammates['total_minutes']).sum() / total_minutes,
            # 'avg_teammate_tov': (teammates['tov_percent'] * teammates['total_minutes']).sum() / total_minutes,
            # 'avg_teammate_ts': (teammates['ts_percent'] * teammates['total_minutes']).sum() / total_minutes,
            # 'avg_teammate_height': (teammates['height_inches'] * teammates['total_minutes']).sum() / total_minutes,            
            # 'teammates_minutes_total': total_minutes,
            # 'teammates_count': len(teammates),
            'rel_bpm': rel_bpm,
            'rel_usg': rel_usg,
            'rel_efg': rel_efg,
            'rel_ast': rel_ast,
            # 'rel_adjt': rel_adjt,
            # 'rel_two_percent': rel_two_percent,
            # 'rel_three_percent': rel_three_percent,
            'bpm_to_predict': bpm_to_predict,  # target value for incoming season BPM
            # 'delta_bpm' : bpm_to_predict - player['bpm'],
            # 'next_year_usg_rate' : next_year_usg_rate,
            'player_name': player['player_name_x'],  # for display/debugging
            'is_transfer' : player['is_transfer']
        }


        feature_rows.append(row)

# Combine all feature rows into a single DataFrame for model training
df = pd.DataFrame(feature_rows)

print(df)
df.to_csv('Analysis/PredictBPM/bpm_features_all.csv')