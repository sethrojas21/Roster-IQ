import csv
import pandas as pd


def team_labels():
    output_path = "Analysis/Clustering/15ClusterData/archetypeInputToGPT.txt"
    with open(output_path, "w", newline='') as fout:
        # Loop over each year’s folder
        for year in range(2021, 2024 + 1):
            profiles_path = f"Analysis/Clustering/15ClusterData/{year}/KClustering/profiles.csv"
            # Open that year’s CSV
            fout.write(f"#####Team Year: {year}######\n")
            fout.write("*Clustering Centers (some IDs might be delted after reassignment)*\n")
            with open(profiles_path, "r", newline='') as fin:
                reader = csv.reader(fin)
                # Write each row to the TXT (comma-separated, then newline)
                for row in reader:
                    fout.write(",".join(row) + "\n")
            
            fout.write("*Loading Matrices*\n")
            loadings_path = f'Analysis/Clustering/15ClusterData/{year}/PCA/loadings.json'
            with open(loadings_path, 'r', newline='') as fin:
                reader = csv.reader(fin)

                for row in reader:
                    fout.write(",".join(row) + "\n")


def player_labels():
    output_path = "Analysis/Clustering/Players/archetypeInputToGPT.txt"
    roles = ['G', 'F', 'C']  
    role_dict = {"G" : "Guards", "F" : "Forwards", "C" : "Centers"}  
    with open(output_path, "w", newline='') as fout:
        # Loop over each year’s folder
        for year in range(2021, 2024 + 1):
            fout.write(f"###Player Year: {year}\n")
            for role in roles:
                fout.write(f"##{role_dict[role]}:\n")
                profiles_path = f"Analysis/Clustering/Players/{year}/KClustering/cluster_profiles_{role}.csv"
                # Open that year’s CSV
                fout.write("#Clustering Centers (some IDs might be delted after reassignment)\n")
                with open(profiles_path, "r", newline='') as fin:
                    reader = csv.reader(fin)
                    # Write each row to the TXT (comma-separated, then newline)
                    for row in reader:
                        fout.write(",".join(row) + "\n")
                
                fout.write("#Loading Matrices*\n")
                loadings_path = f'Analysis/Clustering/Players/{year}/PCA/pca_loadings_{role}.json'
                with open(loadings_path, 'r', newline='') as fin:
                    reader = csv.reader(fin)

                    for row in reader:
                        fout.write(",".join(row) + "\n")

def get_sample_length_plyr_team_archeytpe(plyr_cluster_id : int,
                                          team_cluster_id : int,
                                          year : int,
                                          pos : str):
    
    info_df = pd.read_csv('Analysis/Testing/CSVs/cluster_info.csv')

    line = info_df[
        (info_df['season_year'] == year) &
        (info_df['pos'] == pos) &
        (info_df['team_clu_id'] == team_cluster_id) &
        (info_df['player_clu_id'] == plyr_cluster_id)
    ]

    return line.iloc[0]['length']

if __name__ == '__main__':
    val = get_sample_length_plyr_team_archeytpe(1, 1, 2021, "G")
    print(val)
        
        