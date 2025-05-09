fileNames = [
    "FreshmenJUCO_Rankings/jucoT100Rankings_2017-2020.csv",
    "FreshmenJUCO_Rankings/jucoT100Rankings_2022.csv",
    "FreshmenJUCO_Rankings/jucoT100Rankings_2023.csv",
    "FreshmenJUCO_Rankings/jucoT100Rankings_2024.csv",
    "FreshmenJUCO_Rankings/jucoT100Rankings_2025.csv"
]

import pandas as pd

# Read and clean each CSV file
dataframes = []
for file in fileNames:
    df = pd.read_csv(file)
    # Strip whitespace and special characters from all string columns
    df = df.applymap(lambda x: x.strip().replace('\xa0', ' ') if isinstance(x, str) else x)
    dataframes.append(df)

# Concatenate all cleaned dataframes
merged_df = pd.concat(dataframes, ignore_index=True)

# Optional: save the merged DataFrame to a new CSV
merged_df.to_csv("FreshmenJUCO_Rankings/mergedJucoRankings.csv", index=False)
