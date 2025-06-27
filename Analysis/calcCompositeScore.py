import pandas as pd

def composite_ranking_percentiles(fs_df, vocrp_df):
    # Merge on player_name (or player_id if that’s more reliable)
    df = fs_df.merge(vocrp_df, on='player_name')

    # Compute percentiles (0–1c) for each metric
    df['fit_pct']   = df['sim_score'].rank(pct=True)
    df['value_pct'] = df['vocrp'].rank(pct=True)

    # Build a composite score (sum of percentiles)
    df['composite_score'] = df['fit_pct'] + df['value_pct']

    # Sort by composite descending
    df_sorted = df.sort_values('composite_score', ascending=False)
    df_sorted = df_sorted.reset_index(drop=True)

    return df_sorted

def testing():
    fs_df    = pd.read_csv('gonzagaFS.csv', index_col=0)
    vocrp_df = pd.read_csv('gonzagaVOCRP.csv', index_col=0)
    df = composite_ranking_percentiles(fs_df, vocrp_df)
    print(df.head(10))
    print(df[df['player_name'] == "Aaron Cook"])


testing()