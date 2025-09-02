import sqlite3
import pandas as pd
from Analysis.CalculateScores.calcFitScore import calculate_fit_score_from_transfers
from Analysis.CalculateScores.calcVOCRP import calculate_vocbp_from_transfers
from Analysis.Benchmark.init import InitBenchmarkPlayer
import numpy as np

def _robust_z(series: pd.Series, cap: float = 3.5) -> pd.Series:
    """
    Median–MAD z‑score with winsorising (clipping) at ±cap SD.
    Uses 1.4826 scaling factor to make MAD comparable to standard deviation.
    """
    med = series.median()
    mad = np.median(np.abs(series - med))
    if mad == 0:
        mad = 1e-9  # avoid divide‑by‑zero
    # Scale MAD to be comparable to standard deviation for normal distributions
    mad_scaled = mad * 1.4826
    z = (series - med) / mad_scaled
    return z.clip(lower=-cap, upper=cap)

def composite_ranking_robust(fs_df: pd.DataFrame,
                             vocrp_df: pd.DataFrame,
                             fs_w: float = 0.6,
                             v_w: float = 0.4,
                             cap: float = 3.5,
                             t_scale: bool = True,
                             debug: bool = False) -> pd.DataFrame:
    """
    Robust composite ranking:
    1. Robust z‑score each metric (median/MAD) and winsorise at ±cap SD.
    2. Linear blend with weights fs_w & v_w.
    3. Optionally convert to a 0‑100 T‑score for interpretability.
    """
    df = fs_df.merge(vocrp_df, on="player_name")

    # Robust z‑scores
    df["fit_z"]   = _robust_z(df["sim_score"], cap=cap)
    df["value_z"] = _robust_z(df["vocbp"],     cap=cap)

    # Weighted sum
    df["comp_raw"] = fs_w * df["fit_z"] + v_w * df["value_z"]

    # Add percentile rankings for comparison
    df['fit_pct']   = df['sim_score'].rank(pct=True)
    df['value_pct'] = df['vocbp'].rank(pct=True)
    df['composite_pct'] = df['fit_pct'] * fs_w + df['value_pct'] * v_w

    if t_scale:
        mu, sd = df["comp_raw"].mean(), df["comp_raw"].std(ddof=0) or 1e-9
        df["comp_T"] = 50 + 10 * (df["comp_raw"] - mu) / sd
        sort_col = "comp_T"
    else:
        sort_col = "comp_raw"

    result = df.sort_values(sort_col, ascending=False).reset_index(drop=True)
    
    if debug:
        analyze_composite_metrics(result)

    return result

def analyze_composite_metrics(df: pd.DataFrame) -> None:
    """
    Analyze the relationship between fit score and VOCBP metrics.
    Helps validate the independence assumption and weighting choices.
    """
    correlation = df['sim_score'].corr(df['vocbp'])
    
    print(f"=== COMPOSITE SCORE METRIC ANALYSIS ===")
    print(f"Correlation between Fit Score and VOCBP: {correlation:.3f}")
    
    if abs(correlation) < 0.3:
        print("→ Weak correlation - metrics are relatively independent ✓")
    elif abs(correlation) < 0.7:
        print("→ Moderate correlation - some overlap between metrics")
    else:
        print("→ Strong correlation - metrics may be redundant")
    
    print(f"\nFit Score Stats:")
    print(f"  Mean: {df['sim_score'].mean():.3f}, Std: {df['sim_score'].std():.3f}")
    print(f"  Median: {df['sim_score'].median():.3f}, MAD: {np.median(np.abs(df['sim_score'] - df['sim_score'].median())):.3f}")
    
    print(f"\nVOCBP Stats:")
    print(f"  Mean: {df['vocbp'].mean():.3f}, Std: {df['vocbp'].std():.3f}")
    print(f"  Median: {df['vocbp'].median():.3f}, MAD: {np.median(np.abs(df['vocbp'] - df['vocbp'].median())):.3f}")
    
    print(f"\nComposite Score Distribution:")
    if 'comp_T' in df.columns:
        print(f"  T-Score Mean: {df['comp_T'].mean():.1f}, Std: {df['comp_T'].std():.1f}")
        print(f"  T-Score Range: {df['comp_T'].min():.1f} to {df['comp_T'].max():.1f}")
    
    # Compare robust vs percentile rankings
    if 'composite_pct' in df.columns and 'comp_T' in df.columns:
        ranking_corr = df['comp_T'].corr(df['composite_pct'])
        print(f"\n=== RANKING METHOD COMPARISON ===")
        print(f"Correlation between Robust T-Score and Percentile Composite: {ranking_corr:.3f}")
        
        if ranking_corr > 0.9:
            print("→ Very high correlation - rankings are very similar")
        elif ranking_corr > 0.8:
            print("→ High correlation - rankings are mostly similar")
        elif ranking_corr > 0.7:
            print("→ Moderate correlation - some differences in rankings")
        else:
            print("→ Low correlation - significant differences in rankings")
        
        # Show top 10 differences
        df_temp = df.copy()
        df_temp['robust_rank'] = df_temp['comp_T'].rank(ascending=False)
        df_temp['pct_rank'] = df_temp['composite_pct'].rank(ascending=False)
        df_temp['rank_diff'] = abs(df_temp['robust_rank'] - df_temp['pct_rank'])
        
        biggest_diffs = df_temp.nlargest(5, 'rank_diff')[['player_name', 'robust_rank', 'pct_rank', 'rank_diff', 'comp_T', 'composite_pct']]
        print(f"\nBiggest Ranking Differences (Top 5):")
        for _, row in biggest_diffs.iterrows():
            print(f"  {row['player_name']:<25} Robust: {row['robust_rank']:>3.0f} | Pct: {row['pct_rank']:>3.0f} | Diff: {row['rank_diff']:>3.0f}")

# --- Legacy percentile method (kept for reference) ---
def composite_ranking_percentiles(fs_df, vocrp_df, fs_w = 0.6, v_w = 0.4, sortBy = 'composite_score'):
    # Merge on player_name (or player_id if that’s more reliable)
    try:
        df = fs_df.merge(vocrp_df, on='player_name')

        # Compute percentiles (0–1c) for each metric
        df['fit_pct']   = df[f'sim_score'].rank(pct=True)
        df['value_pct'] = df['vocbp'].rank(pct=True)

        # Build a composite score (sum of percentiles)
        df['composite_score'] = df['fit_pct'] * fs_w + df['value_pct'] * v_w

        # Sort by composite descending
        df_sorted = df.sort_values(sortBy, ascending=False)
        df_sorted = df_sorted.reset_index(drop=True)
    except Exception as e:
        print(e)

    return df_sorted


def composite_score(conn, team_name, season_year, player_id_to_replace, debug=False, specific_name=None):
    """
    Returns the benchmark player information and the rankings from the players inputted.
    Generates a benchmark mark player and computes fit scores and value over clustered replacement player scores using robust median‑MAD scaling.
    """
    bmark_plyr = InitBenchmarkPlayer(conn, team_name, season_year, player_id_to_replace)

    fs_df = calculate_fit_score_from_transfers(bmark_plyr, sort=False, debug=debug, specific_name=specific_name)
    vocbp_df = calculate_vocbp_from_transfers(bmark_plyr, sort=False, debug=debug, specific_name=specific_name)
    cs_df = composite_ranking_robust(fs_df, vocbp_df, debug=debug)
    
    return bmark_plyr, cs_df

def testing():
    conn = sqlite3.connect('rosteriq.db')
    team = "Arizona"

    player_name = "Caleb Love"
    year = 2024
    id = 72413
    
    bmark_plyr, cs_df = composite_score(conn, team, year, id, debug=True, specific_name=player_name)
    print(f"\n=== TOP 25 COMPOSITE SCORES ===")
    print(cs_df.head(25))
    print(f"\n=== SPECIFIC PLAYER: {player_name} ===")
    print(cs_df[cs_df['player_name'] == player_name])
    
if __name__ == '__main__':
    testing()