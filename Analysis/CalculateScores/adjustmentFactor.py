"""
Given a season, calculate schedule‑strength adjustment factors for each
team so we can scale offensive and defensive player stats.  Factors are
derived from regression residuals:

    * off_factor – residual of opponents' defensive efficiency (opp_de)
                   after regressing on the team's own adjusted offense
                   (adjoe).  Lower‑than‑expected opp_de ⇒ tougher D ⇒
                   off_factor > 1.

    * def_factor – residual of opponents' offensive efficiency (opp_oe)
                   after regressing on the team's own adjusted defense
                   (adjde).  Higher‑than‑expected opp_oe ⇒ tougher O ⇒
                   def_factor > 1.

Factors are centered on 1.00 and clipped to [0.85, 1.15] so they remain
relative to other teams without producing extreme swings.
"""
import pandas as pd
import numpy as np

def def_off_factor_year(conn, year):

    team_eff_query = """SELECT
                        team_name,
                        season_year,
                        barthag_rank,
                        sos,
                        adjoe,
                        adjde,
                        opp_oe,
                        opp_de
                        FROM Team_Seasons
                        WHERE season_year = ?"""
    team_eff_df = pd.read_sql(team_eff_query, conn, params=(year,))

    # --- Schedule‑strength regression factors ---
    # k maps ±2 SD of residuals to roughly ±10 % multiplicative bump
    k = 0.05

    # 1) Factor for *offense‑facing* stats (AST %, TS %, OREB %, …)
    #    Tougher opponent defenses (lower opp_de than expected) → boost > 1
    beta_def, intercept_def = np.polyfit(team_eff_df['adjoe'],
                                         team_eff_df['opp_de'], 1)
    expected_opp_de = intercept_def + beta_def * team_eff_df['adjoe']
    resid_def_side = team_eff_df['opp_de'] - expected_opp_de
    z_def_side = (resid_def_side - resid_def_side.mean()) / resid_def_side.std(ddof=0)
    team_eff_df['off_factor'] = 1 - z_def_side * k   # negative z ⇒ boost

    # 2) Factor for *defense‑facing* stats (STL %, BLK %, DREB %, …)
    #    Tougher opponent offences (higher opp_oe than expected) → boost > 1
    beta_off, intercept_off = np.polyfit(team_eff_df['adjde'],
                                         team_eff_df['opp_oe'], 1)
    expected_opp_oe = intercept_off + beta_off * team_eff_df['adjde']
    resid_off_side = team_eff_df['opp_oe'] - expected_opp_oe
    z_off_side = (resid_off_side - resid_off_side.mean()) / resid_off_side.std(ddof=0)
    team_eff_df['def_factor'] = 1 + z_off_side * k   # positive z ⇒ boost

    # Clamp factors to a modest range to avoid extreme inflations
    team_eff_df['off_factor'] = team_eff_df['off_factor'].clip(0.85, 1.15)
    team_eff_df['def_factor'] = team_eff_df['def_factor'].clip(0.85, 1.15)

    return team_eff_df

def def_off_factor_years(conn, start_year, end_year_exc):
    dfs = []

    for year in range(start_year, end_year_exc):
        year_df = def_off_factor_year(conn, year)
        year_df['season_year'] = year
        dfs.append(year_df)

    # Concatenate all DataFrames at once
    if dfs:
        df = pd.concat(dfs, ignore_index=True)
    else:
        # If no data, create empty DataFrame with expected columns
        df = pd.DataFrame(columns=['team_name', 'season_year', 'barthag_rank', 'sos', 'off_factor', 'def_factor'])

    # Sort by team name and season year for consistency
    df = df.sort_values(by=['season_year']).reset_index(drop=True)
    
    # Ensure all columns are present
    expected_columns = ['team_name', 'season_year', 'barthag_rank', 'sos', 'off_factor', 'def_factor']
    for col in expected_columns:
        if col not in df.columns:
            df[col] = np.nan
    
    return df[expected_columns]

def save_def_off_factors(conn, start_year, end_year_exc):
    """
    Save the schedule-strength adjustment factors to the database.
    """
    df = def_off_factor_years(conn, start_year, end_year_exc)

    df.to_csv(f'Analysis/CalculateScores/CSV/def_off_factors.csv', index=False)

def print_vocbp_summary(team_eff_df, year):
    """Print a comprehensive summary of the VOCBP adjustment factors and their relationship to team rankings."""
    print(f"\n=== VOCBP Adjustment Summary for {year} Season ===")
    print(f"Total teams: {len(team_eff_df)}")
    
    # Basic statistics for the factors
    print(f"\nAdjustment Factor Statistics:")
    print(f"Offensive Factor - Mean: {team_eff_df['off_factor'].mean():.3f}, Std: {team_eff_df['off_factor'].std():.3f}")
    print(f"                  Min: {team_eff_df['off_factor'].min():.3f}, Max: {team_eff_df['off_factor'].max():.3f}")
    print(f"Defensive Factor - Mean: {team_eff_df['def_factor'].mean():.3f}, Std: {team_eff_df['def_factor'].std():.3f}")
    print(f"                  Min: {team_eff_df['def_factor'].min():.3f}, Max: {team_eff_df['def_factor'].max():.3f}")
    
    # Correlation analysis with BartHag ranking and SOS
    barthag_off_corr = team_eff_df['barthag_rank'].corr(team_eff_df['off_factor'])
    barthag_def_corr = team_eff_df['barthag_rank'].corr(team_eff_df['def_factor'])
    sos_off_corr = team_eff_df['sos'].corr(team_eff_df['off_factor'])
    sos_def_corr = team_eff_df['sos'].corr(team_eff_df['def_factor'])
    
    # Calculate R² values (coefficient of determination)
    barthag_off_r2 = barthag_off_corr ** 2
    barthag_def_r2 = barthag_def_corr ** 2
    sos_off_r2 = sos_off_corr ** 2
    sos_def_r2 = sos_def_corr ** 2
    
    print(f"\n=== CORRELATION ANALYSIS ===")
    print(f"BartHag Rank vs Offensive Factor: r = {barthag_off_corr:.3f}, R² = {barthag_off_r2:.3f}")
    print(f"BartHag Rank vs Defensive Factor: r = {barthag_def_corr:.3f}, R² = {barthag_def_r2:.3f}")
    print(f"SOS vs Offensive Factor:          r = {sos_off_corr:.3f}, R² = {sos_off_r2:.3f}")
    print(f"SOS vs Defensive Factor:          r = {sos_def_corr:.3f}, R² = {sos_def_r2:.3f}")
    
    # Additional correlations for context
    barthag_sos_corr = team_eff_df['barthag_rank'].corr(team_eff_df['sos'])
    barthag_sos_r2 = barthag_sos_corr ** 2
    print(f"BartHag Rank vs SOS:              r = {barthag_sos_corr:.3f}, R² = {barthag_sos_r2:.3f}")
    
    # Cross-correlation between factors
    factor_corr = team_eff_df['off_factor'].corr(team_eff_df['def_factor'])
    factor_r2 = factor_corr ** 2
    print(f"Offensive vs Defensive Factor:    r = {factor_corr:.3f}, R² = {factor_r2:.3f}")
    
    # Methodology evaluation
    print(f"\n=== METHODOLOGY EVALUATION ===")
    print(f"R² Interpretation (% of variance explained):")
    print(f"  • BartHag → Off Factor: {barthag_off_r2*100:.1f}% (should be low - good teams don't necessarily face easier defenses)")
    print(f"  • BartHag → Def Factor: {barthag_def_r2*100:.1f}% (should be low - good teams don't necessarily face easier offenses)")
    print(f"  • SOS → Off Factor:     {sos_off_r2*100:.1f}% (should be moderate - teams with tough schedules face better defenses)")
    print(f"  • SOS → Def Factor:     {sos_def_r2*100:.1f}% (should be moderate - teams with tough schedules face better offenses)")
    print(f"  • Off ↔ Def Factors:    {factor_r2*100:.1f}% (should be moderate - teams face similar quality on both ends)")
    
    # Evaluation criteria
    print(f"\n=== ADJUSTMENT METHODOLOGY ASSESSMENT ===")
    
    # 1. Independence from team quality (low BartHag correlation)
    if barthag_off_r2 < 0.1 and barthag_def_r2 < 0.1:
        barthag_assessment = "✓ GOOD"
    elif barthag_off_r2 < 0.2 and barthag_def_r2 < 0.2:
        barthag_assessment = "~ ACCEPTABLE"
    else:
        barthag_assessment = "✗ CONCERNING"
    print(f"1. Independence from Team Quality: {barthag_assessment}")
    print(f"   → Low correlation with BartHag ranking ensures adjustments reflect schedule, not team ability")
    
    # 2. Relationship with schedule strength
    if sos_off_r2 > 0.15 and sos_def_r2 > 0.15:
        sos_assessment = "✓ GOOD"
    elif sos_off_r2 > 0.05 and sos_def_r2 > 0.05:
        sos_assessment = "~ ACCEPTABLE"
    else:
        sos_assessment = "✗ CONCERNING"
    print(f"2. Relationship with SOS: {sos_assessment}")
    print(f"   → Moderate correlation with SOS confirms adjustments capture schedule difficulty")
    
    # 3. Factor balance
    if 0.1 < factor_r2 < 0.5:
        balance_assessment = "✓ GOOD"
    elif factor_r2 < 0.7:
        balance_assessment = "~ ACCEPTABLE"
    else:
        balance_assessment = "✗ CONCERNING"
    print(f"3. Offensive/Defensive Balance: {balance_assessment}")
    print(f"   → Moderate correlation between factors shows teams face similar quality on both ends")
    
    # 4. Factor distribution
    off_range = team_eff_df['off_factor'].max() - team_eff_df['off_factor'].min()
    def_range = team_eff_df['def_factor'].max() - team_eff_df['def_factor'].min()
    if 0.15 < off_range < 0.35 and 0.15 < def_range < 0.35:
        range_assessment = "✓ GOOD"
    elif 0.1 < off_range < 0.4 and 0.1 < def_range < 0.4:
        range_assessment = "~ ACCEPTABLE"
    else:
        range_assessment = "✗ CONCERNING"
    print(f"4. Factor Range: {range_assessment}")
    print(f"   → Off range: {off_range:.3f}, Def range: {def_range:.3f} (should be 0.15-0.30 for meaningful adjustments)")
    
    # Overall assessment
    assessments = [barthag_assessment, sos_assessment, balance_assessment, range_assessment]
    good_count = sum(1 for a in assessments if "✓" in a)
    acceptable_count = sum(1 for a in assessments if "~" in a)
    
    print(f"\n=== OVERALL METHODOLOGY RATING ===")
    if good_count >= 3:
        overall = "EXCELLENT - Methodology is sound and effective"
    elif good_count >= 2 or (good_count >= 1 and acceptable_count >= 2):
        overall = "GOOD - Methodology is solid with minor concerns"
    elif good_count >= 1 or acceptable_count >= 3:
        overall = "ACCEPTABLE - Methodology works but has notable limitations"
    else:
        overall = "POOR - Methodology needs significant revision"
    
    print(f"Rating: {overall}")
    print(f"✓ Good: {good_count}/4, ~ Acceptable: {acceptable_count}/4, ✗ Concerning: {4-good_count-acceptable_count}/4")
    
    # Interpretation of correlations
    print(f"\n=== DETAILED CORRELATION INTERPRETATIONS ===")
    
    # BartHag correlations
    if abs(barthag_off_corr) > 0.3:
        off_strength = "strong" if abs(barthag_off_corr) > 0.5 else "moderate"
        off_direction = "negative" if barthag_off_corr < 0 else "positive"
        print(f"→ {off_strength.title()} {off_direction} correlation between BartHag ranking and offensive adjustment")
        if barthag_off_corr < 0:
            print("  (Better ranked teams tend to have higher offensive adjustments)")
        else:
            print("  (Worse ranked teams tend to have higher offensive adjustments)")
    else:
        print("→ Weak correlation between BartHag ranking and offensive adjustment (GOOD)")
        
    if abs(barthag_def_corr) > 0.3:
        def_strength = "strong" if abs(barthag_def_corr) > 0.5 else "moderate"
        def_direction = "negative" if barthag_def_corr < 0 else "positive"
        print(f"→ {def_strength.title()} {def_direction} correlation between BartHag ranking and defensive adjustment")
        if barthag_def_corr < 0:
            print("  (Better ranked teams tend to have higher defensive adjustments)")
        else:
            print("  (Worse ranked teams tend to have higher defensive adjustments)")
    else:
        print("→ Weak correlation between BartHag ranking and defensive adjustment (GOOD)")
    
    # SOS correlations
    if abs(sos_off_corr) > 0.3:
        sos_off_strength = "strong" if abs(sos_off_corr) > 0.5 else "moderate"
        sos_off_direction = "positive" if sos_off_corr > 0 else "negative"
        print(f"→ {sos_off_strength.title()} {sos_off_direction} correlation between SOS and offensive adjustment")
        if sos_off_corr > 0:
            print("  (Teams with tougher schedules get higher offensive adjustments - GOOD)")
        else:
            print("  (Teams with easier schedules get higher offensive adjustments - UNEXPECTED)")
    else:
        print("→ Weak correlation between SOS and offensive adjustment (CONCERNING)")
        
    if abs(sos_def_corr) > 0.3:
        sos_def_strength = "strong" if abs(sos_def_corr) > 0.5 else "moderate"
        sos_def_direction = "positive" if sos_def_corr > 0 else "negative"
        print(f"→ {sos_def_strength.title()} {sos_def_direction} correlation between SOS and defensive adjustment")
        if sos_def_corr > 0:
            print("  (Teams with tougher schedules get higher defensive adjustments - GOOD)")
        else:
            print("  (Teams with easier schedules get higher defensive adjustments - UNEXPECTED)")
    else:
        print("→ Weak correlation between SOS and defensive adjustment (CONCERNING)")
    
    # Top and bottom teams by BartHag ranking
    print(f"\n=== TOP 10 TEAMS BY BARTHAG RANKING ===")
    top_teams = team_eff_df.nsmallest(10, 'barthag_rank')[['team_name', 'barthag_rank', 'sos', 'off_factor', 'def_factor']]
    for _, row in top_teams.iterrows():
        print(f"{row['team_name']:<25} Rank: {row['barthag_rank']:>3} | SOS: {row['sos']:>6.2f} | Off: {row['off_factor']:.3f} | Def: {row['def_factor']:.3f}")
    
    print(f"\n=== BOTTOM 10 TEAMS BY BARTHAG RANKING ===")
    bottom_teams = team_eff_df.nlargest(10, 'barthag_rank')[['team_name', 'barthag_rank', 'sos', 'off_factor', 'def_factor']]
    for _, row in bottom_teams.iterrows():
        print(f"{row['team_name']:<25} Rank: {row['barthag_rank']:>3} | SOS: {row['sos']:>6.2f} | Off: {row['off_factor']:.3f} | Def: {row['def_factor']:.3f}")
    
    # Teams with extreme adjustment factors
    print(f"\n=== TEAMS WITH HIGHEST SCHEDULE STRENGTH ADJUSTMENTS ===")
    print("Highest Offensive Adjustment (faced toughest defenses):")
    high_off_factor = team_eff_df.nlargest(5, 'off_factor')[['team_name', 'barthag_rank', 'sos', 'off_factor']]
    for _, row in high_off_factor.iterrows():
        print(f"  {row['team_name']:<25} Rank: {row['barthag_rank']:>3} | SOS: {row['sos']:>6.2f} | Off Factor: {row['off_factor']:.3f}")
    
    print("\nHighest Defensive Adjustment (faced toughest offenses):")
    high_def_factor = team_eff_df.nlargest(5, 'def_factor')[['team_name', 'barthag_rank', 'sos', 'def_factor']]
    for _, row in high_def_factor.iterrows():
        print(f"  {row['team_name']:<25} Rank: {row['barthag_rank']:>3} | SOS: {row['sos']:>6.2f} | Def Factor: {row['def_factor']:.3f}")
    
    print(f"\n=== TEAMS WITH LOWEST SCHEDULE STRENGTH ADJUSTMENTS ===")
    print("Lowest Offensive Adjustment (faced weakest defenses):")
    low_off_factor = team_eff_df.nsmallest(5, 'off_factor')[['team_name', 'barthag_rank', 'sos', 'off_factor']]
    for _, row in low_off_factor.iterrows():
        print(f"  {row['team_name']:<25} Rank: {row['barthag_rank']:>3} | SOS: {row['sos']:>6.2f} | Off Factor: {row['off_factor']:.3f}")
    
    print("\nLowest Defensive Adjustment (faced weakest offenses):")
    low_def_factor = team_eff_df.nsmallest(5, 'def_factor')[['team_name', 'barthag_rank', 'sos', 'def_factor']]
    for _, row in low_def_factor.iterrows():
        print(f"  {row['team_name']:<25} Rank: {row['barthag_rank']:>3} | SOS: {row['sos']:>6.2f} | Def Factor: {row['def_factor']:.3f}")
    
    # Teams with extreme SOS values
    print(f"\n=== TEAMS WITH MOST EXTREME SOS VALUES ===")
    print("Toughest Schedule (highest SOS):")
    toughest_sos = team_eff_df.nlargest(5, 'sos')[['team_name', 'barthag_rank', 'sos', 'off_factor', 'def_factor']]
    for _, row in toughest_sos.iterrows():
        print(f"  {row['team_name']:<25} Rank: {row['barthag_rank']:>3} | SOS: {row['sos']:>6.2f} | Off: {row['off_factor']:.3f} | Def: {row['def_factor']:.3f}")
    
    print("\nEasiest Schedule (lowest SOS):")
    easiest_sos = team_eff_df.nsmallest(5, 'sos')[['team_name', 'barthag_rank', 'sos', 'off_factor', 'def_factor']]
    for _, row in easiest_sos.iterrows():
        print(f"  {row['team_name']:<25} Rank: {row['barthag_rank']:>3} | SOS: {row['sos']:>6.2f} | Off: {row['off_factor']:.3f} | Def: {row['def_factor']:.3f}")
        
    print(f"\n=== STATISTICAL COLUMNS BEING ADJUSTED ===")
    print(f"Offensive stats (OFF_STAT): {OFF_STAT}")
    print(f"Defensive stats (DEF_STAT): {DEF_STAT}")
    print(f"→ These correspond to the VOCBP query columns that will be adjusted by the factors")
    
    return {
        'barthag_off_corr': barthag_off_corr,
        'barthag_def_corr': barthag_def_corr,
        'sos_off_corr': sos_off_corr,
        'sos_def_corr': sos_def_corr,
        'barthag_off_r2': barthag_off_r2,
        'barthag_def_r2': barthag_def_r2,
        'sos_off_r2': sos_off_r2,
        'sos_def_r2': sos_def_r2,
        'factor_corr': factor_corr,
        'factor_r2': factor_r2,
        'overall_assessment': overall
    }

OFF_STAT = ['ast_percent', 'oreb_percent', 'ts_percent', 'porpag']
DEF_STAT = ['dreb_percent', 'stl_percent', 'blk_percent', 'dporpag']

def get_adjustment_factor_year(season_year):

    df = pd.read_csv(f'Analysis/CalculateScores/CSV/def_off_factors.csv')
    print(f"Loaded adjustment factors for {season_year} from CSV")
    season_df = df[df['season_year'] == season_year]
    columns = ['team_name', 'season_year', 'off_factor', 'def_factor']
    return season_df[columns]

def get_adjustment_factors_team_year(team_name, season_year):
    """
    Get the adjustment factors for a specific team in a given season.
    """
    df = get_adjustment_factor_year(season_year)
    team_df = df[df['team_name'] == team_name]
    
    if team_df.empty:
        raise ValueError(f"No adjustment factors found for {team_name} in {season_year}")
    
    return team_df.iloc[0][['off_factor', 'def_factor']].to_dict()

def apply_adj_fact_to_plyr_srs(player_stats : pd.Series,
                             off_factor : float,
                             def_factor : float):
    """
    Apply the adjustment factors to the player's stats.
    """
    off_columns = [col for col in player_stats.index if col in OFF_STAT]
    def_columns = [col for col in player_stats.index if col in DEF_STAT]
    adjusted_stats = player_stats.copy()
    adjusted_stats[off_columns] *= off_factor
    adjusted_stats[def_columns] *= def_factor
    return adjusted_stats

def apply_adj_fact_to_plyr_df(player_stats_df : pd.DataFrame,
                              season_year : int):
    """
    Apply adjustment factors to all players in the DataFrame based on their team's schedule strength.
    
    Args:
        player_stats_df: DataFrame containing player statistics with 'team_name' column
        season_year: Season year to get adjustment factors for
    
    Returns:
        pd.DataFrame: Copy of the DataFrame with adjusted statistics
    """
    # Create a copy to avoid modifying the original DataFrame
    adjusted_df = player_stats_df.copy()
    
    # Get adjustment factors for all teams in this season
    adj_fact_team_df = get_adjustment_factor_year(season_year=season_year)

    print("INSIDE THE FUNCTION")
    
    # Apply adjustments for each player
    for idx, (_, plyr) in enumerate(adjusted_df.iterrows()):
        team_name = plyr.get('team_name', None)
        if team_name:
            # Find team adjustment factors
            team_matches = adj_fact_team_df[adj_fact_team_df['team_name'] == team_name]
            if not team_matches.empty:
                team_adj_srs = team_matches.iloc[0]
                off_factor = team_adj_srs['off_factor']
                def_factor = team_adj_srs['def_factor']
                
                # Apply adjustments to this player's stats
                adj_plyr_stats = apply_adj_fact_to_plyr_srs(player_stats=plyr,
                                                            off_factor=off_factor,
                                                            def_factor=def_factor)
                
                # Update the adjusted DataFrame with the new stats
                adjusted_df.iloc[idx] = adj_plyr_stats
    
    return adjusted_df


### TESTING

if __name__ == "__main__":
    import sqlite3
    from Analysis.config import Config
    
    # Connect to database
    conn = sqlite3.connect('rosteriq.db')
    
    # Test with 2023 season as an example
    test_year = 2023
    print(f"Generating adjustment factors and analysis for {test_year} season...")
    
    # Generate the adjustment factors for the test year
    team_eff_df = def_off_factor_year(conn, test_year)
    
    # Run comprehensive summary
    results = print_vocbp_summary(team_eff_df, test_year)
    
    # Also test a few years to see consistency
    print(f"\n" + "="*80)
    print("MULTI-YEAR CONSISTENCY CHECK")
    print("="*80)
    
    years_to_test = [2021, 2022, 2023, 2024]
    multi_year_results = {}
    
    for year in years_to_test:
        print(f"\n--- {year} Season Summary ---")
        try:
            year_df = def_off_factor_year(conn, year)
            year_results = print_vocbp_summary(year_df, year)
            multi_year_results[year] = year_results
            print(f"{year} ✓ Complete")
        except Exception as e:
            print(f"{year} ✗ Error: {e}")
    
    # Summary across years
    print(f"\n" + "="*80)
    print("CROSS-YEAR METHODOLOGY ASSESSMENT")
    print("="*80)
    
    if multi_year_results:
        print("Consistency Metrics Across Years:")
        print(f"{'Year':<6} {'BH-Off R²':<10} {'BH-Def R²':<10} {'SOS-Off R²':<11} {'SOS-Def R²':<11} {'Assessment'}")
        print("-" * 75)
        
        for year, results in multi_year_results.items():
            print(f"{year:<6} {results['barthag_off_r2']:<10.3f} {results['barthag_def_r2']:<10.3f} {results['sos_off_r2']:<11.3f} {results['sos_def_r2']:<11.3f} {results['overall_assessment']}")
            
        # Calculate averages
        avg_barthag_off_r2 = np.mean([r['barthag_off_r2'] for r in multi_year_results.values()])
        avg_barthag_def_r2 = np.mean([r['barthag_def_r2'] for r in multi_year_results.values()])
        avg_sos_off_r2 = np.mean([r['sos_off_r2'] for r in multi_year_results.values()])
        avg_sos_def_r2 = np.mean([r['sos_def_r2'] for r in multi_year_results.values()])
        
        print("-" * 75)
        print(f"{'AVG':<6} {avg_barthag_off_r2:<10.3f} {avg_barthag_def_r2:<10.3f} {avg_sos_off_r2:<11.3f} {avg_sos_def_r2:<11.3f}")
        
        print(f"\nFinal Methodology Assessment:")
        print(f"• Independence from team quality (low BartHag R²): {avg_barthag_off_r2:.3f}, {avg_barthag_def_r2:.3f}")
        print(f"• Relationship with schedule strength (SOS R²): {avg_sos_off_r2:.3f}, {avg_sos_def_r2:.3f}")
        
        if avg_barthag_off_r2 < 0.1 and avg_barthag_def_r2 < 0.1 and avg_sos_off_r2 > 0.15 and avg_sos_def_r2 > 0.15:
            final_assessment = "✓ EXCELLENT - Methodology is statistically sound across multiple years"
        elif avg_barthag_off_r2 < 0.2 and avg_barthag_def_r2 < 0.2 and avg_sos_off_r2 > 0.1 and avg_sos_def_r2 > 0.1:
            final_assessment = "✓ GOOD - Methodology is solid with consistent performance"
        else:
            final_assessment = "~ NEEDS IMPROVEMENT - Some metrics show concerning patterns"
            
        print(f"\nOVERALL: {final_assessment}")
    
    conn.close()