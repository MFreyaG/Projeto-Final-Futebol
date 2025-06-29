# File: injury_analysis.py

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

def analyze_injuries():
    """
    Loads clustered player data and injury data, merges them,
    and performs a detailed analysis of injury patterns per cluster.
    """
    # --- 1. DEFINE FILE PATHS ---
    clustered_filepath = 'files/clustered_players_all_teams.csv'
    # !!! IMPORTANT: Replace with the actual path to your injury file.
    injury_filepath = 'files/injuries_enriched_bra.csv' 
    
    # --- 2. LOAD DATA ---
    print("--- Starting Injury Analysis ---")
    try:
        df_clustered = pd.read_csv(clustered_filepath)
        df_injuries = pd.read_csv(injury_filepath)
        print("Successfully loaded clustered player data and injury data.")
    except FileNotFoundError as e:
        print(f"ERROR: Could not find a required file. Details: {e}")
        print("Please make sure 'clustered_players.csv' is in the same directory and that the injury file path is correct.")
        return

    # --- 3. PREPARE AND MERGE DATA ---
    # Clean player names in injury data
    df_injuries['player_name_cleaned'] = df_injuries['player_name'].str.replace(r'^#\d+', '', regex=True).str.strip()

    # Aggregate injury data to one row per player
    injury_summary_cols = ['player_name_cleaned', 'total_days_3y', 'n_events_3y', 'most_common_injury', 'severity_idx', 'high_risk_flag']
    df_injury_summary = df_injuries[injury_summary_cols].drop_duplicates(subset=['player_name_cleaned']).reset_index(drop=True)

    # Merge with clustered data
    df_merged = pd.merge(df_clustered, df_injury_summary, left_on='nickname', right_on='player_name_cleaned', how='left')

    # Fill missing injury data with 0 for analysis
    injury_numeric_cols = ['total_days_3y', 'n_events_3y', 'severity_idx']
    for col in injury_numeric_cols:
        df_merged[col] = df_merged[col].fillna(0)
    
    # --- 4. ANALYZE AND PRINT RESULTS ---
    print("\n--- Injury Metrics by Cluster ---")
    
    # Calculate averages for numerical columns
    injury_analysis = df_merged.groupby('cluster')[injury_numeric_cols].mean()
    
    # Calculate the proportion of high-risk players
    high_risk_proportion = df_merged.groupby('cluster')['high_risk_flag'].value_counts(normalize=True).unstack(fill_value=0)
    if True in high_risk_proportion.columns:
        injury_analysis['high_risk_player_%'] = high_risk_proportion[True] * 100
    else:
        injury_analysis['high_risk_player_%'] = 0
    print(injury_analysis)

    print("\n--- Most Common Injury Types by Cluster ---")
    common_injuries = df_merged[df_merged['most_common_injury'].notna()].groupby('cluster')['most_common_injury'].apply(lambda x: x.value_counts().head(3))
    print(common_injuries)

    # --- 5. VISUALIZE THE ANALYSIS ---
    print("\nDisplaying analysis plots. Close each plot to see the next one.")
    sns.set_style("whitegrid")
    
    # Plot 1: Average Days Missed
    plt.figure(figsize=(12, 7))
    sns.barplot(data=injury_analysis.reset_index(), x='cluster', y='total_days_3y', palette='viridis')
    plt.title('Average Total Days Missed due to Injury by Cluster', fontsize=16)
    plt.ylabel('Average Days Missed (in 3 years)')
    plt.xlabel('Cluster')
    plt.show()

    # Plot 2: High-Risk Player Proportion
    plt.figure(figsize=(12, 7))
    sns.barplot(data=injury_analysis.reset_index(), x='cluster', y='high_risk_player_%', palette='plasma')
    plt.title('Proportion of High-Risk Players by Cluster', fontsize=16)
    plt.ylabel('Percentage of Players Flagged as High-Risk (%)')
    plt.xlabel('Cluster')
    plt.show()
    
    print("\nInjury analysis finished.")

if __name__ == '__main__':
    analyze_injuries()