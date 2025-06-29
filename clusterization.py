import pandas as pd
import glob
import os
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
import matplotlib.pyplot as plt
import seaborn as sns

def load_and_preprocess_data(directory_path):
    """
    Loads all player data files from a directory, combines them, 
    and preprocesses the data for clustering.
    """
    pattern = os.path.join(directory_path, 'stats_medias_*.csv')
    file_list = glob.glob(pattern)

    if not file_list:
        print(f"Error: No files matching the pattern 'stats_medias_*.csv' were found in the directory '{directory_path}'.")
        return None, None, None, None

    print(f"Found {len(file_list)} files to process.")
    
    all_dfs = []
    for filepath in file_list:
        try:
            df_temp = pd.read_csv(filepath)
            filename = os.path.basename(filepath)
            team_name = filename.replace('stats_medias_', '').replace('.csv', '')
            df_temp['team_name'] = team_name
            all_dfs.append(df_temp)
        except Exception as e:
            print(f"Warning: Could not process file {filepath}. Error: {e}")

    df = pd.concat(all_dfs, ignore_index=True)

    df['partidas_jogadas'] = df['partidas_jogadas'].replace(0, 1)

    for col in ['distancia_total_m', 'hsr_count', 'sprint_count', 'alta_aceleracao_count', 'aceleracao_explosiva_count']:
        df[col + '_por_partida'] = df[col] / df['partidas_jogadas']

    features_por_partida = [
        'age', 'height', 'distancia_total_m_por_partida', 'aceleracao_maxima_ms2',
        'aceleracao_media_positiva_ms2', 'alta_aceleracao_count_por_partida',
        'aceleracao_explosiva_count_por_partida'
    ]
    
    for col in features_por_partida:
        if df[col].isnull().any():
            df[col] = df[col].fillna(df[col].mean())

    X = df[features_por_partida]
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    return df, X_scaled, features_por_partida

def find_optimal_clusters(data):
    """
    Uses the Elbow Method to find the optimal number of clusters for K-Means.
    """
    wcss = []
    for i in range(1, 11):
        kmeans = KMeans(n_clusters=i, init='k-means++', max_iter=300, n_init=10, random_state=42)
        kmeans.fit(data)
        wcss.append(kmeans.inertia_)
    
    plt.figure(figsize=(10, 5))
    plt.plot(range(1, 11), wcss, marker='o', linestyle='--')
    plt.title('Elbow Method for Optimal Number of Clusters')
    plt.xlabel('Number of Clusters')
    plt.ylabel('WCSS (Within-Cluster Sum of Squares)')
    plt.grid(True)
    plt.show()

def cluster_players(df, data, n_clusters):
    """
    Performs K-Means clustering on the player data.
    """
    kmeans = KMeans(n_clusters=n_clusters, init='k-means++', max_iter=300, n_init=10, random_state=42)
    clusters = kmeans.fit_predict(data)
    df['cluster'] = clusters
    return df

def plot_clusters(df_clustered, scaled_data):
    """
    Visualizes the clusters using PCA.

    Args:
        df_clustered (pd.DataFrame): The DataFrame with player and cluster info.
        scaled_data (np.ndarray): The scaled data used for clustering.
    """
    # --- Apply PCA ---
    pca = PCA(n_components=2)
    principal_components = pca.fit_transform(scaled_data)
    
    # Add PCA results to the DataFrame
    df_clustered['pca1'] = principal_components[:, 0]
    df_clustered['pca2'] = principal_components[:, 1]

    # --- Create the Plot ---
    plt.figure(figsize=(16, 10))
    
    # Use seaborn to create a scatter plot
    # hue='cluster' -> colors points by cluster
    # style='team_name' -> uses different shapes for each team
    sns.scatterplot(
        x='pca1', y='pca2',
        hue='cluster',
        style='team_name',
        data=df_clustered,
        palette='deep',  # A nice color palette
        s=100,           # Marker size
        alpha=0.8
    )

    # --- Customize and Show Plot ---
    plt.title('Player Clusters by Physical Performance', fontsize=16)
    plt.xlabel('Principal Component 1', fontsize=12)
    plt.ylabel('Principal Component 2', fontsize=12)
    plt.legend(title='Legend', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.grid(True)
    plt.tight_layout() # Adjust layout to make room for the legend
    
    print("\nDisplaying the cluster plot. Close the plot window to end the script.")
    plt.show()


if __name__ == '__main__':
    # --- 1. Load and Preprocess Data ---
    data_directory = './files/player_stats' 
    
    df_original, X_scaled, feature_names = load_and_preprocess_data(data_directory)

    if df_original is not None:
        # --- 2. Find the Optimal Number of Clusters ---
        print("\nDisplaying the Elbow Method plot to help you choose the optimal number of clusters (k).")
        print("Look for the 'elbow' point where the rate of decrease in WCSS slows down.")
        find_optimal_clusters(X_scaled)

        # --- 3. Perform Clustering ---
        try:
            optimal_k = int(input("Enter the optimal number of clusters (k): "))
        except (ValueError, EOFError):
            print("\nInvalid input or script interrupted. Using a default of 4 clusters.")
            optimal_k = 4
        
        df_clustered = cluster_players(df_original.copy(), X_scaled, optimal_k)

        # --- 4. Analyze the Results ---
        print("\nClustering Complete!")
        print(f"\nPlayers have been grouped into {optimal_k} clusters.")
        print("\nHere's a sample of the clustered data with team names:")
        print(df_clustered[['nickname', 'team_name', 'cluster']].head())

        print("\nTo understand the characteristics of each cluster, let's look at the average values of the features for each group:")
        cluster_analysis = df_clustered.groupby('cluster')[feature_names].mean()
        print(cluster_analysis)

        print("\nLet's also see the distribution of clusters per team:")
        team_cluster_distribution = pd.crosstab(df_clustered['team_name'], df_clustered['cluster'])
        print(team_cluster_distribution)

        df_clustered.to_csv('clustered_players_all_teams.csv', index=False)
        print("\nClustered data for all teams has been saved to 'clustered_players_all_teams.csv'")
        
        # --- 5. Plot the Clusters ---
        plot_clusters(df_clustered, X_scaled)