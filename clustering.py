import ast
import json
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler


class Metrics:
    def preprocess_tracking_data(self, data):
        df = pd.DataFrame(data)
        df.sort_values(by=["playerId", "frame"], inplace=True, ignore_index=True)
        df["timestamp"] = df["time_ms"] / 1000

        return df

    def compute_acceleration(self, df):
        acceleration = np.zeros(len(df))
        ids = df['playerId'].values
        speeds = df['speed'].values
        timestamps = df['timestamp'].values

        unique_ids, id_starts = np.unique(ids, return_index=True)
        id_starts = np.append(id_starts, len(df))

        acc_results = []

        for i in range(len(unique_ids)):
            start, end = id_starts[i], id_starts[i+1]
            spd = speeds[start:end]
            t = timestamps[start:end]

            if len(spd) < 2:
                acc = np.zeros_like(spd)
            else:
                delta_s = np.diff(spd)
                delta_t = np.diff(t)

                delta_t[delta_t == 0] = 1e-6

                acc = delta_s / delta_t
                acc = np.insert(acc, 0, acc[0])

            acceleration[start:end] = acc

            acc_results.append({
                'playerId': unique_ids[i],
                'max_acceleration': np.max(acc),
                'mean_acceleration': np.mean(acc)
            })

        df['acceleration'] = acceleration
        acc_df = pd.DataFrame(acc_results).set_index('playerId')
        return acc_df

    def compute_distance_metrics(self, df):
        distance = np.zeros(len(df))

        ids = df['playerId'].values
        x = df['x'].values
        y = df['y'].values

        unique_ids, id_starts = np.unique(ids, return_index=True)
        id_starts = np.append(id_starts, len(df))

        dist_results = []

        for i in range(len(unique_ids)):
            start, end = id_starts[i], id_starts[i+1]
            x_vals = x[start:end]
            y_vals = y[start:end]

            dx = np.diff(x_vals)
            dy = np.diff(y_vals)
            d = np.sqrt(dx**2 + dy**2)
            d = np.insert(d, 0, 0.0)

            distance[start:end] = d

            dist_results.append({
                'playerId': unique_ids[i],
                'mean_distance_per_frame': np.mean(d),
                'max_distance_in_a_frame': np.max(d),
                'total_distance': np.sum(d)
            })

        df['distance'] = distance
        dist_df = pd.DataFrame(dist_results).set_index('playerId')
        return dist_df

    def compute_player_metrics(self, data):
        df = self.preprocess_tracking_data(data)
        acc_df = self.compute_acceleration(df)
        dist_df = self.compute_distance_metrics(df)

        final_df = acc_df.join(dist_df)
        final_df.reset_index(inplace=True) 
        return final_df
    
    
class Clustering:
    def __init__(self):
        self.metrics = Metrics()
        self.df_games = pd.read_csv("./files/metadata/metadata.csv")
        self.df_players = pd.read_csv("./files/metadata/players.csv")
        self.df_rosters = pd.read_csv("./files/metadata/rosters.csv")
    
    def parse_dict_column(self, col):
        return col.apply(ast.literal_eval)
    
    def determine_home_or_away(self, row):
        if row["teamId"] == row["homeTeamId"]:
            return "home"
        elif row["teamId"] == row["awayTeamId"]:
            return "away"
        else:
            return "unk"
    
    def create_playar_data(self):
        self.df_games["homeTeam"] = self.parse_dict_column(self.df_games["homeTeam"])
        self.df_games["awayTeam"] = self.parse_dict_column(self.df_games["awayTeam"])

        self.df_games["homeTeamId"] = self.df_games["homeTeam"].apply(lambda x: x["id"])
        self.df_games["homeTeamName"] = self.df_games["homeTeam"].apply(lambda x: x["name"])
        self.df_games["awayTeamId"] = self.df_games["awayTeam"].apply(lambda x: x["id"])
        self.df_games["awayTeamName"] = self.df_games["awayTeam"].apply(lambda x: x["name"])

        self.df_rosters["player"] = self.df_rosters["player"].apply(ast.literal_eval)
        self.df_rosters["team"] = self.df_rosters["team"].apply(ast.literal_eval)

        self.df_rosters["playerId"] = self.df_rosters["player"].apply(lambda x: x["id"])
        self.df_rosters["teamId"] = self.df_rosters["team"].apply(lambda x: x["id"])
        self.df_rosters["teamName"] = self.df_rosters["team"].apply(lambda x: x["name"])

        merged = self.df_rosters.merge(
            self.df_games[["id", "homeTeamId", "awayTeamId"]],
            left_on="game_id", right_on="id", how="left"
        )
        
        merged["isAwayorHome"] = merged.apply(self.determine_home_or_away, axis=1)

        df_player_info = merged[["game_id", "shirtNumber", "playerId", "teamName", "isAwayorHome"]]
        df_player_info.columns = ["gameId", "shirtNumber", "playerId", "team", "isAwayorHome"]

        return df_player_info.set_index(["gameId", "shirtNumber", "isAwayorHome"])
    
    def read_game_data(self, game_id):
        with open(f"./files/games/{game_id}.jsonl", "r") as f:
            data = [json.loads(line) for line in f]
        
        return data

    def get_player_id(self, df_player_info, gameId, jerseyNumber, awayOrHome):
       try:
           return int(df_player_info.loc[(gameId, jerseyNumber, awayOrHome), "playerId"])
       except KeyError:
           return None 
       
    def process_frames(self, data, df_player_info, gameId):
        frames = []

        for frame in data:
            frame_num = frame.get("frameNum")
            period = frame.get("period")
            time = frame.get("videoTimeMs")

            for team, players in [('home', frame.get("homePlayers", [])),
                                  ('away', frame.get("awayPlayers", []))]:
                for p in players:
                    frames.append({
                        "frame": frame_num,
                        "period": period,
                        "time_ms": time,
                        "team": team,
                        "jersey": p.get("jerseyNum"),
                        "x": p.get("x"),
                        "y": p.get("y"),
                        "speed": p.get("speed"),
                        "playerId": self.get_player_id(df_player_info, int(gameId), int(p.get("jerseyNum")), team),
                        "matchId": gameId
                    })
        
        return frames
    
    def pairplot_metrics(self, df_frames):
        X = df_frames[["max_acceleration", "mean_acceleration", "mean_distance_per_frame", "max_distance_in_a_frame", "total_distance"]]
        x_scaled = StandardScaler().fit_transform(X)

        kmeans = KMeans(n_clusters=3, random_state=0)
        df_frames["cluster"] = kmeans.fit_predict(x_scaled)

        sns.pairplot(df_frames, hue="cluster", diag_kind="kde")
        plt.show()
        
        return x_scaled
    
    def plot_player_clusters(self, df_frames, x_scaled):
        pca = PCA(n_components=2)
        components = pca.fit_transform(x_scaled)
        
        plt.figure(figsize=(8, 6))
        plt.scatter(components[:, 0], components[:, 1], c=df_frames["cluster"], cmap="viridis")
        plt.xlabel("PC 1")
        plt.ylabel("PC 2")
        plt.title("PCA of Player Physical Profiles")
        plt.colorbar(label="Cluster")
        plt.tight_layout()
        plt.show()