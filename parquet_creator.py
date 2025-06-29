import pandas as pd
import numpy as np
import json
import ast
import os
import bz2
import pyarrow as pa
import pyarrow.parquet as pq

df_games = pd.read_csv("./files/metadata/metadata.csv")
df_players = pd.read_csv("./files/metadata/players.csv")
df_rosters = pd.read_csv("./files/metadata/rosters.csv")

def parse_dict_column(col):
    return col.apply(ast.literal_eval)

df_games["homeTeam"] = parse_dict_column(df_games["homeTeam"])
df_games["awayTeam"] = parse_dict_column(df_games["awayTeam"])

df_games["homeTeamId"] = df_games["homeTeam"].apply(lambda x: x["id"])
df_games["homeTeamName"] = df_games["homeTeam"].apply(lambda x: x["name"])
df_games["awayTeamId"] = df_games["awayTeam"].apply(lambda x: x["id"])
df_games["awayTeamName"] = df_games["awayTeam"].apply(lambda x: x["name"])

df_rosters["player"] = df_rosters["player"].apply(ast.literal_eval)
df_rosters["team"] = df_rosters["team"].apply(ast.literal_eval)

df_rosters["playerId"] = df_rosters["player"].apply(lambda x: x["id"])
df_rosters["teamId"] = df_rosters["team"].apply(lambda x: x["id"])
df_rosters["teamName"] = df_rosters["team"].apply(lambda x: x["name"])

merged = df_rosters.merge(
    df_games[["id", "homeTeamId", "awayTeamId"]],
    left_on="game_id", right_on="id", how="left"
)

def determine_home_or_away(row):
    if row["teamId"] == row["homeTeamId"]:
        return "home"
    elif row["teamId"] == row["awayTeamId"]:
        return "away"
    else:
        return "unk"

merged["isAwayorHome"] = merged.apply(determine_home_or_away, axis=1)

df_player_info = merged[["game_id", "shirtNumber", "playerId", "teamName", "isAwayorHome"]]
df_player_info.columns = ["gameId", "shirtNumber", "playerId", "team", "isAwayorHome"]

df_player_info = df_player_info.set_index(["gameId", "shirtNumber", "isAwayorHome"])

# Set the directory where your .jsonl.bz2 files are located
input_directory = "./files/compressed_matches/BR_23"
output_directory = "./files/parquets"
os.makedirs(output_directory, exist_ok=True)

for filename in os.listdir(input_directory):
    if filename.endswith(".jsonl.bz2"):
        input_path = os.path.join(input_directory, filename)
        GAME_ID = filename.split('.')[0]
        output_path = os.path.join(output_directory, f"{GAME_ID}_frames.parquet")

        print(f"Processing {filename} -> {output_path}")

        schema = pa.schema([
            ("frame", pa.int64()),
            ("period", pa.int64()),
            ("time_ms", pa.float64()),
            ("team", pa.string()),
            ("jersey", pa.int64()),
            ("x", pa.float64()),
            ("y", pa.float64()),
            ("speed", pa.float64()),
            ("playerId", pa.int64()),
            ("matchId", pa.string())
        ])

        writer = None
        buffer = []
        chunk_size = 5000  # Adjust based on memory (can be 10000+ if RAM is plenty)

        def get_player_id(gameId, jerseyNumber, awayOrHome):
            try:
                return int(df_player_info.loc[(gameId, jerseyNumber, awayOrHome), "playerId"])
            except KeyError:
                return None

        with bz2.open(input_path, 'rt') as f:  # 'rt' = read as text
            for line in f:
                frame = json.loads(line)
                frame_num = frame.get("frameNum")
                period = frame.get("period")
                time = frame.get("videoTimeMs")

                for team, players in [('home', frame.get("homePlayers", [])),
                                      ('away', frame.get("awayPlayers", []))]:
                    for p in players:
                        buffer.append({
                            "frame": frame_num,
                            "period": period,
                            "time_ms": time,
                            "team": team,
                            "jersey": int(p.get("jerseyNum")),
                            "x": p.get("x"),
                            "y": p.get("y"),
                            "speed": p.get("speed"),
                            "playerId": get_player_id(int(GAME_ID), int(p.get("jerseyNum")), team),
                            "matchId": GAME_ID
                        })

                if len(buffer) >= chunk_size:
                    df_chunk = pd.DataFrame(buffer)
                    table = pa.Table.from_pandas(df_chunk, schema=schema, preserve_index=False)
                    if writer is None:
                        writer = pq.ParquetWriter(output_path, schema)
                    writer.write_table(table)
                    buffer.clear()

        # Write remaining data (after loop finishes)
        if buffer:
            df_chunk = pd.DataFrame(buffer)

            # Skip if df_chunk is empty or all NaNs
            if not df_chunk.empty:
                table = pa.Table.from_pandas(df_chunk, schema=schema, preserve_index=False)
                if writer is None:
                    writer = pq.ParquetWriter(output_path, schema)
                writer.write_table(table)
                print(f"Wrote final chunk of {len(df_chunk)} rows")
            buffer.clear()

        # Close the writer if it was ever created
        if writer:
            writer.close()
            print(f"Successfully saved {output_path}")
        else:
            print(f"Skipped writing {output_path} — no data")