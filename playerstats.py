import pandas as pd
import numpy as np
import ast
import os
from datetime import datetime
import scipy.signal as signal

# ===================================================================
# CONFIGURAÇÃO GERAL E PARÂMETROS FINAIS
# ===================================================================

# Parâmetros do Filtro Savitzky-Golay (Ponto de Equilíbrio)
WINDOW = 11            # Janela de suavização moderada (deve ser ímpar)
POLYORDER = 2          # Ordem do polinômio quadrático (bom para curvas)
MAX_SPEED_OUTLIER = 12 # m/s

# Limiares Padrão para Métricas Físicas
HSR_THRESHOLD_SPEED = 5.5
SPRINT_THRESHOLD_SPEED = 6.9
HIGH_ACCEL_THRESHOLD = 3.0
MIN_EVENT_DURATION = 0.7
EXPLOSIVE_ACCEL_START_SPEED = 2.5
MOVEMENT_THRESHOLD_MS = 0.5

# Caminhos dos Arquivos
METADATA_DIR = 'files/metadata/'
GAMES_DIR = 'files/games/'
FRAMES_DIR = 'files/parquets'

def load_metadata(metadata_path: str, games_path: str):
    """Carrega e prepara os DataFrames de metadados uma única vez."""
    players_info = pd.read_csv(os.path.join(metadata_path, 'players.csv'), parse_dates=['dob']).rename(columns={'id': 'player_id', 'dob': 'birth_date'})
    players_info = players_info[['player_id', 'nickname', 'height', 'birth_date']].drop_duplicates(subset=['player_id'])

    try:
        rosters = pd.read_csv(os.path.join(metadata_path, 'rosters.csv'), converters={'player': ast.literal_eval, 'team': ast.literal_eval})
    except (ValueError, SyntaxError):
        rosters = pd.read_csv(os.path.join(metadata_path, 'rosters.csv'), converters={'player': lambda x: ast.literal_eval(x.replace("'", '"')), 'team': lambda x: ast.literal_eval(x.replace("'", '"'))})

    rosters['player_id'] = pd.to_numeric(rosters['player'].apply(lambda d: d.get('id')))
    rosters['team_name'] = rosters['team'].apply(lambda d: d.get('name'))
    rosters = rosters[['game_id', 'player_id', 'team_name']].drop_duplicates()
    
    matches_summary = pd.read_csv(os.path.join(games_path, 'matches_summary.csv'))
    game_dates = pd.read_csv(os.path.join(metadata_path, 'metadata.csv'), usecols=['id', 'date'], parse_dates=['date']).rename(columns={'id': 'game_id'})
    
    return players_info, rosters, matches_summary, game_dates

def calculate_physics(df_frames: pd.DataFrame) -> pd.DataFrame:
    df = df_frames.sort_values(by=['player_id', 'timestamp']).copy()
    
    dt_series = df.groupby('player_id')['timestamp'].diff()
    median_dt = dt_series.median()
    if pd.isna(median_dt) or median_dt == 0: median_dt = 0.04

    # 1. Calcula as componentes da velocidade (vx, vy)
    df['vx'] = np.divide(df.groupby('player_id')['x'].diff(), dt_series, out=np.zeros_like(dt_series, dtype=float), where=dt_series!=0)
    df['vy'] = np.divide(df.groupby('player_id')['y'].diff(), dt_series, out=np.zeros_like(dt_series, dtype=float), where=dt_series!=0)

    # 2. Remove outliers de velocidade (erros de tracking)
    raw_speed = np.sqrt(df['vx']**2 + df['vy']**2)
    df.loc[raw_speed > MAX_SPEED_OUTLIER, ['vx', 'vy']] = np.nan
    
    # 3. Suaviza as componentes da velocidade
    df['vx_smooth'] = df.groupby(['player_id', 'period'])['vx'].transform(lambda s: signal.savgol_filter(s.interpolate(), window_length=WINDOW, polyorder=POLYORDER))
    df['vy_smooth'] = df.groupby(['player_id', 'period'])['vy'].transform(lambda s: signal.savgol_filter(s.interpolate(), window_length=WINDOW, polyorder=POLYORDER))
    
    # 4. Calcula a VELOCIDADE e a ACELERAÇÃO finais a partir dos mesmos dados suavizados
    df['speed'] = np.sqrt(df['vx_smooth']**2 + df['vy_smooth']**2)
    df['acceleration'] = df.groupby(['player_id', 'period'])['speed'].transform(lambda s: signal.savgol_filter(s, window_length=WINDOW, polyorder=POLYORDER, deriv=1, delta=median_dt))
    
    # 5. Calcula a DISTÂNCIA com o limiar de ruído
    df['delta_t'] = dt_series
    df['distance_per_frame'] = df['speed'] * df['delta_t'].fillna(0)
    df['distance_for_total'] = df['distance_per_frame']
    df.loc[df['speed'] < MOVEMENT_THRESHOLD_MS, 'distance_for_total'] = 0.0
    
    return df.drop(columns=['vx','vy','vx_smooth','vy_smooth'], errors='ignore')

def count_events(player_series: pd.Series, min_duration: float, time_diffs: pd.Series) -> int:
    """Conta o número de eventos que atendem a uma duração mínima."""
    event_starts = (player_series & ~player_series.shift(1).fillna(False))
    event_ids = event_starts.cumsum()
    player_events = event_ids[player_series]
    if player_events.empty: return 0
    
    dt = time_diffs.median()
    if pd.isna(dt) or dt == 0: return 0
    
    event_durations = player_events.groupby(player_events).count() * dt
    return (event_durations >= min_duration).sum()

def aggregate_player_stats(df_phys: pd.DataFrame) -> pd.DataFrame:
    """Calcula as métricas de performance para cada jogador em uma partida."""
    stats_list = []
    for player_id, df_player in df_phys.groupby('player_id'):
        if df_player.empty: continue
            
        stats = {
            'player_id': player_id,
            'distancia_total_m': df_player['distance_for_total'].sum(),
            'aceleracao_maxima_ms2': df_player['acceleration'].max(),
            'aceleracao_media_positiva_ms2': df_player[df_player['acceleration'] > 0]['acceleration'].mean(),
            'hsr_count': count_events(df_player['speed'] >= HSR_THRESHOLD_SPEED, MIN_EVENT_DURATION, df_player['delta_t']),
            'sprint_count': count_events(df_player['speed'] >= SPRINT_THRESHOLD_SPEED, MIN_EVENT_DURATION, df_player['delta_t']),
            'alta_aceleracao_count': count_events(df_player['acceleration'].abs() >= HIGH_ACCEL_THRESHOLD, MIN_EVENT_DURATION, df_player['delta_t']),
        }
        
        # Para aceleração explosiva, usamos a aceleração bruta para identificar o início do evento
        is_high_accel = df_player['acceleration'].abs() >= HIGH_ACCEL_THRESHOLD
        event_starts = (is_high_accel & ~is_high_accel.shift(1).fillna(False))
        event_id_series = event_starts.cumsum()
        high_accel_event_ids = event_id_series[is_high_accel].unique()
        
        explosive_accel_count = 0
        for event_id in high_accel_event_ids:
            if event_id == 0: continue
            event_frames = df_player[event_id_series == event_id]
            if event_frames.empty or event_frames['delta_t'].sum() < MIN_EVENT_DURATION: continue
                
            if event_frames['speed'].iloc[0] < EXPLOSIVE_ACCEL_START_SPEED:
                next_frame_loc = df_player.index.get_loc(event_frames.index[-1]) + 1
                if next_frame_loc < len(df_player) and df_player['speed'].iloc[next_frame_loc] >= HSR_THRESHOLD_SPEED:
                    explosive_accel_count += 1
                    
        stats['aceleracao_explosiva_count'] = explosive_accel_count
        stats_list.append(stats)
        
    return pd.DataFrame(stats_list).fillna(0)


def main():
    """Orquestra o processo completo: carregar, processar, agregar e salvar."""
    
    print("Iniciando processo de análise de dados físicos...")
    players_info, rosters, matches_summary, game_dates = load_metadata(METADATA_DIR, GAMES_DIR)
    
    all_matches_stats = []
    
    for i, match_id in enumerate(matches_summary['id']):
        print(f"\rProcessando partida {i+1}/{len(matches_summary)} (ID: {match_id})", end="")
        try:
            filename = os.path.join(FRAMES_DIR, f"{match_id}_frames.parquet")
            if not os.path.exists(filename): continue
            
            df_frames = pd.read_parquet(filename).rename(columns={'playerId': 'player_id'})
            df_frames['timestamp'] = df_frames['time_ms'] / 1000.0
            df_frames.dropna(subset=['player_id'], inplace=True)
            
            df_phys = calculate_physics(df_frames)
            df_stats = aggregate_player_stats(df_phys)
            df_stats['game_id'] = match_id
            all_matches_stats.append(df_stats)
            
        except Exception as e:
            print(f"\nERRO ao processar a partida {match_id}: {e}")
    
    print("\nProcessamento em lote concluído. Agregando resultados...")

    if not all_matches_stats:
        print("Nenhuma partida foi processada. Finalizando.")
        return

    df_full_stats = pd.concat(all_matches_stats, ignore_index=True)
    df_full_stats = pd.merge(df_full_stats, rosters, on=['game_id', 'player_id'])
    df_full_stats = pd.merge(df_full_stats, game_dates, on='game_id')
    df_full_stats = pd.merge(df_full_stats, players_info, on='player_id')
    
    df_full_stats['age'] = (df_full_stats['date'] - df_full_stats['birth_date']).dt.days / 365.25

    aggregation_logic = {
        'age': 'mean',
        'height': 'first',
        'distancia_total_m': 'mean',
        'aceleracao_maxima_ms2': 'mean',
        'aceleracao_media_positiva_ms2': 'mean',
        'hsr_count': 'mean',
        'sprint_count': 'mean',
        'alta_aceleracao_count': 'mean',
        'aceleracao_explosiva_count': 'mean',
        'game_id': 'count'
    }
    df_aggregated = df_full_stats.groupby(['team_name', 'player_id', 'nickname']).agg(aggregation_logic).reset_index()
    df_aggregated.rename(columns={'game_id': 'partidas_jogadas'}, inplace=True)
    df_aggregated['age'] = df_aggregated['age'].fillna(df_aggregated['age'].mean())
    df_aggregated['age'] = df_aggregated['age'].astype(int)
    
    final_columns_order = [
        'nickname', 'age', 'height', 'partidas_jogadas', 'distancia_total_m', 'aceleracao_maxima_ms2',
        'aceleracao_media_positiva_ms2', 'hsr_count', 'sprint_count', 'alta_aceleracao_count',
        'aceleracao_explosiva_count', 'player_id'
    ]
    
    for team_name in df_aggregated['team_name'].unique():
        df_team = df_aggregated[df_aggregated['team_name'] == team_name].copy()
        
        df_team_report = df_team[final_columns_order]
        output_filename = f"stats_medias_{team_name.replace(' ', '_')}.csv"
        df_team_report.to_csv(output_filename, index=False, float_format='%.2f')
        print(f"-> Relatório para '{team_name}' salvo em: {output_filename}")

    print("\nProcesso concluído com sucesso!")

if __name__ == '__main__':
    main()