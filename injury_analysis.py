# injury_analysis.py
"""
Liga-BRA 2023 – Correlação entre clusters físicos e histórico de lesões
-----------------------------------------------------------------------

Uso básico
----------
>>> from injury_analysis import InjuryClusterAnalyzer
>>> an = InjuryClusterAnalyzer("clustered_players_all_teams.csv",
...                            "injuries_enriched_bra_standardized.csv")
>>> summary, corr, merged = an.run()

Objetos retornados
------------------
summary : % de jogadores com/sem lesão em cada cluster
corr    : correlação (Pearson) entre métricas físicas e gravidade das lesões
merged  : DataFrame completo já “casado”, pronto para exploração extra
"""

from __future__ import annotations

import os
import re
from typing import Tuple

import numpy as np
import pandas as pd
from unidecode import unidecode


class InjuryClusterAnalyzer:
    """
    Analisa a relação entre *clusters* de desempenho físico (season 2023)
    e o histórico de lesões do mesmo elenco (últimos anos).

    Principais etapas
    -----------------
    1. Carrega os dois CSVs.
    2. Normaliza nomes e clubes (remove camisa, acentos, caixa, etc.).
    3. Gera *player_key* = nome + clube e agrega as lesões por jogador.
    4. Faz o *merge* left (clusters ⟕ lesões) e calcula:
       • Cobertura de lesões por cluster  
       • Correlação entre variáveis numéricas dos clusters e `severity_idx`
    """

    # ------------- inicialização ------------------------------------------------

    def __init__(self, cluster_csv: str, injuries_csv: str) -> None:
        self.cluster_csv = cluster_csv
        self.injuries_csv = injuries_csv

        self.cluster: pd.DataFrame
        self.injuries_raw: pd.DataFrame
        self.injuries_agg: pd.DataFrame
        self.merged: pd.DataFrame
        self.summary: pd.DataFrame
        self.corr: pd.DataFrame

        # fluxo principal
        self._load()
        self._prepare_keys()
        self._aggregate_injuries()
        self._merge()
        self._build_summary()
        self._calc_correlations()

    # ------------- API pública ---------------------------------------------------

    def run(
        self,
    ) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """
        Retorna (summary, corr, merged) – prontos para uso no notebook.
        """
        return self.summary, self.corr, self.merged

    # Opcional: exporta resultados em CSV
    def to_csv(self, out_dir: str = ".") -> None:
        self.summary.to_csv(os.path.join(out_dir, "cluster_injury_summary.csv"), index=False)
        self.corr.to_csv(os.path.join(out_dir, "feature_severity_correlation.csv"), index=False)
        self.merged.to_csv(os.path.join(out_dir, "merged_cluster_injury.csv"), index=False)

    # ------------- etapas internas ----------------------------------------------

    # 1. carga
    def _load(self) -> None:
        self.cluster = pd.read_csv(self.cluster_csv)
        self.injuries_raw = pd.read_csv(self.injuries_csv)

    # 2. chaves normalizadas
    @staticmethod
    def _normalize(txt: str | int | float) -> str:
        """Remove acentos, caixa, espaços e caracteres não alfanuméricos."""
        if pd.isna(txt):
            return ""
        txt = unidecode(str(txt)).lower()
        return re.sub(r"[^a-z0-9]", "", txt)

    def _prepare_keys(self) -> None:
        # --- CLUSTER: nickname + clube
        self.cluster["player_key"] = (
            self.cluster["nickname"].fillna("")
            + "_"
            + self.cluster["team_name"].fillna("")
        ).map(self._normalize)

        # --- LESÕES: remove "#10", "#1" etc. do início do nome
        inj = self.injuries_raw.copy()
        inj["player_base"] = inj["player_name"].str.replace(r"^#?\d+", "", regex=True).str.strip()
        inj["player_key"] = (
            inj["player_base"].fillna("")
            + "_"
            + inj["Club"].fillna("")
        ).map(self._normalize)

        self.injuries_raw = inj

    # 3. agrega lesões por jogador
    def _aggregate_injuries(self) -> None:
        num_cols = ["Days Missed", "total_days_3y", "n_events_3y", "severity_idx"]
        agg = {c: "sum" for c in num_cols}
        agg.update(
            {
                "Injury": "count",
                "most_common_injury": lambda x: x.value_counts().idxmax()
                if x.dropna().size
                else np.nan,
            }
        )

        self.injuries_agg = (
            self.injuries_raw.groupby("player_key", as_index=False)
            .agg(agg)
            .rename(columns={"Injury": "injury_events", "most_common_injury": "top_injury"})
        )

    # 4. merge L⟕R
    def _merge(self) -> None:
        self.merged = self.cluster.merge(
            self.injuries_agg, on="player_key", how="left", indicator=True
        )

    # 5. porcentagens por cluster
    def _build_summary(self) -> None:
        grp = self.merged.groupby("cluster", dropna=False)
        self.summary = (
            grp.apply(
                lambda df: pd.Series(
                    {
                        "n_players": len(df),
                        "n_with_injury": df["injury_events"].notna().sum(),
                        "n_without_injury": df["injury_events"].isna().sum(),
                        "pct_without_injury": round(df["injury_events"].isna().mean() * 100, 2),
                    }
                )
            )
            .reset_index()
            .sort_values("cluster")
        )

    # 6. correlações entre métricas físicas × gravidade
    def _calc_correlations(self) -> None:
        numeric_feats = (
            self.cluster.select_dtypes(include=["number"])
            .columns.difference(["cluster", "player_id"])
        )
        valid = self.merged.dropna(subset=["severity_idx"])
        if valid.empty:
            self.corr = pd.DataFrame(columns=["feature", "corr_with_severity"])
            return

        corr_vals = {
            feat: valid[feat].corr(valid["severity_idx"])
            for feat in numeric_feats
        }

        self.corr = (
            pd.Series(corr_vals, name="corr_with_severity")
            .to_frame()
            .abs()  # ordenar por |ρ|
            .sort_values("corr_with_severity", ascending=False)
            .join(pd.Series(corr_vals, name="corr_raw"))
            .reset_index()
            .rename(columns={"index": "feature"})
        )

    # def analyze_injuries():
    # """
    # Loads clustered player data and injury data, merges them,
    # and performs a detailed analysis of injury patterns per cluster.
    # """
    # # --- 1. DEFINE FILE PATHS ---
    # clustered_filepath = 'files/clustered_players_all_teams.csv'
    # # !!! IMPORTANT: Replace with the actual path to your injury file.
    # injury_filepath = 'files/injuries_enriched_bra.csv' 
    
    # # --- 2. LOAD DATA ---
    # print("--- Starting Injury Analysis ---")
    # try:
    #     df_clustered = pd.read_csv(clustered_filepath)
    #     df_injuries = pd.read_csv(injury_filepath)
    #     print("Successfully loaded clustered player data and injury data.")
    # except FileNotFoundError as e:
    #     print(f"ERROR: Could not find a required file. Details: {e}")
    #     print("Please make sure 'clustered_players.csv' is in the same directory and that the injury file path is correct.")
    #     return

    # # --- 3. PREPARE AND MERGE DATA ---
    # # Clean player names in injury data
    # df_injuries['player_name_cleaned'] = df_injuries['player_name'].str.replace(r'^#\d+', '', regex=True).str.strip()

    # # Aggregate injury data to one row per player
    # injury_summary_cols = ['player_name_cleaned', 'total_days_3y', 'n_events_3y', 'most_common_injury', 'severity_idx', 'high_risk_flag']
    # df_injury_summary = df_injuries[injury_summary_cols].drop_duplicates(subset=['player_name_cleaned']).reset_index(drop=True)

    # # Merge with clustered data
    # df_merged = pd.merge(df_clustered, df_injury_summary, left_on='nickname', right_on='player_name_cleaned', how='left')

    # # Fill missing injury data with 0 for analysis
    # injury_numeric_cols = ['total_days_3y', 'n_events_3y', 'severity_idx']
    # for col in injury_numeric_cols:
    #     df_merged[col] = df_merged[col].fillna(0)
    
    # # --- 4. ANALYZE AND PRINT RESULTS ---
    # print("\n--- Injury Metrics by Cluster ---")
    
    # # Calculate averages for numerical columns
    # injury_analysis = df_merged.groupby('cluster')[injury_numeric_cols].mean()
    
    # # Calculate the proportion of high-risk players
    # high_risk_proportion = df_merged.groupby('cluster')['high_risk_flag'].value_counts(normalize=True).unstack(fill_value=0)
    # if True in high_risk_proportion.columns:
    #     injury_analysis['high_risk_player_%'] = high_risk_proportion[True] * 100
    # else:
    #     injury_analysis['high_risk_player_%'] = 0
    # print(injury_analysis)

    # print("\n--- Most Common Injury Types by Cluster ---")
    # common_injuries = df_merged[df_merged['most_common_injury'].notna()].groupby('cluster')['most_common_injury'].apply(lambda x: x.value_counts().head(3))
    # print(common_injuries)

    # # --- 5. VISUALIZE THE ANALYSIS ---
    # print("\nDisplaying analysis plots. Close each plot to see the next one.")
    # sns.set_style("whitegrid")
    
    # # Plot 1: Average Days Missed
    # plt.figure(figsize=(12, 7))
    # sns.barplot(data=injury_analysis.reset_index(), x='cluster', y='total_days_3y', palette='viridis')
    # plt.title('Average Total Days Missed due to Injury by Cluster', fontsize=16)
    # plt.ylabel('Average Days Missed (in 3 years)')
    # plt.xlabel('Cluster')
    # plt.show()

    # # Plot 2: High-Risk Player Proportion
    # plt.figure(figsize=(12, 7))
    # sns.barplot(data=injury_analysis.reset_index(), x='cluster', y='high_risk_player_%', palette='plasma')
    # plt.title('Proportion of High-Risk Players by Cluster', fontsize=16)
    # plt.ylabel('Percentage of Players Flagged as High-Risk (%)')
    # plt.xlabel('Cluster')
    # plt.show()
    
    # print("\nInjury analysis finished.")
