import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from scipy.stats import entropy
from sklearn.linear_model import LinearRegression


class Metrics():
    def __init__(self, df):
        self.df = df
        
    def _first_of_year(self, year):      
        return pd.Timestamp(year, 1, 1)
    
    def _cagr(self, f, l, n):            
        return np.nan if f == 0 else (l/f)**(1/n) - 1
    
    # Severity and Recurrence
    def calculate_aggravation(self, g):
        rep = g["same_as_prev"]
        if not rep.any(): return np.nan
        first  = g.loc[~rep, "Days Missed"].median()
        repeat = g.loc[ rep, "Days Missed"].median()
        return np.nan if first in [0, np.nan] else repeat / first
    
    def calculate_reinjury_rate(self):
        return self.df.groupby("player_name")["same_as_prev"].mean().rename("reinjury_rate")
    
    def calculate_mean_days_severe(self):
        return self.df[self.df["severe_flag"]].groupby("player_name")["Days Missed"].mean().rename("mean_days_severe")
    
    def calculate_aggrav_idx(self):
        return self.df.groupby("player_name").apply(self.calculate_aggravation).rename("agrav_idx")
        
    # Player availability
    def calculate_risk_share(self):
        return self.df.groupby(["Club","season_year"])["high_risk_flag"].mean().rename("risk_share")
    
    def calculate_days_lost(self):
        return self.df.groupby(["Club","season_year"])["Days Missed"].sum().rename("total_days_lost")
    
    def calculate_number_of_players(self):
        return self.df.groupby(["Club","season_year"])["player_name"].nunique().rename("n_players")
    
    def calculate_availability(self):
        days_lost = self.calculate_days_lost()
        player_number = self.calculate_number_of_players()
        return (pd.concat([days_lost, player_number], axis=1)
                  .assign(availability_idx=lambda d:
                          1 - d["total_days_lost"]/(d["n_players"]*365))
                  [["availability_idx"]])
    
    # Temporal scale
    def slope_days(self, p):
        y = p.groupby("season_year")["Days Missed"].sum().reset_index()
        return np.nan if len(y) < 2 else LinearRegression().fit(y[["season_year"]], y["Days Missed"]).coef_[0]
    
    def gap_first_season(self, g):
        first = g.iloc[0]
        return (first["Start"] - self._first_of_year(first["season_year"])).days
    
    def calculate_days_to_first(self):
        return (self.df.groupby(["player_name","season_year"])
                   .apply(self.gap_first_season)
                   .rename("days_to_first"))
    
    def calculate_mean_days_to_first(self):
        days_to_first = self.calculate_days_to_first()
        return (days_to_first.groupby("player_name")
                        .mean()
                        .rename("mean_days_to_first"))
    
    def calculate_recovery_speed(self):
        return self.df.groupby("player_name").apply(self.slope_days).rename("recovery_slope_days_per_year")
    
    # Cost and diversity
    def calculate_injury_entropy(self):
        return self.df.groupby(["Club","season_year"])["Injury"].apply(lambda s: entropy(s.value_counts(normalize=True), base=2)).rename("injury_entropy")
    
    def calculate_top3_costly(self):
        top3_costly = self.df.groupby("Injury")["Days Missed"].sum().sort_values(ascending=False).head(3)
        print("Top-3 tipos de lesão mais custosos (dias):")
        print(top3_costly, "\n")
        return top3_costly

    # Current dynamic
    def calculate_inj_by_club_year(self):
        return self.df.groupby(["Club","season_year"])["Injury"].count().rename("n_injuries")
    
    def generate_cagr(self, inj_by_club_year):
        cagr = {}
        for club, g in inj_by_club_year.unstack().iterrows():
            if {2021,2024}.issubset(g.index):
                cagr[club] = self._cagr(g[2021], g[2024], 3)
        return pd.Series(cagr, name="cagr_2021_24")
    
    def generate_cv(self, inj_by_club_year):
        cv = (inj_by_club_year.unstack().std(axis=1) / inj_by_club_year.unstack().mean(axis=1))
        cv.name = "inj_cv"
        return cv
    
    def compute_injury_metrics(self):
        reinjury_rate = self.calculate_reinjury_rate()
        mean_days_severe = self.calculate_mean_days_severe()
        aggrav_idx = self.calculate_aggrav_idx()
        mean_days_to_first = self.calculate_mean_days_to_first()
        recovery_speed = self.calculate_recovery_speed()
        
        risk_share = self.calculate_risk_share()
        availability = self.calculate_availability()
        injury_entropy = self.calculate_injury_entropy()
        inj_by_club_year = self.calculate_inj_by_club_year()
        
        cagr = self.generate_cagr(inj_by_club_year)
        cv = self.generate_cv(inj_by_club_year)
        
        metrics = {
            "player_level": pd.concat(
                [reinjury_rate, mean_days_severe,
                 aggrav_idx, mean_days_to_first,
                 recovery_speed], axis=1),

            "club_year_level": pd.concat(
                [risk_share, availability,
                 injury_entropy, inj_by_club_year], axis=1),

            "club_static": pd.concat([cagr, cv], axis=1)
        }
        
        print(metrics["club_year_level"].sort_values("risk_share", ascending=False).head(10))
        return metrics

class Injuries():
    def __init__(self):
        self.df = pd.read_csv("files/injuries_enriched_bra.csv", parse_dates=["Start"])
    
    def init_aux_columns(self):
        self.df["season_year"]  = self.df["Start"].dt.year
        self.df["severe_flag"]  = self.df["Days Missed"] >= 28
        self.df = self.df.sort_values(["Club", "season_year", "player_name", "Start"])
        self.df["same_as_prev"] = (
            self.df.groupby("player_name")["Injury"]
              .transform(lambda s: s.eq(s.shift()))
        )
    
    def init_metrics(self):
        self.metrics = Metrics(self.df)
    
    def generate_metrics(self):
        return self.metrics.compute_injury_metrics()
    
    def init_references(self, metrics):
        self.club_yr = metrics["club_year_level"]
        self.inj_wide = self.club_yr["n_injuries"].unstack(fill_value=0)
        self.risk_wide = self.club_yr["risk_share"].unstack()
        self.days_wide = self.club_yr["availability_idx"].unstack()
    
    def plot_team_total_injuries(self):
        totals = self.inj_wide.sum(axis=1).sort_values(ascending=False)
        top15 = totals.head(15)

        plt.figure()
        sns.barplot(y=top15.index, x=top15.values, palette="crest")
        plt.title("Total de lesões por clube (2021-2024)")
        plt.xlabel("Nº de lesões no período")
        plt.ylabel("")
        for i, v in enumerate(top15.values):
            plt.text(v + 0.5, i, v, va="center")
        plt.tight_layout()
        plt.show()
        
    def plot_anual_injuries_evolution(self):
        year_totals = self.inj_wide.sum(axis=0)
        plt.figure()
        sns.lineplot(x=year_totals.index, y=year_totals.values, marker="o")
        plt.title("Total de lesões por temporada")
        plt.ylabel("Nº de lesões")
        plt.xlabel("Ano da temporada")
        for x, y in zip(year_totals.index, year_totals.values):
            plt.text(x, y + 1, int(y), ha="center")
        plt.tight_layout()
        plt.show()
    
    def high_risk_players(self):
        risk_2024 = self.risk_wide[2024].dropna().sort_values(ascending=False).head(15)
        plt.figure()
        sns.barplot(x=risk_2024.index, y=risk_2024.values, palette="rocket_r")
        plt.xticks(rotation=45, ha="right")
        plt.title("Fração de jogadores high-risk – temporada 2024", pad=20)
        plt.ylabel("Risk share")
        plt.ylim(0, 1)
        for x, y in zip(range(len(risk_2024)), risk_2024.values):
            plt.text(x, y + 0.02, f"{y:.2f}", ha="center")
        plt.tight_layout()
        plt.show()
        
    def plot_availability_per_season(self):
        avail_melt = self.days_wide.stack().reset_index()
        avail_melt.columns = ["Club", "Season", "Availability"]

        plt.figure()
        sns.boxplot(x="Season", y="Availability", data=avail_melt, color="skyblue")
        plt.title("Distribuição do Availability Index por temporada")
        plt.ylim(0, 1)
        plt.tight_layout()
        plt.show()
    
    def plot_risk_x_days_lost(self):
        df24 = self.club_yr.xs(2024, level="season_year")
        plt.figure()
        sns.scatterplot(x="risk_share", y="total_days_lost", data=df24,
                        size="n_players", sizes=(30, 300), hue="Club", legend=False)
        plt.title("2024 · Risk share vs. dias perdidos")
        plt.xlabel("Risk share")
        plt.ylabel("Dias perdidos (total)")
        plt.tight_layout()
        plt.show()

    def _ensure_year(self):
        if "season_year" not in self.df.columns:
            self.df = self.df.copy()
            self.df["season_year"] = self.df["Start"].dt.year
        return self.df

    def injuries_by_club(self, year):
        """Total de lesões em determinado ano"""
        return (self._ensure_year()
                .query("season_year == @year")
                .groupby("Club")
                .size()
                .sort_values(ascending=False)
                .rename("injury_count"))

    def days_missed_by_club(self, year):
        """Soma de dias perdidos em determinado ano"""
        return (self._ensure_year()
                .query("season_year == @year")
                .groupby("Club")["Days Missed"]
                .sum()
                .sort_values(ascending=False)
                .rename("days_missed"))

    def severe_prop_by_club(self, threshold=28):
        """Proporção de lesões severas (≥ threshold dias)"""
        tmp = self._ensure_year().query("season_year == @year").copy()
        tmp["severe"] = tmp["Days Missed"] >= threshold
        return (tmp.groupby("Club")["severe"]
                   .mean()
                   .sort_values(ascending=False)
                   .rename("severe_prop"))
    
    def plot_top_10_injured_teams(self):
        YEARS = [2021, 2022, 2023, 2024]
        for y in YEARS:
            top10 = self.injuries_by_club(y).head(10)
            plt.figure(figsize=(8,4))
            sns.barplot(x=top10.values, y=top10.index, color="firebrick")
            plt.title(f"Top-10 clubes com mais lesões – {y}")
            plt.xlabel("Número de lesões")
            plt.ylabel("")
            plt.tight_layout()
            plt.show()
    
    def plot_anual_injury_evolution_per_team(self):
        pivot = (self._ensure_year()
                 .query("season_year >= 2021 & season_year <= 2024")
                 .pivot_table(index="Club", columns="season_year",
                              values="Injury", aggfunc="size", fill_value=0))
        plt.figure(figsize=(10, max(6, len(pivot)/2)))
        sns.heatmap(pivot.sort_values(2024, ascending=False),
                    annot=True, fmt="d", cmap="Reds")
        plt.title("Lesões por clube e temporada (2021-2024)")
        plt.xlabel("Ano da temporada")
        plt.ylabel("")
        plt.tight_layout()
        plt.show()