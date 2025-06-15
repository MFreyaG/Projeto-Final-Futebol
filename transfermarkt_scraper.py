import re
import time
from io import StringIO
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
import pandas as pd

# ------------------------------------------------------------------
# Configurações gerais
# ------------------------------------------------------------------
BASE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}
BASE_URL = "https://www.transfermarkt.us"

LEAGUE_MAP = {
    "Spain": ("laliga", "ES1"),
    "England": ("premier-league", "GB1"),
    "Italy": ("serie-a", "IT1"),
    "Germany": ("bundesliga", "L1"),
    "France": ("ligue-1", "FR1"),
}


# ------------------------------------------------------------------
# Helper: aplainar MultiIndex de colunas
# ------------------------------------------------------------------
def _flatten_columns(cols):
    if isinstance(cols, pd.MultiIndex):
        # se nível 1 não vazio, use-o; senão, use nível 0
        return [lvl1 if lvl1 not in ("", None) else lvl0 for lvl0, lvl1 in cols]
    else:
        return list(cols)


# ------------------------------------------------------------------
# 1) Lesões por liga
# ------------------------------------------------------------------
def get_league_injuries(
    *,
    country_name: str = None,
    league_slug: str = None,
    league_code: str = None,
    session: requests.Session = None,
    timeout: int = 30,
) -> pd.DataFrame:
    if country_name:
        try:
            league_slug, league_code = LEAGUE_MAP[country_name]
        except KeyError:
            raise ValueError("País não mapeado; informe slug e código manualmente.")
    if not league_slug or not league_code:
        raise ValueError("Faltou league_slug ou league_code.")

    url = f"{BASE_URL}/{league_slug}/verletztespieler/wettbewerb/{league_code}"
    sess = session or requests.Session()
    resp = sess.get(url, headers=BASE_HEADERS, timeout=timeout)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "lxml")
    table = soup.find("table", class_="items")
    if table is None:
        raise RuntimeError("Tabela de lesões não encontrada.")

    df = pd.read_html(StringIO(str(table)), flavor="lxml")[0]
    df.columns = _flatten_columns(df.columns)

    # separar Player / Position
    split_cols = (
        df["Player/Position"]
        .str.split("\n", n=1, expand=True)
        .rename(columns={0: "Player", 1: "Position"})
    )
    if "Position" not in split_cols:
        split_cols["Position"] = pd.NA

    df = pd.concat([df.drop(columns="Player/Position"), split_cols], axis=1)

    # renomear
    df.rename(
        columns={
            "Club": "Team",
            "Injury": "Injury Type",
            "until": "Expected Return",
            "Market Value": "Market Value (€)",
        },
        inplace=True,
    )

    # strip em todas as colunas de texto
    df = df.apply(lambda col: col.str.strip() if col.dtype == "object" else col)
    return df


# ------------------------------------------------------------------
# 2) URLs de jogadores de um time
# ------------------------------------------------------------------
def get_team_player_urls(
    team_url: str,
    *,
    session: requests.Session = None,
    timeout: int = 30,
    pause: float = 1.0,
) -> list[str]:
    sess = session or requests.Session()
    resp = sess.get(team_url, headers=BASE_HEADERS, timeout=timeout)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "lxml")
    anchors = soup.select("a[href*='/profil/spieler/']")
    root = f"{urlparse(team_url).scheme}://{urlparse(team_url).netloc}"

    urls = {urljoin(root, a["href"].split("?")[0]) for a in anchors}
    time.sleep(pause)
    return sorted(urls)


# ------------------------------------------------------------------
# 3) Histórico de lesões de 1 jogador
# ------------------------------------------------------------------
def get_player_injury_history(
    player_url: str,
    *,
    session: requests.Session = None,
    timeout: int = 30,
) -> pd.DataFrame:
    injury_url = re.sub(r"/profil/", "/verletzungen/", player_url.rstrip("/"))
    sess = session or requests.Session()
    resp = sess.get(injury_url, headers=BASE_HEADERS, timeout=timeout)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "lxml")
    table = soup.find("table", class_="items")
    if table is None:
        raise RuntimeError("Histórico de lesões não encontrado.")

    df = pd.read_html(StringIO(str(table)), flavor="lxml")[0]
    df.columns = _flatten_columns(df.columns)

    df.rename(
        columns={
            "From": "Start",
            "Until": "End",
            "Days": "Days Missed",
            "Games missed": "Games Missed",
            "Matches missed": "Games Missed",
        },
        inplace=True,
    )

    player_name = soup.select_one("h1").get_text(strip=True)
    df.insert(0, "Player", player_name)
    df["Profile URL"] = player_url

    df = df.apply(lambda col: col.str.strip() if col.dtype == "object" else col)
    return df


# ------------------------------------------------------------------
# 4) Vários jogadores de uma vez
# ------------------------------------------------------------------
def get_multiple_players_injury_history(
    player_urls: list[str],
    *,
    session: requests.Session = None,
    timeout: int = 30,
    pause: float = 1.0,
) -> pd.DataFrame:
    sess = session or requests.Session()
    frames = []
    for url in player_urls:
        try:
            frames.append(get_player_injury_history(url, session=sess, timeout=timeout))
            time.sleep(pause)
        except Exception as ex:
            print(f"[WARN] falhou em {url}: {ex}")
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


# ------------------------------------------------------------------
# Exemplo de uso
# ------------------------------------------------------------------
# def main():
#     sess = requests.Session()

#     # Liga
#     liga_df = get_league_injuries(country_name="Spain", session=sess)
#     print("=== Top 5 lesões na La Liga hoje ===")
#     print(liga_df.head(), "\n")

#     # Time → URLs de jogadores
#     burnley_url = (
#         "https://www.transfermarkt.com/"
#         "fc-burnley/startseite/verein/1132/saison_id/2021"
#     )
#     player_urls = get_team_player_urls(burnley_url, session=sess)
#     print(f"Found {len(player_urls)} jogadores em Burnley 21/22\n")

#     # Histórico de 5 primeiros
#     history_df = get_multiple_players_injury_history(
#         player_urls[:5], session=sess
#     )
#     print("=== Histórico de lesões (5 primeiros jogadores) ===")
#     print(history_df.head())


# if __name__ == "__main__":
#     main()
