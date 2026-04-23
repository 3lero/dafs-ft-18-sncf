from pathlib import Path
import time
import pandas as pd
import requests_cache
import openmeteo_requests
from retry_requests import retry


PROCESSED_DIR = Path("data/processed")
BATCH_DIR = Path("data/batches")
CACHE_DIR = Path(".cache")

PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
BATCH_DIR.mkdir(parents=True, exist_ok=True)
CACHE_DIR.mkdir(parents=True, exist_ok=True)


DAILY_VARS = [
    "temperature_2m_mean",
    "temperature_2m_max",
    "temperature_2m_min",
    "precipitation_sum",
    "rain_sum",
    "snowfall_sum",
    "wind_speed_10m_max",
    "wind_gusts_10m_max",
    "daylight_duration",
]

BATCH_SIZE = 10
PAUSE_BETWEEN_BATCHES = 65
WAIT_SECONDS_ON_LIMIT = 3600
WAIT_SECONDS_ON_ERROR = 300


def save_dataframe(df: pd.DataFrame, output_path: Path) -> None:
    df.to_csv(output_path, index=False, encoding="utf-8")
    print(f"Fichier sauvegardé : {output_path}")


def create_openmeteo_client():
    cache_session = requests_cache.CachedSession(str(CACHE_DIR / "openmeteo_cache"), expire_after=-1)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    return openmeteo_requests.Client(session=retry_session)


def get_date_range_from_regularite(df_regularite: pd.DataFrame) -> tuple[str, str]:
    df_regularite = df_regularite.copy()
    df_regularite["Date"] = pd.to_datetime(df_regularite["Date"])

    start_date = df_regularite["Date"].min().strftime("%Y-%m-%d")
    end_date = (df_regularite["Date"].max() + pd.offsets.MonthEnd(0)).strftime("%Y-%m-%d")

    return start_date, end_date


def build_monthly_weather_for_batch(
    batch: pd.DataFrame,
    responses,
    daily_vars: list[str],
) -> pd.DataFrame:
    batch_results = []

    for station_name, response in zip(batch["Gare de départ"], responses):
        daily = response.Daily()

        daily_data = {
            "date": pd.date_range(
                start=pd.to_datetime(
                    daily.Time() + response.UtcOffsetSeconds(),
                    unit="s",
                    utc=True,
                ),
                end=pd.to_datetime(
                    daily.TimeEnd() + response.UtcOffsetSeconds(),
                    unit="s",
                    utc=True,
                ),
                freq=pd.Timedelta(seconds=daily.Interval()),
                inclusive="left",
            )
        }

        for idx, var in enumerate(daily_vars):
            daily_data[var] = daily.Variables(idx).ValuesAsNumpy()

        df_daily = pd.DataFrame(daily_data)
        df_daily["date"] = pd.to_datetime(df_daily["date"]).dt.tz_localize(None)
        df_daily["Date"] = df_daily["date"].dt.to_period("M").dt.to_timestamp()
        df_daily["Gare de départ"] = station_name

        df_monthly = (
            df_daily.groupby(["Gare de départ", "Date"], as_index=False)
            .agg({
                "temperature_2m_mean": "mean",
                "temperature_2m_max": "max",
                "temperature_2m_min": "min",
                "precipitation_sum": "sum",
                "rain_sum": "sum",
                "snowfall_sum": "sum",
                "wind_speed_10m_max": "max",
                "wind_gusts_10m_max": "max",
                "daylight_duration": "mean",
            })
        )

        batch_results.append(df_monthly)

    if not batch_results:
        return pd.DataFrame()

    return pd.concat(batch_results, ignore_index=True)


def fetch_weather_batches(
    df_stations: pd.DataFrame,
    start_date: str,
    end_date: str,
    daily_vars: list[str],
    openmeteo_client,
    batch_size: int = BATCH_SIZE,
    pause: int = PAUSE_BETWEEN_BATCHES,
    save_dir: str | Path = BATCH_DIR,
) -> pd.DataFrame:
    save_dir = Path(save_dir)
    save_dir.mkdir(parents=True, exist_ok=True)

    url = "https://archive-api.open-meteo.com/v1/archive"
    results = []

    stations = (
        df_stations[["Gare de départ", "latitude", "longitude"]]
        .drop_duplicates()
        .dropna(subset=["latitude", "longitude"])
        .reset_index(drop=True)
    )

    total_batches = (len(stations) + batch_size - 1) // batch_size
    print(f"Nombre total de batches météo : {total_batches}")

    for batch_num, start_idx in enumerate(range(0, len(stations), batch_size), start=1):
        batch = stations.iloc[start_idx:start_idx + batch_size].copy()
        output_file = save_dir / f"batch_{batch_num}.csv"

        if output_file.exists():
            print(f"Batch {batch_num} déjà présent, ignoré.")
            try:
                df_existing = pd.read_csv(output_file)
                df_existing["Date"] = pd.to_datetime(df_existing["Date"])
                results.append(df_existing)
            except Exception as exc:
                print(f"Impossible de relire {output_file.name} : {exc}")
            continue

        params = {
            "latitude": batch["latitude"].tolist(),
            "longitude": batch["longitude"].tolist(),
            "start_date": start_date,
            "end_date": end_date,
            "daily": daily_vars,
            "timezone": "Europe/Paris",
        }

        while True:
            try:
                responses = openmeteo_client.weather_api(url, params=params)
                df_batch = build_monthly_weather_for_batch(batch, responses, daily_vars)

                if df_batch.empty:
                    print(f"Batch {batch_num}/{total_batches} vide.")
                else:
                    df_batch.to_csv(output_file, index=False, encoding="utf-8")
                    results.append(df_batch)
                    print(f"Batch {batch_num}/{total_batches} sauvegardé : {output_file}")

                if batch_num < total_batches:
                    time.sleep(pause)

                break

            except Exception as exc:
                error_message = str(exc).lower()

                is_likely_limit = any(
                    keyword in error_message
                    for keyword in ["429", "rate limit", "too many requests", "quota"]
                )

                if is_likely_limit:
                    print(
                        f"Limite atteinte sur le batch {batch_num} : {exc}\n"
                        f"Pause automatique de {WAIT_SECONDS_ON_LIMIT / 60:.0f} minutes..."
                    )
                    time.sleep(WAIT_SECONDS_ON_LIMIT)
                    continue

                print(
                    f"Erreur temporaire sur le batch {batch_num} : {exc}\n"
                    f"Nouvelle tentative dans {WAIT_SECONDS_ON_ERROR / 60:.0f} minutes..."
                )
                time.sleep(WAIT_SECONDS_ON_ERROR)

    if not results:
        return pd.DataFrame()

    weather = pd.concat(results, ignore_index=True)
    weather["Date"] = pd.to_datetime(weather["Date"])
    return weather


def combine_weather_batches(save_dir: str | Path = BATCH_DIR) -> pd.DataFrame:
    save_dir = Path(save_dir)
    files = sorted(save_dir.glob("batch_*.csv"))

    if not files:
        return pd.DataFrame()

    weather = pd.concat((pd.read_csv(file) for file in files), ignore_index=True)
    weather["Date"] = pd.to_datetime(weather["Date"])
    return weather


def main() -> None:
    print("Chargement des données transformées...")
    df_regularite = pd.read_csv(PROCESSED_DIR / "regularite_liaisons_clean.csv")
    df_stations_clean = pd.read_csv(PROCESSED_DIR / "stations_clean.csv")

    required_regularite_cols = ["Date"]
    required_station_cols = ["Gare de départ", "latitude", "longitude"]

    missing_regularite = [col for col in required_regularite_cols if col not in df_regularite.columns]
    missing_stations = [col for col in required_station_cols if col not in df_stations_clean.columns]

    if missing_regularite:
        raise ValueError(
            f"Colonnes manquantes dans regularite_liaisons_clean.csv : {missing_regularite}"
        )

    if missing_stations:
        raise ValueError(
            f"Colonnes manquantes dans stations_clean.csv : {missing_stations}"
        )

    start_date, end_date = get_date_range_from_regularite(df_regularite)

    print("start_date =", start_date)
    print("end_date   =", end_date)

    openmeteo = create_openmeteo_client()

    df_weather = fetch_weather_batches(
        df_stations=df_stations_clean,
        start_date=start_date,
        end_date=end_date,
        daily_vars=DAILY_VARS,
        openmeteo_client=openmeteo,
        batch_size=BATCH_SIZE,
        pause=PAUSE_BETWEEN_BATCHES,
        save_dir=BATCH_DIR,
    )

    if df_weather.empty:
        print("Aucune nouvelle donnée récupérée, recombinaison des batches existants...")
        df_weather = combine_weather_batches(BATCH_DIR)

    if df_weather.empty:
        print("Aucune donnée météo disponible.")
        return

    df_weather = (
        df_weather
        .sort_values(["Gare de départ", "Date"])
        .drop_duplicates()
        .reset_index(drop=True)
    )

    save_dataframe(df_weather, PROCESSED_DIR / "weather_monthly.csv")
    print("Extraction météo terminée.")


if __name__ == "__main__":
    main()