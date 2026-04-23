from pathlib import Path
import os
import math
import pandas as pd
import requests
from dotenv import load_dotenv


load_dotenv()

PROCESSED_DIR = Path("data/processed")

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

CHUNK_SIZE = 500

COLUMN_RENAME_MAP = {
    "Prct retard pour cause prise en compte voyageurs (affluence, gestions PSH, correspondances)": "prct_retard_cause_voyageurs",
    "Prct retard pour cause gestion en gare et réutilisation de matériel": "prct_retard_cause_gare_materiel",
    "Retard moyen trains en retard > 15 (si liaison concurrencée par vol)": "retard_moyen_trains_retard_15_vol",
}


def validate_env() -> None:
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise ValueError(
            "Variables d'environnement manquantes. "
            "Ajoute SUPABASE_URL et SUPABASE_KEY dans ton fichier .env"
        )


def prepare_for_supabase(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df = df.rename(columns=COLUMN_RENAME_MAP)

    # Convertit explicitement tous les NaN/NaT pandas en None Python
    df = df.astype(object)
    df = df.where(pd.notna(df), None)

    return df


def read_csv_for_upload(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Fichier introuvable : {path}")
    return pd.read_csv(path)


def try_get_supabase_client():
    """
    Essaie d'importer la librairie officielle supabase-py.
    Retourne un client si disponible, sinon None.
    """
    try:
        from supabase import create_client
        return create_client(SUPABASE_URL, SUPABASE_KEY)
    except Exception as exc:
        print(f"Client supabase-py indisponible, fallback REST utilisé. Détail : {exc}")
        return None


def upload_with_supabase_py(client, table_name: str, df: pd.DataFrame, chunk_size: int = CHUNK_SIZE) -> None:
    records = df.to_dict(orient="records")
    total = len(records)

    for i in range(0, total, chunk_size):
        chunk = records[i:i + chunk_size]
        client.table(table_name).insert(chunk).execute()
        print(f"{table_name} : {min(i + chunk_size, total)}/{total} lignes envoyées via supabase-py")


def upload_with_rest(table_name: str, df: pd.DataFrame, chunk_size: int = CHUNK_SIZE) -> None:
    url = f"{SUPABASE_URL}/rest/v1/{table_name}"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal",
    }

    records = df.to_dict(orient="records")
    total = len(records)

    for i in range(0, total, chunk_size):
        chunk = records[i:i + chunk_size]

        cleaned_chunk = []
        for row in chunk:
            cleaned_row = {
                key: (None if pd.isna(value) else value)
                for key, value in row.items()
            }
            cleaned_chunk.append(cleaned_row)

        response = requests.post(url, headers=headers, json=cleaned_chunk, timeout=120)

        if not response.ok:
            raise RuntimeError(
                f"Erreur REST sur {table_name} "
                f"(batch {i // chunk_size + 1}): "
                f"{response.status_code} - {response.text}"
            )

        print(f"{table_name} : {min(i + chunk_size, total)}/{total} lignes envoyées via REST")


def upload_dataframe(table_name: str, file_path: Path, client=None) -> None:
    print(f"Préparation de {file_path.name} pour la table {table_name}...")
    df = read_csv_for_upload(file_path)
    df = prepare_for_supabase(df)

    if client is not None:
        upload_with_supabase_py(client, table_name, df)
    else:
        upload_with_rest(table_name, df)


def main() -> None:
    validate_env()

    files_to_upload = {
    "regularite_liaisons": PROCESSED_DIR / "regularite_liaisons_clean.csv",
    "weather_monthly": PROCESSED_DIR / "weather_monthly.csv",
    "info_geo": PROCESSED_DIR / "info_geo.csv",
    "stations_clean": PROCESSED_DIR / "stations_clean.csv",
    }

    client = try_get_supabase_client()

    for table_name, file_path in files_to_upload.items():
        upload_dataframe(table_name, file_path, client=client)

    print("Chargement Supabase terminé.")


if __name__ == "__main__":
    main()