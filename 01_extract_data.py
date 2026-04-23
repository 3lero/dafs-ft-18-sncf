from pathlib import Path
import io
import requests
import pandas as pd


BASE_URL = "https://ressources.data.sncf.com/api/explore/v2.1/catalog/datasets"

DATASETS = {
    "regularite_liaisons": "regularite-mensuelle-tgv-aqst",
    "gares": "gares-de-voyageurs",
}

DEPARTEMENTS_URL = (
    "https://static.data.gouv.fr/resources/"
    "departements-de-france/20200425-135513/departements-france.csv"
)

RAW_DIR = Path("data/raw")
RAW_DIR.mkdir(parents=True, exist_ok=True)

INSEE_COMMUNES_FILE = RAW_DIR / "insee_communes.csv"


def extract_sncf_dataset(dataset_id: str) -> pd.DataFrame:
    url = f"{BASE_URL}/{dataset_id}/exports/csv"
    params = {
        "delimiter": ";",
        "use_labels": "true",
        "timezone": "Europe/Paris",
    }

    response = requests.get(url, params=params, timeout=60)
    response.raise_for_status()

    return pd.read_csv(io.StringIO(response.text), sep=";")


def extract_csv_from_url(url: str, sep: str = ",") -> pd.DataFrame:
    return pd.read_csv(url, sep=sep)


def extract_local_csv(path: Path, sep: str = ",") -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(
            f"Fichier introuvable : {path}\n"
            "Ajoute le fichier INSEE dans data/raw/ avant d'exécuter le script."
        )
    return pd.read_csv(path, sep=sep)


def save_dataframe(df: pd.DataFrame, output_path: Path) -> None:
    df.to_csv(output_path, index=False, encoding="utf-8")
    print(f"Fichier sauvegardé : {output_path}")


def main() -> None:
    print("Début de l'extraction des données brutes...")

    dataframes = {}

    for name, dataset_id in DATASETS.items():
        print(f"Extraction de {name}...", end=" ")
        df = extract_sncf_dataset(dataset_id)
        dataframes[name] = df
        print(f"{len(df)} lignes × {len(df.columns)} colonnes")

    print("Extraction du référentiel départements...", end=" ")
    df_departements = extract_csv_from_url(DEPARTEMENTS_URL)
    print(f"{len(df_departements)} lignes × {len(df_departements.columns)} colonnes")

    print("Chargement du référentiel communes INSEE local...", end=" ")
    df_communes = extract_local_csv(INSEE_COMMUNES_FILE)
    print(f"{len(df_communes)} lignes × {len(df_communes.columns)} colonnes")

    save_dataframe(dataframes["regularite_liaisons"], RAW_DIR / "regularite_liaisons.csv")
    save_dataframe(dataframes["gares"], RAW_DIR / "gares.csv")
    save_dataframe(df_departements, RAW_DIR / "departements.csv")
    save_dataframe(df_communes, RAW_DIR / "communes.csv")

    print("Extraction terminée.")


if __name__ == "__main__":
    main()