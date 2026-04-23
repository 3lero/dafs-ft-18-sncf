from pathlib import Path
import re
import unicodedata
import pandas as pd


RAW_DIR = Path("data/raw")
PROCESSED_DIR = Path("data/processed")
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)


GARES_ETRANGERES = [
    "BARCELONA", "FRANCFORT", "GENEVE", "ITALIE",
    "LAUSANNE", "MADRID", "STUTTGART", "ZURICH",
]

CORRECTIONS_GARES = {
    "BELLEGARDE (AIN)": "Bellegarde-sur-Valserine",
    "BORDEAUX ST JEAN": "Bordeaux Saint-Jean",
    "DIJON VILLE": "Dijon",
    "LA ROCHELLE VILLE": "La Rochelle",
    "LILLE": "Lille Europe",
    "MACON LOCHE": "Mâcon Loché TGV",
    "MARNE LA VALLEE": "Marne-la-Vallée Chessy",
    "MARSEILLE ST CHARLES": "Marseille Saint-Charles",
    "MONTPELLIER": "Montpellier Sud de France",
    "MULHOUSE VILLE": "Mulhouse",
    "NICE VILLE": "Nice",
    "PARIS LYON": "Paris Gare de Lyon",
    "ST MALO": "Saint-Malo",
    "ST PIERRE DES CORPS": "Saint-Pierre-des-Corps",
    "VALENCE ALIXAN TGV": "Valence TGV Rhône-Alpes Sud",
    "LE CREUSOT MONTCEAU MONTCHANIN": "LE CREUSOT MONTCEAU LES MINES MONTCHANIN TGV",
    "PARIS NORD": "PARIS GARE DU NORD",
}

def check_required_files():
    required_files = [
        "regularite_liaisons.csv",
        "gares.csv",
        "communes.csv",
        "departements.csv",
    ]

    missing_files = [
        f for f in required_files if not (RAW_DIR / f).exists()
    ]

    if missing_files:
        raise FileNotFoundError(
            "Fichiers manquants dans data/raw/ :\n"
            + "\n".join(f"- {f}" for f in missing_files)
        )

def save_dataframe(df: pd.DataFrame, output_path: Path) -> None:
    df.to_csv(output_path, index=False, encoding="utf-8")
    print(f"Fichier sauvegardé : {output_path}")


def clean_gare(name: str) -> str:
    """Normalise un nom de gare pour faciliter l'appariement."""
    if pd.isna(name):
        return name

    name = unicodedata.normalize("NFKD", str(name)).encode("ascii", "ignore").decode("utf-8")
    name = name.upper()
    name = re.sub(r"[^A-Z0-9 ]", " ", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name


def normalize_code(series: pd.Series, width: int) -> pd.Series:
    """Convertit une série en code texte propre en conservant les zéros à gauche."""
    return (
        series.astype(str)
        .str.replace(".0", "", regex=False)
        .str.strip()
        .replace({"nan": pd.NA, "None": pd.NA, "": pd.NA, "<NA>": pd.NA})
        .str.zfill(width)
    )


def add_lat_lon_from_position(
    df: pd.DataFrame,
    position_col: str = "Position géographique"
) -> pd.DataFrame:
    """Extrait latitude et longitude depuis une colonne 'lat,lon'."""
    out = df.copy()
    coords = out[position_col].astype(str).str.split(",", expand=True)
    out["latitude"] = pd.to_numeric(coords[0], errors="coerce")
    out["longitude"] = pd.to_numeric(coords[1], errors="coerce")
    return out


def build_station_reference(df_regularite: pd.DataFrame, df_gares: pd.DataFrame) -> pd.DataFrame:
    """Construit la table de correspondance gare d'origine -> gare référentielle + coordonnées."""
    gares_depart = pd.DataFrame(
        {"Gare de départ": sorted(df_regularite["Gare de départ"].dropna().unique())}
    )

    gares_depart = gares_depart[
        ~gares_depart["Gare de départ"].astype(str).str.isnumeric()
    ].copy()

    gares_depart = gares_depart[
        ~gares_depart["Gare de départ"].isin(GARES_ETRANGERES)
    ].copy()

    gares_depart["Gare_match"] = gares_depart["Gare de départ"].replace(CORRECTIONS_GARES)
    gares_depart["gare_clean"] = gares_depart["Gare_match"].apply(clean_gare)

    gares_ref = df_gares.copy()
    gares_ref["gare_clean"] = gares_ref["Nom_Gare"].apply(clean_gare)

    merged = gares_depart.merge(
        gares_ref[["Nom_Gare", "gare_clean", "Position géographique", "Code commune"]],
        on="gare_clean",
        how="left",
    )

    merged = add_lat_lon_from_position(merged)
    return merged


def build_stations_clean(df_station_reference: pd.DataFrame) -> pd.DataFrame:
    """Construit la table finale des gares avec coordonnées."""
    df_stations_clean = (
        df_station_reference[["Gare de départ", "latitude", "longitude"]]
        .drop_duplicates()
        .dropna(subset=["latitude", "longitude"])
        .reset_index(drop=True)
    )
    return df_stations_clean


def clean_info_geo_for_export(df_info_geo: pd.DataFrame) -> pd.DataFrame:
    """
    Nettoie les colonnes finales de info_geo pour export :
    - codes administratifs en texte
    - latitude/longitude en numérique
    """
    df_info_geo = df_info_geo.copy()

    code_columns = {
        "Code commune": 5,
        "code_departement": 2,
        "code_region": 2,
    }

    for col, width in code_columns.items():
        if col in df_info_geo.columns:
            df_info_geo[col] = normalize_code(df_info_geo[col], width=width)

    numeric_columns = ["latitude", "longitude"]
    for col in numeric_columns:
        if col in df_info_geo.columns:
            df_info_geo[col] = pd.to_numeric(df_info_geo[col], errors="coerce")

    return df_info_geo


def build_info_geo(
    df_station_reference: pd.DataFrame,
    df_communes: pd.DataFrame,
    df_departements: pd.DataFrame,
) -> pd.DataFrame:
    """Construit la table géographique enrichie."""
    df_geo = (
        df_station_reference[
            ["Gare de départ", "Gare_match", "Nom_Gare", "latitude", "longitude", "Code commune"]
        ]
        .drop_duplicates()
        .copy()
    )

    df_geo["Code commune"] = normalize_code(df_geo["Code commune"], width=5)

    df_communes_ref = df_communes[["COM", "DEP", "REG"]].copy()
    df_communes_ref["COM"] = normalize_code(df_communes_ref["COM"], width=5)
    df_communes_ref["DEP"] = normalize_code(df_communes_ref["DEP"], width=2)
    df_communes_ref["REG"] = normalize_code(df_communes_ref["REG"], width=2)

    df_communes_ref = df_communes_ref.dropna(subset=["COM"])

    df_communes_ref = (
        df_communes_ref
        .assign(nb_na=df_communes_ref[["DEP", "REG"]].isna().sum(axis=1))
        .sort_values(["COM", "nb_na"])
        .drop_duplicates(subset=["COM"], keep="first")
        .drop(columns="nb_na")
    )

    df_departements_ref = df_departements.copy()
    df_departements_ref["code_departement"] = normalize_code(
        df_departements_ref["code_departement"], width=2
    )

    if "code_region" in df_departements_ref.columns:
        df_departements_ref["code_region"] = normalize_code(
            df_departements_ref["code_region"], width=2
        )

    df_departements_ref = (
        df_departements_ref
        .dropna(subset=["code_departement"])
        .drop_duplicates(subset=["code_departement"])
    )

    df_info_geo = (
        df_geo
        .merge(
            df_communes_ref,
            left_on="Code commune",
            right_on="COM",
            how="left",
            validate="many_to_one"
        )
        .merge(
            df_departements_ref,
            left_on="DEP",
            right_on="code_departement",
            how="left",
            validate="many_to_one"
        )
        .drop(columns=["COM", "Gare_match", "Nom_Gare", "DEP", "REG"], errors="ignore")
    )

    df_info_geo = clean_info_geo_for_export(df_info_geo)
    df_info_geo = df_info_geo.dropna().reset_index(drop=True)

    return df_info_geo


def main() -> None:
    print("Chargement des données brutes...")
    check_required_files()
    df_regularite = pd.read_csv(RAW_DIR / "regularite_liaisons.csv")
    df_gares = pd.read_csv(RAW_DIR / "gares.csv")
    df_communes = pd.read_csv(RAW_DIR / "communes.csv")
    df_departements = pd.read_csv(RAW_DIR / "departements.csv")

    print("Construction de la référence gares...")
    df_station_reference = build_station_reference(df_regularite, df_gares)

    print("Construction des gares nettoyées...")
    df_stations_clean = build_stations_clean(df_station_reference)
    print("Nombre de gares avec coordonnées :", len(df_stations_clean))

    print("Construction de l'enrichissement géographique...")
    df_info_geo = build_info_geo(df_station_reference, df_communes, df_departements)

    print("Dimensions de df_info_geo :", df_info_geo.shape)
    print("Valeurs manquantes par colonne :")
    print(df_info_geo.isna().sum().sort_values(ascending=False))

    save_dataframe(df_regularite, PROCESSED_DIR / "regularite_liaisons_clean.csv")
    save_dataframe(df_station_reference, PROCESSED_DIR / "station_reference.csv")
    save_dataframe(df_stations_clean, PROCESSED_DIR / "stations_clean.csv")
    save_dataframe(df_info_geo, PROCESSED_DIR / "info_geo.csv")

    print("Transformation terminée.")


if __name__ == "__main__":
    main()