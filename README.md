# Analyse de l’impact des conditions météorologiques sur le transport ferroviaire

## Contexte et objectif

Ce projet s’inscrit dans une démarche d’analyse de données visant à étudier l’influence des conditions météorologiques sur le transport ferroviaire en France.

L’objectif principal est de construire un jeu de données propre, enrichi et exploitable, en combinant plusieurs sources :

* données de gares ferroviaires
* données météorologiques
* référentiels géographiques (communes, départements, régions)

Ce travail constitue une base pour des analyses exploratoires avancées ou le développement de modèles prédictifs.

---

## Données utilisées

Le projet repose sur plusieurs types de données :

* informations sur les gares (localisation, identifiants)
* données météorologiques agrégées
* référentiels administratifs français (codes commune, département, région)

Les fichiers intermédiaires issus de traitements par lots sont stockés dans `data/batches/`.

Une attention particulière est portée à la cohérence des clés de jointure (codes géographiques) et à la qualité des données.

---

## Sources de données

Les données utilisées dans ce projet proviennent de sources ouvertes et institutionnelles :

* Données des gares ferroviaires
  https://ressources.data.sncf.com/explore/dataset/gares-de-voyageurs
  - Informations sur les gares de voyageurs en France (localisation, identifiants)

* Régularité mensuelle des TGV
  https://ressources.data.sncf.com/explore/dataset/regularite-mensuelle-tgv-aqst
  - Indicateurs de performance et ponctualité du transport ferroviaire

* Référentiel des départements français
  https://www.data.gouv.fr/datasets/departements-de-france
  - Informations administratives sur les départements (codes, noms)

* Référentiel des communes (INSEE)
  https://www.insee.fr/fr/information/8740222
  - Codes officiels des communes, départements et régions

Ces sources ont été nettoyées, harmonisées et croisées afin de construire un dataset exploitable pour l’analyse.

---

## Méthodologie

Les principales étapes du traitement sont les suivantes :

1. Nettoyage des données

   * suppression des doublons
   * gestion des valeurs manquantes
   * homogénéisation des formats

2. Normalisation des variables clés

   * standardisation des codes géographiques (format texte avec padding)
   * sécurisation des types de données

3. Enrichissement des données

   * jointures entre gares et référentiels géographiques
   * ajout des informations départementales et régionales

4. Transformation des données météo

   * agrégation mensuelle
   * préparation pour analyse temporelle

5. Contrôle de qualité

   * détection et correction des doublons issus des jointures
   * validation des relations (many-to-one) entre tables

---

## Résultats

Le projet produit plusieurs jeux de données structurés dans `data/processed/` :

* `stations_clean.csv` : données gares nettoyées
* `weather_monthly.csv` : données météo agrégées
* `info_geo.csv` : données enrichies avec informations géographiques

Ces datasets sont prêts à être utilisés pour :

* analyse exploratoire
* visualisation
* modélisation statistique ou machine learning

---

## Structure du projet

```id="proj_struct"
.
├── data/
│   ├── raw/
│   ├── batches/      # fichiers intermédiaires générés par lots
│   └── processed/
├── notebooks/
│   └── enrichissement_geo_meteo_gares.ipynb
├── README.md
```

---

## Compétences mobilisées

* Manipulation de données avec Python (pandas)
* Nettoyage et préparation de données
* Jointures complexes et gestion de référentiels
* Structuration d’un pipeline de transformation de données
* Bonnes pratiques de reproductibilité (organisation des fichiers, séparation des données)

---

## Perspectives

Ce travail peut être étendu de plusieurs façons :

* analyse de l’impact météo sur les retards ou perturbations
* construction de variables explicatives avancées
* développement de modèles prédictifs
* visualisations interactives (cartes, séries temporelles)

---

## Exécution

```bash id="run_proj"
git clone https://github.com/ton-username/ton-repo.git
cd ton-repo
pip install -r requirements.txt
jupyter notebook notebooks/enrichissement_geo_meteo_gares.ipynb
```

---

## Auteur

Projet réalisé dans le cadre d’une montée en compétences en data science, avec une attention particulière portée à la qualité des données et à la structuration du code.
