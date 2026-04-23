# Rail Traffic & Weather Data Pipeline

## Overview

This project implements an end-to-end data pipeline that collects, cleans, enriches, and loads railway traffic data into a cloud database.

It combines multiple data sources:

* SNCF railway traffic datasets
* Geographic reference data (INSEE, departments)
* Historical weather data (Open-Meteo)

The final datasets are structured and loaded into Supabase (PostgreSQL) for analysis.

---

## Objectives

* Build a modular and reproducible data pipeline
* Handle real-world messy data (missing values, inconsistencies)
* Enrich transport data with external signals (weather)
* Prepare data for analytics and BI use cases

---

## Data Sources

* SNCF Open Data (rail traffic, stations)
* INSEE (commune reference dataset)
* Data.gouv (departments)
* Open-Meteo API (historical weather)

---

## Pipeline Architecture

```text
RAW DATA
   │
   ▼
[01_extract_data.py]
   │
   ▼
[02_transform_data.py]
   │
   ▼
[03_extract_weather.py]
   │
   ▼
[04_export_supabase.py]
   │
   ▼
SUPABASE DATABASE
```

---

## Project Structure

```text
.
├── 01_extract_data.py
├── 02_transform_data.py
├── 03_extract_weather.py
├── 04_export_supabase.py
├── data/
│   ├── raw/
│   ├── batches/
│   └── processed/
├── requirements.txt
├── .env.example
└── README.md
```

---

## Required Local Data

One dataset must be downloaded manually:

* `insee_communes.csv`

### How to obtain it

Download from INSEE:

https://www.insee.fr/fr/information/8740222

Place the file here:

```bash
data/raw/insee_communes.csv
```

The pipeline will not run without this file.

---

## Installation

```bash
git clone https://github.com/your-username/your-repo.git
cd your-repo

python -m venv .venv
.venv\Scripts\activate

pip install -r requirements.txt
```

---

## Configuration

Create a `.env` file:

```env
SUPABASE_URL=your_project_url
SUPABASE_KEY=your_service_role_key
```

---

## Execution

Run the pipeline step by step:

```bash
python 01_extract_data.py
python 02_transform_data.py
python 03_extract_weather.py
python 04_export_supabase.py
```

---

## Key Features

* Modular pipeline (4 independent scripts)
* Data cleaning and normalization (stations, geographic codes)
* Batch processing for API limits (weather)
* Automatic retry and resume mechanism
* Integration of multiple data sources
* Cloud database loading (Supabase)

---

## Technical Stack

* Python
* Pandas
* Requests
* Open-Meteo API
* Supabase (PostgreSQL)

---

## Challenges & Solutions

* **Data inconsistency (station names)**
  → normalization and manual mapping

* **API rate limits (weather)**
  → batch processing + retry logic

* **Geographic enrichment complexity**
  → multi-step joins with cleaned reference tables

---

## Author

Data pipeline project developed as part of a data engineering / data science training.