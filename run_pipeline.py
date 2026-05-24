import sys
import os
from pathlib import Path
import pandas as pd
import json
import tempfile

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

DATA_LAKE_ROOT = "./data_lake_root"
SOURCE_FILE = "./data/sample_offres.json"

from pipeline.bronze_ingestion import ingerer_bronze
from pipeline.silver_transform import charger_depuis_bronze, nettoyer_titres_postes, normaliser_salaires, normaliser_experience
from pipeline.silver_nlp import extraire_competences, sauvegarder_silver
from pipeline.gold_aggregation import construire_gold

# 1. Bronze
print("=== INGESTION BRONZE ===")
stats = ingerer_bronze(SOURCE_FILE, DATA_LAKE_ROOT)
print(stats)

# 2. Silver : chargement + transformations
print("=== TRANSFORMATION SILVER ===")
df = charger_depuis_bronze(DATA_LAKE_ROOT)
df = nettoyer_titres_postes(df)
df = normaliser_salaires(df)
df = normaliser_experience(df)

# Ajouter des colonnes minimales pour Gold
df['ville_std'] = df.get('ville', 'Inconnue')
df['type_contrat_std'] = df.get('type_contrat', 'Non spécifié')
df['region_admin'] = 'Inconnue'

# Extraire annee et mois
df['date_publication'] = pd.to_datetime(df['date_publication'], errors='coerce')
df['annee'] = df['date_publication'].dt.year.fillna(0).astype(int)
df['mois'] = df['date_publication'].dt.month.fillna(0).astype(int)

# ========== CORRECTION : nettoyage des colonnes problématiques ==========
# 1. nb_postes
if 'nb_postes' in df.columns:
    df['nb_postes'] = pd.to_numeric(df['nb_postes'].astype(str).str.extract(r'(\d+)')[0], errors='coerce').fillna(1).astype(int)

# 2. langue_requise : tout convertir en chaîne propre
if 'langue_requise' in df.columns:
    def clean_langue(x):
        if isinstance(x, list):
            return ', '.join(str(v) for v in x if v)
        elif pd.isna(x):
            return ''
        else:
            return str(x)
    df['langue_requise'] = df['langue_requise'].apply(clean_langue)

# 3. Autres colonnes suspectes
for col in ['competences_brut', 'teletravail']:
    if col in df.columns:
        df[col] = df[col].astype(str).fillna('')
# ======================================================================

# Sauvegarde des offres nettoyées (Silver)
silver_offres_dir = Path(DATA_LAKE_ROOT) / "silver" / "offres_clean"
silver_offres_dir.mkdir(parents=True, exist_ok=True)
df.to_parquet(silver_offres_dir / "offres_clean.parquet", index=False)

# 3. Extraction compétences (si référentiel existe)
if Path("referentiel.json").exists():
    df_comp = extraire_competences(df, "referentiel.json")
    sauvegarder_silver(df, df_comp, DATA_LAKE_ROOT)
else:
    print("⚠️ Référentiel manquant, skip NLP")

# 4. Gold
print("=== AGRÉGATIONS GOLD ===")
construire_gold(DATA_LAKE_ROOT)