import sys
import os
import pandas as pd
from pathlib import Path

# Ajoute le chemin du projet
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Paramètres
DATA_LAKE_ROOT = "./data_lake_root"   # à ajuster
SOURCE_FILE = "./data/sample_offres.json"   # tu dois créer un petit fichier test

# Import des modules
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
df['region_admin'] = 'Inconnue'  # à améliorer plus tard

# ⭐ EXTRACTION ANNÉE ET MOIS (obligatoire pour Gold)

df['date_publication'] = pd.to_datetime(df['date_publication'], errors='coerce')
df['annee'] = df['date_publication'].dt.year.fillna(0).astype(int)
df['mois'] = df['date_publication'].dt.month.fillna(0).astype(int)
# Sauvegarde des offres nettoyées (Silver)
silver_offres_dir = Path(DATA_LAKE_ROOT) / "silver" / "offres_clean"
silver_offres_dir.mkdir(parents=True, exist_ok=True)
df.to_parquet(silver_offres_dir / "offres_clean.parquet", index=False)

# 3. Extraction compétences (NLP)
# Il te faut un référentiel. Crée un fichier minimal (ex: referentiel.json)
# Je te donne un exemple plus bas.
if Path("referentiel.json").exists():
    df_comp = extraire_competences(df, "referentiel.json")
    sauvegarder_silver(df, df_comp, DATA_LAKE_ROOT)
else:
    print("⚠️ Référentiel manquant, skip NLP")

# 4. Gold
print("=== AGRÉGATIONS GOLD ===")
construire_gold(DATA_LAKE_ROOT)