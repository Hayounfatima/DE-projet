import os
import json
import pandas as pd

def get_data_lake_root():
    """Retourne le chemin absolu vers data_lake_root"""
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(current_dir)
    return os.path.join(project_root, "data_lake_root")

def ensure_dir(path):
    """Crée un dossier si inexistant"""
    os.makedirs(path, exist_ok=True)

def save_json(data, filepath):
    """Sauvegarde des données en JSON"""
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def load_json(filepath):
    """Charge un fichier JSON"""
    with open(filepath, 'r', encoding='utf-8') as f:
        return json.load(f)

def save_parquet(df, filepath):
    """Sauvegarde un DataFrame en Parquet"""
    df.to_parquet(filepath, index=False)

def load_parquet(filepath):
    """Charge un fichier Parquet"""
    return pd.read_parquet(filepath)

def load_all_json_from_source(source_name):
    """Charge tous les JSON d'une source (rekrute, marocannonce, linkedin)"""
    bronze_path = os.path.join(get_data_lake_root(), "bronze", source_name)
    all_data = []
    if os.path.exists(bronze_path):
        for filename in os.listdir(bronze_path):
            if filename.endswith('.json'):
                filepath = os.path.join(bronze_path, filename)
                data = load_json(filepath)
                if isinstance(data, list):
                    all_data.extend(data)
                else:
                    all_data.append(data)
    return all_data