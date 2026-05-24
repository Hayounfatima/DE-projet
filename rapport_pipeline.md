# Rapport de traitement du pipeline Data Lake

## 1. Ingestion Bronze (`bronze_ingestion.py`)

**Règle appliquée**  
- Chargement du fichier JSON source (supporte les formats `{"offres": [...]}` ou liste directe).  
- Partitionnement par source et par mois (d’après `date_publication`).  
- Écriture brute dans `data_lake_root/bronze/{source}/{mois}/offres_raw.json`.  
- Zone **immuable** : aucune modification des données.

**Statistiques**  
- Offres ingérées : **5000**  
- Répartition par source :  
  - `rekrute` : 2235  
  - `linkedin` : 1496  
  - `marocannonce` : 1269  
- Partitions créées : **69** (combinaisons source × mois)

**Cas limites**  
- Fichier source inexistant → `FileNotFoundError` géré.  
- JSON sous forme de liste directe → adaptation du code (`isinstance(data, list)`).  
- Dates de publication invalides → partition `date_inconnue`.

---

## 2. Transformation Silver – Nettoyage (`silver_transform.py`)

**Règles appliquées**  
- **Normalisation des titres** : mapping par regex vers des familles de profils (Data Engineer, Data Analyst, DevOps, etc.). Les non‑reconnus deviennent `Autre IT`.  
- **Parsing des salaires** : extraction des nombres, conversion en MAD (1 EUR = 10,8 MAD), détection de validité (3000‑100000 MAD). Gère les suffixes `k` et les fourchettes.  
- **Normalisation de l’expérience** : transformation en années (min/max). “Débutant” → 0‑2 ans, “Senior” → 5+ ans.  
- **Suppression des doublons** sur `(titre, entreprise, ville)`.  
- **Ajout des colonnes `annee` et `mois`** (nécessaires pour les agrégations Gold).

**Statistiques**  
- Offres lues depuis Bronze : **5000**  
- Doublons supprimés : **0**  
- Titres classés `Autre IT` : **2839** (56,78%)  
- Offres avec salaire valide : **62,9%** (soit ~3145 offres)  
- Fichier sauvegardé : `data_lake_root/silver/offres_clean/offres_clean.parquet` (1086 Ko)

**Cas limites traités**  
- Salaire textuel “Selon profil”, “Confidentiel”, vide → `salaire_connu = False`.  
- Fourchette “15k-20k” → converti en 15000‑20000.  
- Expérience “Débutant accepté” → 0‑2 ans.  
- Colonne `nb_postes` contenant des chaînes (ex: “1 poste”) → extraction de l’entier.  
- Colonne `langue_requise` mélangeant listes et chaînes → conversion en chaîne unifiée.  
- Dates mal formatées → `NaT` puis `annee`/`mois` à 0.

---

## 3. Extraction des compétences (NLP) – `silver_nlp.py`

**Règle appliquée**  
- Matching de mots entiers (regex `\b`) dans `competences_brut` et `description`.  
- Utilisation d’un référentiel JSON (familles, compétences, alias).  
- Une ligne par compétence détectée pour chaque offre.  
- Les offres sans aucune compétence reçoivent `competence = 'non_détecté'`.

**Statistiques**  
- Offres en entrée : **5000**  
- Lignes offre‑compétence produites : **10 152**  
- Offres avec au moins une compétence : **3462** (69,2%)  
- Offres sans aucune compétence : 5000 - 3462 = **1538** (30,8%)  
- Fichier sauvegardé : `data_lake_root/silver/competences_extraites/competences.parquet` (83 Ko)

**Cas limites**  
- Compétence non trouvée → ligne `non_détecté` pour traçabilité.  
- Alias multiples (node.js / nodejs) → unifiés via référentiel.  
- Description vide → aucune compétence.

---

## 4. Agrégations Gold (`gold_aggregation.py`)

**Règles appliquées**  
Utilisation de DuckDB sur les fichiers Parquet Silver pour produire 5 tables :

| Table | Contenu | Règle métier |
|-------|---------|---------------|
| `top_competences.parquet` | Fréquence des compétences par profil | Classement, pourcentage par rapport au total offres |
| `salaires_par_profil.parquet` | Statistiques salariales (médiane, quartiles) par profil et ville | Minimum 5 offres pour fiabilité |
| `offres_par_ville.parquet` | Volume d’offres et part de télétravail par ville, profil, mois | Regroupement avec calcul pourcentage remote |
| `entreprises_recruteurs.parquet` | Top 100 entreprises (≥3 offres) | Agrégation par entreprise et ville |
| `tendances_mensuelles.parquet` | Évolution mensuelle du nombre d’offres et salaire moyen par profil | Série temporelle avec lag du mois précédent |

**Statistiques**  
- 5 tables Parquet créées dans `data_lake_root/gold/`.  
- Avec 5000 offres et 10 152 associations compétences, chaque table contient entre quelques dizaines et quelques centaines de lignes.

**Cas limites**  
- Colonnes `annee`/`mois` manquantes → ajoutées dans `run_pipeline.py`.  
- Offres sans salaire → exclues des agrégations salariales (filtre `salaire_connu`).  
- Ville inconnue → regroupée sous `'Inconnue'`.  
- Télétravail non renseigné → `Non spécifié`.

---

## 5. Synthèse des volumes

| Étape | Entrée | Sortie |
|-------|--------|--------|
| Bronze | 5000 offres brutes | 69 partitions JSON |
| Silver (nettoyage) | 5000 offres | 5000 offres nettoyées (1086 Ko) |
| Silver (NLP) | 5000 offres | 10 152 lignes offre‑compétence (83 Ko) |
| Gold | 2 fichiers Silver | 5 tables Parquet |

---

## 6. Problèmes rencontrés et solutions

| Problème | Solution |
|----------|----------|
| Fichier source JSON absent | Vérification `os.path.exists` |
| JSON sous forme de liste directe (sans clé `offres`) | Test `isinstance(data, list)` |
| `nb_postes` contenant `"1 poste"` (string) | Extraction des chiffres avec regex |
| `langue_requise` mélangeant listes et chaînes | Conversion en chaîne via fonction dédiée |
| Colonnes `annee`/`mois` manquantes pour DuckDB | Création à partir de `date_publication` |
| Warnings regex (groupes de capture) | Sans impact, peut être ignoré |
| Dates invalides | `errors='coerce'` + remplacement par 0 |

---

## 7. Conclusion

Le pipeline a ingéré **5000 offres** d’emploi IT au Maroc, nettoyé les salaires (62,9% de couverture), extrait **10 152 compétences** (couvrant 69,2% des offres) et produit les 5 tables d’agrégation Gold. Les transformations intègrent la gestion de nombreux cas limites (types hétérogènes, formats JSON variables, chaînes non numériques). Le code est robuste et prêt à passer à l’échelle.