# NYC Taxi Data Pipeline
## Pipeline de Données Massives avec Snowflake, dbt et Airflow

---

## Vue d'ensemble

Ce projet implémente un pipeline de données complet pour analyser ~40 millions de trajets de taxis jaunes de NYC (année 2024). Il couvre l'ingestion automatisée depuis les fichiers Parquet TLC, la transformation multi-couches avec dbt, l'orchestration avec Airflow, et les tests de qualité de données.

### Dataset
- **Source** : [NYC Taxi & Limousine Commission](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- **Volume** : ~40 millions de trajets (année 2024 complète)
- **Format** : Fichiers Parquet mensuels
- **Taille** : ~8 GB de données

---

## Architecture

```
Fichiers Parquet → RAW → STAGING → INTERMEDIATE → MARTS
                     ↓        ↓           ↓           ↓
              Données brutes → Nettoyage → Catégories → Analyses
```

### Schémas Snowflake
1. **RAW** : Données brutes importées (`YELLOW_TAXI_TRIPS`)
2. **STAGING** : Données nettoyées et enrichies (`stg_yellow_taxi_trips`)
3. **INTERMEDIATE** : Catégorisations business (`int_trip_metrics`)
4. **MARTS** : Tables analytiques finales (couche de consommation BI)
   - `daily_summary` : Métriques quotidiennes
   - `zone_analysis` : Analyses par zone
   - `hourly_patterns` : Patterns horaires

> **Choix d'architecture** : Le schéma `MARTS` suit la convention standard dbt (*Data Marts* = tables prêtes à consommer par la BI). Une couche `INTERMEDIATE` est ajoutée entre `STAGING` et `MARTS` pour isoler les catégorisations métier (types de trajets, périodes, jours).

---

## Installation et Configuration

### Prérequis
- Python 3.12+
- Docker & Docker Compose
- Compte Snowflake
- Astro CLI (pour Airflow)

### 1. Configuration Snowflake

Exécutez le script SQL d'initialisation dans la console Snowflake :
```sql
-- Voir snowflake/INITIALISATION_WH_DB.sql
-- Crée : role TRANSFORM, Warehouse NYC_TAXI_WH, Database NYC_TAXI_DB, User dbt_taxi, Permissions
```

**Preuves de configuration Snowflake** :

![Init Snowflake](Screenshots/01_INIT_SNOWFLAKE_TAXI.png)
*Initialisation du compte Snowflake*

![Compute Warehouse](Screenshots/02_COMPUTE_TAXI_WH.png)
*Création du Warehouse NYC_TAXI_WH*

![Warehouse créé](Screenshots/03_TAXI_WH_CREATION.png)
*Warehouse opérationnel*

![Test User & Database](Screenshots/05_1_TEST_USER_DATABASE.png)
*Vérification de l'utilisateur dbt_taxi et de la database NYC_TAXI_DB*

![Test User & Database 2](Screenshots/05_2_TEST_USER_DATABASE.png)
*Vérification des permissions*

### 2. Configuration Airflow

Copiez et configurez les fichiers d'environnement :
```bash
cd dbt-dag
cp .env.exemple .env
```

Éditez `.env` avec vos credentials Snowflake :
```bash
SF_ACCOUNT=votre-account-identifier
SF_USER=dbt_taxi
SF_PASSWORD=votre-mot-de-passe
SF_ROLE=TRANSFORM
SF_WAREHOUSE=NYC_TAXI_WH
SF_DATABASE=NYC_TAXI_DB
SF_SCHEMA=RAW
```

### 3. Démarrage d'Airflow

```bash
cd dbt-dag
astro dev start
```

Accédez à l'interface : `http://localhost:8080` (admin/admin)

![Lancement Airflow](Screenshots/06_LANCEMENT_ASTRO_DEV.png)
*Démarrage d'Airflow avec Astro CLI*

![DAGs disponibles](Screenshots/07_Dags_Taxi.png)
*DAGs taxi_ingestion_dag et dbt_transformation_dag disponibles dans Airflow*

---

## Exécution du Pipeline

### Étape 1 : Ingestion des Données

1. Dans Airflow, activez le DAG `taxi_ingestion_dag`
2. Déclenchez-le manuellement (bouton Play)
3. **Durée estimée** : 60-90 minutes (12 mois × ~5 min par fichier)
4. **Résultat** : Table `RAW.YELLOW_TAXI_TRIPS` créée avec ~40M lignes

> Le script est **idempotent** : si un mois est déjà chargé, il est automatiquement ignoré.

**Preuves d'exécution** :

![Ingestion en cours](Screenshots/08_Ingestion_Data_Taxi.png)
*DAG taxi_ingestion_dag en cours d'exécution*

![Téléchargement Parquet](Screenshots/09_Downloading_Parquet_Taxi.png)
*Téléchargement des fichiers Parquet mensuels*

![Fin téléchargement](Screenshots/10_End_Downloading_Parquet.png)
*Ingestion des 12 mois terminée avec succès*

**Vérification dans Snowflake** :

> **Note** : Les colonnes datetime sont stockées en `NUMBER` (microsecondes epoch). Utiliser `TO_TIMESTAMP` pour les convertir.

```sql
SELECT COUNT(*) FROM NYC_TAXI_DB.RAW.YELLOW_TAXI_TRIPS;
-- ~40 millions de trajets (2024 complet)

-- Vérification par mois
SELECT
    MONTH(TO_TIMESTAMP(TPEP_PICKUP_DATETIME / 1000000)) as mois,
    COUNT(*) as nb_trajets
FROM NYC_TAXI_DB.RAW.YELLOW_TAXI_TRIPS
GROUP BY 1
ORDER BY 1;
-- Doit retourner 12 lignes (janvier → décembre 2024)
```

![Aperçu données RAW](Screenshots/11_Data_Preview_Exemple.png)
*Aperçu des données brutes dans RAW.YELLOW_TAXI_TRIPS*

![Structure table 1](Screenshots/12_DESCRIBE_1.png)
*Structure de la table RAW.YELLOW_TAXI_TRIPS (colonnes 1/5)*

![Structure table 2](Screenshots/12_DESCRIBE_2.png)
*Structure de la table RAW.YELLOW_TAXI_TRIPS (colonnes 2/5)*

![Structure table 3](Screenshots/12_DESCRIBE_3.png)
*Structure de la table RAW.YELLOW_TAXI_TRIPS (colonnes 3/5)*

![Structure table 4](Screenshots/12_DESCRIBE_4.png)
*Structure de la table RAW.YELLOW_TAXI_TRIPS (colonnes 4/5)*

![Structure table 5](Screenshots/12_DESCRIBE_5.png)
*Structure de la table RAW.YELLOW_TAXI_TRIPS (colonnes 5/5)*

### Étape 2 : Transformation dbt

1. Dans Airflow, activez le DAG `dbt_transformation_dag`
2. Déclenchez-le manuellement
3. **Durée estimée** : 5-10 minutes
4. **Résultat** :
   - Vue `STAGING.stg_yellow_taxi_trips`
   - Vue `INTERMEDIATE.int_trip_metrics`
   - Tables `MARTS.*` (daily_summary, zone_analysis, hourly_patterns)

**Preuves d'exécution** :

![DAG dbt - Exécution](Screenshots/13_DBT_TRANSFORMATION_1.png)
*DAG de transformation dbt en cours d'exécution*

![DBT staging](Screenshots/13_DBT_TRANSFORMATION_stg_yellow_taxi_trips.png)
*Modèle stg_yellow_taxi_trips - 15 tests au vert*

![DBT intermediate](Screenshots/13_DBT_TRANSFORMATION_int_fact.png)
*Modèle int_trip_metrics exécuté avec succès*

![Snowflake - Tables MARTS](Screenshots/14_DBT_TRANSFORMATION_RESULT_SNOWFLAKE.png)
*Tables finales créées dans le schéma MARTS de Snowflake*

**Vérification dans Snowflake** :
```sql
-- Données nettoyées
SELECT * FROM NYC_TAXI_DB.STAGING.stg_yellow_taxi_trips LIMIT 10;

-- Résumés quotidiens
SELECT * FROM NYC_TAXI_DB.MARTS.daily_summary ORDER BY date DESC LIMIT 10;

-- Top 10 zones
SELECT * FROM NYC_TAXI_DB.MARTS.zone_analysis ORDER BY trip_count DESC LIMIT 10;

-- Patterns horaires
SELECT * FROM NYC_TAXI_DB.MARTS.hourly_patterns ORDER BY pickup_hour;
```

---

## Tests de Qualité

Le pipeline inclut **15 tests dbt automatiques** :
- Tests de non-nullité sur colonnes essentielles
- Tests de plages de valeurs (distances 0.1-100 miles, vitesse 0-150 mph)
- Tests de cohérence des montants (>= 0)
- Tests de catégorisations (distance, période, jour)

Résultat : **PASS=15 WARN=0 ERROR=0**

---

## Transformations Implémentées

### Nettoyage des Données (Staging)
- Conversion des timestamps NUMBER (microsecondes epoch) en TIMESTAMP via `TO_TIMESTAMP(col / 1000000)`
- Filtrage des montants négatifs
- Exclusion des trajets avec dates incohérentes
- Suppression des distances aberrantes (< 0.1 ou > 100 miles)
- Filtrage des vitesses aberrantes (> 150 mph — erreurs GPS/timestamps)
- Gestion des valeurs manquantes

### Enrichissements (Staging)
- Durée du trajet (minutes)
- Vitesse moyenne (mph)
- Pourcentage de pourboire
- Dimensions temporelles (heure, jour, mois, année, nom du jour)

### Catégorisations Business (Intermediate)
- **Distances** : Courts (≤1 mile), Moyens (1-5), Longs (5-10), Très longs (>10)
- **Périodes** : Rush Matinal (6h-9h), Journée (10h-15h), Rush Soir (16h-19h), Soirée (20h-23h), Nuit (0h-5h)
- **Types de jours** : Semaine vs Weekend

---

## Composants du Projet

1. **Architecture Snowflake** : 4 schémas (RAW, STAGING, INTERMEDIATE, MARTS)
2. **Script d'ingestion Python** : `ingest_data.py` — téléchargement et chargement automatisé des fichiers Parquet
3. **Orchestration Airflow** : 2 DAGs indépendants (ingestion + transformation dbt)
4. **Modèles dbt** : 5 modèles SQL avec tests et documentation
   - 1 modèle staging (nettoyage + enrichissement)
   - 1 modèle intermediate (catégorisations métier)
   - 3 modèles marts (tables analytiques)
5. **Tests de qualité** : 15 tests automatiques intégrés à dbt

---

## Structure du Projet

```
Taxi_NYC_Analyse/
├── dbt-dag/                          # Projet Airflow (Astro)
│   ├── dags/
│   │   ├── scripts/
│   │   │   └── ingest_data.py        # Script d'ingestion Python
│   │   ├── ingestion_dag.py          # DAG Airflow ingestion
│   │   ├── dbt_dag.py                # DAG Airflow dbt
│   │   └── dbt/taxi_nyc_pipeline/    # Projet dbt
│   │       ├── models/
│   │       │   ├── staging/          # Modèles de nettoyage
│   │       │   ├── intermediate/     # Catégorisations
│   │       │   └── marts/            # Tables finales
│   │       └── dbt_project.yml
│   ├── .env                          # Credentials Snowflake (non versionné)
│   ├── .env.exemple                  # Template de configuration
│   └── airflow_settings.yaml         # Config connexions Airflow
├── snowflake/
│   └── INITIALISATION_WH_DB.sql      # Script setup Snowflake
├── Screenshots/                      # Preuves d'exécution
├── docs/                             # Documentation dbt (GitHub Pages)
└── README.md
```

---

## KPIs Calculés

Les tables MARTS permettent d'analyser :
- **Volume** : Nombre de trajets par jour/heure/zone
- **Revenus** : Revenus totaux et moyens
- **Performance** : Distance moyenne, vitesse moyenne, durée moyenne
- **Comportement** : Pourcentage de pourboire, patterns horaires
- **Géographie** : Top zones de départ, zones les plus lucratives

---

## Documentation dbt

La documentation interactive des modèles dbt est disponible en ligne :
**[Documentation dbt](https://DaiTensa.github.io/brief_dbt_airflow_snowflake/)**

Cette documentation inclut :
- Lignage des données (graphe de dépendances)
- Description de chaque modèle et colonne
- Liste des tests de qualité
- Code SQL source
- Métadonnées complètes depuis Snowflake

### Générer la documentation localement

```bash
# 1. Créer un profil temporaire
mkdir -p /tmp/dbt_profiles
cat > /tmp/dbt_profiles/profiles.yml << 'EOF'
taxi_nyc_pipeline:
  outputs:
    dev:
      type: snowflake
      account: votre-account-identifier
      user: dbt_taxi
      password: votre-mot-de-passe
      role: TRANSFORM
      warehouse: NYC_TAXI_WH
      database: NYC_TAXI_DB
      schema: RAW
  target: dev
EOF

# 2. Générer la documentation depuis le conteneur Airflow
docker exec $(docker ps -q -f name=scheduler) /bin/bash -c \
  "cd /usr/local/airflow/dags/dbt/taxi_nyc_pipeline && \
   /usr/local/airflow/dbt_venv/bin/dbt docs generate --profiles-dir /tmp/dbt_profiles"

# 3. Copier les fichiers vers docs/
docker cp $(docker ps -q -f name=scheduler):/usr/local/airflow/dags/dbt/taxi_nyc_pipeline/target/catalog.json docs/
docker cp $(docker ps -q -f name=scheduler):/usr/local/airflow/dags/dbt/taxi_nyc_pipeline/target/manifest.json docs/

# 4. Pousser sur GitHub Pages
git add docs/
git commit -m "Update dbt documentation"
git push origin main
```

### Configuration GitHub Pages (première fois uniquement)

1. Allez dans **Settings** → **Pages** de votre repository
2. **Source** : Deploy from a branch
3. **Branch** : `main`
4. **Folder** : `/docs`
5. Cliquez sur **Save**

---

## Technologies Utilisées

- **Snowflake** : Data Warehouse cloud
- **dbt Core** : Transformation de données (ELT)
- **Apache Airflow** : Orchestration (Astronomer Cosmos)
- **Python** : Scripts d'ingestion (pandas, snowflake-connector)
- **Docker** : Conteneurisation Airflow
- **Git** : Versioning

---

## Auteur

Projet réalisé dans le cadre de la formation Data Engineering - Simplon

---

## Notes

- Les données couvrent l'année 2024 complète (~40M trajets, 12 fichiers Parquet)
- Les timestamps sont stockés en `NUMBER` (microsecondes epoch) → utiliser `TO_TIMESTAMP(col / 1000000)`
- Le script d'ingestion est idempotent : relancer ne crée pas de doublons
- Les tests dbt s'exécutent automatiquement à chaque run du DAG dbt
- Les vitesses > 150 mph sont filtrées (erreurs GPS/timestamps dans les données source)
