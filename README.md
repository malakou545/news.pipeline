# Plateforme Big Data : Analyse d'Articles de Presse

## Prérequis
- Docker et Docker-Compose installés
- Au moins 8 Go de RAM alloués à Docker

## Lancement du Projet

1. Clonez ce dépôt ou rendez-vous dans le dossier du projet.
2. Démarrez l'infrastructure avec la commande :
   ```bash
   docker-compose up -d --build
   ```
3. Les services prendront **3 à 5 minutes** pour démarrer et s'initialiser.

## Accès aux Services

| Service | URL | Identifiants |
|---------|-----|--------------|
| **MinIO (Data Lake)** | http://localhost:9001 | `admin` / `password` |
| **Airflow (Orchestration)** | http://localhost:8080 | `admin` / `admin` |
| **Spark Master (UI)** | http://localhost:8081 | — |
| **PostgreSQL (Data Warehouse)** | `localhost:5432` | `airflow` / `airflow` |
| **Metabase (Visualisation)** | http://localhost:3000 | À configurer au premier lancement |

## Fonctionnement du Pipeline

### Pipeline Batch (automatique — toutes les heures via Airflow)
```
[Al Jazeera + BBC + Hespress] → [bronze/] → [silver/] → [PostgreSQL Gold]
```
1. **Scraping Batch** : Airflow lance `batch_scraper.py` (Al Jazeera, BBC, Hespress). Les fichiers JSON sont déposés dans le bucket MinIO `bronze`.
2. **Nettoyage (Silver)** : Airflow lance `silver_cleaning.py` avec Spark :
   - Suppression HTML, normalisation du texte
   - Détection de langue réelle (`langdetect`)
   - Filtrage qualité (titre vide, date manquante, contenu < 30 chars)
   - Sauvegardé en Parquet dans le bucket `silver`
3. **Agrégation (Gold)** : Airflow lance `gold_aggregations.py` avec Spark et peuple 6 tables dans PostgreSQL :
   - `gold_articles_per_day` — articles par jour
   - `gold_articles_per_source` — articles par source
   - `gold_articles_per_category` — articles par catégorie
   - `gold_articles_per_country` — articles par pays
   - `gold_articles_per_language` — articles par langue
   - `gold_top_keywords` — top 200 mots-clés des titres

### Pipeline Streaming (manuel)
```
[Hespress + Akhbarona] → [Kafka topic: news_events]
```
Lancez manuellement le streaming scraper dans le conteneur Airflow :
```bash
docker exec -it <airflow-webserver-container> python /opt/airflow/scrapers/streaming_scraper.py
```

## Configuration Metabase (Dashboards)

1. Ouvrez http://localhost:3000
2. Créez un compte administrateur
3. Ajoutez une source de données **PostgreSQL** :
   - Host : `postgres`
   - Port : `5432`
   - Database : `airflow`
   - User : `airflow` / Password : `airflow`
4. Créez des questions/dashboards à partir des tables `gold_*`

### Dashboards recommandés
- **Évolution quotidienne** : graphe linéaire sur `gold_articles_per_day`
- **Répartition par source** : histogramme sur `gold_articles_per_source`
- **Distribution pays** : camembert sur `gold_articles_per_country`
- **Top mots-clés** : barre horizontale sur `gold_top_keywords` (tri par fréquence)
- **Distribution langues** : camembert sur `gold_articles_per_language`

## Architecture des Fichiers

```
.
├── dags/
│   └── news_pipeline_dag.py       # Orchestration Airflow (batch)
├── scrapers/
│   ├── batch_scraper.py           # Scraping Al Jazeera, BBC, Hespress → MinIO
│   └── streaming_scraper.py       # Scraping Hespress, Akhbarona → Kafka
├── jobs/
│   ├── silver_cleaning.py         # Spark ETL Bronze → Silver
│   └── gold_aggregations.py       # Spark ETL Silver → Gold (PostgreSQL)
├── Dockerfile.airflow             # Image Airflow customisée
├── Dockerfile.spark               # Image Spark customisée
├── docker-compose.yml             # Infrastructure complète
├── Rapport.md                     # Rapport technique détaillé
└── Presentation.md                # Support de présentation
```

## Technologies Utilisées

| Technologie | Rôle |
|-------------|------|
| Python + BeautifulSoup | Web scraping |
| Apache Kafka + Zookeeper | Ingestion streaming |
| MinIO | Data Lake (compatible S3) |
| Apache Spark (PySpark) | Transformation ETL |
| Apache Airflow | Orchestration du pipeline batch |
| PostgreSQL | Data Warehouse |
| Metabase | Visualisation et dashboards |
| langdetect | Détection de langue |
| Docker + Docker-Compose | Conteneurisation |
