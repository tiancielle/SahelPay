# SahelPay - Plateforme Big Data (Spark, Kafka, GraphX, GraphFrames)



Chaine : `producer.py -> Kafka -> Spark Structured Streaming -> MinIO (Bronze/Silver/Gold) -> GraphX / GraphFrames`

## Avancement

| Jalon | Contenu | Etat |
|---|---|---|
| 1 (29/11/2026) | Parties 1 a 3 : cluster, RDD, DataFrames/SQL | termine |
| 2 (20/12/2026) | Parties 4 a 6 : streaming, pipeline, graphes | en cours |

## Versions

| Composant | Version |
|---|---|
| Spark | 3.5.3 (image `apache/spark:3.5.3-python3`, Java 11, Python 3.8) |
| PySpark (Jupyter) | 3.5.3 |
| Kafka | 3.7.0 (mode KRaft) |
| Delta Lake | 3.2.0 |
| hadoop-aws / aws-java-sdk-bundle | 3.3.4 / 1.12.262 |
| spark-sql-kafka | 3.5.3 |
| MinIO | fork `pgsty/minio:latest` (client `mcli`) |

## Lancement

```bash
docker compose up -d --build     # premiere fois : quelques minutes
docker compose ps                # tous les services doivent etre "running"
```

| Service | URL |
|---|---|
| Spark Master UI | http://localhost:8080 |
| Spark UI du Driver (pendant un job) | http://localhost:4040 |
| JupyterLab | http://localhost:8888 |
| Console MinIO (minioadmin / minioadmin) | http://localhost:9001 |

Le topic Kafka `transactions` (3 partitions) est cree automatiquement.
Le bucket `lake` se cree dans la console MinIO (http://localhost:9001).

## Donnees

```bash
# historique (Parties 2 et 3), depuis le conteneur jupyter
docker exec jupyter python /workspace/scripts/producer.py --mode file --n 50000

# copie dans MinIO (le client mcli est dans le conteneur minio)
docker cp data/transactions_history.jsonl minio:/tmp/
docker exec minio mcli alias set local http://localhost:9000 minioadmin minioadmin
docker exec minio mcli cp /tmp/transactions_history.jsonl local/lake/bronze/history/
docker cp data/merchants.json minio:/tmp/
docker exec minio mcli cp /tmp/merchants.json local/lake/bronze/reference/

# flux temps reel (Parties 4 et 5) : lance depuis le notebook partie4.ipynb,
# ou a la main :
docker exec -d jupyter python /workspace/scripts/producer.py --mode kafka --rate 20 --bootstrap kafka:29092
```

Verifier le contenu du lac :

```bash
docker exec minio mcli ls --recursive local/lake/
```

## Notebooks (dossier `notebooks/`, dans l'ordre)

| Notebook | Partie | Contenu |
|---|---|---|
| `partie1.ipynb` | 1 | SparkSession sur le cluster, MinIO, Kafka |
| `partie2.ipynb` | 2 | RDD : totaux par ville, top 5, lineage, doublons |
| `partie3.ipynb` | 3 | DataFrames, SQL, jointure, UDF, Parquet, `explain(True)` |
| `partie4.ipynb` | 4 | Structured Streaming depuis Kafka, Bronze, watermark |

Chaque notebook se termine par `spark.stop()` : la session consomme les 4 coeurs du cluster, il faut l'arreter avant d'ouvrir le suivant. Faire les captures Spark UI avant de l'arreter.

## Structure

```
docker-compose.yml
jupyter/Dockerfile       image du Driver (PySpark + Jupyter)
scripts/                 producer.py, spark_session.py
notebooks/               un notebook par partie
graphx/                  code Scala (Partie 6)
data/                    merchants.json, historique genere (ignore par git)
report/                  rapport PDF
```

## Arret

```bash
docker compose stop      # garde les donnees
docker compose down -v   # supprime aussi les volumes (repart de zero)
```
