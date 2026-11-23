"""
Creation d'une SparkSession connectee au cluster, a MinIO (S3A), a Kafka et a Delta Lake.
Utilisation dans un notebook :

    import sys; sys.path.append("/workspace/scripts")
    from spark_session import get_spark
    spark = get_spark("Partie1")
"""
from pyspark.sql import SparkSession

PACKAGES = ",".join([
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3",   # Kafka
    "org.apache.hadoop:hadoop-aws:3.3.4",                 # S3A (MinIO)
    "com.amazonaws:aws-java-sdk-bundle:1.12.262",
    "io.delta:delta-spark_2.12:3.2.0",                    # Delta Lake
])


def get_spark(app_name="SahelPay", master="spark://spark-master:7077"):
    return (
        SparkSession.builder
        .appName(app_name)
        .master(master)
        # --- le Driver (conteneur jupyter) doit etre joignable par les Executors
        .config("spark.driver.host", "jupyter")
        .config("spark.driver.bindAddress", "0.0.0.0")
        .config("spark.driver.port", "7078")
        .config("spark.blockManager.port", "7079")
        # --- ressources demandees au cluster (2 workers x 2 coeurs)
        .config("spark.executor.memory", "1g")
        .config("spark.cores.max", "4")
        .config("spark.pyspark.python", "python3")
        # --- librairies (telechargees au premier lancement : patience)
        .config("spark.jars.packages", PACKAGES)
        # --- MinIO via S3A
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # --- Delta Lake
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
