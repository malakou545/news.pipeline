from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    count, col, to_date, explode, split,
    lower, regexp_replace, trim, length
)

# ─────────────────────────────────────────────────────
# Spark Session
# ─────────────────────────────────────────────────────
def get_spark():
    return SparkSession.builder \
        .appName("GoldAggregationJob") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password") \
        .config("spark.hadoop.fs.s3a.path.style.access", True) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.6.0") \
        .getOrCreate()

# ─────────────────────────────────────────────────────
# PostgreSQL Writer
# ─────────────────────────────────────────────────────
JDBC_URL = "jdbc:postgresql://postgres:5432/airflow"
JDBC_PROPS = {
    "user": "airflow",
    "password": "airflow",
    "driver": "org.postgresql.Driver"
}

def write_to_postgres(df, table_name):
    try:
        df.write.jdbc(url=JDBC_URL, table=table_name, mode="overwrite", properties=JDBC_PROPS)
        print(f"  ✅ Table '{table_name}' written ({df.count()} rows).")
    except Exception as e:
        print(f"  ❌ Error writing '{table_name}': {e}")

# ─────────────────────────────────────────────────────
# Stopwords (FR + EN + AR) à exclure des mots-clés
# ─────────────────────────────────────────────────────
STOPWORDS = {
    "the", "a", "an", "and", "or", "in", "on", "at", "to", "for", "of",
    "is", "was", "are", "be", "been", "with", "that", "this", "it", "by",
    "from", "as", "has", "have", "had", "but", "not", "its", "their",
    "le", "la", "les", "de", "du", "des", "un", "une", "et", "en", "au",
    "aux", "que", "qui", "sur", "par", "dans", "est", "ont", "son", "sa",
    "se", "si", "plus", "il", "elle", "nous", "vous", "ils", "elles",
    "article", "apercu", "preview", "content", "news", "hespress", "bbc",
    ""
}

# ─────────────────────────────────────────────────────
# Main Aggregation Job
# ─────────────────────────────────────────────────────
def run_aggregation():
    spark = get_spark()

    # ── Load Silver data ───────────────────────────
    try:
        df_silver = spark.read.parquet("s3a://silver/articles/")
    except Exception as e:
        print("No data in silver:", e)
        spark.stop()
        return

    total = df_silver.count()
    print(f"Silver records loaded: {total}")

    # ── 1. Articles par Jour ───────────────────────
    print("\n[Gold] Computing articles per day...")
    df_daily = df_silver \
        .withColumn("publish_day", to_date(col("published_date"))) \
        .groupBy("publish_day") \
        .agg(count("*").alias("article_count")) \
        .orderBy("publish_day", ascending=False)
    write_to_postgres(df_daily, "gold_articles_per_day")

    # ── 2. Articles par Source ─────────────────────
    print("\n[Gold] Computing articles per source...")
    df_source = df_silver \
        .groupBy("source") \
        .agg(count("*").alias("article_count")) \
        .orderBy("article_count", ascending=False)
    write_to_postgres(df_source, "gold_articles_per_source")

    # ── 3. Articles par Catégorie ──────────────────
    print("\n[Gold] Computing articles per category...")
    df_category = df_silver \
        .groupBy("category") \
        .agg(count("*").alias("article_count")) \
        .orderBy("article_count", ascending=False)
    write_to_postgres(df_category, "gold_articles_per_category")

    # ── 4. Articles par Pays ───────────────────────  ← NOUVEAU
    print("\n[Gold] Computing articles per country...")
    df_country = df_silver \
        .groupBy("country") \
        .agg(count("*").alias("article_count")) \
        .orderBy("article_count", ascending=False)
    write_to_postgres(df_country, "gold_articles_per_country")

    # ── 5. Articles par Langue ─────────────────────  ← NOUVEAU
    print("\n[Gold] Computing articles per language...")
    df_language = df_silver \
        .groupBy("language") \
        .agg(count("*").alias("article_count")) \
        .orderBy("article_count", ascending=False)
    write_to_postgres(df_language, "gold_articles_per_language")

    # ── 6. Top Mots-Clés (Fréquence) ──────────────  ← NOUVEAU
    print("\n[Gold] Computing top keywords from titles...")
    stopwords_broadcast = spark.sparkContext.broadcast(STOPWORDS)

    # Tokenize titles : minuscules, supprimer ponctuation, éclater en mots
    df_words = df_silver \
        .withColumn("title_clean_lower",
                    lower(regexp_replace(col("title_clean"), r'[^\w\s]', ''))) \
        .withColumn("word", explode(split(col("title_clean_lower"), r'\s+'))) \
        .withColumn("word", trim(col("word"))) \
        .filter(length(col("word")) > 3)   # ignorer mots trop courts

    # Filtrer les stopwords via un filtre Python (broadcast)
    df_keywords = df_words \
        .filter(~col("word").isin(list(stopwords_broadcast.value))) \
        .groupBy("word") \
        .agg(count("*").alias("frequency")) \
        .orderBy("frequency", ascending=False) \
        .limit(200)   # top 200 mots-clés

    write_to_postgres(df_keywords, "gold_top_keywords")

    print("\n✅ All Gold tables successfully written to PostgreSQL Data Warehouse.")
    spark.stop()

if __name__ == "__main__":
    run_aggregation()
