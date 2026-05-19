from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, current_timestamp, length
from pyspark.sql.types import StringType
import re

# ─────────────────────────────────────────────────────
# HTML Cleaning UDF
# ─────────────────────────────────────────────────────
def remove_html(text):
    if not text:
        return ""
    clean = re.compile('<.*?>')
    return re.sub(clean, '', text).strip()

remove_html_udf = udf(remove_html, StringType())

# ─────────────────────────────────────────────────────
# Text Normalization UDF
# ─────────────────────────────────────────────────────
def normalize_text(text):
    if not text:
        return ""
    # Lowercase + remove extra spaces
    text = text.lower().strip()
    text = re.sub(r'\s+', ' ', text)
    # Remove special characters (keep letters, digits, spaces, basic punctuation)
    text = re.sub(r'[^\w\s\.\,\!\?\-]', '', text)
    return text

normalize_text_udf = udf(normalize_text, StringType())

# ─────────────────────────────────────────────────────
# Language Detection UDF (real — uses langdetect)
# ─────────────────────────────────────────────────────
def detect_language(text):
    if not text or len(text.strip()) < 10:
        return "unknown"
    try:
        from langdetect import detect
        return detect(text)
    except Exception:
        return "unknown"

detect_lang_udf = udf(detect_language, StringType())

# ─────────────────────────────────────────────────────
# Main Cleaning Job
# ─────────────────────────────────────────────────────
def run_cleaning():
    spark = SparkSession.builder \
        .appName("SilverCleaningJob") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password") \
        .config("spark.hadoop.fs.s3a.path.style.access", True) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
        .getOrCreate()

    # ── Read raw bronze data ──────────────────────────
    try:
        df = spark.read.json("s3a://bronze/*.json")
    except Exception as e:
        print("No data found in bronze or error reading:", e)
        spark.stop()
        return

    print(f"Bronze records loaded: {df.count()}")

    # ── Data Quality Filters ─────────────────────────
    # Complétude : titre, date, contenu présents
    # Validité   : contenu d'au moins 30 caractères (articles non vides)
    df_valid = df.filter(
        (col("title").isNotNull()) &
        (col("title") != "") &
        (col("published_date").isNotNull()) &
        (col("content").isNotNull()) &
        (length(col("content")) >= 30)   # ← CONTENU TROP COURT éliminé
    )

    rejected = df.count() - df_valid.count()
    print(f"Records after quality filters: {df_valid.count()} (rejected: {rejected})")

    # ── Transformations Silver ────────────────────────
    df_silver = df_valid \
        .withColumn("content_clean",  remove_html_udf(col("content"))) \
        .withColumn("title_clean",    remove_html_udf(col("title"))) \
        .withColumn("content_normalized", normalize_text_udf(col("content_clean"))) \
        .withColumn("title_normalized",   normalize_text_udf(col("title_clean"))) \
        .withColumn("language",       detect_lang_udf(col("content_clean"))) \
        .withColumn("processed_at",   current_timestamp())

    # ── Cohérence : standardiser le champ country ────
    from pyspark.sql.functions import when, lower
    df_silver = df_silver.withColumn(
        "country",
        when(col("country").isNull(), "Unknown")
        .when(lower(col("country")).isin("maroc", "morocco", "ma"), "Maroc")
        .otherwise(col("country"))
    )

    # ── Write to Silver (Parquet) ─────────────────────
    try:
        if not spark.catalog.databaseExists("default"):
            pass  # MinIO bucket 'silver' must already exist
        df_silver.write.mode("overwrite").parquet("s3a://silver/articles/")
        print(f"✅ Cleaned data saved to Silver layer ({df_silver.count()} records).")
    except Exception as e:
        print("Error writing to silver:", e)

    spark.stop()

if __name__ == "__main__":
    run_cleaning()
