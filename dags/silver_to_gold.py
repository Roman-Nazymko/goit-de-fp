from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, current_timestamp, when, col, regexp_extract, split, trim, isnan, isnull, regexp_replace
from pyspark.sql.types import DoubleType
import os

# Створення сесії Spark для обробки даних
spark = SparkSession.builder \
    .appName("SilverToGold") \
    .getOrCreate()

# Завантаження таблиць із silver-шару
athlete_bio_df = spark.read.parquet("/tmp/silver/athlete_bio")
athlete_event_results_df = spark.read.parquet("/tmp/silver/athlete_event_results")

# Перейменування колонок для уникнення неоднозначності при об'єднанні
athlete_bio_df = athlete_bio_df.withColumnRenamed("country_noc", "bio_country_noc")

def process_range_column_robust(df, column_name):
    """
    Робастна версія обробки колонок з діапазонами значень
    Підтримує формати: '87-90', '67, 72', '67,72', '72 ' (з пробілами)
    Повертає середнє значення діапазону або оригінальне значення
    """
    return df.withColumn(
        column_name,
        when(
            # Спочатку очищаємо пробіли та перевіряємо на null/empty
            (col(column_name).isNull()) | (trim(col(column_name)) == ""), 
            None
        ).when(
            # Перевіряємо наявність дефіса (формат: число-число)
            trim(col(column_name)).rlike(r"^\s*\d+(\.\d+)?\s*-\s*\d+(\.\d+)?\s*$"),
            (
                regexp_extract(trim(col(column_name)), r"^(\d+(?:\.\d+)?)", 1).cast(DoubleType()) +
                regexp_extract(trim(col(column_name)), r"-\s*(\d+(?:\.\d+)?)", 1).cast(DoubleType())
            ) / 2
        ).when(
            # Перевіряємо наявність коми (формат: число, число або число,число)
            trim(col(column_name)).rlike(r"^\s*\d+(\.\d+)?\s*,\s*\d+(\.\d+)?\s*$"),
            (
                regexp_extract(trim(col(column_name)), r"^(\d+(?:\.\d+)?)", 1).cast(DoubleType()) +
                regexp_extract(trim(col(column_name)), r",\s*(\d+(?:\.\d+)?)", 1).cast(DoubleType())
            ) / 2
        ).when(
            # Якщо це просто число (можливо з пробілами)
            trim(col(column_name)).rlike(r"^\s*\d+(\.\d+)?\s*$"),
            trim(col(column_name)).cast(DoubleType())
        ).otherwise(
            # Якщо формат не розпізнано, повертаємо null
            None
        )
    )

def process_range_column_with_try_cast(df, column_name):
    """
    Альтернативна версія з використанням try_cast для безпечного кастингу
    """
    from pyspark.sql.functions import try_cast
    
    return df.withColumn(
        column_name + "_cleaned",
        # Спочатку очищаємо дані від зайвих пробілів та символів
        regexp_replace(trim(col(column_name)), r"[^\d.,-]", "")
    ).withColumn(
        column_name,
        when(
            # Перевіряємо наявність дефіса
            col(column_name + "_cleaned").contains("-"),
            (
                try_cast(split(col(column_name + "_cleaned"), "-")[0], DoubleType()) +
                try_cast(split(col(column_name + "_cleaned"), "-")[1], DoubleType())
            ) / 2
        ).when(
            # Перевіряємо наявність коми
            col(column_name + "_cleaned").contains(","),
            (
                try_cast(trim(split(col(column_name + "_cleaned"), ",")[0]), DoubleType()) +
                try_cast(trim(split(col(column_name + "_cleaned"), ",")[1]), DoubleType())
            ) / 2
        ).otherwise(
            # Інакше просто конвертуємо в DOUBLE з try_cast
            try_cast(col(column_name + "_cleaned"), DoubleType())
        )
    ).drop(column_name + "_cleaned")

# Спочатку подивимося на проблемні дані для діагностики
print("=== Діагностика даних height ===")
athlete_bio_df.select("height").filter(
    col("height").rlike(r".*\s+.*") | 
    ~col("height").rlike(r"^\s*\d+(\.\d+)?(\s*[-,]\s*\d+(\.\d+)?)?\s*$")
).distinct().show(20, truncate=False)

print("=== Діагностика даних weight ===")
athlete_bio_df.select("weight").filter(
    col("weight").rlike(r".*\s+.*") | 
    ~col("weight").rlike(r"^\s*\d+(\.\d+)?(\s*[-,]\s*\d+(\.\d+)?)?\s*$")
).distinct().show(20, truncate=False)

# Використання робастної функції обробки
athlete_bio_df = process_range_column_robust(athlete_bio_df, "height")
athlete_bio_df = process_range_column_robust(athlete_bio_df, "weight")

# Перевірка результатів після обробки
print("=== Результати після обробки ===")
athlete_bio_df.select("height", "weight").filter(
    col("height").isNull() | col("weight").isNull()
).count()

# Об'єднання таблиць за колонкою "athlete_id"
joined_df = athlete_event_results_df.join(athlete_bio_df, "athlete_id")

# Обчислення середніх значень для кожної групи (виключаємо null значення)
aggregated_df = joined_df.filter(
    col("height").isNotNull() & col("weight").isNotNull()
).groupBy("sport", "medal", "sex", "country_noc") \
    .agg(
        avg("height").alias("avg_height"),     # Середній зріст
        avg("weight").alias("avg_weight"),     # Середня вага
        current_timestamp().alias("timestamp") # Час виконання агрегації
    )

# Створення директорії для збереження результатів у gold-шар
output_path = "/tmp/gold/avg_stats"
os.makedirs(output_path, exist_ok=True)

# Збереження оброблених даних у форматі parquet
aggregated_df.write.mode("overwrite").parquet(output_path)
print(f"Data saved to {output_path}")

# Повторне читання parquet-файлу для перевірки даних
df = spark.read.parquet(output_path)
df.show(truncate=False)

# Завершення роботи Spark-сесії
spark.stop()