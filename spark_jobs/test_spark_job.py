# test_spark.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Создаем SparkSession
spark = SparkSession.builder \
    .appName("SparkDockerTest") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.memory", "512m") \
    .getOrCreate()

try:
    # Тест 1: Создание DataFrame
    data = [("Alice", 34), ("Bob", 45), ("Charlie", 28)]
    df = spark.createDataFrame(data, ["Name", "Age"])
    
    print("=== DataFrame создан ===")
    df.show()
    
    # Тест 2: Трансформации
    filtered_df = df.filter(col("Age") > 30)
    count = filtered_df.count()
    
    print(f"\n=== Людей старше 30: {count} ===")
    filtered_df.show()
    
    # Тест 3: Партиционирование
    rdd = spark.sparkContext.parallelize(range(1000), 10)
    rdd_count = rdd.count()
    
    print(f"\n=== RDD элементов: {rdd_count} ===")
    
    # Тест 4: Проверка кластера
    print("\n=== Информация о кластере ===")
    print(f"Spark версия: {spark.version}")
    print(f"Master URL: {spark.sparkContext.master}")
    print(f"Application ID: {spark.sparkContext.applicationId}")
    
    print("\n✅ Spark успешно работает в Docker!")
    
except Exception as e:
    print(f"\n❌ Ошибка: {e}")
    raise
    
finally:
    spark.stop()