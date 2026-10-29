"""
goit-de-hw-03 — обробка даних в Apache Spark (PySpark)

Вхідні файли (поруч зі скриптом, папка data/):
    users.csv, purchases.csv, products.csv
Запуск:
    python main.py
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

DATA_DIR = "data"

spark = SparkSession.builder.appName("goit-de-hw-03").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# ---------------------------------------------------------------
# 1. Завантаження кожного CSV-файлу як окремого DataFrame
# ---------------------------------------------------------------
users_df = spark.read.csv(f"{DATA_DIR}/users.csv", header=True, inferSchema=True)
purchases_df = spark.read.csv(f"{DATA_DIR}/purchases.csv", header=True, inferSchema=True)
products_df = spark.read.csv(f"{DATA_DIR}/products.csv", header=True, inferSchema=True)

print("=== 1. Users ===")
users_df.show(10)
print("=== 1. Purchases ===")
purchases_df.show(10)
print("=== 1. Products ===")
products_df.show(10)

# ---------------------------------------------------------------
# 2. Очищення даних: видалення рядків із пропущеними значеннями
# ---------------------------------------------------------------
print("=== 2. Кількість рядків до / після очищення ===")
for name, df in [("users", users_df), ("purchases", purchases_df), ("products", products_df)]:
    print(f"{name}: {df.count()} -> {df.dropna().count()}")

users_df = users_df.dropna()
purchases_df = purchases_df.dropna()
products_df = products_df.dropna()

print("=== 2. Users (cleaned) ===")
users_df.show(10)
print("=== 2. Purchases (cleaned) ===")
purchases_df.show(10)
print("=== 2. Products (cleaned) ===")
products_df.show(10)

# Об'єднуємо покупки з продуктами та користувачами
# і рахуємо суму кожної покупки = quantity * price
purchases_full = (
    purchases_df
    .join(products_df, on="product_id", how="inner")
    .join(users_df, on="user_id", how="inner")
    .withColumn("total_amount", F.col("quantity") * F.col("price"))
)

# ---------------------------------------------------------------
# 3. Загальна сума покупок за кожною категорією продуктів
# ---------------------------------------------------------------
total_by_category = (
    purchases_full
    .groupBy("category")
    .agg(F.round(F.sum("total_amount"), 2).alias("total_amount"))
    .orderBy(F.desc("total_amount"))
)
print("=== 3. Загальна сума покупок за категоріями ===")
total_by_category.show()

# ---------------------------------------------------------------
# 4. Сума покупок за категоріями для віку 18–25 (включно)
# ---------------------------------------------------------------
purchases_18_25 = purchases_full.filter(F.col("age").between(18, 25))

total_by_category_18_25 = (
    purchases_18_25
    .groupBy("category")
    .agg(F.round(F.sum("total_amount"), 2).alias("total_amount_18_25"))
    .orderBy(F.desc("total_amount_18_25"))
)
print("=== 4. Сума покупок за категоріями (вік 18–25) ===")
total_by_category_18_25.show()

# ---------------------------------------------------------------
# 5. Частка кожної категорії від сумарних витрат групи 18–25 (у %)
# ---------------------------------------------------------------
total_spent_18_25 = purchases_18_25.agg(F.sum("total_amount")).collect()[0][0]

share_by_category_18_25 = (
    purchases_18_25
    .groupBy("category")
    .agg(F.sum("total_amount").alias("total_amount_18_25"))
    .withColumn(
        "percentage",
        F.round(F.col("total_amount_18_25") / F.lit(total_spent_18_25) * 100, 2),
    )
    .withColumn("total_amount_18_25", F.round("total_amount_18_25", 2))
    .orderBy(F.desc("percentage"))
)
print(f"=== 5. Частка витрат за категоріями (вік 18–25), загалом: {total_spent_18_25:.2f} ===")
share_by_category_18_25.show()

# ---------------------------------------------------------------
# 6. Топ-3 категорії з найвищим відсотком витрат (вік 18–25)
# ---------------------------------------------------------------
top3_categories = share_by_category_18_25.limit(3)
print("=== 6. Топ-3 категорії за відсотком витрат (вік 18–25) ===")
top3_categories.show()

spark.stop()
