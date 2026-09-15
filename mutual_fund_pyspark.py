from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    when,
    trim,
    round,
    row_number
)
from pyspark.sql.window import Window


# ============================================================
# 1. CONFIGURATION
# ============================================================

S3_BUCKET = "s3a://mutual-fund-data-engineering-bhagvat-2026"

INPUT_PATH = f"{S3_BUCKET}/raw/mutual_funds.csv"
OUTPUT_PATH = f"{S3_BUCKET}/processed/mutual_funds_processed"


# ============================================================
# 2. CREATE SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("MutualFundETL")
    .config(
        "spark.jars",
        r"C:\Users\Hp\.ivy2.5.2\jars\org.apache.hadoop_hadoop-aws-3.5.0.jar,"
        r"C:\Users\Hp\.ivy2.5.2\jars\software.amazon.awssdk_bundle-2.35.4.jar,"
        r"C:\Users\Hp\.ivy2.5.2\jars\org.wildfly.openssl_wildfly-openssl-2.2.5.Final.jar"
    )
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("Spark started successfully!")
print("Spark Version:", spark.version)


# ============================================================
# 3. EXTRACT - READ DATA FROM S3
# ============================================================

print("\n================ EXTRACT ================")

df = spark.read.csv(
    INPUT_PATH,
    header=True,
    inferSchema=True
)

print("\nOriginal Data:")
df.show(truncate=False)


# ============================================================
# 4. DATA PROFILING
# ============================================================

print("\n================ DATA PROFILING ================")

print("\nSchema:")
df.printSchema()

print("\nTotal Rows:")
print(df.count())


# ============================================================
# 5. MISSING VALUES CHECK
# ============================================================

print("\n================ MISSING VALUES ================")

missing_values = df.select([
    count(
        when(col(c).isNull(), c)
    ).alias(c)
    for c in df.columns
])

missing_values.show()


# ============================================================
# 6. DUPLICATE CHECK
# ============================================================

print("\n================ DUPLICATE CHECK ================")

duplicate_funds = (
    df.groupBy("Fund_Name")
    .count()
    .filter(col("count") > 1)
)

duplicate_funds.show()


# ============================================================
# 7. DATA CLEANING
# ============================================================

print("\n================ DATA CLEANING ================")

df = df.withColumn(
    "Fund_Name",
    trim(col("Fund_Name"))
)

df = df.withColumn(
    "Category",
    trim(col("Category"))
)

df = df.withColumn(
    "Risk_Rating",
    trim(col("Risk_Rating"))
)

df = df.dropDuplicates(["Fund_Name"])

print("\nCleaned Data:")
df.show(truncate=False)


# ============================================================
# 8. DATA VALIDATION
# ============================================================

print("\n================ DATA VALIDATION ================")


print("\nInvalid Returns:")

invalid_returns = df.filter(
    (col("1Y_Return") < 0)
    | (col("3Y_Return") < 0)
    | (col("5Y_Return") < 0)
)

invalid_returns.show(truncate=False)


print("\nInvalid Expense Ratio:")

invalid_expense = df.filter(
    col("Expense_Ratio") < 0
)

invalid_expense.show(truncate=False)


print("\nInvalid AUM:")

invalid_aum = df.filter(
    col("AUM_Crore") < 0
)

invalid_aum.show(truncate=False)


print("\nInvalid Risk Rating:")

valid_risk = [
    "Low",
    "Moderate",
    "High",
    "Very High"
]

invalid_risk = df.filter(
    ~col("Risk_Rating").isin(valid_risk)
)

invalid_risk.show(truncate=False)


# ============================================================
# 9. TRANSFORMATION - PERFORMANCE SCORE
# ============================================================

print("\n================ TRANSFORMATION ================")

df = df.withColumn(
    "Performance_Score",
    round(
        (
            col("1Y_Return") * 0.20
            + col("3Y_Return") * 0.30
            + col("5Y_Return") * 0.50
        ),
        2
    )
)

print("\nPerformance Score Added:")

df.select(
    "Fund_Name",
    "1Y_Return",
    "3Y_Return",
    "5Y_Return",
    "Performance_Score"
).show(truncate=False)


# ============================================================
# 10. OVERALL RANKING
# ============================================================

print("\n================ OVERALL RANKING ================")

window_spec = Window.orderBy(
    col("Performance_Score").desc()
)

df = df.withColumn(
    "Rank",
    row_number().over(window_spec)
)

print("\nFinal Ranking:")

df.select(
    "Fund_Name",
    "Category",
    "1Y_Return",
    "3Y_Return",
    "5Y_Return",
    "Expense_Ratio",
    "Risk_Rating",
    "AUM_Crore",
    "Performance_Score",
    "Rank"
).orderBy("Rank").show(
    20,
    truncate=False
)


# ============================================================
# 11. BEST PERFORMING FUND
# ============================================================

print("\n================ BEST FUND ================")

best_fund = (
    df.orderBy(col("Rank"))
    .select(
        "Fund_Name",
        "Category",
        "Performance_Score",
        "Rank"
    )
    .first()
)

if best_fund:
    print("\nBest Performing Fund:")
    print("Fund Name:", best_fund["Fund_Name"])
    print("Category:", best_fund["Category"])
    print("Performance Score:", best_fund["Performance_Score"])
    print("Rank:", best_fund["Rank"])


# ============================================================
# 12. CATEGORY-WISE RANKING
# ============================================================

print("\n================ CATEGORY-WISE RANKING ================")

category_window = (
    Window
    .partitionBy("Category")
    .orderBy(col("Performance_Score").desc())
)

df = df.withColumn(
    "Category_Rank",
    row_number().over(category_window)
)

print("\nCategory-wise Ranking:")

df.select(
    "Fund_Name",
    "Category",
    "Performance_Score",
    "Category_Rank"
).orderBy(
    "Category",
    "Category_Rank"
).show(
    50,
    truncate=False
)


# ============================================================
# 13. CREATE FINAL PROCESSED DATASET
# ============================================================

print("\n================ FINAL PROCESSED DATA ================")

final_df = df.select(
    "Fund_Name",
    "Category",
    "1Y_Return",
    "3Y_Return",
    "5Y_Return",
    "Expense_Ratio",
    "Risk_Rating",
    "AUM_Crore",
    "Performance_Score",
    "Rank",
    "Category_Rank"
)

final_df.orderBy("Rank").show(
    50,
    truncate=False
)


# ============================================================
# 14. SAVE PROCESSED DATA TO S3 AS PARQUET
# ============================================================

print("\n================ SAVING DATA TO S3 ================")

final_df.write \
    .mode("overwrite") \
    .parquet(OUTPUT_PATH)

print("\nProcessed Parquet data saved successfully to:")
print(OUTPUT_PATH)


# ============================================================
# 15. STOP SPARK
# ============================================================

spark.stop()

print("\nSpark stopped successfully!")
print("\nETL Pipeline completed successfully!")