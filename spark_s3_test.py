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

HADOOP_AWS_JAR = (
    r"C:\Users\Hp\.ivy2.5.2\jars"
    r"\org.apache.hadoop_hadoop-aws-3.5.0.jar"
)


# ============================================================
# 2. CREATE SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName("MutualFundETL")

    # Hadoop AWS S3A JAR
    .config(
        "spark.jars",
        HADOOP_AWS_JAR
    )

    # S3A filesystem
    .config(
        "spark.hadoop.fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem"
    )

    # AWS credentials provider
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider"
    )

    .getOrCreate()
)


try:

    # ========================================================
    # 3. READ RAW DATA FROM S3
    # ========================================================

    print("\n" + "=" * 60)
    print("READING RAW DATA FROM S3")
    print("=" * 60)

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(INPUT_PATH)
    )

    print("\nRaw Data:")
    df.show(truncate=False)

    print("\nSchema:")
    df.printSchema()

    print("\nTotal Records:", df.count())


    # ========================================================
    # 4. REQUIRED COLUMNS VALIDATION
    # ========================================================

    required_columns = [
        "Fund_Name",
        "Category",
        "1Y_Return",
        "3Y_Return",
        "5Y_Return",
        "Expense_Ratio",
        "Risk_Rating",
        "AUM_Crore"
    ]

    missing_columns = [
        column
        for column in required_columns
        if column not in df.columns
    ]

    if missing_columns:
        raise Exception(
            f"Missing required columns: {missing_columns}"
        )

    print("\nRequired column validation: PASSED")


    # ========================================================
    # 5. MISSING VALUE CHECK
    # ========================================================

    print("\n" + "=" * 60)
    print("MISSING VALUE CHECK")
    print("=" * 60)

    missing_values = df.select([
        count(
            when(
                col(c).isNull(),
                c
            )
        ).alias(c)
        for c in df.columns
    ])

    missing_values.show()


    # ========================================================
    # 6. TRIM STRING COLUMNS
    # ========================================================

    df = (
        df
        .withColumn("Fund_Name", trim(col("Fund_Name")))
        .withColumn("Category", trim(col("Category")))
        .withColumn("Risk_Rating", trim(col("Risk_Rating")))
    )


    # ========================================================
    # 7. DUPLICATE CHECK
    # ========================================================

    print("\n" + "=" * 60)
    print("DUPLICATE CHECK")
    print("=" * 60)

    duplicate_count = (
        df.groupBy("Fund_Name")
        .count()
        .filter(col("count") > 1)
        .count()
    )

    print("Duplicate Fund Names:", duplicate_count)

    df = df.dropDuplicates(["Fund_Name"])


    # ========================================================
    # 8. DATA VALIDATION
    # ========================================================

    print("\n" + "=" * 60)
    print("DATA VALIDATION")
    print("=" * 60)

    negative_returns = df.filter(
        (col("1Y_Return") < 0) |
        (col("3Y_Return") < 0) |
        (col("5Y_Return") < 0)
    ).count()

    invalid_expense = df.filter(
        col("Expense_Ratio") < 0
    ).count()

    invalid_aum = df.filter(
        col("AUM_Crore") < 0
    ).count()

    valid_risk_ratings = [
        "Low",
        "Moderate",
        "High",
        "Very High"
    ]

    invalid_risk = df.filter(
        ~col("Risk_Rating").isin(valid_risk_ratings)
    ).count()

    print("Negative Returns:", negative_returns)
    print("Invalid Expense Ratio:", invalid_expense)
    print("Invalid AUM:", invalid_aum)
    print("Invalid Risk Rating:", invalid_risk)


    # ========================================================
    # 9. PERFORMANCE SCORE
    # ========================================================

    print("\n" + "=" * 60)
    print("CREATING PERFORMANCE SCORE")
    print("=" * 60)

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


    # ========================================================
    # 10. OVERALL RANK
    # ========================================================

    overall_window = Window.orderBy(
        col("Performance_Score").desc()
    )

    df = df.withColumn(
        "Rank",
        row_number().over(overall_window)
    )


    # ========================================================
    # 11. CATEGORY RANK
    # ========================================================

    category_window = (
        Window
        .partitionBy("Category")
        .orderBy(
            col("Performance_Score").desc()
        )
    )

    df = df.withColumn(
        "Category_Rank",
        row_number().over(category_window)
    )


    # ========================================================
    # 12. FINAL DATASET
    # ========================================================

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


    # ========================================================
    # 13. SHOW FINAL RESULT
    # ========================================================

    print("\n" + "=" * 60)
    print("FINAL MUTUAL FUND RANKING")
    print("=" * 60)

    final_df.orderBy("Rank").show(
        truncate=False
    )


    # ========================================================
    # 14. BEST FUND
    # ========================================================

    best_fund = (
        final_df
        .orderBy("Rank")
        .limit(1)
        .collect()
    )

    if best_fund:
        print("\n" + "=" * 60)
        print("BEST PERFORMING FUND")
        print("=" * 60)

        print(
            "Fund:",
            best_fund[0]["Fund_Name"]
        )

        print(
            "Performance Score:",
            best_fund[0]["Performance_Score"]
        )


    # ========================================================
    # 15. WRITE PROCESSED DATA TO S3
    # ========================================================

    print("\n" + "=" * 60)
    print("WRITING PROCESSED DATA TO S3")
    print("=" * 60)

    (
        final_df
        .write
        .mode("overwrite")
        .parquet(OUTPUT_PATH)
    )

    print("\nSUCCESS!")
    print("Processed data written to:")
    print(OUTPUT_PATH)


except Exception as e:

    print("\n" + "=" * 60)
    print("ERROR")
    print("=" * 60)

    print(type(e).__name__)
    print(e)

    raise


finally:

    spark.stop()

    print("\nSpark session stopped.")