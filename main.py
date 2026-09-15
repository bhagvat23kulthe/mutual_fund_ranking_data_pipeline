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

spark = None

try:

    spark = (
        SparkSession.builder
        .appName("MutualFundETL")

        # ====================================================
        # S3A / AWS JAR CONFIGURATION
        # ====================================================

        .config(
            "spark.driver.extraClassPath",
            r"C:\Users\Hp\.ivy2.5.2\jars\org.apache.hadoop_hadoop-aws-3.5.0.jar;"
            r"C:\Users\Hp\.ivy2.5.2\jars\software.amazon.awssdk_bundle-2.35.4.jar;"
            r"C:\Users\Hp\.ivy2.5.2\jars\software.amazon.s3.analyticsaccelerator_analyticsaccelerator-s3-1.3.1.jar"
        )

        .config(
            "spark.executor.extraClassPath",
            r"C:\Users\Hp\.ivy2.5.2\jars\org.apache.hadoop_hadoop-aws-3.5.0.jar;"
            r"C:\Users\Hp\.ivy2.5.2\jars\software.amazon.awssdk_bundle-2.35.4.jar;"
            r"C:\Users\Hp\.ivy2.5.2\jars\software.amazon.s3.analyticsaccelerator_analyticsaccelerator-s3-1.3.1.jar"
        )

        # ====================================================
        # S3A CONFIGURATION
        # ====================================================

        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem"
        )

        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider"
        )

        # ====================================================
        # S3A FAST UPLOAD CONFIGURATION
        # ====================================================

        .config(
            "spark.hadoop.fs.s3a.fast.upload",
            "true"
        )

        .config(
            "spark.hadoop.fs.s3a.fast.upload.buffer",
            "bytebuffer"
        )

        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    print("\n============================================")
    print("Spark started successfully!")
    print("Spark Version:", spark.version)
    print("============================================")


    # ========================================================
    # 3. EXTRACT
    # ========================================================

    print("\n================ EXTRACT ================")

    print("\nReading data from:")
    print(INPUT_PATH)

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(INPUT_PATH)
    )

    print("\nRaw Data:")
    df.show(5, truncate=False)

    print("\nSchema:")
    df.printSchema()

    print("\nTotal Rows:", df.count())
    print("Total Columns:", len(df.columns))


    # ========================================================
    # 4. REQUIRED COLUMN VALIDATION
    # ========================================================

    print("\n================ COLUMN VALIDATION ================")

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
        raise ValueError(
            f"Missing required columns: {missing_columns}"
        )

    print("All required columns are present.")


    # ========================================================
    # 5. DATA PROFILING
    # ========================================================

    print("\n================ DATA PROFILING ================")

    print("\nMissing Values:")

    missing_values = df.select([
        count(
            when(col(column).isNull(), column)
        ).alias(column)
        for column in df.columns
    ])

    missing_values.show()


    print("\nDuplicate Fund Names:")

    duplicate_funds = (
        df.groupBy("Fund_Name")
        .count()
        .filter(col("count") > 1)
    )

    duplicate_funds.show()


    # ========================================================
    # 6. DATA CLEANING
    # ========================================================

    print("\n================ DATA CLEANING ================")

    # Remove unnecessary spaces

    df = (
        df
        .withColumn(
            "Fund_Name",
            trim(col("Fund_Name"))
        )
        .withColumn(
            "Category",
            trim(col("Category"))
        )
        .withColumn(
            "Risk_Rating",
            trim(col("Risk_Rating"))
        )
    )

    # Remove duplicate funds

    df = df.dropDuplicates(["Fund_Name"])

    print("Duplicates removed.")

    print("\nData after cleaning:")

    df.show(5, truncate=False)


    # ========================================================
    # 7. DATA VALIDATION
    # ========================================================

    print("\n================ DATA VALIDATION ================")


    # --------------------------------------------------------
    # Invalid return values
    # --------------------------------------------------------

    invalid_returns = df.filter(
        (col("1Y_Return") < 0) |
        (col("3Y_Return") < 0) |
        (col("5Y_Return") < 0)
    )

    print("\nInvalid Returns:")

    invalid_returns.show()


    # --------------------------------------------------------
    # Invalid expense ratio
    # --------------------------------------------------------

    invalid_expense = df.filter(
        col("Expense_Ratio") < 0
    )

    print("\nInvalid Expense Ratio:")

    invalid_expense.show()


    # --------------------------------------------------------
    # Invalid AUM
    # --------------------------------------------------------

    invalid_aum = df.filter(
        col("AUM_Crore") < 0
    )

    print("\nInvalid AUM:")

    invalid_aum.show()


    # --------------------------------------------------------
    # Invalid risk rating
    # --------------------------------------------------------

    valid_risk_ratings = [
        "Low",
        "Moderate",
        "High",
        "Very High"
    ]

    invalid_risk = df.filter(
        ~col("Risk_Rating").isin(valid_risk_ratings)
    )

    print("\nInvalid Risk Ratings:")

    invalid_risk.show()


    # ========================================================
    # 8. TRANSFORMATION
    # ========================================================

    print("\n================ TRANSFORMATION ================")

    # Weighted Performance Score
    #
    # 1Y Return = 20%
    # 3Y Return = 30%
    # 5Y Return = 50%

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

    print("\nPerformance Score:")

    df.select(
        "Fund_Name",
        "1Y_Return",
        "3Y_Return",
        "5Y_Return",
        "Performance_Score"
    ).show(15, truncate=False)


    # ========================================================
    # 9. OVERALL FUND RANKING
    # ========================================================

    print("\n================ OVERALL RANKING ================")

    overall_window = Window.orderBy(
        col("Performance_Score").desc()
    )

    df = df.withColumn(
        "Final_Rank",
        row_number().over(overall_window)
    )

    print("\nFinal Fund Ranking:")

    df.select(
        "Fund_Name",
        "Category",
        "Performance_Score",
        "Final_Rank"
    ).orderBy(
        "Final_Rank"
    ).show(15, truncate=False)


    # ========================================================
    # 10. CATEGORY-WISE RANKING
    # ========================================================

    print("\n================ CATEGORY RANKING ================")

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

    print("\nCategory-wise Ranking:")

    df.select(
        "Fund_Name",
        "Category",
        "Performance_Score",
        "Category_Rank"
    ).orderBy(
        "Category",
        "Category_Rank"
    ).show(20, truncate=False)


    # ========================================================
    # 11. FINAL DATASET
    # ========================================================

    print("\n================ FINAL DATASET ================")

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
        "Final_Rank",
        "Category_Rank"
    )

    print("\nFinal Schema:")

    final_df.printSchema()

    print("\nFinal Data:")

    final_df.show(15, truncate=False)


    # ========================================================
    # 12. LOAD
    # ========================================================

    print("\n================ LOAD ================")

    print("\nWriting processed data to:")

    print(OUTPUT_PATH)

    (
        final_df
        .write
        .mode("overwrite")
        .parquet(OUTPUT_PATH)
    )


    print("\n============================================")
    print("ETL PIPELINE COMPLETED SUCCESSFULLY!")
    print("============================================")

    print("\nProcessed Parquet location:")

    print(OUTPUT_PATH)


except Exception as error:

    print("\n============================================")
    print("ETL PIPELINE FAILED")
    print("============================================")

    print("\nError:")

    print(error)

    raise


finally:

    if spark is not None:

        spark.stop()

        print("\nSpark session stopped.")