import sys
from pyspark.sql import SparkSession
import argparse
from pyspark.sql.functions import col, when, count, sum, lit, round


def create_dataframe(filepath, format, spark):
    
    if format.lower() == "csv":
        # Inferred schema so numeric columns remain numeric
        spark_df = (
            spark.read
                 .option("header", "true")
                 .option("inferSchema", "true")
                 .csv(filepath)
        )
    elif format.lower() == "json":
        spark_df = spark.read.json(filepath)
    else:
        raise ValueError(f"Unsupported file format: {format}")

    return spark_df


def transform_nhis_data(nhis_df):
    
    from pyspark.sql.functions import col, when, lit

    # Filter out invalid or null SEX values: keep only 1 or 2
    nhis_df = nhis_df.filter(col("SEX").isin([1, 2]))

    # Convert SEX to double type to match BRFSS format
    transformed_df = nhis_df.withColumn("SEX", col("SEX").cast("double"))

    # Create an age group column (_AGEG5YR) based on AGE_P
    transformed_df = transformed_df.withColumn(
        "_AGEG5YR",
        when((col("AGE_P") >= 18) & (col("AGE_P") <= 24), 1.0)
        .when((col("AGE_P") >= 25) & (col("AGE_P") <= 29), 2.0)
        .when((col("AGE_P") >= 30) & (col("AGE_P") <= 34), 3.0)
        .when((col("AGE_P") >= 35) & (col("AGE_P") <= 39), 4.0)
        .when((col("AGE_P") >= 40) & (col("AGE_P") <= 44), 5.0)
        .when((col("AGE_P") >= 45) & (col("AGE_P") <= 49), 6.0)
        .when((col("AGE_P") >= 50) & (col("AGE_P") <= 54), 7.0)
        .when((col("AGE_P") >= 55) & (col("AGE_P") <= 59), 8.0)
        .when((col("AGE_P") >= 60) & (col("AGE_P") <= 64), 9.0)
        .when((col("AGE_P") >= 65) & (col("AGE_P") <= 69), 10.0)
        .when((col("AGE_P") >= 70) & (col("AGE_P") <= 74), 11.0)
        .when((col("AGE_P") >= 75) & (col("AGE_P") <= 79), 12.0)
        .when(col("AGE_P") >= 80, 13.0)
    )

    # Map race categories to match BRFSS _IMPRACE format
    transformed_df = transformed_df.withColumn(
        "_IMPRACE",
        when(col("MRACBPI2") == 1, 1.0)  # White
        .when(col("MRACBPI2") == 2, 2.0)  # Black
        .when(col("MRACBPI2") == 4, 3.0)  # Asian
        .when(col("MRACBPI2") == 3, 4.0)  # American Indian/Alaska Native
        .when((col("HISPAN_I") != 12) & (col("HISPAN_I") != 0), 5.0)  # Hispanic
        .otherwise(6.0)  # Other race
    )

    # Add _LLCPWT column with default value of 1 if it doesn't exist
    if "_LLCPWT" not in nhis_df.columns:
        transformed_df = transformed_df.withColumn("_LLCPWT", lit(1))

    return transformed_df


def join_data(brfss_df, nhis_df):
    
    joined_df = brfss_df.join(
        nhis_df.select("SEX", "_AGEG5YR", "_IMPRACE", "DIBEV1"),
        on=["SEX", "_AGEG5YR", "_IMPRACE"],
        how="inner"
    )

    # Drop rows with null values
    joined_df = joined_df.na.drop()

    return joined_df


def calculate_statistics(joined_df):
   
    # Add indicator column
    stats_df = joined_df.withColumn(
        "disease_indicator", when(col("DIBEV1") == 1, 1).otherwise(0)
    )

    # By sex
    diabetes_by_sex = (
        stats_df.groupBy("SEX")
        .agg(
            sum(col("_LLCPWT")).alias("total_weighted"),
            sum(when(col("disease_indicator") == 1, col("_LLCPWT")).otherwise(0)).alias("diabetes_weighted")
        )
        .withColumn("prevalence", round(col("diabetes_weighted")/col("total_weighted"), 4))
    )
    diabetes_by_sex.coalesce(1).write.csv("output/prevalence_by_sex", mode="overwrite", header=True)

    # By age group
    diabetes_by_age = (
        stats_df.groupBy("_AGEG5YR")
        .agg(
            sum(col("_LLCPWT")).alias("total_weighted"),
            sum(when(col("disease_indicator") == 1, col("_LLCPWT")).otherwise(0)).alias("diabetes_weighted")
        )
        .withColumn("prevalence", round(col("diabetes_weighted")/col("total_weighted"), 4))
        .orderBy("_AGEG5YR")
    )
    diabetes_by_age.coalesce(1).write.csv("output/prevalence_by_age", mode="overwrite", header=True)

    # By race
    diabetes_by_race = (
        stats_df.groupBy("_IMPRACE")
        .agg(
            sum(col("_LLCPWT")).alias("total_weighted"),
            sum(when(col("disease_indicator") == 1, col("_LLCPWT")).otherwise(0)).alias("diabetes_weighted")
        )
        .withColumn("prevalence", round(col("diabetes_weighted")/col("total_weighted"), 4))
        .orderBy("_IMPRACE")
    )
    diabetes_by_race.coalesce(1).write.csv("output/prevalence_by_race", mode="overwrite", header=True)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('nhis', type=str, help="NHIS CSV path")
    parser.add_argument('brfss', type=str, help="BRFSS JSON path")
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()
    nhis_df = create_dataframe(args.nhis, 'csv', spark)
    brfss_df = create_dataframe(args.brfss, 'json', spark)

    nhis_df = transform_nhis_data(nhis_df)
    joined_df = join_data(brfss_df, nhis_df)
    calculate_statistics(joined_df)
    joined_df.coalesce(1).write.csv("output/joined_data", mode="overwrite", header=True)
    spark.stop()
