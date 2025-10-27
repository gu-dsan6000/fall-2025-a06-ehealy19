from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, regexp_extract, rand
import sys
import os
import shutil

def create_spark_session():
    """Create a Spark session for Problem 1"""
    spark = (
        SparkSession.builder
        .appName("Problem1")
        .getOrCreate()
    )
    return spark

def save_csv_file(df, output_path):
    """Saving to CSV files instead of Spark default of folders."""
    temp_dir = output_path + "_temp"
    df.coalesce(1).write.csv(temp_dir, header=True, mode="overwrite")
    part_file = [f for f in os.listdir(temp_dir) if f.startswith("part-") and f.endswith(".csv")][0]
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    shutil.move(os.path.join(temp_dir, part_file), output_path)
    shutil.rmtree(temp_dir)

def solve_prob1(input_dir):
    """Computation section for counts, sample, and summary statistics."""

    # starting the spark session
    spark = create_spark_session()

    # defining the input and output paths
    output_counts = "data/output/problem1_counts.csv"
    output_sample = "data/output/problem1_sample.csv"
    output_summary = "data/output/problem1_summary.txt"

    # reading in the log files
    logs = spark.read.text(input_dir)

    # getting the log level
    level_pattern = r"(info|warn|error|debug)"
    logs_with_level = logs.withColumn("log_level", regexp_extract(lower(col("value")), level_pattern, 1))
    logs_filtered = logs_with_level.filter(col("log_level") != "")
    # counting number of each level of logging
    level_counts = logs_filtered.groupBy("log_level").count().orderBy(col("count").desc())
    save_csv_file(level_counts, output_counts)

    # getting the 10 random samples
    sample_logs = logs_filtered.select(
        col("value").alias("log_entry"),
        col("log_level").alias("log_level")
    ).orderBy(rand()).limit(10)
    save_csv_file(sample_logs, output_sample)

    # computing the summary statistics
    total_logs = logs.count()
    total_with_levels = logs_filtered.count()
    level_data = level_counts.collect()
    summary_lines = [
        f"Total log lines processed: {total_logs}",
        f"Total lines with log levels: {total_with_levels}",
        f"Unique log levels found: {len(level_data)}",
        "",
        "Log level distribution:"
    ]
    for row in level_data:
        level = row['log_level'].upper()
        count = row['count']
        pct = (count / total_with_levels) * 100 if total_with_levels else 0
        summary_lines.append(f"  {level:<6}: {count:>10,} ({pct:6.2f}%)")

    summary_text = "\n".join(summary_lines)
    os.makedirs(os.path.dirname(output_summary), exist_ok=True)
    with open(output_summary, "w") as f:
        f.write(summary_text)
    
    spark.stop()


def main():
    """Main function for problem 1."""
    
    input_path = "./raw/"
    solve_prob1(input_path)

if __name__ == "__main__":
    main()