from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, regexp_extract, rand
import sys
import os
import shutil
import time

def create_spark_session(master_url="local[*]"):
    """Create a Spark session for Problem 1"""
    spark = (
        SparkSession.builder
        .appName("Problem1")
        .master(master_url)
        .getOrCreate()
    )
    return spark

def save_csv_file(df, output_path):
    """Saving to CSV files instead of Spark default of folders."""
    temp_dir = output_path + "_temp"
    df.coalesce(1).write.csv(temp_dir, header=True, mode="overwrite")
    for _ in range(10):
        part_files = [f for f in os.listdir(temp_dir) if f.startswith("part-") and f.endswith(".csv")]
        if part_files:
            break
        time.sleep(1)
    if not part_files:
        raise RuntimeError(f"No CSV part files found in {temp_dir}. Check Spark job output.")
    part_file = part_files[0]
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    shutil.move(os.path.join(temp_dir, part_file), output_path)
    shutil.rmtree(temp_dir)

def solve_prob1(input_dir, master_url):
    """Computation section for counts, sample, and summary statistics."""

    # starting the spark session
    spark = create_spark_session(master_url)

    # defining the input and output paths
    output_counts = "data/output/problem1_counts.csv"
    output_sample = "data/output/problem1_sample.csv"
    output_summary = "data/output/problem1_summary.txt"

    # reading in the log files
    logs = spark.read.text(input_dir)

    # getting the log level
    level_pattern = r"\b(INFO|WARN|ERROR|DEBUG)\b"
    logs_with_level = logs.withColumn("log_level", regexp_extract(col("value"), level_pattern, 1))
    logs_with_level = logs_with_level.withColumn("log_level", lower(col("log_level")))
    logs_filtered = logs_with_level.filter(col("log_level") != "")
    # counting number of each level of logging
    level_counts = logs_filtered.groupBy("log_level").count().orderBy(col("count").desc())
    level_counts_local = level_counts.cache()
    level_counts_local.show(5)
    level_counts_local.toPandas().to_csv("/home/ubuntu/spark-cluster/data/output/problem1_counts.csv", index=False)

    # getting the 10 random samples
    sample_logs = logs_filtered.select(
        col("value").alias("log_entry"),
        col("log_level").alias("log_level")
    ).orderBy(rand()).limit(10)
    sample_logs.toPandas().to_csv("/home/ubuntu/spark-cluster/data/output/problem1_sample.csv", index=False)

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
    master_url = sys.argv[1] if len(sys.argv) > 1 else "local[*]"
    input_path = "./raw/"
    solve_prob1(input_path, master_url)

if __name__ == "__main__":
    main()