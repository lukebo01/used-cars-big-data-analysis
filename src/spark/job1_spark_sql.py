#!/usr/bin/env python3
# src/spark/job1_spark_sql.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, min as sql_min, max as sql_max, avg as sql_avg, count as sql_count, collect_set, count, avg,
    lower, lit, concat, format_number, sort_array, collect_list, concat_ws
)
from pyspark.sql.types import FloatType, IntegerType, StringType
import os
import shutil
import argparse  # Per la gestione degli argomenti da riga di comando


def main():
    # Configurazione argomenti da linea di comando
    parser = argparse.ArgumentParser(description="Job 1: Calcolo statistiche di prezzo auto usate per marca e modello (SQL)")
    parser.add_argument("--input", type=str, default="data/samples/used_cars_1k.csv", 
                        help="Percorso del dataset di input")
    parser.add_argument("--output_dir", type=str, default="results/spark_sql", 
                        help="Directory base per i risultati")
    parser.add_argument("--dataset_size", type=float, default=1.0,
                        help="Dimensione del dataset (come frazione, es: 0.01, 0.05, 0.1, ecc.)")
    args = parser.parse_args()
    
    spark = (
        SparkSession.builder
        .appName("UsedCarsStats_Job1_SQL")
        .master("local[*]")   # Usa tutti i core disponibili sulla macchina
        .config("spark.sql.shuffle.partitions", "8")   # Partizioni per shuffle (adatta in base ai core)
        .config("spark.default.parallelism", "8")     # Parallelismo di default
        .getOrCreate()
    )
    
    input_path = args.input
    
    # --- Percorsi di Output ---
    output_dir_base = args.output_dir
    
    # Incorpora la dimensione del dataset nei nomi dei file di output
    size_suffix = f"{args.dataset_size:.2f}".replace('.', '_')
    
    # Per l'output SQL in CSV
    sql_output_path = os.path.join(output_dir_base, f"job1_sql_output_{size_suffix}")
    # Per il singolo file aggregato
    single_file_output_path = os.path.join(output_dir_base, f"job1_sql_output_singlefile_{size_suffix}.txt")
    # Per l'output DataFrame in formato testo
    dataframe_output_path_text = os.path.join(output_dir_base, f"job1_dataframe_output_{size_suffix}")
    
    # Crea la directory di output base se non esiste
    os.makedirs(output_dir_base, exist_ok=True)
    
    # Rimuovi la directory di output SQL precedente, se esiste
    if os.path.exists(sql_output_path):
        shutil.rmtree(sql_output_path)
    # Rimuovi il file singolo precedente, se esiste
    if os.path.exists(single_file_output_path):
        os.remove(single_file_output_path)
    # Rimuovi la directory di output DataFrame precedente, se esiste
    if os.path.exists(dataframe_output_path_text):
        shutil.rmtree(dataframe_output_path_text)

    # --- Inizio Logica Spark ---
    df_raw = spark.read.csv(input_path, header=True, inferSchema=False, escape='"')

    MAKE_NAME_COL = "make_name"
    MODEL_NAME_COL = "model_name"
    PRICE_COL = "price"
    YEAR_COL = "year"

    df_transformed = df_raw.select(
        lower(col(MAKE_NAME_COL)).alias("make_name"),
        lower(col(MODEL_NAME_COL)).alias("model_name"),
        col(PRICE_COL).cast(FloatType()).alias("price"),
        col(YEAR_COL).cast(FloatType()).cast(IntegerType()).alias("year")
    )

    df_filtered = df_transformed.filter(
        (col("price").isNotNull()) & (col("price") > 0) &
        (col("year").isNotNull()) & (col("year").between(1900, 2025)) &
        (col("make_name").isNotNull()) & (col("make_name") != "") &
        (col("model_name").isNotNull()) & (col("model_name") != "")
    )

    model_stats_df = df_filtered.groupBy("make_name", "model_name").agg(
        count("*").alias("num_cars"),
        sql_min("price").alias("min_price_val"),
        sql_max("price").alias("max_price_val"),
        sql_avg("price").alias("avg_price_val"),
        sort_array(collect_set("year")).alias("years_list")
    )

    model_stats_df = model_stats_df.withColumn("min_price_str", format_number(col("min_price_val"), 2)) \
                                   .withColumn("max_price_str", format_number(col("max_price_val"), 2)) \
                                   .withColumn("avg_price_str", format_number(col("avg_price_val"), 2))

    model_details_df = model_stats_df.withColumn(
        "model_detail_str",
        concat(
            lit("Model: "), col("model_name"),
            lit(", Count: "), col("num_cars").cast(StringType()),
            lit(", MinPrice: "), col("min_price_str"),
            lit(", MaxPrice: "), col("max_price_str"),
            lit(", AvgPrice: "), col("avg_price_str"),
            lit(", Years: "), col("years_list").cast(StringType())
        )
    ).select("make_name", "model_name", "model_detail_str")

    make_summary_df = model_details_df.orderBy("make_name", "model_name") \
                                      .groupBy("make_name") \
                                      .agg(collect_list("model_detail_str").alias("models_details_array"))

    make_summary_df = make_summary_df.withColumn(
        "models_details_string", concat_ws("; ", col("models_details_array"))
    )
    
    final_output_df = make_summary_df.select(
        concat(
            lit("Make: "), col("make_name"),
            lit("\tModels: ["), col("models_details_string"), lit("]")
        ).alias("output_line")
    ).orderBy("make_name")

    # 1. Salva l'output DataFrame standard (directory con part-files)
    #    .coalesce(1) forza l'output in un singolo file part-xxxxx dentro la directory
    #    .text() salva la colonna "output_line" come righe di testo
    final_output_df.select("output_line").coalesce(1).write.mode("overwrite").text(dataframe_output_path_text)
    print(f"Output DataFrame (come testo, con part-files) salvato in: {dataframe_output_path_text}")

    # 2. Raccogli i risultati sul driver e scrivili in un singolo file
    results_list = [row.output_line for row in final_output_df.collect()]
    
    with open(single_file_output_path, 'w') as f:
        for line in results_list:
            f.write(line + '\n')
    print(f"Output aggregato in singolo file salvato in: {single_file_output_path}")

    # Stampa le prime 10 righe del risultato (come prima)
    print("\n--- JOB 1 SPARK SQL (DATAFRAME API) RESULTS (FIRST 10 FROM COLLECTED DATA) ---")
    for record_str in results_list[:10]:
        print(record_str)
    
    spark.stop()

if __name__ == "__main__":
    main()