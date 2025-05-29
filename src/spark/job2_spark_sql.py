#!/usr/bin/env python3
# src/spark/job2_spark_sql.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, min as sql_min, max as sql_max, avg as sql_avg, count as sql_count,
    lower, lit, concat, format_number, sort_array, collect_list, concat_ws,
    when, struct, trim, count, avg
)
from pyspark.sql.types import FloatType, IntegerType, StringType
import os
import shutil
import re
from collections import Counter
from pyspark.sql.functions import udf
import argparse  # Per la gestione degli argomenti da riga di comando

# Definizione delle User Defined Functions necessarie
def clean_tokenize(text):
    if not text:
        return []
    # Rimuovi caratteri speciali e converti in minuscolo
    text = re.sub(r'[^\w\s]', ' ', text.lower())
    # Dividi in token e filtra token vuoti
    tokens = [token.strip() for token in text.split() if token.strip()]
    return tokens

def get_top_n_words(words_list, n=10):
    if not words_list:
        return []
    # Appiattisci la lista di liste in una singola lista
    all_words = [word for sublist in words_list for word in sublist if word]
    # Conta le parole e restituisci le N più frequenti
    counter = Counter(all_words)
    return [word for word, count in counter.most_common(n)]

# Registra le UDF
clean_tokenize_udf = udf(clean_tokenize, StringType())
get_top_n_words_udf = udf(get_top_n_words, StringType())

def main():
    # Configurazione argomenti da linea di comando
    parser = argparse.ArgumentParser(description="Job 2: Calcolo statistiche per tipo di carrozzeria e colore (SQL)")
    parser.add_argument("--input", type=str, default="data/samples/used_cars_1k.csv", 
                        help="Percorso del dataset di input")
    parser.add_argument("--output_dir", type=str, default="results/spark_sql", 
                        help="Directory base per i risultati")
    parser.add_argument("--dataset_size", type=float, default=1.0,
                        help="Dimensione del dataset (come frazione, es: 0.01, 0.05, 0.1, ecc.)")
    args = parser.parse_args()
    
    spark = (
        SparkSession.builder
        .appName("UsedCarsStats_Job2_SQL")
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
    sql_output_path = os.path.join(output_dir_base, f"job2_sql_output_{size_suffix}")
    # Per il singolo file aggregato
    single_file_output_path = os.path.join(output_dir_base, f"job2_sql_output_singlefile_{size_suffix}.txt")
    # Definisci il percorso di output per il DataFrame come testo
    dataframe_output_path_text = os.path.join(output_dir_base, f"job2_dataframe_output_{size_suffix}")
    
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
    
    df_raw = spark.read.csv(input_path, header=True, inferSchema=False, escape='"')

    # Seleziona e casta le colonne necessarie
    df = df_raw.select(
        lower(col("city")).alias("city"),
        col("year").cast(FloatType()).cast(IntegerType()).alias("vehicle_year"), # anno del veicolo
        col("price").cast(FloatType()).alias("price"),
        col("daysonmarket").cast(IntegerType()).alias("daysonmarket"),
        # Usa description_cleaned se non vuota, altrimenti description
        when(col("description_cleaned").isNotNull() & (trim(col("description_cleaned")) != ""), col("description_cleaned"))
            .otherwise(col("description")).alias("description_text")
    )

    # Filtra record con valori essenziali nulli o invalidi
    df_filtered = df.filter(
        col("city").isNotNull() & (col("city") != "") &
        col("vehicle_year").isNotNull() & col("vehicle_year").between(1900, 2025) &
        col("price").isNotNull() & (col("price") > 0) &
        col("daysonmarket").isNotNull() & (col("daysonmarket") >= 0)
    )

    # Aggiungi colonna fascia di prezzo
    df_price_range = df_filtered.withColumn("price_range",
        when(col("price") < 20000, lit("basso"))
        .when((col("price") >= 20000) & (col("price") <= 50000), lit("medio"))
        .otherwise(lit("alto"))
    )

    # Applica UDF per tokenizzare le descrizioni
    df_tokenized = df_price_range.withColumn("tokens", clean_tokenize_udf(col("description_text")))

    # Raggruppa per (city, vehicle_year, price_range) e calcola le aggregazioni
    # Nota: collect_list(col("tokens")) creerà una lista di liste di parole
    df_grouped_by_range = df_tokenized.groupBy("city", "vehicle_year", "price_range").agg(
        count("*").alias("num_cars"),
        avg("daysonmarket").alias("avg_daysonmarket"),
        collect_list(col("tokens")).alias("list_of_token_lists") # Lista di liste di parole
    )
    
    # Applica l'UDF per ottenere le top 3 parole dalla lista di liste di token
    df_with_top_words = df_grouped_by_range.withColumn(
        "top_3_words", get_top_n_words_udf(col("list_of_token_lists"))
    )

    # Pivot o struttura per avere le fasce come "colonne" o elementi di una mappa
    # Per l'output testuale richiesto, è più facile aggregare per (city, vehicle_year)
    # e poi costruire la stringa.
    
    # Creiamo una struct per ogni fascia per facilitare l'aggregazione
    df_range_struct = df_with_top_words.select(
        col("city"),
        col("vehicle_year"),
        struct(
            col("price_range").alias("fascia"),
            col("num_cars").alias("num_auto"),
            col("avg_daysonmarket").alias("avg_dom"),
            col("top_3_words").alias("top_words")
        ).alias("range_data")
    )

    # Raggruppa per city, year e raccogli i dati delle fasce
    df_city_year_agg = df_range_struct.groupBy("city", "vehicle_year") \
        .agg(collect_list("range_data").alias("ranges_list"))

    # UDF per formattare l'output finale per ogni riga (city, year)
    @udf(StringType())
    def format_final_report_udf(city, year, ranges_list_struct):
        output_str = f"City: {city}, Year: {year}"
        fasce_map = {item['fascia']: item for item in ranges_list_struct}
        
        fasce_output_parts = []
        for pr in ["basso", "medio", "alto"]:
            data = fasce_map.get(pr)
            if data:
                avg_dom_formatted = f"{data['avg_dom']:.2f}" if data['avg_dom'] is not None else "0.00"
                fasce_output_parts.append(
                    f"FasciaPrezzo: {pr}, "
                    f"NumAuto: {data['num_auto']}, "
                    f"AvgDaysOnMarket: {avg_dom_formatted}, "
                    f"TopWords: {str(data['top_words'])}"
                )
            else:
                fasce_output_parts.append(
                    f"FasciaPrezzo: {pr}, NumAuto: 0, AvgDaysOnMarket: 0.00, TopWords: []"
                )
        output_str += "\tReportFasce: [" + "; ".join(fasce_output_parts) + "]"
        return output_str

    final_report_df = df_city_year_agg.withColumn(
        "output_line",
        format_final_report_udf(col("city"), col("vehicle_year"), col("ranges_list"))
    ).select("output_line").orderBy("output_line") # Ordina per la stringa intera, o specifici campi se vuoi


    # --- Salvataggio e Stampa ---
    final_report_df.select("output_line").coalesce(1).write.mode("overwrite").text(dataframe_output_path_text)
    print(f"Output DataFrame Job2 (come testo, con part-files) salvato in: {dataframe_output_path_text}")

    results_list = [row.output_line for row in final_report_df.collect()]
    with open(single_file_output_path, 'w') as f:
        for line in results_list:
            f.write(line + '\n')
    print(f"Output Job2 aggregato in singolo file salvato in: {single_file_output_path}")

    print("\n--- JOB 2 SPARK SQL (DATAFRAME API) RESULTS (FIRST 10) ---")
    for record_str in results_list[:10]:
        print(record_str)

    spark.stop()

if __name__ == "__main__":
    main()