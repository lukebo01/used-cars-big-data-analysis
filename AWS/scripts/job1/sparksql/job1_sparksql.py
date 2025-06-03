# Crea Job1 Spark SQL basato sul codice esistente, adattato per AWS EMR
#cat > job1_sparksql.py << 'EOF'
#!/usr/bin/env python3
"""
Job1 Spark SQL per AWS EMR: Analisi Make/Model con aggregazioni SQL
STESSO OUTPUT del job1_spark_sql.py locale ma per cluster AWS EMR
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, min as sql_min, max as sql_max, avg as sql_avg, count as sql_count, 
    collect_set, count, avg, lower, lit, concat, format_number, sort_array, 
    collect_list, concat_ws
)
from pyspark.sql.types import FloatType, IntegerType, StringType
import sys

def main():
    # Parametri da linea di comando per AWS EMR
    input_path = sys.argv[1] if len(sys.argv) > 1 else "s3://used-cars-big-data-analysis-1748698020/data/used_cars_filtered.csv"
    output_path = sys.argv[2] if len(sys.argv) > 2 else "s3://used-cars-big-data-analysis-1748698020/output/sparksql/job1/"
    
    print(f"Job1 Spark SQL - Input: {input_path}")
    print(f"Job1 Spark SQL - Output: {output_path}")
    
    # Configurazione Spark SQL per EMR
    spark = (SparkSession.builder
        .appName("Job1-MakeModel-Analysis-SparkSQL-EMR")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate())

    print("Lettura dataset con Spark SQL...")
    
    # Leggi CSV come DataFrame (stesso del codice locale)
    df_raw = spark.read.csv(input_path, header=True, inferSchema=False, escape='"')
    
    print(f"Dataset caricato: {df_raw.count()} righe")
    
    # Colonne richieste (stesso del codice locale)
    MAKE_NAME_COL = "make_name"
    MODEL_NAME_COL = "model_name"
    PRICE_COL = "price"
    YEAR_COL = "year"

    print("Trasformazione e pulizia dati...")
    
    # Trasformazione dati (stesso del codice locale)
    df_transformed = df_raw.select(
        lower(col(MAKE_NAME_COL)).alias("make_name"),
        lower(col(MODEL_NAME_COL)).alias("model_name"),
        col(PRICE_COL).cast(FloatType()).alias("price"),
        col(YEAR_COL).cast(FloatType()).cast(IntegerType()).alias("year")
    )

    # Filtri di validazione (stesso del codice locale)
    df_filtered = df_transformed.filter(
        (col("price").isNotNull()) & (col("price") > 0) &
        (col("year").isNotNull()) & (col("year").between(1900, 2025)) &
        (col("make_name").isNotNull()) & (col("make_name") != "") &
        (col("model_name").isNotNull()) & (col("model_name") != "")
    )

    print("Aggregazione statistiche per modello...")
    
    # Statistiche per modello (stesso del codice locale)
    model_stats_df = df_filtered.groupBy("make_name", "model_name").agg(
        count("*").alias("num_cars"),
        sql_min("price").alias("min_price_val"),
        sql_max("price").alias("max_price_val"),
        sql_avg("price").alias("avg_price_val"),
        sort_array(collect_set("year")).alias("years_list")
    )

    # Formattazione prezzi (stesso del codice locale)
    model_stats_df = model_stats_df.withColumn("min_price_str", format_number(col("min_price_val"), 2)) \
                                   .withColumn("max_price_str", format_number(col("max_price_val"), 2)) \
                                   .withColumn("avg_price_str", format_number(col("avg_price_val"), 2))

    print("Formattazione dettagli modelli...")
    
    # Dettagli modelli (stesso del codice locale)
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

    print("Aggregazione per marca...")
    
    # Aggregazione per marca (stesso del codice locale)
    make_summary_df = model_details_df.orderBy("make_name", "model_name") \
                                      .groupBy("make_name") \
                                      .agg(collect_list("model_detail_str").alias("models_details_array"))

    make_summary_df = make_summary_df.withColumn(
        "models_details_string", concat_ws("; ", col("models_details_array"))
    )
    
    print("Formattazione output finale...")
    
    # Output finale (stesso del codice locale)
    final_output_df = make_summary_df.select(
        concat(
            lit("Make: "), col("make_name"),
            lit("\tModels: ["), col("models_details_string"), lit("]")
        ).alias("output_line")
    ).orderBy("make_name")

    print(f"Salvataggio risultati in: {output_path}")
    
    # Salva su S3 con un singolo file (adattato per EMR)
    final_output_df.select("output_line").coalesce(1).write.mode("overwrite").text(output_path)
    
    print("Job1 Spark SQL completato!")
    
    # Mostra alcune statistiche
    total_makes = make_summary_df.count()
    total_models = model_stats_df.count()
    valid_records = df_filtered.count()
    
    print(f"Statistiche:")
    print(f"- Record validi: {valid_records}")
    print(f"- Make distinti: {total_makes}")
    print(f"- Combinazioni Make/Model: {total_models}")
    print(f"- Tecnologia: Spark SQL (DataFrame API)")
    
    spark.stop()

if __name__ == "__main__":
    main()
#EOF
