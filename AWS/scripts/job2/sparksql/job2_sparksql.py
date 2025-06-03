# Sovrascrivi Job2 Spark SQL con la logica corretta del codice locale
#cat > job2_sparksql.py << 'EOF'
#!/usr/bin/env python3
"""
Job2 Spark SQL per AWS EMR: Analisi city/year/price_range con words frequency
BASATO ESATTAMENTE sul job2_spark_sql.py locale, adattato per AWS EMR
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lower, lit, concat, format_number, sort_array, collect_list, concat_ws,
    when, struct, trim, count, avg, udf
)
from pyspark.sql.types import FloatType, IntegerType, StringType, ArrayType
import sys
import re
from collections import Counter

# UDF per pulizia e tokenizzazione (stesso del codice locale)
def clean_tokenize(text):
    if not text:
        return []
    # Rimuovi caratteri speciali e converti in minuscolo
    text = re.sub(r'[^\w\s]', ' ', text.lower())
    # Dividi in token e filtra token vuoti
    tokens = [token.strip() for token in text.split() if token.strip()]
    return tokens

# UDF per top N words (stesso del codice locale)  
def get_top_n_words(words_list, n=3):
    if not words_list:
        return []
    # Appiattisci la lista di liste in una singola lista
    all_words = [word for sublist in words_list for word in sublist if word]
    # Conta le parole e restituisci le N più frequenti
    counter = Counter(all_words)
    return [word for word, count in counter.most_common(n)]

# Registra le UDF
clean_tokenize_udf = udf(clean_tokenize, ArrayType(StringType()))
get_top_n_words_udf = udf(get_top_n_words, ArrayType(StringType()))

def main():
    # Parametri da linea di comando per AWS EMR
    input_path = sys.argv[1] if len(sys.argv) > 1 else "s3://used-cars-big-data-analysis-1748698020/data/used_cars_filtered.csv"
    output_path = sys.argv[2] if len(sys.argv) > 2 else "s3://used-cars-big-data-analysis-1748698020/output/sparksql/job2/"
    
    print(f"Job2 Spark SQL - Input: {input_path}")
    print(f"Job2 Spark SQL - Output: {output_path}")
    
    # Configurazione Spark SQL per EMR (stesso setup del locale)
    spark = (SparkSession.builder
        .appName("Job2-CityYearPriceRange-Analysis-SparkSQL-EMR")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.default.parallelism", "8")
        .getOrCreate())

    print("Lettura dataset con Spark SQL...")
    
    # Leggi CSV come DataFrame (stesso del codice locale)
    df_raw = spark.read.csv(input_path, header=True, inferSchema=False, escape='"')
    
    print(f"Dataset caricato: {df_raw.count()} righe")
    
    # Seleziona e casta le colonne necessarie (stesso del codice locale)
    df = df_raw.select(
        lower(col("city")).alias("city"),
        col("year").cast(FloatType()).cast(IntegerType()).alias("vehicle_year"),
        col("price").cast(FloatType()).alias("price"),
        col("daysonmarket").cast(IntegerType()).alias("daysonmarket"),
        # Usa description_cleaned se non vuota, altrimenti description (stesso del locale)
        when(col("description_cleaned").isNotNull() & (trim(col("description_cleaned")) != ""), col("description_cleaned"))
            .otherwise(col("description")).alias("description_text")
    )

    print("Filtri di validazione...")
    
    # Filtra record con valori essenziali nulli o invalidi (stesso del codice locale)
    df_filtered = df.filter(
        col("city").isNotNull() & (col("city") != "") &
        col("vehicle_year").isNotNull() & col("vehicle_year").between(1900, 2025) &
        col("price").isNotNull() & (col("price") > 0) &
        col("daysonmarket").isNotNull() & (col("daysonmarket") >= 0)
    )

    print("Classificazione fasce di prezzo...")
    
    # Aggiungi colonna fascia di prezzo (stesso del codice locale)
    df_price_range = df_filtered.withColumn("price_range",
        when(col("price") < 20000, lit("basso"))
        .when((col("price") >= 20000) & (col("price") <= 50000), lit("medio"))
        .otherwise(lit("alto"))
    )

    print("Tokenizzazione descrizioni...")
    
    # Applica UDF per tokenizzare le descrizioni (stesso del codice locale)
    df_tokenized = df_price_range.withColumn("tokens", clean_tokenize_udf(col("description_text")))

    print("Aggregazione per city/year/price_range...")
    
    # Raggruppa per (city, vehicle_year, price_range) e calcola le aggregazioni (stesso del locale)
    df_grouped_by_range = df_tokenized.groupBy("city", "vehicle_year", "price_range").agg(
        count("*").alias("num_cars"),
        avg("daysonmarket").alias("avg_daysonmarket"),
        collect_list(col("tokens")).alias("list_of_token_lists")
    )
    
    # Applica l'UDF per ottenere le top 3 parole (stesso del codice locale)
    df_with_top_words = df_grouped_by_range.withColumn(
        "top_3_words", get_top_n_words_udf(col("list_of_token_lists"))
    )

    print("Strutturazione dati per fascia...")
    
    # Creiamo una struct per ogni fascia per facilitare l'aggregazione (stesso del locale)
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

    print("Aggregazione finale per city/year...")
    
    # Raggruppa per city, year e raccogli i dati delle fasce (stesso del locale)
    df_city_year_agg = df_range_struct.groupBy("city", "vehicle_year") \
        .agg(collect_list("range_data").alias("ranges_list"))

    # UDF per formattare l'output finale (ESATTO formato del codice locale)
    @udf(StringType())
    def format_final_report_udf(city, year, ranges_list_struct):
        output_str = f"City: {city}, Year: {year}"
        fasce_map = {item['fascia']: item for item in ranges_list_struct}
        
        fasce_output_parts = []
        for pr in ["basso", "medio", "alto"]:  # Ordine richiesto
            data = fasce_map.get(pr)
            if data:
                avg_dom_formatted = f"{data['avg_dom']:.2f}" if data['avg_dom'] is not None else "0.00"
                top_words_str = str(data['top_words']) if data['top_words'] else "[]"
                fasce_output_parts.append(
                    f"FasciaPrezzo: {pr}, "
                    f"NumAuto: {data['num_auto']}, "
                    f"AvgDaysOnMarket: {avg_dom_formatted}, "
                    f"TopWords: {top_words_str}"
                )
            else:
                fasce_output_parts.append(
                    f"FasciaPrezzo: {pr}, NumAuto: 0, AvgDaysOnMarket: 0.00, TopWords: []"
                )
        output_str += "\tReportFasce: [" + "; ".join(fasce_output_parts) + "]"
        return output_str

    print("Formattazione output finale...")
    
    # Applica UDF di formattazione (stesso del locale)
    final_report_df = df_city_year_agg.withColumn(
        "output_line",
        format_final_report_udf(col("city"), col("vehicle_year"), col("ranges_list"))
    ).select("output_line").orderBy("output_line")

    print(f"Salvataggio risultati in: {output_path}")
    
    # Salva su S3 con un singolo file (adattato per EMR)
    final_report_df.select("output_line").coalesce(1).write.mode("overwrite").text(output_path)
    
    print("Job2 Spark SQL completato!")
    
    # Mostra alcune statistiche
    total_city_years = df_city_year_agg.count()
    valid_records = df_filtered.count()
    total_price_ranges = df_with_top_words.count()
    
    print(f"Statistiche:")
    print(f"- Record validi: {valid_records}")
    print(f"- Combinazioni city/year: {total_city_years}")
    print(f"- Combinazioni city/year/price_range: {total_price_ranges}")
    print(f"- Tecnologia: Spark SQL (DataFrame API + UDF)")
    
    spark.stop()

if __name__ == "__main__":
    main()
#EOF
