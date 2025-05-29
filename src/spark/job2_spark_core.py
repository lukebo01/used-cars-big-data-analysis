#!/usr/bin/env python3
# src/spark/job2_spark_core.py

from pyspark.sql import SparkSession
import csv
from io import StringIO
import os
import shutil
import re
from collections import Counter
import argparse  # Per la gestione degli argomenti da riga di comando

# --- HEADER E INDICI ---
HEADER_FIELDS_STRING = "vin,back_legroom,body_type,city,city_fuel_economy,daysonmarket,dealer_zip,description,engine_cylinders,engine_displacement,engine_type,exterior_color,fleet,frame_damaged,franchise_dealer,franchise_make,front_legroom,fuel_tank_volume,fuel_type,has_accidents,height,highway_fuel_economy,horsepower,interior_color,isCab,is_new,latitude,length,listed_date,listing_color,listing_id,longitude,main_picture_url,major_options,make_name,maximum_seating,mileage,model_name,owner_count,power,price,salvage,savings_amount,seller_rating,sp_id,sp_name,theft_title,torque,transmission,transmission_display,trimId,trim_name,wheel_system,wheel_system_display,wheelbase,width,year,major_options_list,num_major_options,horsepower_extracted,horsepower_rpm,torque_value,torque_rpm,seats,cylinders,listed_year,listed_month,listed_day,listed_dayofweek,description_cleaned,back_legroom_missing,city_fuel_economy_missing,engine_displacement_missing,front_legroom_missing,fuel_tank_volume_missing,height_missing,highway_fuel_economy_missing,horsepower_missing,length_missing,mileage_missing,owner_count_missing,seller_rating_missing,sp_id_missing,wheelbase_missing,width_missing,horsepower_extracted_missing,horsepower_rpm_missing,torque_value_missing,torque_rpm_missing,seats_missing,cylinders_missing,body_type_missing,description_missing,engine_cylinders_missing,engine_type_missing,exterior_color_missing,franchise_make_missing,fuel_type_missing,interior_color_missing,main_picture_url_missing,major_options_missing,maximum_seating_missing,power_missing,torque_missing,transmission_missing,transmission_display_missing,trimId_missing,trim_name_missing,wheel_system_missing,wheel_system_display_missing,description_cleaned_missing,unified_color"
HEADER_FIELDS = HEADER_FIELDS_STRING.split(',')

try:
    IDX_CITY = HEADER_FIELDS.index('city')
    IDX_YEAR = HEADER_FIELDS.index('year')
    IDX_PRICE = HEADER_FIELDS.index('price')
    IDX_DAYSONMARKET = HEADER_FIELDS.index('daysonmarket')
    IDX_DESCRIPTION = HEADER_FIELDS.index('description')
    IDX_DESCRIPTION_CLEANED = HEADER_FIELDS.index('description_cleaned')
except ValueError as e:
    print(f"Errore JOB2_CORE: colonna richiesta non in header: {e}")
    exit(1)

STOP_WORDS = set([ # Stessa lista del mapper MapReduce
    "a", "about", "above", "after", "again", "against", "all", "am", "an", "and", "any", "are", "aren't", "as", "at",
    "be", "because", "been", "before", "being", "below", "between", "both", "but", "by", "can't", "cannot", "could",
    "couldn't", "did", "didn't", "do", "does", "doesn't", "doing", "don't", "down", "during", "each", "few", "for",
    "from", "further", "had", "hadn't", "has", "hasn't", "have", "haven't", "having", "he", "he'd", "he'll", "he's",
    "her", "here", "here's", "hers", "herself", "him", "himself", "his", "how", "how's", "i", "i'd", "i'll", "i'm",
    "i've", "if", "in", "into", "is", "isn't", "it", "it's", "its", "itself", "let's", "me", "more", "most", "mustn't",
    "my", "myself", "no", "nor", "not", "of", "off", "on", "once", "only", "or", "other", "ought", "our", "ours",
    "ourselves", "out", "over", "own", "same", "shan't", "she", "she'd", "she'll", "she's", "should", "shouldn't",
    "so", "some", "such", "than", "that", "that's", "the", "their", "theirs", "them", "themselves", "then", "there",
    "there's", "these", "they", "they'd", "they'll", "they're", "they've", "this", "those", "through", "to", "too",
    "under", "until", "up", "very", "was", "wasn't", "we", "we'd", "we'll", "we're", "we've", "were", "weren't",
    "what", "what's", "when", "when's", "where", "where's", "which", "while", "who", "who's", "whom", "why", "why's",
    "with", "won't", "would", "wouldn't", "you", "you'd", "you'll", "you're", "you've", "your", "yours", "yourself",
    "yourselves", "inc", "com", "www"
])


def get_price_range(price_val):
    if price_val < 20000: return "basso"
    elif 20000 <= price_val <= 50000: return "medio"
    else: return "alto"

def clean_and_tokenize(text):
    if not text: return []
    text = str(text).lower()
    text = re.sub(r'[^\w\s]', '', text)
    text = re.sub(r'\s+', ' ', text).strip()
    words = text.split()
    return [word for word in words if word not in STOP_WORDS and len(word) > 2]

def parse_line_job2(line):
    if not line or line.isspace(): return None
    try:
        reader = csv.reader(StringIO(line))
        fields = next(reader)
        
        max_idx = max(IDX_CITY, IDX_YEAR, IDX_PRICE, IDX_DAYSONMARKET, IDX_DESCRIPTION, IDX_DESCRIPTION_CLEANED)
        if len(fields) <= max_idx: return None

        city = fields[IDX_CITY].strip().lower()
        year = int(float(fields[IDX_YEAR].strip()))
        price = float(fields[IDX_PRICE].strip())
        daysonmarket = int(float(fields[IDX_DAYSONMARKET].strip()))
        
        desc_cleaned = fields[IDX_DESCRIPTION_CLEANED].strip()
        desc_original = fields[IDX_DESCRIPTION].strip()
        description = desc_cleaned if desc_cleaned else desc_original

        if not city or price <= 0 or daysonmarket < 0 or not (1900 <= year <= 2025):
            return None
        
        price_range_label = get_price_range(price)
        tokens = clean_and_tokenize(description)
        
        # Chiave: (city, year, price_range_label)
        # Valore: (daysonmarket, lista_di_tokens)
        return ((city, year, price_range_label), (daysonmarket, tokens))
    except:
        return None


def aggregate_price_range_data(records_iterable):
    # records_iterable è una lista di (daysonmarket, tokens_list)
    total_dom = 0
    dom_count = 0
    all_words = []
    num_cars = 0

    for dom, tokens in records_iterable:
        num_cars += 1
        if dom >= 0: # Assicurati che daysonmarket sia valido
            total_dom += dom
            dom_count += 1
        all_words.extend(tokens)
    
    avg_dom_val = (total_dom / dom_count) if dom_count > 0 else 0.0
    word_counts = Counter(all_words)
    top_3_words_list = [word for word, count in word_counts.most_common(3)]
    
    return (num_cars, avg_dom_val, top_3_words_list)


def format_job2_output(city_year_key, price_range_aggregated_data_map):
    city, year = city_year_key
    output_string = f"City: {city}, Year: {year}"
    
    fasce_output = []
    for price_range_label in ["basso", "medio", "alto"]: # Ordine desiderato
        data = price_range_aggregated_data_map.get(price_range_label)
        if data:
            num_auto, avg_dom, top_words = data
            fascia_str = (
                f"FasciaPrezzo: {price_range_label}, "
                f"NumAuto: {num_auto}, "
                f"AvgDaysOnMarket: {avg_dom:.2f}, "
                f"TopWords: {str(top_words)}"
            )
        else: # Fascia non presente per questa city/year
            fascia_str = (
                f"FasciaPrezzo: {price_range_label}, "
                f"NumAuto: 0, "
                f"AvgDaysOnMarket: 0.00, "
                f"TopWords: []"
            )
        fasce_output.append(fascia_str)
            
    output_string += "\tReportFasce: [" + "; ".join(fasce_output) + "]"
    return output_string


def main():
    # Configurazione argomenti da linea di comando
    parser = argparse.ArgumentParser(description="Job 2: Calcolo statistiche per tipo di carrozzeria e colore (Core)")
    parser.add_argument("--input", type=str, default="data/samples/used_cars_1k.csv", 
                        help="Percorso del dataset di input")
    parser.add_argument("--output_dir", type=str, default="results/spark_core", 
                        help="Directory base per i risultati")
    parser.add_argument("--dataset_size", type=float, default=1.0,
                        help="Dimensione del dataset (come frazione, es: 0.01, 0.05, 0.1, ecc.)")
    args = parser.parse_args()
    
    spark = SparkSession.builder.appName("UsedCarsStats_Job2_Core").getOrCreate()
    sc = spark.sparkContext
    
    input_path = args.input
    
    # --- Percorsi di Output ---
    output_dir_base = args.output_dir
    
    # Incorpora la dimensione del dataset nei nomi dei file di output
    size_suffix = f"{args.dataset_size:.2f}".replace('.', '_')
    
    # Per l'output RDD standard (directory con part-files)
    rdd_output_path = os.path.join(output_dir_base, f"job2_rdd_output_parts_{size_suffix}")
    # Per il singolo file aggregato
    single_file_output_path = os.path.join(output_dir_base, f"job2_output_singlefile_{size_suffix}.txt")
    
    # Crea la directory di output base se non esiste
    os.makedirs(output_dir_base, exist_ok=True)
    
    # Rimuovi la directory di output RDD precedente, se esiste
    if os.path.exists(rdd_output_path):
        shutil.rmtree(rdd_output_path)
    # Rimuovi il file singolo precedente, se esiste
    if os.path.exists(single_file_output_path):
        os.remove(single_file_output_path)
    
    lines_rdd = sc.textFile(input_path)
    actual_header_line = lines_rdd.first()
    data_rdd = lines_rdd.filter(lambda line: line != actual_header_line)

    # 1. Parse e mappa a ((city, year, price_range), (daysonmarket, tokens))
    parsed_data = data_rdd.map(parse_line_job2).filter(lambda x: x is not None)

    # 2. Raggruppa per (city, year, price_range) e aggrega i dati per ogni fascia
    # ((city, year, price_range), (num_cars, avg_dom, top_3_words))
    range_aggregated_rdd = parsed_data.groupByKey().mapValues(aggregate_price_range_data)

    # 3. Riorganizza per (city, year) come chiave per il raggruppamento finale
    # ((city, year), (price_range, num_cars, avg_dom, top_3_words))
    city_year_keyed_rdd = range_aggregated_rdd.map(
        lambda x: ((x[0][0], x[0][1]),  # Nuova chiave: (city, year)
                   (x[0][2], x[1][0], x[1][1], x[1][2])) # Valore: (price_range, num_cars, avg_dom, top_words)
    )

    # 4. Raggruppa per (city, year)
    # ((city, year), iterable_of_price_range_details)
    final_grouped_rdd = city_year_keyed_rdd.groupByKey()

    # 5. Formatta l'output finale
    # Trasforma l'iterable in una mappa price_range -> dati per facilitare la formattazione
    def map_price_ranges(iterable):
        price_map = {}
        for prange, num, avg_d, t_words in iterable:
            price_map[prange] = (num, avg_d, t_words)
        return price_map

    formatted_output_rdd = final_grouped_rdd.map(
        lambda x: format_job2_output(x[0], map_price_ranges(x[1]))
    )
    
    # Ordina per città e anno
    sorted_output_rdd = formatted_output_rdd.sortBy(lambda line: (line.split(", Year: ")[0].split("City: ")[1], int(line.split(", Year: ")[1].split("\t")[0])))


    # --- Salvataggio e Stampa ---
    sorted_output_rdd.coalesce(1).saveAsTextFile(rdd_output_path)
    print(f"Output RDD Job2 (con part-files) salvato in: {rdd_output_path}")

    results_list = sorted_output_rdd.collect()
    with open(single_file_output_path, 'w') as f:
        for line in results_list:
            f.write(line + '\n')
    print(f"Output Job2 aggregato in singolo file salvato in: {single_file_output_path}")

    print("\n--- JOB 2 SPARK CORE RESULTS (FIRST 10) ---")
    for record_str in results_list[:10]:
        print(record_str)

    spark.stop()

if __name__ == "__main__":
    main()