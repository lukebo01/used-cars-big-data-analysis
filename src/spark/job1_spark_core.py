#!/usr/bin/env python3
# src/spark/job1_spark_core.py

from pyspark.sql import SparkSession
import csv
from io import StringIO
import os
import shutil # Per rimuovere la directory temporanea se necessario
import argparse # Per la gestione degli argomenti da riga di comando

# --- HEADER_FIELDS_STRING e indici come prima ---
HEADER_FIELDS_STRING = "vin,back_legroom,body_type,city,city_fuel_economy,daysonmarket,dealer_zip,description,engine_cylinders,engine_displacement,engine_type,exterior_color,fleet,frame_damaged,franchise_dealer,franchise_make,front_legroom,fuel_tank_volume,fuel_type,has_accidents,height,highway_fuel_economy,horsepower,interior_color,isCab,is_new,latitude,length,listed_date,listing_color,listing_id,longitude,main_picture_url,major_options,make_name,maximum_seating,mileage,model_name,owner_count,power,price,salvage,savings_amount,seller_rating,sp_id,sp_name,theft_title,torque,transmission,transmission_display,trimId,trim_name,wheel_system,wheel_system_display,wheelbase,width,year,major_options_list,num_major_options,horsepower_extracted,horsepower_rpm,torque_value,torque_rpm,seats,cylinders,listed_year,listed_month,listed_day,listed_dayofweek,description_cleaned,back_legroom_missing,city_fuel_economy_missing,engine_displacement_missing,front_legroom_missing,fuel_tank_volume_missing,height_missing,highway_fuel_economy_missing,horsepower_missing,length_missing,mileage_missing,owner_count_missing,seller_rating_missing,sp_id_missing,wheelbase_missing,width_missing,horsepower_extracted_missing,horsepower_rpm_missing,torque_value_missing,torque_rpm_missing,seats_missing,cylinders_missing,body_type_missing,description_missing,engine_cylinders_missing,engine_type_missing,exterior_color_missing,franchise_make_missing,fuel_type_missing,interior_color_missing,main_picture_url_missing,major_options_missing,maximum_seating_missing,power_missing,torque_missing,transmission_missing,transmission_display_missing,trimId_missing,trim_name_missing,wheel_system_missing,wheel_system_display_missing,description_cleaned_missing,unified_color"
HEADER_FIELDS = HEADER_FIELDS_STRING.split(',')
try:
    IDX_MAKE_NAME = HEADER_FIELDS.index('make_name')
    IDX_MODEL_NAME = HEADER_FIELDS.index('model_name')
    IDX_PRICE = HEADER_FIELDS.index('price')
    IDX_YEAR = HEADER_FIELDS.index('year')
except ValueError as e:
    print(f"Errore critico: colonna richiesta non trovata nell'header: {e}")
    exit(1)

def parse_csv_line(line):
    if not line or line.isspace():
        return None
    try:
        reader = csv.reader(StringIO(line)) 
        fields = next(reader)
        max_idx_needed = max(IDX_MAKE_NAME, IDX_MODEL_NAME, IDX_PRICE, IDX_YEAR)
        if len(fields) <= max_idx_needed:
            return None 
        make_name = fields[IDX_MAKE_NAME].strip().lower()
        model_name = fields[IDX_MODEL_NAME].strip().lower()
        price_str = fields[IDX_PRICE].strip()
        year_str = fields[IDX_YEAR].strip()
        if not make_name or not model_name or not price_str or not year_str:
            return None
        price = float(price_str)
        year = int(float(year_str)) 
        if price <= 0 or not (1900 <= year <= 2025):
            return None
        return (make_name, model_name, price, year)
    except (ValueError, IndexError, csv.Error):
        return None

def calculate_model_stats(data_iterable):
    prices = []
    years = set()
    for price, year in data_iterable:
        prices.append(price)
        years.add(year)
    if not prices:
        return (0, 0.0, 0.0, 0.0, [])
    count = len(prices)
    min_price = min(prices)
    max_price = max(prices)
    avg_price = sum(prices) / count
    return (count, min_price, max_price, avg_price, sorted(list(years)))

def format_output_line(make_name, models_stats_list): # Rinominata per chiarezza
    models_output_strings = []
    # Ordina i modelli per nome per un output consistente
    sorted_models_stats_list = sorted(models_stats_list, key=lambda x: x[0]) 

    for model_name, count, min_p, max_p, avg_p, years in sorted_models_stats_list:
        model_str = (
            f"Model: {model_name}, "
            f"Count: {count}, "
            f"MinPrice: {min_p:.2f}, "
            f"MaxPrice: {max_p:.2f}, "
            f"AvgPrice: {avg_p:.2f}, "
            f"Years: {str(years)}"
        )
        models_output_strings.append(model_str)
    
    return f"Make: {make_name}\tModels: [{'; '.join(models_output_strings)}]"

def main():
    # Configurazione argomenti da linea di comando
    parser = argparse.ArgumentParser(description="Job 1: Calcolo statistiche di prezzo auto usate per marca e modello")
    parser.add_argument("--input", type=str, default="data/samples/used_cars_1k.csv", 
                        help="Percorso del dataset di input")
    parser.add_argument("--output_dir", type=str, default="results/spark_core", 
                        help="Directory base per i risultati")
    parser.add_argument("--dataset_size", type=float, default=1.0,
                        help="Dimensione del dataset (come frazione, es: 0.01, 0.05, 0.1, ecc.)")
    args = parser.parse_args()
    
    spark = SparkSession.builder.appName("UsedCarsStats_Job1_Core").getOrCreate()
    sc = spark.sparkContext

    input_path = args.input
    
    # --- Percorsi di Output ---
    output_dir_base = args.output_dir
    
    # Incorpora la dimensione del dataset nei nomi dei file di output
    size_suffix = f"{args.dataset_size:.2f}".replace('.', '_')
    
    # Per l'output RDD standard (directory con part-files)
    rdd_output_path = os.path.join(output_dir_base, f"job1_rdd_output_parts_{size_suffix}") 
    # Per il singolo file aggregato
    single_file_output_path = os.path.join(output_dir_base, f"job1_output_singlefile_{size_suffix}.txt")
    
    # Crea la directory di output base se non esiste
    os.makedirs(output_dir_base, exist_ok=True)
    
    # Rimuovi la directory di output RDD precedente, se esiste, per evitare errori
    if os.path.exists(rdd_output_path):
        shutil.rmtree(rdd_output_path)
    # Rimuovi il file singolo precedente, se esiste
    if os.path.exists(single_file_output_path):
        os.remove(single_file_output_path)

    # --- Inizio Logica Spark ---
    lines_rdd = sc.textFile(input_path)
    actual_header_line = lines_rdd.first()
    data_rdd = lines_rdd.filter(lambda line: line != actual_header_line)
    
    parsed_rdd = data_rdd.map(parse_csv_line).filter(lambda x: x is not None)
    
    make_model_keyed_rdd = parsed_rdd.map(
        lambda x: ((x[0], x[1]), (x[2], x[3]))
    )
    
    model_stats_rdd = make_model_keyed_rdd.groupByKey().mapValues(calculate_model_stats)
    
    valid_model_stats_rdd = model_stats_rdd.filter(lambda x: x[1][0] > 0) # Filtra modelli senza auto valide
    
    make_keyed_rdd = valid_model_stats_rdd.map(
        lambda x: (x[0][0], (x[0][1], x[1][0], x[1][1], x[1][2], x[1][3], x[1][4]))
    )
    
    final_grouped_rdd = make_keyed_rdd.groupByKey()
    
    # RDD con le stringhe di output formattate
    formatted_output_rdd = final_grouped_rdd.map(lambda x: format_output_line(x[0], list(x[1])))
    
    # Ordina l'RDD per marca per un output consistente
    sorted_output_rdd = formatted_output_rdd.sortBy(lambda line: line.split('\t')[0])

    # 1. Salva l'output RDD standard (directory con part-files)
    #    coalesce(1) forza l'output in un singolo file part-00000 dentro la directory
    sorted_output_rdd.coalesce(1).saveAsTextFile(rdd_output_path)
    print(f"Output RDD (con part-files) salvato in: {rdd_output_path}")

    # 2. Raccogli i risultati sul driver e scrivili in un singolo file
    #    ATTENZIONE: .collect() porta tutti i dati sul nodo driver. 
    #    Usare con cautela per dataset molto grandi. Per un file di 1k righe è ok.
    results_list = sorted_output_rdd.collect()
    
    with open(single_file_output_path, 'w') as f:
        for line in results_list:
            f.write(line + '\n')
    print(f"Output aggregato in singolo file salvato in: {single_file_output_path}")

    # Stampa le prime 10 righe del risultato (come prima)
    print("\n--- JOB 1 SPARK CORE RESULTS (FIRST 10 FROM COLLECTED DATA) ---")
    for record_str in results_list[:10]:
        print(record_str)

    spark.stop()

if __name__ == "__main__":
    main()