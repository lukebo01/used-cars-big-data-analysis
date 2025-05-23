#!/usr/bin/env python3
# src/mapreduce/job2/mapper.py

import sys
import csv
import re

# --- HEADER E INDICI DAL TUO CSV PREPROCESSATO ---
HEADER_PREPROCESSED_STRING = "vin,back_legroom,body_type,city,city_fuel_economy,daysonmarket,dealer_zip,description,engine_cylinders,engine_displacement,engine_type,exterior_color,fleet,frame_damaged,franchise_dealer,franchise_make,front_legroom,fuel_tank_volume,fuel_type,has_accidents,height,highway_fuel_economy,horsepower,interior_color,isCab,is_new,latitude,length,listed_date,listing_color,listing_id,longitude,main_picture_url,major_options,make_name,maximum_seating,mileage,model_name,owner_count,power,price,salvage,savings_amount,seller_rating,sp_id,sp_name,theft_title,torque,transmission,transmission_display,trimId,trim_name,wheel_system,wheel_system_display,wheelbase,width,year,major_options_list,num_major_options,horsepower_extracted,horsepower_rpm,torque_value,torque_rpm,seats,cylinders,listed_year,listed_month,listed_day,listed_dayofweek,description_cleaned,back_legroom_missing,city_fuel_economy_missing,engine_displacement_missing,front_legroom_missing,fuel_tank_volume_missing,height_missing,highway_fuel_economy_missing,horsepower_missing,length_missing,mileage_missing,owner_count_missing,seller_rating_missing,sp_id_missing,wheelbase_missing,width_missing,horsepower_extracted_missing,horsepower_rpm_missing,torque_value_missing,torque_rpm_missing,seats_missing,cylinders_missing,body_type_missing,description_missing,engine_cylinders_missing,engine_type_missing,exterior_color_missing,franchise_make_missing,fuel_type_missing,interior_color_missing,main_picture_url_missing,major_options_missing,maximum_seating_missing,power_missing,torque_missing,transmission_missing,transmission_display_missing,trimId_missing,trim_name_missing,wheel_system_missing,wheel_system_display_missing,description_cleaned_missing,unified_color"
HEADER_PREPROCESSED = HEADER_PREPROCESSED_STRING.split(',')

try:
    COL_IDX = {
        'city': HEADER_PREPROCESSED.index('city'),
        'year': HEADER_PREPROCESSED.index('year'), # Anno del veicolo
        'price': HEADER_PREPROCESSED.index('price'),
        'daysonmarket': HEADER_PREPROCESSED.index('daysonmarket'),
        'description': HEADER_PREPROCESSED.index('description'),
        'description_cleaned': HEADER_PREPROCESSED.index('description_cleaned')
    }
except ValueError as e:
    sys.stderr.write(f"FATAL_MAPPER_JOB2: Critical column not found in header: {e}\n")
    sys.exit(1)

# Semplice lista di stop words
STOP_WORDS = set([
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
    "yourselves", "jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec",
    "inc", "com", "www", "vehicle", "car", "truck", "suv", "van" # Aggiunte alcune parole generiche
])

def get_price_range(price_val):
    if price_val < 20000:
        return "basso"
    elif 20000 <= price_val <= 50000:
        return "medio"
    else: # > 50000
        return "alto"

def clean_and_tokenize(text_input):
    # Controlla se text_input è None o una stringa vuota/solo spazi
    if text_input is None or not str(text_input).strip():
        return []
    
    text_as_string = str(text_input).lower() # Assicura che sia una stringa e in minuscolo
    text_no_punct = re.sub(r'[^\w\s]', '', text_as_string) # Rimuove punteggiatura
    text_single_space = re.sub(r'\s+', ' ', text_no_punct).strip() # Sostituisce spazi multipli con singolo spazio
    
    if not text_single_space: # Se dopo la pulizia la stringa è vuota
        return []
        
    words = text_single_space.split()
    # Filtra parole molto corte e stop words
    return [word for word in words if word not in STOP_WORDS and len(word) > 2]

is_first_line = True

for line in sys.stdin:
    line_content_for_header_check = line.strip()
    if is_first_line:
        try:
            processed_header_fields = [h.strip() for h in next(csv.reader([line_content_for_header_check]))]
            if processed_header_fields == HEADER_PREPROCESSED:
                is_first_line = False
                continue
            else:
                is_first_line = False 
        except csv.Error:
            is_first_line = False # Non è un CSV valido, assumi sia dato

    line_content = line_content_for_header_check # Usa la riga già letta e stripata
    if not line_content:
        continue

    try:
        row_values = next(csv.reader([line_content]))
    except csv.Error:
        # sys.stderr.write(f"WARN_MAPPER_JOB2: Skipping malformed CSV line: {line_content[:100]}\n")
        continue

    # Verifica che la riga abbia abbastanza colonne per gli indici massimi richiesti
    max_idx_needed = 0
    try:
        max_idx_needed = max(COL_IDX.values())
        if len(row_values) <= max_idx_needed:
            # sys.stderr.write(f"WARN_MAPPER_JOB2: Line too short for required columns. Len: {len(row_values)}, MaxIdx: {max_idx_needed}. Line: {line_content[:100]}\n")
            continue
    except Exception: # Se COL_IDX è vuoto, o altri problemi
        continue


    try:
        city_raw = row_values[COL_IDX['city']].strip()
        year_raw = row_values[COL_IDX['year']].strip() 
        price_raw = row_values[COL_IDX['price']].strip()
        daysonmarket_raw = row_values[COL_IDX['daysonmarket']].strip()
        
        # Gestione delle descrizioni
        desc_cleaned_raw = ""
        if COL_IDX['description_cleaned'] < len(row_values): # Verifica indice
            desc_cleaned_raw = row_values[COL_IDX['description_cleaned']].strip()
        
        desc_raw = ""
        if COL_IDX['description'] < len(row_values): # Verifica indice
            desc_raw = row_values[COL_IDX['description']].strip()
        
        description_text = desc_cleaned_raw if desc_cleaned_raw else desc_raw

        if not city_raw or not year_raw or not price_raw or not daysonmarket_raw:
            # sys.stderr.write(f"WARN_MAPPER_JOB2: Missing essential fields (city, year, price, daysonmarket) in {line_content[:100]}\n")
            continue

        city = city_raw.lower()
        # Ulteriore controllo per città vuota dopo la conversione a minuscolo
        if not city:
            # sys.stderr.write(f"WARN_MAPPER_JOB2: Empty city after lowercasing. Line: {line_content[:100]}\n")
            continue
            
        year_cleaned = year_raw.split('.')[0] # Gestisce "2010.0"
        year = int(year_cleaned)
        price = float(price_raw)
        daysonmarket_cleaned = daysonmarket_raw.split('.')[0] # Se per caso fosse float
        daysonmarket = int(daysonmarket_cleaned)

        if price <= 0 or daysonmarket < 0 or not (1900 <= year <= 2025): # Range anno aggiornato
            # sys.stderr.write(f"WARN_MAPPER_JOB2: Invalid numeric value (price, daysonmarket, year) in {line_content[:100]}\n")
            continue

    except (ValueError, IndexError) as e:
        # sys.stderr.write(f"WARN_MAPPER_JOB2: Error parsing line's core fields: {e} in {line_content[:100]}\n")
        continue

    price_range = get_price_range(price)
    
    tokenized_words = clean_and_tokenize(description_text) 
    words_str = ",".join(tokenized_words) if tokenized_words else "NO_WORDS" # NO_WORDS se non ci sono token validi

    # Output: city, year, price_range, 1 (conteggio auto), daysonmarket, stringa_parole_separate_da_virgola
    print(f"{city}\t{year}\t{price_range}\t1\t{daysonmarket}\t{words_str}")