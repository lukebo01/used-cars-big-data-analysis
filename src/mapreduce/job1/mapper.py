#!/usr/bin/env python3
# src/mapreduce/job1/mapper.py

import sys
import csv

# --- INCOLLA QUI L'HEADER ESATTO DEL TUO CSV PREPROCESSATO ---
HEADER_PREPROCESSED_STRING = "vin,back_legroom,body_type,city,city_fuel_economy,daysonmarket,dealer_zip,description,engine_cylinders,engine_displacement,engine_type,exterior_color,fleet,frame_damaged,franchise_dealer,franchise_make,front_legroom,fuel_tank_volume,fuel_type,has_accidents,height,highway_fuel_economy,horsepower,interior_color,isCab,is_new,latitude,length,listed_date,listing_color,listing_id,longitude,main_picture_url,major_options,make_name,maximum_seating,mileage,model_name,owner_count,power,price,salvage,savings_amount,seller_rating,sp_id,sp_name,theft_title,torque,transmission,transmission_display,trimId,trim_name,wheel_system,wheel_system_display,wheelbase,width,year,major_options_list,num_major_options,horsepower_extracted,horsepower_rpm,torque_value,torque_rpm,seats,cylinders,listed_year,listed_month,listed_day,listed_dayofweek,description_cleaned,back_legroom_missing,city_fuel_economy_missing,engine_displacement_missing,front_legroom_missing,fuel_tank_volume_missing,height_missing,highway_fuel_economy_missing,horsepower_missing,length_missing,mileage_missing,owner_count_missing,seller_rating_missing,sp_id_missing,wheelbase_missing,width_missing,horsepower_extracted_missing,horsepower_rpm_missing,torque_value_missing,torque_rpm_missing,seats_missing,cylinders_missing,body_type_missing,description_missing,engine_cylinders_missing,engine_type_missing,exterior_color_missing,franchise_make_missing,fuel_type_missing,interior_color_missing,main_picture_url_missing,major_options_missing,maximum_seating_missing,power_missing,torque_missing,transmission_missing,transmission_display_missing,trimId_missing,trim_name_missing,wheel_system_missing,wheel_system_display_missing,description_cleaned_missing,unified_color"
# -----------------------------------------------------------------

HEADER_PREPROCESSED = HEADER_PREPROCESSED_STRING.split(',')

try:
    COL_IDX = {
        'make_name': HEADER_PREPROCESSED.index('make_name'),
        'model_name': HEADER_PREPROCESSED.index('model_name'),
        'price': HEADER_PREPROCESSED.index('price'),
        'year': HEADER_PREPROCESSED.index('year')
    }
except ValueError as e:
    sys.stderr.write(f"FATAL: Critical column not found in expected header: {e}\n")
    sys.exit(1)


is_first_line = True

def get_value_from_row(row_values, col_name):
    try:
        idx = COL_IDX[col_name]
        if idx < len(row_values):
            return row_values[idx].strip()
        else:
            return None
    except Exception:
        return None

for line in sys.stdin:
    line_content_for_header_check = line.strip()
    if is_first_line:
        # Leggi la prima riga come CSV per gestire correttamente eventuali delimitatori/escape
        try:
            # csv.reader si aspetta un iterabile, quindi passiamo la linea dentro una lista
            processed_header_fields = next(csv.reader([line_content_for_header_check]))
            # Rimuovi eventuali spazi bianchi extra da ogni campo dell'header letto
            processed_header_fields = [h.strip() for h in processed_header_fields]
        except csv.Error:
            # Se la prima riga non è parsabile come CSV, probabilmente non è un header valido
            processed_header_fields = []

        if processed_header_fields == HEADER_PREPROCESSED:
            is_first_line = False
            continue
        else:
            # Se non è l'header esatto, potrebbe essere un file senza header o un header diverso.
            # In questo caso, consideriamo la riga come dato.
            # Potresti voler aggiungere un warning se l'header non corrisponde ma non è vuoto
            # sys.stderr.write(f"WARN: First line does not match expected header. Processing as data.\n")
            # sys.stderr.write(f"Expected: {HEADER_PREPROCESSED[:5]}...\n")
            # sys.stderr.write(f"Actual:   {processed_header_fields[:5]}...\n")
            is_first_line = False
            # Non fare 'continue', così la riga viene processata qui sotto
    
    line_content = line.strip() # Rileggi o usa line_content_for_header_check
    if not line_content:
        continue

    try:
        row_values = next(csv.reader([line_content]))
    except csv.Error:
        # sys.stderr.write(f"WARN: Mapper skipping malformed CSV line: {line_content[:100]}\n")
        continue
    
    if len(row_values) < max(COL_IDX.values()) + 1 :
        # sys.stderr.write(f"WARN: Mapper skipping short line (cols: {len(row_values)}): {line_content[:100]}\n")
        continue

    make_name_raw = get_value_from_row(row_values, 'make_name')
    model_name_raw = get_value_from_row(row_values, 'model_name')
    price_raw = get_value_from_row(row_values, 'price')
    year_raw = get_value_from_row(row_values, 'year')

    if not make_name_raw or not model_name_raw or not price_raw or not year_raw:
        # sys.stderr.write(f"WARN: Mapper skipping line due to missing essential field(s): {line_content[:100]}\n")
        continue
    
    make_name = make_name_raw.strip().lower()
    model_name = model_name_raw.strip().lower()

    if not make_name or not model_name:
        # sys.stderr.write(f"WARN: Mapper skipping line due to empty make or model after cleaning: {line_content[:100]}\n")
        continue

    try:
        price = float(price_raw)
        year_val_cleaned = year_raw.split('.')[0]
        year = int(year_val_cleaned)
    except ValueError:
        # sys.stderr.write(f"WARN: Mapper skipping line, non-numeric price/year: '{price_raw}', '{year_raw}' in {line_content[:100]}\n")
        continue

    if price <= 0 or not (1900 <= year <= 2025):
        # sys.stderr.write(f"WARN: Mapper skipping line due to invalid price/year: {price}/{year} in {line_content[:100]}\n")
        continue
        
    print(f"{make_name}\t{model_name}\t{price}\t{year}")