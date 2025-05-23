#!/usr/bin/env python3
# src/mapreduce/job1/reducer.py

import sys
from collections import defaultdict

# Struttura per accumulare i dati per il corrente (make_name, model_name)
# Esempio: current_model_data = {"prices": [15000.0, 16000.0], "years": {2018, 2019}}
current_processing_key = None # Sarà una tupla (make_name, model_name)
current_model_data = None

# Dizionario finale per raggruppare i modelli per marca e poi aggregare
# make_statistics_accumulator = {
# "make_name": [
# (model_name, count, min_price, max_price, avg_price, sorted_years_list),
# ...
# ], ...
# }
make_statistics_accumulator = defaultdict(list)

for line in sys.stdin:
    line = line.strip()
    try:
        # L'input dal mapper è: make_name\tmodel_name\tprice\tyear
        make_name, model_name, price_str, year_str = line.split('\t')
        price = float(price_str)
        year = int(year_str) # Il mapper dovrebbe già aver convertito l'anno a int
    except ValueError:
        # sys.stderr.write(f"WARN: Reducer skipping malformed line: {line}\n")
        continue # Ignora righe malformate

    # La chiave di raggruppamento che il reducer si aspetta (grazie a sort)
    # è (make_name, model_name)
    incoming_key = (make_name, model_name)

    # Se la chiave (make_name, model_name) cambia (o è la prima riga),
    # processiamo il gruppo precedente.
    if current_processing_key and current_processing_key != incoming_key:
        if current_model_data and current_model_data["prices"]: # Assicurati che ci siano dati da processare
            # Calcola statistiche per il modello (current_processing_key)
            mk, mdl = current_processing_key # Estrai make e model dalla chiave
            
            prices_list = current_model_data["prices"]
            years_set = current_model_data["years"]
            
            count = len(prices_list)
            min_price = min(prices_list)
            max_price = max(prices_list)
            avg_price = sum(prices_list) / count if count > 0 else 0.0 # Evita divisione per zero
            years_list_sorted = sorted(list(years_set))
            
            # Prepara la tupla con i dettagli del modello
            model_details_tuple = (
                mdl, # model_name
                count,
                f"{min_price:.2f}", # Formatta a 2 decimali come stringa
                f"{max_price:.2f}",
                f"{avg_price:.2f}",
                years_list_sorted # Manteniamo la lista, la convertiremo in stringa al momento della stampa finale
            )
            # Aggiungi alla lista dei modelli per la marca corrente
            make_statistics_accumulator[mk].append(model_details_tuple)

        # Resetta per il nuovo gruppo (make_name, model_name)
        current_model_data = {"prices": [], "years": set()}

    # Se current_model_data non è inizializzato (prima riga o dopo un reset)
    if not current_model_data:
        current_model_data = {"prices": [], "years": set()}

    # Aggiorna current_processing_key e accumula i dati per il gruppo corrente
    current_processing_key = incoming_key
    current_model_data["prices"].append(price)
    current_model_data["years"].add(year)

# Non dimenticare di processare l'ultimo gruppo dopo che il ciclo è finito
if current_processing_key and current_model_data and current_model_data["prices"]:
    mk, mdl = current_processing_key
    
    prices_list = current_model_data["prices"]
    years_set = current_model_data["years"]

    count = len(prices_list)
    min_price = min(prices_list)
    max_price = max(prices_list)
    avg_price = sum(prices_list) / count if count > 0 else 0.0
    years_list_sorted = sorted(list(years_set))

    model_details_tuple = (
        mdl,
        count,
        f"{min_price:.2f}",
        f"{max_price:.2f}",
        f"{avg_price:.2f}",
        years_list_sorted
    )
    make_statistics_accumulator[mk].append(model_details_tuple)

# Ora stampa l'output finale raggruppato per marca
# Ordina le marche per nome per un output consistente (opzionale ma raccomandato)
sorted_makes_names = sorted(make_statistics_accumulator.keys())

for make_name_output_key in sorted_makes_names:
    # Lista delle tuple (model_name, count, min_p_str, max_p_str, avg_p_str, years_list_obj)
    models_data_list_for_make = make_statistics_accumulator[make_name_output_key]
    
    # Ordina i modelli per nome all'interno di ogni marca (opzionale, per output consistente)
    models_data_list_for_make.sort(key=lambda x: x[0]) # x[0] è model_name

    models_output_strings_list = []
    for model_tuple in models_data_list_for_make:
        # model_tuple è (model_name, count, min_p_str, max_p_str, avg_p_str, years_list_obj)
        model_str_representation = (
            f"Model: {model_tuple[0]}, " # model_name
            f"Count: {model_tuple[1]}, " # count
            f"MinPrice: {model_tuple[2]}, " # min_price_str
            f"MaxPrice: {model_tuple[3]}, " # max_price_str
            f"AvgPrice: {model_tuple[4]}, " # avg_price_str
            f"Years: {str(model_tuple[5])}" # Converte la lista di anni in stringa es. "[2010, 2011]"
        )
        models_output_strings_list.append(model_str_representation)
    
    # Output finale per marca: (a) nome marca, (b) lista di modelli con stats
    # Il formato richiesto è una "lista", qui usiamo un separatore ';' per i modelli.
    print(f"Make: {make_name_output_key}\tModels: [{'; '.join(models_output_strings_list)}]")