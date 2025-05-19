#!/usr/bin/env python3
# src/mapreduce/job1/reducer.py

import sys
from collections import defaultdict

# Struttura per accumulare i dati:
# {
#   "ford": {
#     "focus": {"prices": [10k, 12k], "years": {2010, 2011}, "count": 2},
#     "mustang": {"prices": [25k], "years": {2018}, "count": 1}
#   },
#   "toyota": { ... }
# }
# Usiamo una struttura più diretta durante l'iterazione e poi la convertiamo.

current_make_model_key = None
current_prices = []
current_years = set()

# Dizionario finale per raggruppare i modelli per marca
# make_statistics = { "make_name": [ (model_name, count, min_p, max_p, avg_p, year_list_str), ... ], ... }
make_statistics_final = defaultdict(list)

for line in sys.stdin:
    line = line.strip()
    try:
        make_name, model_name, price_str, year_str = line.split('\t')
        price = float(price_str)
        year = int(year_str)
    except ValueError:
        # sys.stderr.write(f"WARN: Reducer skipping malformed line: {line}\n")
        continue # Ignora righe malformate

    # La chiave di raggruppamento logica è (make_name, model_name)
    make_model_key = (make_name, model_name)

    # Se la chiave (make_name, model_name) cambia, processiamo il gruppo precedente
    if current_make_model_key and current_make_model_key != make_model_key:
        if current_prices: # Assicurati che ci siano dati da processare
            # Calcola statistiche per il modello (current_make_model_key)
            mk, mdl = current_make_model_key
            count = len(current_prices)
            min_price = min(current_prices)
            max_price = max(current_prices)
            avg_price = sum(current_prices) / count if count > 0 else 0
            # Anni unici e ordinati
            years_list = sorted(list(current_years)) 
            
            # Formatta la stringa per i dettagli del modello
            # (i) num auto, (ii) prezzo min, max, medio, (iv) elenco anni
            model_details_tuple = (
                mdl,
                count,
                f"{min_price:.2f}", # Formatta a 2 decimali
                f"{max_price:.2f}",
                f"{avg_price:.2f}",
                years_list # Manteniamo la lista, la convertiremo in stringa al momento della stampa finale
            )
            make_statistics_final[mk].append(model_details_tuple)

        # Resetta per il nuovo gruppo (make_name, model_name)
        current_prices = []
        current_years = set()

    # Aggiorna current_make_model_key e accumula i dati per il gruppo corrente
    current_make_model_key = make_model_key
    current_prices.append(price)
    current_years.add(year)

# Non dimenticare di processare l'ultimo gruppo dopo che il ciclo è finito
if current_make_model_key and current_prices:
    mk, mdl = current_make_model_key
    count = len(current_prices)
    min_price = min(current_prices)
    max_price = max(current_prices)
    avg_price = sum(current_prices) / count if count > 0 else 0
    years_list = sorted(list(current_years))

    model_details_tuple = (
        mdl,
        count,
        f"{min_price:.2f}",
        f"{max_price:.2f}",
        f"{avg_price:.2f}",
        years_list
    )
    make_statistics_final[mk].append(model_details_tuple)

# Ora stampa l'output finale raggruppato per marca
# Ordina le marche per nome per un output consistente (opzionale)
sorted_makes = sorted(make_statistics_final.keys())

for make_name_key in sorted_makes:
    models_data_list = make_statistics_final[make_name_key]
    
    # Ordina i modelli per nome all'interno di ogni marca (opzionale, per output consistente)
    # models_data_list.sort(key=lambda x: x[0]) # x[0] è model_name

    models_output_strings = []
    for model_tuple in models_data_list:
        # model_tuple = (model_name, count, min_p_str, max_p_str, avg_p_str, years_list_obj)
        model_str = (
            f"Model: {model_tuple[0]}, "
            f"Count: {model_tuple[1]}, "
            f"MinPrice: {model_tuple[2]}, "
            f"MaxPrice: {model_tuple[3]}, "
            f"AvgPrice: {model_tuple[4]}, "
            f"Years: {str(model_tuple[5])}" # Converte la lista di anni in stringa
        )
        models_output_strings.append(model_str)
    
    # Output finale per marca: (a) nome marca, (b) lista di modelli con stats
    # Il formato richiesto è una "lista", qui usiamo un separatore ';' per i modelli.
    # Adatta il formato se una struttura JSON o simile è preferibile/permessa.
    print(f"Make: {make_name_key}\tModels: [{'; '.join(models_output_strings)}]")