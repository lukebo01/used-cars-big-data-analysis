#!/usr/bin/env python3
# src/mapreduce/job1/mapper.py

import sys
import csv

# --- INDICI CORRETTI BASATI SULL'HEADER FORNITO ---
COL_IDX = {
    'make_name': 42,
    'model_name': 45,
    'price': 48,
    'year': 65
}
# ----------------------------------------------------

is_first_line = True 

def get_value_from_row(row_values, col_name):
    """Estrae e pulisce un valore dalla lista di valori della riga."""
    try:
        # Verifica che l'indice esista nella riga prima di accedervi
        if COL_IDX[col_name] < len(row_values):
            return row_values[COL_IDX[col_name]].strip()
        else:
            # sys.stderr.write(f"WARN: Column index {COL_IDX[col_name]} for '{col_name}' is out of bounds for row with {len(row_values)} columns.\n")
            return None # Indice fuori range per questa riga specifica
    except KeyError:
        # sys.stderr.write(f"WARN: Column name '{col_name}' not found in COL_IDX.\n") # Questo non dovrebbe accadere se COL_IDX è corretto
        return None
    except Exception as e: # Cattura altre eccezioni impreviste durante l'accesso
        # sys.stderr.write(f"WARN: Unexpected error getting value for '{col_name}': {e}\n")
        return None


for line in sys.stdin:
    if is_first_line:
        is_first_line = False
        continue

    line = line.strip()
    if not line:
        continue

    try:
        row_values = next(csv.reader([line]))
    except csv.Error:
        # sys.stderr.write(f"WARN: Mapper skipping malformed CSV line (csv.Error): {line}\n")
        continue

    # Controlla che la riga abbia abbastanza colonne
    # Il numero minimo di colonne necessarie è max(COL_IDX.values()) + 1
    # Per esempio, se l'indice più alto è 65, la riga deve avere almeno 66 colonne.
    # Questa è una verifica approssimativa, dato che csv.reader gestisce righe "corte"
    # riempiendo con stringhe vuote se la riga ha meno campi dell'header, ma
    # se una colonna chiave non esiste, get_value_from_row restituirà None.
    # Una verifica più robusta potrebbe essere len(row_values) == NUMERO_TOTALE_COLONNE_ATTESE
    # ma per ora ci affidiamo alla gestione di get_value_from_row.


    make_name_raw = get_value_from_row(row_values, 'make_name')
    model_name_raw = get_value_from_row(row_values, 'model_name')
    price_raw = get_value_from_row(row_values, 'price')
    year_raw = get_value_from_row(row_values, 'year')

    if not all([make_name_raw, model_name_raw, price_raw, year_raw]):
        # sys.stderr.write(f"WARN: Mapper skipping line due to missing essential field(s) for Job1: {line}\n")
        continue

    make_name = make_name_raw.lower()
    model_name = model_name_raw.lower()

    try:
        price = float(price_raw)
        year = int(float(year_raw)) # Gestisce "2010.0"
    except ValueError:
        # sys.stderr.write(f"WARN: Mapper skipping line due to invalid numeric format (price/year): {line}\n")
        continue

    if price <= 0:
        # sys.stderr.write(f"WARN: Mapper skipping line due to invalid price: {price} in {line}\n")
        continue
    if not (1900 <= year <= 2025): # Anno corrente + 1 o simile
        # sys.stderr.write(f"WARN: Mapper skipping line due to invalid year: {year} in {line}\n")
        continue
    if not make_name or not model_name: # Ricontrolla dopo lower()
        # sys.stderr.write(f"WARN: Mapper skipping line due to empty make_name or model_name after processing: {line}\n")
        continue
        
    print(f"{make_name}\t{model_name}\t{price}\t{year}")