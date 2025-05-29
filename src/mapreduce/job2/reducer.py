#!/usr/bin/env python3
# src/mapreduce/job2/reducer.py

import sys
import time
from collections import defaultdict, Counter

# Start timing for benchmarking
start_time = time.time()
line_count = 0

# Struttura dati per accumulare i risultati per ogni (città, anno)
report_data = defaultdict(lambda: defaultdict(lambda: {
    "count": 0, 
    "daysonmarket_sum": 0, 
    "daysonmarket_records": 0,  # Numero di record con un valore daysonmarket valido per la media
    "word_counts": Counter()
}))

# Questa è la soglia per differenziare le fasce di prezzo
PRICE_THRESHOLDS = {
    "basso": 15000,  # < $15,000
    "medio": 35000,  # $15,000 - $35,000
    "alto": float('inf')  # > $35,000
}

# Funzione per determinare la fascia di prezzo
def get_price_category(price):
    if price < PRICE_THRESHOLDS["basso"]:
        return "basso"
    elif price < PRICE_THRESHOLDS["medio"]:
        return "medio"
    else:
        return "alto"

for line in sys.stdin:
    line_count += 1
    line = line.strip()
    
    try:
        # Format: city\tyear\tprice_category\tcount\tdaysonmarket\tdescription
        parts = line.split('\t')
        if len(parts) < 6:
            continue
            
        # Extract the parts according to the new format
        city = parts[0]
        year_str = parts[1]
        price_category = parts[2]  # directly using price_category ("basso", "medio", "alto")
        count_str = parts[3]       # typically "1"
        dom_str = parts[4]
        
        # Combine all remaining parts as the description (handle tabs in description)
        description = '\t'.join(parts[5:])
        
        # Converti i valori numerici
        year = int(year_str)
        count = int(count_str)
        days_on_market = float(dom_str) if dom_str and dom_str.lower() != 'null' else 0.0
        
        # Estrai parole dalla descrizione per word count
        words_list = []
        if description:
            # Semplice tokenization delle parole (si potrebbe migliorare)
            words = description.lower().split()
            # Filtra parole comuni o troppo brevi
            stopwords = ['and', 'the', 'is', 'in', 'it', 'to', 'for', 'with', 'on', 'at', 'from', 'by', 'an', 'a', 'of']
            words_list = [word for word in words if word not in stopwords and len(word) > 2]
        
    except (ValueError, IndexError) as e:
        sys.stderr.write(f"Errore nella riga: {line} - {str(e)}\n")
        continue
    
    # La chiave è (city, year)
    key_city_year = (city, year)
    
    # Aggiorna i dati per la specifica fascia di prezzo all'interno di (city, year)
    current_range_stats = report_data[key_city_year][price_category]
    
    current_range_stats["count"] += count  # Aggiungi il count specificato (di solito 1)
    current_range_stats["daysonmarket_sum"] += days_on_market
    if days_on_market >= 0:  # Considera solo daysonmarket validi per il conteggio della media
        current_range_stats["daysonmarket_records"] += 1
    
    if words_list:  # Aggiungi le parole solo se ce ne sono
        current_range_stats["word_counts"].update(words_list)

# Dopo aver processato tutte le righe, stampa il report finale
# Ordina per città (alfabeticamente) e poi per anno (numericamente) per un output consistente
sorted_city_year_keys = sorted(report_data.keys(), key=lambda k: (k[0], k[1]))

for city_year_key_tuple in sorted_city_year_keys:
    city_value, year_value = city_year_key_tuple
    
    # Prepara la stringa di output per la riga corrente (city, year)
    output_line_parts = [f"City: {city_value}, Year: {year_value}"]
    
    report_fasce_details_list = []
    # Itera sulle fasce di prezzo in un ordine predefinito per consistenza
    for price_range_label in ["basso", "medio", "alto"]:
        # Ottieni i dati per la fascia di prezzo corrente, se esistono
        range_specific_data = report_data[city_year_key_tuple].get(price_range_label)
        
        if range_specific_data and range_specific_data["count"] > 0:
            num_auto_in_range = range_specific_data["count"]
            
            avg_dom_in_range = 0.00  # Default a 0.00
            if range_specific_data["daysonmarket_records"] > 0:
                avg_dom_in_range = range_specific_data["daysonmarket_sum"] / range_specific_data["daysonmarket_records"]
            
            # Estrai le top 3 parole
            top_3_words_in_range = [word for word, count in range_specific_data["word_counts"].most_common(3)]
            
            fascia_detail_str = (
                f"FasciaPrezzo: {price_range_label}, "
                f"NumAuto: {num_auto_in_range}, "
                f"AvgDaysOnMarket: {avg_dom_in_range:.2f}, "  # Formatta a 2 decimali
                f"TopWords: {str(top_3_words_in_range)}"
            )
            report_fasce_details_list.append(fascia_detail_str)
        else:
            # Se non ci sono auto in questa fascia per questa (city, year), stampa con zeri
            fascia_detail_str = (
                f"FasciaPrezzo: {price_range_label}, "
                f"NumAuto: 0, "
                f"AvgDaysOnMarket: 0.00, "
                f"TopWords: []"
            )
            report_fasce_details_list.append(fascia_detail_str)

    output_line_parts.append(f"ReportFasce: [{'; '.join(report_fasce_details_list)}]")
    print("\t".join(output_line_parts))

# Stampa statistiche di esecuzione per il benchmark
end_time = time.time()
duration = end_time - start_time
sys.stderr.write(f"\nReducer Statistics:\n")
sys.stderr.write(f"Processed {line_count} lines in {duration:.2f} seconds\n")
sys.stderr.write(f"Average processing rate: {line_count/duration:.2f} lines/second\n")