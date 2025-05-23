#!/usr/bin/env python3
# src/mapreduce/job2/reducer_job2.py

import sys
from collections import defaultdict, Counter

# Struttura dati per accumulare i risultati per ogni (città, anno)
# Esempio:
# report_data = {
#   ("los angeles", 2018): {
#     "basso": {"count": 10, "daysonmarket_sum": 150, "daysonmarket_records": 10, "word_counts": Counter({...})},
#     "medio": {"count": 5, "daysonmarket_sum": 200, "daysonmarket_records": 5, "word_counts": Counter({...})},
#     ...
#   },
#   ...
# }
report_data = defaultdict(lambda: defaultdict(lambda: {
    "count": 0, 
    "daysonmarket_sum": 0, 
    "daysonmarket_records": 0, # Numero di record con un valore daysonmarket valido per la media
    "word_counts": Counter()
}))

# Non è necessario current_key per questo approccio di reducer perché aggreghiamo
# direttamente nella struttura report_data. Il sort prima del reducer
# garantisce che i dati per la stessa (city, year, price_range) arrivino insieme,
# ma questo reducer non ne ha strettamente bisogno per raggruppare,
# bensì aggrega man mano che le righe arrivano.

for line in sys.stdin:
    line = line.strip()
    try:
        # L'input dal mapper è: city\tyear\tprice_range\t1\tdaysonmarket\twords_str
        city, year_str, price_range, count_one_str, dom_str, words_comma_sep = line.split('\t')
        
        year = int(year_str)
        # count_one_str è sempre "1", ma lo leggiamo per coerenza
        # count_val = int(count_one_str) # Non strettamente necessario se è sempre 1
        dom_val = int(dom_str)
        
        words_list = []
        # Se words_str è "NO_WORDS" o vuoto, words_list rimane vuota
        if words_comma_sep and words_comma_sep != "NO_WORDS":
            words_list = words_comma_sep.split(',')

    except ValueError:
        # sys.stderr.write(f"WARN_REDUCER_JOB2: Skipping malformed line: {line}\n")
        continue

    # La chiave principale per il nostro report è (city, year)
    key_city_year = (city, year)
    
    # Aggiorna i dati per la specifica fascia di prezzo all'interno di (city, year)
    current_range_stats = report_data[key_city_year][price_range]
    
    current_range_stats["count"] += 1 # Ogni riga dal mapper rappresenta un'auto
    current_range_stats["daysonmarket_sum"] += dom_val
    if dom_val >= 0: # Considera solo daysonmarket validi per il conteggio della media
        current_range_stats["daysonmarket_records"] += 1
    
    if words_list: # Aggiungi le parole solo se ce ne sono
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
            
            avg_dom_in_range = 0.00 # Default a 0.00
            if range_specific_data["daysonmarket_records"] > 0:
                avg_dom_in_range = range_specific_data["daysonmarket_sum"] / range_specific_data["daysonmarket_records"]
            
            # Estrai le top 3 parole
            top_3_words_in_range = [word for word, count in range_specific_data["word_counts"].most_common(3)]
            
            fascia_detail_str = (
                f"FasciaPrezzo: {price_range_label}, "
                f"NumAuto: {num_auto_in_range}, "
                f"AvgDaysOnMarket: {avg_dom_in_range:.2f}, " # Formatta a 2 decimali
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

    output_line_parts.append("ReportFasce: [" + "; ".join(report_fasce_details_list) + "]")
    print("\t".join(output_line_parts))