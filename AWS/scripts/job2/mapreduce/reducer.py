#!/usr/bin/env python3
import sys
from collections import defaultdict, Counter

# Struttura: report_data[city][year][price_category] = {"count": 0, "daysonmarket_sum": 0, "daysonmarket_records": 0, "word_counts": Counter()}
report_data = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: {
    "count": 0, 
    "daysonmarket_sum": 0, 
    "daysonmarket_records": 0,
    "word_counts": Counter()
})))

for line in sys.stdin:
    line = line.strip()
    try:
        parts = line.split('\t')
        if len(parts) < 6:
            continue
            
        city = parts[0]
        year = int(parts[1])
        price_category = parts[2]
        count = int(parts[3])
        daysonmarket = int(parts[4])
        words_str = parts[5]
        
        # Aggiorna i dati
        data = report_data[city][year][price_category]
        data["count"] += count
        data["daysonmarket_sum"] += daysonmarket
        data["daysonmarket_records"] += 1
        
        # Processa le parole
        if words_str != "NO_WORDS":
            words = words_str.split(',')
            data["word_counts"].update(words)
            
    except:
        continue

# Genera output ordinato
for city in sorted(report_data.keys()):
    for year in sorted(report_data[city].keys()):
        output_parts = [f"City: {city}, Year: {year}"]
        
        fasce_details = []
        for price_category in ["basso", "medio", "alto"]:
            data = report_data[city][year].get(price_category)
            
            if data and data["count"] > 0:
                num_auto = data["count"]
                avg_days = data["daysonmarket_sum"] / data["daysonmarket_records"] if data["daysonmarket_records"] > 0 else 0.0
                top_words = [word for word, count in data["word_counts"].most_common(3)]
                
                fascia_str = (f"FasciaPrezzo: {price_category}, "
                             f"NumAuto: {num_auto}, "
                             f"AvgDaysOnMarket: {avg_days:.2f}, "
                             f"TopWords: {top_words}")
            else:
                fascia_str = (f"FasciaPrezzo: {price_category}, "
                             f"NumAuto: 0, "
                             f"AvgDaysOnMarket: 0.00, "
                             f"TopWords: []")
            
            fasce_details.append(fascia_str)
        
        output_parts.append(f"ReportFasce: [{'; '.join(fasce_details)}]")
        print("\t".join(output_parts))