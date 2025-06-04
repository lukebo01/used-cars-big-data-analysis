#!/usr/bin/env python3
import sys
from collections import defaultdict

# Dizionario per raggruppare per marca
make_data = defaultdict(lambda: defaultdict(list))

for line in sys.stdin:
    line = line.strip()
    try:
        make_name, model_name, price_str, year_str = line.split('\t')
        price = float(price_str)
        year = int(year_str)
        
        # Raggruppa per marca e modello
        make_data[make_name][model_name].append((price, year))
    except:
        continue

# Genera output nel formato richiesto
for make_name in sorted(make_data.keys()):
    models_output = []
    
    for model_name in sorted(make_data[make_name].keys()):
        prices_years = make_data[make_name][model_name]
        
        count = len(prices_years)
        prices = [p for p, y in prices_years]
        years = sorted(list(set([y for p, y in prices_years])))
        
        min_price = min(prices)
        max_price = max(prices)
        avg_price = sum(prices) / count
        
        model_str = (f"Model: {model_name}, Count: {count}, "
                    f"MinPrice: {min_price:.2f}, MaxPrice: {max_price:.2f}, "
                    f"AvgPrice: {avg_price:.2f}, Years: {years}")
        
        models_output.append(model_str)
    
    print(f"Make: {make_name}\tModels: [{'; '.join(models_output)}]")