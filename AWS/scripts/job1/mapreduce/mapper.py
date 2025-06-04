#!/usr/bin/env python3
import sys
import csv

for line in sys.stdin:
    try:
        reader = csv.reader([line.strip()])
        row = next(reader)
        
        # Skip header
        if len(row) > 0 and row[0] == 'city':
            continue
            
        if len(row) >= 8:
            # Ordine: city,daysonmarket,description,make_name,model_name,price,year,description_cleaned
            make_name = row[3].strip().lower()
            model_name = row[4].strip().lower()
            price = float(row[5])
            year = int(row[6])
            
            if make_name and model_name and price > 0 and 1900 <= year <= 2025:
                print(f"{make_name}\t{model_name}\t{price}\t{year}")
    except:
        continue