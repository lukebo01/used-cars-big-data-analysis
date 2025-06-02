#!/usr/bin/env python3
import sys
import csv
import re

# Stop words semplici
STOP_WORDS = set([
    "a", "about", "above", "after", "again", "against", "all", "am", "an", "and", "any", "are", "as", "at",
    "be", "because", "been", "before", "being", "below", "between", "both", "but", "by", "can", "could",
    "did", "do", "does", "doing", "down", "during", "each", "few", "for", "from", "had", "has", "have",
    "having", "he", "her", "here", "hers", "herself", "him", "himself", "his", "how", "i", "if", "in",
    "into", "is", "it", "its", "itself", "me", "more", "most", "my", "myself", "no", "nor", "not", "of",
    "off", "on", "once", "only", "or", "other", "our", "ours", "out", "over", "own", "same", "she", "so",
    "some", "such", "than", "that", "the", "their", "theirs", "them", "then", "there", "these", "they",
    "this", "those", "through", "to", "too", "under", "until", "up", "very", "was", "we", "were", "what",
    "when", "where", "which", "while", "who", "why", "with", "would", "you", "your", "yours"
])

def get_price_category(price):
    if price < 15000:
        return "basso"
    elif price < 35000:
        return "medio"
    else:
        return "alto"

def clean_and_tokenize(text):
    if not text or text.strip() == "":
        return []
    
    text = str(text).lower()
    text = re.sub(r'[^\w\s]', '', text)
    text = re.sub(r'\s+', ' ', text).strip()
    
    if not text:
        return []
    
    words = text.split()
    return [word for word in words if word not in STOP_WORDS and len(word) > 2]

for line in sys.stdin:
    try:
        reader = csv.reader([line.strip()])
        row = next(reader)
        
        # Skip header
        if len(row) > 0 and row[0] == 'city':
            continue
            
        if len(row) >= 8:
            # Ordine: city,daysonmarket,description,make_name,model_name,price,year,description_cleaned
            city = row[0].strip().lower()
            daysonmarket = int(row[1])
            description = row[2].strip()
            price = float(row[5])
            year = int(row[6])
            description_cleaned = row[7].strip() if len(row) > 7 else ""
            
            # Usa description_cleaned se disponibile, altrimenti description
            text_to_analyze = description_cleaned if description_cleaned else description
            
            if city and price > 0 and daysonmarket >= 0 and 1900 <= year <= 2025:
                price_category = get_price_category(price)
                words = clean_and_tokenize(text_to_analyze)
                words_str = ",".join(words) if words else "NO_WORDS"
                
                # Output: city, year, price_category, count(1), daysonmarket, words
                print(f"{city}\t{year}\t{price_category}\t1\t{daysonmarket}\t{words_str}")
    except:
        continue