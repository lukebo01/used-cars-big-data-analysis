# Sovrascrivi Job2 Spark con la logica corretta del tuo script locale
#cat > job2_spark.py << 'EOF'
#!/usr/bin/env python3
"""
Job2 Spark Core per AWS EMR: Analisi city/year/price_range con words frequency
STESSO ALGORITMO del job2_spark_core.py locale
Usa solo i campi: city, daysonmarket, description, make_name, model_name, price, year, description_cleaned
"""

from pyspark.sql import SparkSession
import csv
from io import StringIO
import sys
import re
from collections import Counter

# Solo i campi richiesti
REQUIRED_FIELDS = ['city', 'daysonmarket', 'description', 'make_name', 'model_name', 'price', 'year', 'description_cleaned']

# Stop words (stessa lista del tuo script locale)
STOP_WORDS = set([
    "a", "about", "above", "after", "again", "against", "all", "am", "an", "and", "any", "are", "aren't", "as", "at",
    "be", "because", "been", "before", "being", "below", "between", "both", "but", "by", "can't", "cannot", "could",
    "couldn't", "did", "didn't", "do", "does", "doesn't", "doing", "don't", "down", "during", "each", "few", "for",
    "from", "further", "had", "hadn't", "has", "hasn't", "have", "haven't", "having", "he", "he'd", "he'll", "he's",
    "her", "here", "here's", "hers", "herself", "him", "himself", "his", "how", "how's", "i", "i'd", "i'll", "i'm",
    "i've", "if", "in", "into", "is", "isn't", "it", "it's", "its", "itself", "let's", "me", "more", "most", "mustn't",
    "my", "myself", "no", "nor", "not", "of", "off", "on", "once", "only", "or", "other", "ought", "our", "ours",
    "ourselves", "out", "over", "own", "same", "shan't", "she", "she'd", "she'll", "she's", "should", "shouldn't",
    "so", "some", "such", "than", "that", "that's", "the", "their", "theirs", "them", "themselves", "then", "there",
    "there's", "these", "they", "they'd", "they'll", "they're", "they've", "this", "those", "through", "to", "too",
    "under", "until", "up", "very", "was", "wasn't", "we", "we'd", "we'll", "we're", "we've", "were", "weren't",
    "what", "what's", "when", "when's", "where", "where's", "which", "while", "who", "who's", "whom", "why", "why's",
    "with", "won't", "would", "wouldn't", "you", "you'd", "you'll", "you're", "you've", "your", "yours", "yourself",
    "yourselves", "inc", "com", "www"
])

def get_field_indices(header_line):
    """Ottieni gli indici dei campi richiesti dall'header"""
    header_fields = header_line.split(',')
    indices = {}
    
    for field in REQUIRED_FIELDS:
        try:
            indices[field] = header_fields.index(field)
        except ValueError:
            print(f"Errore critico: campo '{field}' non trovato nell'header")
            return None
    
    return indices

def get_price_range(price_val):
    """Classifica il prezzo in fasce (stesso algoritmo del locale)"""
    if price_val < 20000: 
        return "basso"
    elif 20000 <= price_val <= 50000: 
        return "medio"
    else: 
        return "alto"

def clean_and_tokenize(text):
    """Pulisce e tokenizza il testo (stesso algoritmo del locale)"""
    if not text: 
        return []
    
    text = str(text).lower()
    text = re.sub(r'[^\w\s]', '', text)
    text = re.sub(r'\s+', ' ', text).strip()
    words = text.split()
    
    return [word for word in words if word not in STOP_WORDS and len(word) > 2]

def parse_line_job2(line, field_indices):
    """Parse CSV line per Job2 analisi (stesso algoritmo del locale)"""
    if not line or line.isspace():
        return None
    
    try:
        reader = csv.reader(StringIO(line))
        fields = next(reader)
        
        # Verifica che ci siano abbastanza campi
        max_idx = max(field_indices.values())
        if len(fields) <= max_idx:
            return None
        
        # Estrai campi richiesti
        city = fields[field_indices['city']].strip().lower()
        year_str = fields[field_indices['year']].strip()
        price_str = fields[field_indices['price']].strip()
        daysonmarket_str = fields[field_indices['daysonmarket']].strip()
        description = fields[field_indices['description']].strip()
        description_cleaned = fields[field_indices['description_cleaned']].strip()
        
        # Validazione dati essenziali
        if not city or not year_str or not price_str:
            return None
        
        try:
            year = int(float(year_str))
            price = float(price_str)
            daysonmarket = int(float(daysonmarket_str)) if daysonmarket_str else 0
        except ValueError:
            return None
        
        # Filtri di ragionevolezza
        if price <= 0 or daysonmarket < 0 or not (1900 <= year <= 2025):
            return None
        
        # Usa description_cleaned se disponibile, altrimenti description
        final_description = description_cleaned if description_cleaned else description
        
        price_range_label = get_price_range(price)
        tokens = clean_and_tokenize(final_description)
        
        # Chiave: (city, year, price_range_label)
        # Valore: (daysonmarket, lista_di_tokens)
        return ((city, year, price_range_label), (daysonmarket, tokens))
        
    except (ValueError, IndexError, csv.Error):
        return None

def aggregate_price_range_data(records_iterable):
    """Aggrega dati per fascia di prezzo (stesso algoritmo del locale)"""
    records_list = list(records_iterable)
    
    total_dom = 0
    dom_count = 0
    all_words = []
    num_cars = 0

    for dom, tokens in records_list:
        num_cars += 1
        if dom >= 0:  # Assicurati che daysonmarket sia valido
            total_dom += dom
            dom_count += 1
        all_words.extend(tokens)
    
    avg_dom_val = (total_dom / dom_count) if dom_count > 0 else 0.0
    word_counts = Counter(all_words)
    top_3_words_list = [word for word, count in word_counts.most_common(3)]
    
    return (num_cars, avg_dom_val, top_3_words_list)

def format_job2_output(city_year_key, price_range_aggregated_data_map):
    """Formatta output finale (stesso formato del locale)"""
    city, year = city_year_key
    output_string = f"City: {city}, Year: {year}"
    
    fasce_output = []
    for price_range_label in ["basso", "medio", "alto"]:  # Ordine desiderato
        data = price_range_aggregated_data_map.get(price_range_label)
        if data:
            num_auto, avg_dom, top_words = data
            fascia_str = (
                f"FasciaPrezzo: {price_range_label}, "
                f"NumAuto: {num_auto}, "
                f"AvgDaysOnMarket: {avg_dom:.2f}, "
                f"TopWords: {str(top_words)}"
            )
        else:  # Fascia non presente per questa city/year
            fascia_str = (
                f"FasciaPrezzo: {price_range_label}, "
                f"NumAuto: 0, "
                f"AvgDaysOnMarket: 0.00, "
                f"TopWords: []"
            )
        fasce_output.append(fascia_str)
            
    output_string += "\tReportFasce: [" + "; ".join(fasce_output) + "]"
    return output_string

def main():
    # Parametri da linea di comando per AWS EMR
    input_path = sys.argv[1] if len(sys.argv) > 1 else "s3://used-cars-big-data-analysis-1748698020/data/used_cars_filtered.csv"
    output_path = sys.argv[2] if len(sys.argv) > 2 else "s3://used-cars-big-data-analysis-1748698020/output/spark/job2/"
    
    print(f"Job2 Spark - Input: {input_path}")
    print(f"Job2 Spark - Output: {output_path}")
    print(f"Campi usati: {', '.join(REQUIRED_FIELDS)}")
    
    # Configurazione Spark per EMR
    spark = (SparkSession.builder
        .appName("Job2-CityYearPriceRange-Analysis-Spark-EMR")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate())

    sc = spark.sparkContext
    
    print("Lettura dataset...")
    lines_rdd = sc.textFile(input_path)
    
    # Ottieni header e indici dei campi
    header_line = lines_rdd.first()
    field_indices = get_field_indices(header_line)
    
    if field_indices is None:
        print("Errore: impossibile trovare tutti i campi richiesti")
        spark.stop()
        return
    
    print(f"Indici campi: {field_indices}")
    
    # Rimuovi header
    data_rdd = lines_rdd.filter(lambda line: line != header_line)
    
    print("Parsing dati...")
    # Parse e mappa a ((city, year, price_range), (daysonmarket, tokens))
    parsed_data = data_rdd.map(lambda line: parse_line_job2(line, field_indices)).filter(lambda x: x is not None)
    
    print("Aggregazione dati per fascia prezzo...")
    # Raggruppa per (city, year, price_range) e aggrega i dati per ogni fascia
    range_aggregated_rdd = parsed_data.groupByKey().mapValues(aggregate_price_range_data)
    
    print("Riorganizzazione per city/year...")
    # Riorganizza per (city, year) come chiave per il raggruppamento finale
    city_year_keyed_rdd = range_aggregated_rdd.map(
        lambda x: ((x[0][0], x[0][1]),  # Nuova chiave: (city, year)
                   (x[0][2], x[1][0], x[1][1], x[1][2]))  # Valore: (price_range, num_cars, avg_dom, top_words)
    )
    
    # Raggruppa per (city, year)
    final_grouped_rdd = city_year_keyed_rdd.groupByKey()
    
    print("Formattazione output finale...")
    # Trasforma l'iterable in una mappa price_range -> dati per facilitare la formattazione
    def map_price_ranges(iterable):
        price_map = {}
        for prange, num, avg_d, t_words in iterable:
            price_map[prange] = (num, avg_d, t_words)
        return price_map
    
    formatted_output_rdd = final_grouped_rdd.map(
        lambda x: format_job2_output(x[0], map_price_ranges(x[1]))
    )
    
    # Ordina per città e anno
    sorted_output_rdd = formatted_output_rdd.sortBy(
        lambda line: (line.split(", Year: ")[0].split("City: ")[1], 
                     int(line.split(", Year: ")[1].split("\t")[0]))
    )
    
    print(f"Salvataggio risultati in: {output_path}")
    # Salva su S3 con un singolo file
    sorted_output_rdd.coalesce(1).saveAsTextFile(output_path)
    
    print("Job2 Spark completato!")
    
    # Mostra alcune statistiche
    total_records = data_rdd.count()
    valid_records = parsed_data.count()
    total_city_years = final_grouped_rdd.count()
    
    print(f"Statistiche:")
    print(f"- Record totali: {total_records}")
    print(f"- Record validi: {valid_records}")
    print(f"- Combinazioni city/year: {total_city_years}")
    print(f"- Campi analizzati: {', '.join(REQUIRED_FIELDS)}")
    
    spark.stop()

if __name__ == "__main__":
    main()
#EOF
