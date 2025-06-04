#cat > job1_spark.py << 'EOF'
#!/usr/bin/env python3
"""
Job1 Spark Core per AWS EMR: Analisi delle combinazioni Make/Model più popolari
Usa solo i campi: city, daysonmarket, description, make_name, model_name, price, year, description_cleaned
"""

from pyspark.sql import SparkSession
import csv
from io import StringIO
import sys

# Solo i campi richiesti
REQUIRED_FIELDS = ['city', 'daysonmarket', 'description', 'make_name', 'model_name', 'price', 'year', 'description_cleaned']

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

def parse_csv_line(line, field_indices):
    """Parse CSV line e estrai solo i campi richiesti"""
    if not line or line.isspace():
        return None
    
    try:
        reader = csv.reader(StringIO(line)) 
        fields = next(reader)
        
        # Verifica che ci siano abbastanza campi
        max_idx = max(field_indices.values())
        if len(fields) <= max_idx:
            return None 
        
        # Estrai solo i campi richiesti
        city = fields[field_indices['city']].strip()
        daysonmarket = fields[field_indices['daysonmarket']].strip()
        description = fields[field_indices['description']].strip()
        make_name = fields[field_indices['make_name']].strip().lower()
        model_name = fields[field_indices['model_name']].strip().lower()
        price_str = fields[field_indices['price']].strip()
        year_str = fields[field_indices['year']].strip()
        description_cleaned = fields[field_indices['description_cleaned']].strip()
        
        # Validazione dati essenziali per l'analisi
        if not make_name or not model_name or not price_str or not year_str:
            return None
        
        try:
            price = float(price_str)
            year = int(float(year_str))
        except ValueError:
            return None
        
        # Filtri di ragionevolezza
        if price <= 0 or not (1900 <= year <= 2025):
            return None
        
        return {
            'city': city,
            'daysonmarket': daysonmarket,
            'description': description,
            'make_name': make_name,
            'model_name': model_name,
            'price': price,
            'year': year,
            'description_cleaned': description_cleaned
        }
        
    except (ValueError, IndexError, csv.Error):
        return None

def calculate_model_stats(data_iterable):
    """Calcola statistiche per un modello usando solo i campi disponibili"""
    records = list(data_iterable)
    
    if not records:
        return {
            'count': 0,
            'min_price': 0.0,
            'max_price': 0.0,
            'avg_price': 0.0,
            'years': [],
            'cities': []
        }
    
    prices = [r['price'] for r in records]
    years = set(r['year'] for r in records)
    cities = set(r['city'] for r in records if r['city'])  # Solo città non vuote
    
    count = len(records)
    min_price = min(prices)
    max_price = max(prices)
    avg_price = sum(prices) / count
    
    return {
        'count': count,
        'min_price': min_price,
        'max_price': max_price,
        'avg_price': avg_price,
        'years': sorted(list(years)),
        'cities': sorted(list(cities))[:5]  # Prime 5 città per brevità
    }

def format_output_line(make_name, models_stats_list):
    """Formatta la linea di output per una marca"""
    models_output_strings = []
    # Ordina per numero di auto (count) decrescente
    sorted_models_stats_list = sorted(models_stats_list, key=lambda x: x[1]['count'], reverse=True)

    for model_name, stats in sorted_models_stats_list:
        years_str = str(stats['years'])
        cities_str = str(stats['cities'])
        
        model_str = (
            f"Model: {model_name}, "
            f"Count: {stats['count']}, "
            f"MinPrice: {stats['min_price']:.2f}, "
            f"MaxPrice: {stats['max_price']:.2f}, "
            f"AvgPrice: {stats['avg_price']:.2f}, "
            f"Years: {years_str}, "
            f"TopCities: {cities_str}"
        )
        models_output_strings.append(model_str)
    
    return f"Make: {make_name}\tModels: [{'; '.join(models_output_strings)}]"

def main():
    # Parametri da linea di comando per AWS EMR
    input_path = sys.argv[1] if len(sys.argv) > 1 else "s3://used-cars-big-data-analysis-1748698020/data/used_cars_filtered.csv"
    output_path = sys.argv[2] if len(sys.argv) > 2 else "s3://used-cars-big-data-analysis-1748698020/output/spark/job1/"
    
    print(f"Job1 Spark - Input: {input_path}")
    print(f"Job1 Spark - Output: {output_path}")
    print(f"Campi usati: {', '.join(REQUIRED_FIELDS)}")
    
    # Configurazione Spark per EMR
    spark = (SparkSession.builder
        .appName("Job1-MakeModel-Analysis-Spark-EMR")
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
    parsed_rdd = data_rdd.map(lambda line: parse_csv_line(line, field_indices)).filter(lambda x: x is not None)
    
    # Raggruppa per (make, model) 
    make_model_keyed_rdd = parsed_rdd.map(
        lambda record: ((record['make_name'], record['model_name']), record)
    )
    
    print("Calcolo statistiche per modello...")
    model_stats_rdd = make_model_keyed_rdd.groupByKey().mapValues(calculate_model_stats)
    
    # Filtra modelli validi (almeno 1 auto)
    valid_model_stats_rdd = model_stats_rdd.filter(lambda x: x[1]['count'] > 0)
    
    # Raggruppa per marca
    make_keyed_rdd = valid_model_stats_rdd.map(
        lambda x: (x[0][0], (x[0][1], x[1]))  # (make, (model, stats))
    )
    
    print("Raggruppamento finale per marca...")
    final_grouped_rdd = make_keyed_rdd.groupByKey()
    
    # Formatta output
    formatted_output_rdd = final_grouped_rdd.map(lambda x: format_output_line(x[0], list(x[1])))
    
    # Ordina per marca
    sorted_output_rdd = formatted_output_rdd.sortBy(lambda line: line.split('\t')[0])
    
    print(f"Salvataggio risultati in: {output_path}")
    # Salva su S3 con un singolo file
    sorted_output_rdd.coalesce(1).saveAsTextFile(output_path)
    
    print("Job1 Spark completato!")
    
    # Mostra alcune statistiche
    total_records = data_rdd.count()
    valid_records = parsed_rdd.count()
    total_makes = final_grouped_rdd.count()
    
    print(f"Statistiche:")
    print(f"- Record totali: {total_records}")
    print(f"- Record validi: {valid_records}")
    print(f"- Marche trovate: {total_makes}")
    print(f"- Campi analizzati: {', '.join(REQUIRED_FIELDS)}")
    
    spark.stop()

if __name__ == "__main__":
    main()
#EOF
