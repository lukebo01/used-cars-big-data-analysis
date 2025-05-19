# src/spark/job1_spark_core.py
from pyspark import SparkContext
import csv
import sys

# Indici (VERIFICARE E AGGIORNARE!)
# make_name: 38, model_name: 40, price: 43, year: 64
COL_IDX = {'make_name': 38, 'model_name': 40, 'price': 43, 'year': 64}

def parse_line_job1(line_with_header_info):
    line, is_first_line = line_with_header_info
    if is_first_line: # Salta la riga di intestazione se è la prima partizione e la prima riga
        return []

    try:
        values = next(csv.reader([line]))
        # ... (resto della logica di parsing come prima)
        make_name = values[COL_IDX['make_name']].strip().lower()
        model_name = values[COL_IDX['model_name']].strip().lower()
        price = float(values[COL_IDX['price']])
        year = int(float(values[COL_IDX['year']]))

        if not make_name or not model_name or price <= 0 or not (1950 <= year <= 2025):
            return []
        return [((make_name, model_name), (price, year))]
    except (ValueError, IndexError, csv.Error):
        return []

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: job1_spark_core.py <input_path> <output_path>", file=sys.stderr)
        sys.exit(-1)

    input_path = sys.argv[1]
    output_path = sys.argv[2]

    sc = SparkContext(appName="Job1SparkCore")
    
    lines = sc.textFile(input_path)
    
    # Gestione Header: Rimuovere l'header è più sicuro che usare zipWithIndex se il file è grande
    # e diviso in molte partizioni, poiché zipWithIndex può essere costoso.
    # Un modo comune è leggere l'header, poi filtrarlo.
    header = lines.first()
    data_lines = lines.filter(lambda row: row != header)

    parsed_rdd = data_lines.flatMap(lambda line: parse_line_job1((line, False))) # Passiamo False perché l'header è già rimosso

    # ... (resto della logica RDD come prima nel job1_spark_core.py) ...
    agg_rdd = parsed_rdd.combineByKey(
        lambda val: (val[0], 1, val[0], val[0], {val[1]}),
        lambda acc, val: (
            acc[0] + val[0], acc[1] + 1, min(acc[2], val[0]),
            max(acc[3], val[0]), acc[4].union({val[1]})
        ),
        lambda acc1, acc2: (
            acc1[0] + acc2[0], acc1[1] + acc2[1], min(acc1[2], acc2[2]),
            max(acc1[3], acc2[3]), acc1[4].union(acc2[4])
        )
    )

    stats_by_model_rdd = agg_rdd.mapValues(
        lambda acc: (
            acc[1], acc[2], acc[3], f"{acc[0] / acc[1]:.2f}", sorted(list(acc[4]))
        )
    )

    final_rdd_intermediate = stats_by_model_rdd.map(
        lambda x: (x[0][0], (x[0][1], x[1][0], x[1][1], x[1][2], x[1][3], x[1][4]))
    )
    
    # Raggruppa per marca e formatta l'output
    # (make_name, list_of_model_details_strings)
    result_rdd = final_rdd_intermediate.groupByKey().mapValues(list).map(
        lambda x: (
            x[0], # make_name
            [
                f"Model: {model_data[0]}, Count: {model_data[1]}, MinPrice: {model_data[2]}, MaxPrice: {model_data[3]}, AvgPrice: {model_data[4]}, Years: {model_data[5]}"
                for model_data in x[1] # x[1] è la lista di tuple (model, count, min_p, max_p, avg_p, years_list)
            ]
        )
    )
    
    # Per l'output testuale richiesto (prime 10 righe)
    # Formattiamo l'output come richiesto dal Job 1
    formatted_output_rdd = result_rdd.map(
        lambda x: f"Make: {x[0]}\tModels: [{'; '.join(x[1])}]"
    )

    # Salva i risultati (o prendi i primi N per il report)
    # formatted_output_rdd.saveAsTextFile(output_path) # Salva su HDFS/locale
    
    # Per il report, prendi le prime 10 e stampale o salvale in un file specifico
    top_10_results = formatted_output_rdd.take(10)
    print("--- Job 1 Spark Core: Top 10 Results ---")
    for record in top_10_results:
        print(record)
    
    # Salva le prime 10 righe in un file per il report
    with open(f"{output_path}_top10.txt", "w") as f:
        for record in top_10_results:
            f.write(record + "\n")

    sc.stop()