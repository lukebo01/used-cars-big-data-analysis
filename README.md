# Progetto Big Data: Analisi Dataset Auto Usate USA

Questo progetto è stato sviluppato per il corso di Big Data e analizza il dataset "US Used Cars Dataset" da Kaggle.  
Contiene circa 3 milioni di record con informazioni dettagliate su auto usate in vendita fino al 2020.

## Indice

- [Descrizione del Progetto](#descrizione-del-progetto)
- [Dataset](#dataset)
- [Analisi Implementate](#analisi-implementate)
    - [Job 1: Statistiche per Marca/Modello](#job-1-statistiche-per-marcamodello)
    - [Job 3: Gruppi di Modelli con Motori Simili](#job-3-gruppi-di-modelli-con-motori-simili)
- [Tecnologie Utilizzate](#tecnologie-utilizzate)
- [Struttura del Repository](#struttura-del-repository)
- [Prerequisiti](#prerequisiti)
- [Preparazione dei Dati](#preparazione-dei-dati)
    - [Creazione dei Campioni](#creazione-dei-campioni)
- [Come Eseguire i Job](#come-eseguire-i-job)
    - [MapReduce (Hadoop Streaming)](#mapreduce-hadoop-streaming)
    - [Apache Spark (Core e SQL)](#apache-spark-core-e-sql)
- [Risultati Attesi](#risultati-attesi)
- [Benchmark e Performance](#benchmark-e-performance)
- [Autore](#autore)

## Descrizione del Progetto

L'obiettivo di questo progetto è progettare e realizzare analisi su un vasto dataset di auto usate, utilizzando diverse tecnologie Big Data.  
Il progetto include la preparazione dei dati, l'implementazione di job analitici specifici e un confronto delle performance delle tecnologie scelte.

## Dataset

Il dataset utilizzato è [US Used Cars Dataset di Kaggle](https://www.kaggle.com/datasets/ananaymital/us-used-cars-dataset).  
Esso contiene circa 3 milioni di record e 66 colonne. Per scopi di sviluppo e test di scalabilità, sono stati generati sottoinsiemi del dataset.

- **Dataset Completo (originale):** Non incluso nel repository per motivi di dimensione. Scaricabile dal link sopra.  
- **Campioni:** Situati in `data/samples/`, con dimensioni crescenti (es. 10k, 100k, 1M righe).

## Analisi Implementate

### Job 1: Statistiche per Marca/Modello

Genera statistiche per ciascuna marca di automobile (`make_name`), indicando:  
- Nome della marca.  
- Lista di modelli (`model_name`) con:  
    - Numero di auto presenti.  
    - Prezzo minimo, massimo e medio.  
    - Elenco degli anni (`year`) in cui il modello è presente.

### Job 3: Gruppi di Modelli con Motori Simili

Identifica gruppi di modelli con caratteristiche del motore "simili". Due modelli sono simili se:  
`abs(v1 - v2) / max(v1, v2) <= 0.1` per potenza (`horsepower`) e cilindrata (`engine_displacement`).  
Per ciascun gruppo:  
- Prezzo medio del gruppo.  
- Modello con maggiore potenza.

## Tecnologie Utilizzate

Le analisi sono state implementate con:  
1. **MapReduce:** Hadoop Streaming con script Python.  
2. **Spark Core:** PySpark RDD API.  
3. **Spark SQL:** PySpark DataFrame API e Spark SQL.  

Il file system distribuito di riferimento è HDFS.

## Struttura del Repository

```plaintext
used-cars-bigdata-analysis/
├── data/
│   └── samples/          # Campioni del dataset
├── notebooks/            # Jupyter notebooks per EDA
├── src/
│   ├── common/           # Moduli Python condivisi
│   ├── mapreduce/        # Codice per i job MapReduce
│   ├── spark/            # Codice per i job Spark
├── results/              # Output dei job
├── report/               # Report finale
├── .gitignore
├── README.md             # Questo file
└── requirements.txt      # Dipendenze Python
```

## Prerequisiti

- Python 3.x  
- Apache Hadoop (Hadoop Streaming)  
- Apache Spark  
- Java  
- Librerie Python (`pyspark`, `pandas`, ecc.) specificate in `requirements.txt`.  

Installazione delle dipendenze Python:  
```bash
pip install -r requirements.txt
```

## Preparazione dei Dati

Include:  
- Selezione delle colonne rilevanti.  
- Conversione tipi di dato (es. `price` a float).  
- Gestione valori mancanti e outlier.  
- Standardizzazione stringhe.  

La logica è implementata in `src/common/data_utils.py`.

### Creazione dei Campioni

Esempio per creare un campione di 10k righe:  
```bash
ORIGINAL_FILE="path/to/US_used_cars_Kaggle.csv"
DEST_DIR="data/samples"
mkdir -p $DEST_DIR
head -n 1 "$ORIGINAL_FILE" > "$DEST_DIR/used_cars_10k.csv"
tail -n +2 "$ORIGINAL_FILE" | head -n 9999 >> "$DEST_DIR/used_cars_10k.csv"
```

## Come Eseguire i Job

### MapReduce (Hadoop Streaming)

Esecuzione locale (simulata):  
```bash
INPUT_FILE="data/samples/used_cars_10k.csv"
OUTPUT_PREFIX="results/mr_job1_10k"
MAPPER="src/mapreduce/job1/mapper.py"
REDUCER="src/mapreduce/job1/reducer.py"

cat "$INPUT_FILE" | python3 "$MAPPER" | sort -t$'\t' -k1,1 | python3 "$REDUCER" > "${OUTPUT_PREFIX}_output.txt"
```

### Apache Spark (Core e SQL)

Esecuzione locale:  
```bash
INPUT_FILE="data/samples/used_cars_10k.csv"
OUTPUT_PREFIX="results/spark_core_job1_10k"
SCRIPT_PATH="src/spark/job1_spark_core.py"

spark-submit --master local[*] "$SCRIPT_PATH" "$INPUT_FILE" "$OUTPUT_PREFIX"
```

## Risultati Attesi

Le prime 10 righe dell'output di ciascun job saranno salvate in `results/`.  
Esempio: `results/mr_job1_10k_output_top10.txt`.

## Benchmark e Performance

Il report finale includerà:  
- Confronto tempi di esecuzione (locale e cluster).  
- Test con dimensioni crescenti del dataset.  
- Grafici e tabelle per i risultati.
