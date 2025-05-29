# Analisi Big Data su Auto Usate USA

## Panoramica del Progetto

Questo progetto analizza un vasto dataset di auto usate in vendita negli Stati Uniti (circa 3 milioni di record) utilizzando tecnologie Big Data. Il dataset contiene informazioni dettagliate sulle auto usate in vendita fino al 2020, tra cui specifiche tecniche, prezzi, caratteristiche e condizioni.

## Tecnologie Utilizzate

- **Apache Spark**: Core API e SQL API
- **Python 3.x**: Framework principale per l'implementazione
- **Jupyter Notebook**: Esplorazione dati e analisi preliminari
- **Pandas**: Manipolazione dati durante la fase esplorativa
- **PySpark**: API Python per Spark

## Job Implementati

### Job 1: Statistiche per Marca/Modello

Questo job genera statistiche dettagliate per ciascuna marca di automobile (`make_name`), fornendo:
- Nome della marca
- Lista di modelli (`model_name`) con:
  - Numero di auto disponibili
  - Prezzo minimo, massimo e medio
  - Elenco degli anni (`year`) in cui il modello è presente


### Job 2: Analisi Auto per Città, Anno e Fascia di Prezzo

Questo job analizza le auto raggruppandole per città, anno e fascia di prezzo. Per ogni combinazione:
- Classifica le auto in tre fasce di prezzo:
  - Basso: < 20.000$
  - Medio: 20.000$ - 50.000$
  - Alto: > 50.000$
- Calcola per ciascuna fascia di prezzo:
  - Numero di auto disponibili
  - Permanenza media sul mercato (giorni)
  - Le 3 parole più frequenti nelle descrizioni (dopo rimozione delle stop words)
- Genera report dettagliati per ciascuna combinazione città/anno

## Struttura del Repository

```
used-cars-bigdata-analysis/
├── data/
│   └── samples/          # Campioni del dataset a diverse dimensioni
├── notebooks/            # Notebook Jupyter per EDA e preprocessing
├── src/
│   ├── mapreduce/        # Implementazione MapReduce
│   │   ├── job1/         
│   │   │   ├── mapper.py
│   │   │   ├── reducer.py
│   │   ├── job2/         
│   │   │   ├── mapper.py
│   │   │   ├── reducer.py
│   │   └── run_benchmark.py
│   ├── spark/            # Implementazione degli job Spark
│   │   ├── job1_spark_core.py
│   │   ├── job1_spark_sql.py
│   │   ├── job2_spark_core.py
│   │   ├── job2_spark_sql.py
│   │   └── run_benchmark.py
├── results/              # Output dei job
├── README.md             # Questo file
├── run_scalability_mapreduce.sh 
├── run_scalability_spark.sh 
└── requirements.txt      # Dipendenze Python
```

## Preprocessing Dati

Le operazioni di preprocessing eseguite includono:
- Pulizia e standardizzazione di campi testuali
- Conversione dei tipi di dato appropriati
- Gestione di valori mancanti e outlier
- Estrazione di caratteristiche numeriche da campi testuali (es. potenza del motore)
- Normalizzazione dei valori anomali

## Come Eseguire il Progetto

### Prerequisiti

```bash
# Installare le dipendenze
pip install -r requirements.txt
```

### Esecuzione Benchmark

Per confrontare le prestazioni dei diversi engine e dimensioni del dataset:

```bash
./run_scalability_mapreduce.sh
./run_scalability_spark.sh
```

Questo script:
1. Crea campioni di diverse dimensioni (1%, 5%, 10%, 25%, 50%, 100%)
2. Esegue i job su ciascun campione con entrambi gli engine (Core e SQL)
3. Produce un report dettagliato su tempi di esecuzione, utilizzo memoria e scalabilità

## Personalizzazione

È possibile personalizzare l'esecuzione modificando i parametri negli script:

- `-i, --input`: Percorso del file CSV di input
- `-o, --output`: Directory dove salvare i risultati
- `-j, --jobs`: Quali job eseguire (job1, job2)
- `-e, --engines`: Quali engine utilizzare (core, sql)
- `-s, --sizes`: Dimensioni del dataset da testare (es: 0.01, 0.05, 0.1, 0.25, 0.5, 1.0)

## Risultati

I risultati vengono salvati nella cartella `results/` in formato JSON e CSV, organizzati per tipo di job ed engine. Per i benchmark, vengono generati report dettagliati in formato testuale per facilitare il confronto delle prestazioni.