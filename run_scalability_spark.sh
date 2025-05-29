#!/bin/bash

# Assicurati che lo script si interrompa in caso di errore
set -e

# Definisci il percorso del dataset principale
# Assicurati che il percorso sia relativo alla directory da cui esegui questo script
# o usa un percorso assoluto.
DATASET="data/samples/used_cars_1k.csv" # Adatta se necessario

# Crea directory per i risultati dei benchmark se non esiste
# Sarà relativa alla directory da cui esegui lo script
RESULTS_DIR="results/spark_benchmark"
mkdir -p "$RESULTS_DIR"
echo "I risultati del benchmark verranno salvati in: $(pwd)/$RESULTS_DIR"


# --- GESTIONE AMBIENTE VIRTUALE (Opzionale ma consigliato) ---
# Se usi un ambiente virtuale (es. .venv) per questo progetto:
# VENV_PATH=".venv" # O il percorso del tuo ambiente virtuale
# if [ -d "$VENV_PATH" ]; then
#     echo "Attivazione dell'ambiente virtuale: $VENV_PATH"
#     source "$VENV_PATH/bin/activate"
# else
#     echo "ATTENZIONE: Ambiente virtuale $VENV_PATH non trovato."
#     echo "Le dipendenze Python potrebbero non essere gestite correttamente."
# fi
# --------------------------------------------------------------


# Verifica che psutil sia installato (necessario per il monitoraggio memoria)
echo "Verifica del modulo Python 'psutil'..."
if ! python3 -c "import psutil" &> /dev/null; then
    echo "Installazione del modulo psutil necessario per il monitoraggio della memoria..."
    # È meglio che l'utente gestisca le proprie installazioni Python,
    # ma se vuoi automatizzarlo, assicurati che pip sia quello giusto (es. python3 -m pip)
    python3 -m pip install psutil
fi

# Controlla che PySpark sia installato
echo "Verifica del modulo Python 'pyspark'..."
if ! python3 -c "import pyspark; print(pyspark.__version__)" &> /dev/null; then
    echo "ERRORE: PySpark non è installato o non è importabile."
    echo "Assicurati che PySpark sia installato nell'ambiente Python correntemente attivo."
    echo "Prova a installarlo con: python3 -m pip install pyspark" # Adatta la versione se necessario
    exit 1
else
    echo "PySpark trovato. Versione: $(python3 -c "import pyspark; print(pyspark.__version__)")"
fi

# Imposta PYSPARK_PYTHON esplicitamente
# Questo dice a Spark quale interprete Python usare per i worker.
# Assicurati che `which python3` punti all'interprete Python
# che ha accesso a pyspark e alle altre dipendenze.
export PYSPARK_PYTHON=$(which python3)
if [ -z "$PYSPARK_PYTHON" ]; then
    echo "ERRORE: Impossibile trovare 'python3'. Assicurati che sia nel PATH."
    exit 1
fi
echo "PYSPARK_PYTHON impostato a: $PYSPARK_PYTHON"

# Verifica che Spark (spark-submit) sia accessibile
echo "Verificando l'installazione di Spark (tramite spark-submit)..."
if ! command -v spark-submit &> /dev/null; then
    echo "ERRORE: Il comando 'spark-submit' non è stato trovato nel PATH."
    echo "Controlla che SPARK_HOME sia impostato correttamente e che $SPARK_HOME/bin sia nel PATH."
    echo "Assicurati di aver eseguito 'source ~/.bashrc' o di aver aperto un nuovo terminale dopo aver configurato Spark."
    exit 1
fi

# Tenta di ottenere la versione di Spark.
# L'output di 'spark-submit --version' può essere verboso o variare.
# Avviare spark-shell e uscire rapidamente è un modo più robusto per vedere la versione stampata.
echo "Tentativo di ottenere la versione di Spark..."
# spark-submit --version può dare output misto, proviamo ad avviare e chiudere la shell
# Nota: questo potrebbe essere lento. Un'alternativa è fidarsi che se spark-submit è lì, Spark è configurato.
if spark-submit --master local --version &> /dev/null || spark-shell --master local <<< ":q" 2>&1 | grep "version"; then
    echo "Spark (spark-submit) sembra accessibile."
    # Mostra la versione se possibile in modo pulito
    echo "Versione di Spark (dal banner di spark-shell):"
    spark-shell --master local <<< ":q" 2>&1 | grep "version" | head -n 1
else
    echo "AVVISO: Impossibile confermare pienamente la funzionalità di spark-submit o ottenere la versione in modo pulito."
    echo "Tuttavia, 'spark-submit' è stato trovato. Il benchmark procederà."
    echo "Se il benchmark fallisce, controlla la tua installazione di Spark."
fi


# Esegui il benchmark con diverse dimensioni di dataset e motori Spark
echo ""
echo "--------------------------------------------------------------------"
echo "Avvio benchmark di scalabilità Spark (Core e SQL)..."
echo "Dataset principale: $DATASET"
echo "Directory dei risultati: $RESULTS_DIR"
echo "--------------------------------------------------------------------"
echo ""

# Assicurati che il percorso di src/spark/run_benchmark.py sia corretto
# rispetto a dove esegui questo script.
# Se questo script è nella root del progetto, e run_benchmark.py è in src/spark/, allora va bene.
PYTHON_BENCHMARK_SCRIPT="src/spark/run_benchmark.py"

if [ ! -f "$PYTHON_BENCHMARK_SCRIPT" ]; then
    echo "ERRORE: Script di benchmark Python non trovato: $PYTHON_BENCHMARK_SCRIPT"
    echo "Assicurati che il percorso sia corretto e che lo script esista."
    exit 1
fi

# Esegui con dimensioni del dataset crescenti
# python3 o l'interprete Python del tuo ambiente virtuale
python3 "$PYTHON_BENCHMARK_SCRIPT" \
    --input "$DATASET" \
    --output "$RESULTS_DIR" \
    --jobs job1 job2 \
    --engines core sql \
    --sizes 0.1 1.0 \
    --use-existing-samples # Rimuovi questa opzione se vuoi sempre rigenerare i campioni

echo ""
echo "--------------------------------------------------------------------"
echo "Benchmark completato."
echo "I risultati sono disponibili in: $(pwd)/$RESULTS_DIR"
echo "Consulta i file spark_benchmark_report_*.txt e spark_benchmark_results_*.json per i dettagli."
echo "Per una visualizzazione grafica, puoi usare gli script nella directory utils/"
echo "--------------------------------------------------------------------"

# Se vuoi confrontare direttamente con MapReduce, decommentare e adattare i percorsi:
# echo ""
# echo "Confronto tra le implementazioni Spark e MapReduce su dataset di dimensione massima:"
# python3 src/utils/compare_benchmarks.py \
#     --mr-results benchmark_results/mapreduce/mr_benchmark_results_*.json \
#     --spark-results $RESULTS_DIR/spark_benchmark_results_*.json