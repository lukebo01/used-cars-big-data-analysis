#!/bin/bash

# Definisci il percorso del dataset
DATASET="data/used_cars_cleaned.csv"

# Crea directory per i risultati dei benchmark se non esiste
RESULTS_DIR="results/mapreduce"
mkdir -p $RESULTS_DIR

# Esegui il benchmark con diverse dimensioni di dataset e numero di nodi
echo "Avvio benchmark di scalabilità MapReduce..."

# Determina il numero di core disponibili
NUM_CORES=$(nproc)
echo "Rilevati $NUM_CORES core del processore"

# Esegui con dimensioni del dataset crescenti
python3 src/mapreduce/run_benchmark.py \
    --input "$DATASET" \
    --output "$RESULTS_DIR" \
    --jobs job1 job2 \
    --sizes 0.01 0.05 0.1 \
    --nodes $NUM_CORES

echo "Benchmark completato. I risultati sono disponibili in $RESULTS_DIR"
echo "Consulta i file benchmark_report_*.txt per i dettagli sulla scalabilità"
