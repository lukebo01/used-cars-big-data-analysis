#!/bin/bash

# Configura qui il tuo ambiente se necessario
# export SPARK_HOME=...
# export PATH=$SPARK_HOME/bin:$PATH
# export PYSPARK_PYTHON=python3 # o il path del tuo python3

# --- Parametri Input ---
INPUT_SAMPLE_1K="data/samples/used_cars_1k.csv"
# INPUT_SAMPLE_10K="data/samples/used_cars_10k.csv" # Per test di scalabilità
# INPUT_FULL="data/full_dataset_cleaned.csv" # Dataset completo pulito

# --- Directory di Output Base ---
OUTPUT_DIR_MR_BASE="results/mapreduce"
OUTPUT_DIR_SPARK_CORE_BASE="results/spark_core"
OUTPUT_DIR_SPARK_SQL_BASE="results/spark_sql"

# Crea le cartelle di output base se non esistono
mkdir -p $OUTPUT_DIR_MR_BASE
mkdir -p $OUTPUT_DIR_SPARK_CORE_BASE
mkdir -p $OUTPUT_DIR_SPARK_SQL_BASE

# Rendi eseguibili gli script Python (fallo una volta o includilo qui per sicurezza)
chmod +x src/mapreduce/job1/mapper.py src/mapreduce/job1/reducer.py
chmod +x src/mapreduce/job2/mapper.py src/mapreduce/job2/reducer.py
chmod +x src/spark/job1_spark_core.py src/spark/job1_spark_sql.py
chmod +x src/spark/job2_spark_core.py src/spark/job2_spark_sql.py

echo "========================================================"
echo "ESECUZIONE DI TUTTI I JOB DEL PROGETTO BIG DATA"
echo "Dataset di input: $INPUT_SAMPLE_1K"
echo "========================================================"
echo ""

#-------------------------------------------------------------------------------
# JOB 1
#-------------------------------------------------------------------------------
echo ">>> INIZIO ESECUZIONE JOB 1 <<<"
echo "--------------------------------------"

# --- MapReduce Job 1 ---
echo "[Job 1] Avvio MapReduce (Python)..."
OUTPUT_MR_JOB1="$OUTPUT_DIR_MR_BASE/job1_output.txt"
cat $INPUT_SAMPLE_1K | src/mapreduce/job1/mapper.py | \
    sort -t $'\t' -k1,1 -k2,2 | \
    src/mapreduce/job1/reducer.py > $OUTPUT_MR_JOB1
echo "[Job 1] Output MapReduce salvato in $OUTPUT_MR_JOB1"
echo "[Job 1] Prime 5 righe MapReduce:"
head -n 5 $OUTPUT_MR_JOB1
echo "--------------------------------------"

# --- Spark Core Job 1 ---
echo "[Job 1] Avvio Spark Core..."
# Lo script Python ora salva i risultati direttamente nei file specificati
spark-submit src/spark/job1_spark_core.py
echo "[Job 1] Output Spark Core salvato nelle sottocartelle di $OUTPUT_DIR_SPARK_CORE_BASE/"
echo "[Job 1] Controlla i file job1_rdd_output_parts/ e job1_output_singlefile.txt"
echo "--------------------------------------"

# --- Spark SQL Job 1 ---
echo "[Job 1] Avvio Spark SQL (DataFrame API)..."
spark-submit src/spark/job1_spark_sql.py
echo "[Job 1] Output Spark SQL (DataFrame API) salvato nelle sottocartelle di $OUTPUT_DIR_SPARK_SQL_BASE/"
echo "[Job 1] Controlla i file job1_df_output_parts_text/ e job1_output_singlefile.txt"
echo "--------------------------------------"
echo ">>> FINE ESECUZIONE JOB 1 <<<"
echo ""


#-------------------------------------------------------------------------------
# JOB 2
#-------------------------------------------------------------------------------
echo ">>> INIZIO ESECUZIONE JOB 2 <<<"
echo "--------------------------------------"

# --- MapReduce Job 2 ---
echo "[Job 2] Avvio MapReduce (Python)..."
OUTPUT_MR_JOB2="$OUTPUT_DIR_MR_BASE/job2_output.txt"
# Il sort per il reducer del job 2: city (string), year (numeric), price_range (string)
# Il reducer del Job 2 è stato scritto per gestire i dati anche se non perfettamente pre-raggruppati per price_range,
# ma l'ordinamento aiuta e sarebbe necessario per un reducer più tradizionale.
cat $INPUT_SAMPLE_1K | src/mapreduce/job2/mapper.py | \
    sort -t $'\t' -k1,1 -k2,2n -k3,3 | \
    src/mapreduce/job2/reducer.py > $OUTPUT_MR_JOB2
echo "[Job 2] Output MapReduce salvato in $OUTPUT_MR_JOB2"
echo "[Job 2] Prime 5 righe MapReduce:"
head -n 5 $OUTPUT_MR_JOB2
echo "--------------------------------------"

# --- Spark Core Job 2 ---
echo "[Job 2] Avvio Spark Core..."
spark-submit src/spark/job2_spark_core.py
echo "[Job 2] Output Spark Core salvato nelle sottocartelle di $OUTPUT_DIR_SPARK_CORE_BASE/"
echo "[Job 2] Controlla i file job2_rdd_output_parts/ e job2_output_singlefile.txt"
echo "--------------------------------------"

# --- Spark SQL Job 2 ---
echo "[Job 2] Avvio Spark SQL (DataFrame API)..."
spark-submit src/spark/job2_spark_sql.py
echo "[Job 2] Output Spark SQL (DataFrame API) salvato nelle sottocartelle di $OUTPUT_DIR_SPARK_SQL_BASE/"
echo "[Job 2] Controlla i file job2_df_output_parts_text/ e job2_output_singlefile.txt"
echo "--------------------------------------"
echo ">>> FINE ESECUZIONE JOB 2 <<<"
echo ""

echo "========================================================"
echo "ESECUZIONE DI TUTTI I JOB COMPLETATA."
echo "Controlla la cartella 'results' per tutti gli output."
echo "========================================================"