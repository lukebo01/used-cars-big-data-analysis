#!/bin/bash

JOB_NAME=$1
INPUT_PATH=$2 # es: data/samples/used_cars_10k.csv o hdfs:///user/tuoutente/input_10k
OUTPUT_PATH=$3 # es: results/mr_job1_output_10k o hdfs:///user/tuoutente/output_mr_job1_10k

MAPPER_SCRIPT="src/mapreduce/${JOB_NAME}/mapper.py"
REDUCER_SCRIPT="src/mapreduce/${JOB_NAME}/reducer.py"

# Per HDFS (quando sarà su AWS)
# HADOOP_STREAMING_JAR="/path/to/your/hadoop-streaming.jar"
# hdfs dfs -rm -r -skipTrash "$OUTPUT_PATH" # Rimuovi output precedente

# Esecuzione locale (simulazione)
# Per test locali, puoi semplicemente usare cat e sort
echo "Running MapReduce job ${JOB_NAME} locally..."
cat "$INPUT_PATH" | python3 "$MAPPER_SCRIPT" | sort -k1,1 | python3 "$REDUCER_SCRIPT" > "${OUTPUT_PATH}.txt"
echo "Local MR job ${JOB_NAME} finished. Output in ${OUTPUT_PATH}.txt"

# Esempio con Hadoop Streaming (da adattare per il tuo ambiente)
# Nota: gli script mapper/reducer devono essere accessibili dal cluster
# yarn jar "$HADOOP_STREAMING_JAR" \
#     -D mapreduce.job.name="MR_${JOB_NAME}" \
#     -files "${MAPPER_SCRIPT},${REDUCER_SCRIPT}" \
#     -mapper "python3 $(basename ${MAPPER_SCRIPT})" \
#     -reducer "python3 $(basename ${REDUCER_SCRIPT})" \
#     -input "$INPUT_PATH" \
#     -output "$OUTPUT_PATH"