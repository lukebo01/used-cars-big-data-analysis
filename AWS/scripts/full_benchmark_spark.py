# Modifica il benchmark Spark per includere tutti i controlli del MapReduce
#cat > full_benchmark_spark.py << 'EOF'
#!/usr/bin/env python3
"""
Benchmark completo Spark Core per AWS EMR
Testa Job1 e Job2 Spark con diversi sample e configurazioni cluster
Include tutti i controlli del benchmark MapReduce
"""

import subprocess
import time
import json
import os
from datetime import datetime

# Configurazione
BUCKET = "used-cars-big-data-analysis-1748698020"
DATASET_FILE = "used_cars_filtered.csv"
REGION = "us-east-1"

def run_command(cmd):
    """Esegue un comando e restituisce l'output"""
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Errore nel comando: {cmd}")
        print(f"Errore: {result.stderr}")
        return None
    return result.stdout.strip()

def terminate_all_active_clusters():
    """Termina tutti i cluster EMR attivi per evitare conflitti"""
    print("Verificando cluster attivi...")
    
    cmd = 'aws emr list-clusters --active --query "Clusters[].Id" --output text'
    active_clusters = run_command(cmd)
    
    if not active_clusters or active_clusters == "None":
        print("Nessun cluster attivo trovato")
        return
    
    cluster_ids = active_clusters.split()
    print(f"Trovati {len(cluster_ids)} cluster attivi: {cluster_ids}")
    
    for cluster_id in cluster_ids:
        print(f"Terminando cluster {cluster_id}...")
        result = run_command(f"aws emr terminate-clusters --cluster-ids {cluster_id}")
        if result is not None:
            print(f"✓ Cluster {cluster_id} terminato")
        else:
            print(f"✗ Errore terminando cluster {cluster_id}")
    
    print("Aspettando che tutti i cluster siano terminati...")
    max_wait = 10
    wait_count = 0
    
    while wait_count < max_wait:
        cmd = 'aws emr list-clusters --active --query "Clusters[].Id" --output text'
        remaining = run_command(cmd)
        
        if not remaining or remaining == "None":
            print("✓ Tutti i cluster sono stati terminati")
            break
        
        print(f"Aspettando terminazione... ({wait_count+1}/{max_wait})")
        time.sleep(60)
        wait_count += 1
    
    if wait_count >= max_wait:
        print("⚠️  Timeout aspettando terminazione cluster")

def create_cluster(instance_count):
    """Crea un nuovo cluster EMR per Spark"""
    print(f"Creazione cluster Spark con {instance_count} istanze...")
    
    cmd = f'aws emr create-cluster --name "spark-benchmark-{instance_count}nodes" --release-label emr-6.4.0 --instance-type m5.xlarge --instance-count {instance_count} --applications Name=Hadoop Name=Spark --ec2-attributes KeyName=vockey,InstanceProfile=EMR_EC2_DefaultRole --service-role EMR_DefaultRole --log-uri s3://{BUCKET}/logs/ --enable-debugging --query "ClusterId" --output text'
    
    cluster_id = run_command(cmd)
    if not cluster_id:
        print("Errore nella creazione del cluster")
        return None
    
    print(f"Cluster creato: {cluster_id}")
    
    print("Aspettando che il cluster sia pronto...")
    max_attempts = 25
    attempts = 0
    
    while attempts < max_attempts:
        state_cmd = f'aws emr describe-cluster --cluster-id {cluster_id} --query "Cluster.Status.State" --output text'
        state = run_command(state_cmd)
        print(f"Stato cluster: {state} (tentativo {attempts+1}/{max_attempts})")
        
        if state == "WAITING":
            print("Cluster pronto!")
            return cluster_id
        elif state in ["TERMINATED", "TERMINATING", "TERMINATED_WITH_ERRORS", "FAILED"]:
            error_cmd = f'aws emr describe-cluster --cluster-id {cluster_id} --query "Cluster.Status.StateChangeReason" --output json'
            error = run_command(error_cmd)
            print(f"Cluster fallito: {error}")
            return None
        
        attempts += 1
        time.sleep(60)
    
    print("Timeout nella creazione del cluster")
    return None

def check_existing_samples():
    """Controlla se i samples esistono già su S3 (stesso del MapReduce)"""
    print("Controllo samples esistenti...")
    
    samples = [0.01, 0.05, 0.1, 0.5]
    existing_samples = {}
    missing_samples = []
    
    for sample_rate in samples:
        sample_filename = f"sample_{int(sample_rate*100)}pct.csv"
        
        cmd = f'aws s3 ls s3://{BUCKET}/data/{sample_filename}'
        result = run_command(cmd)
        
        if result:
            print(f"✓ Sample {sample_rate*100}% già esistente")
            existing_samples[sample_rate] = sample_filename
        else:
            print(f"✗ Sample {sample_rate*100}% mancante")
            missing_samples.append(sample_rate)
    
    return existing_samples, missing_samples

def create_sampling_scripts():
    """Crea gli script di sampling localmente e li carica su S3"""
    print("Creando script di sampling...")
    
    mapper_content = '''#!/usr/bin/env python3
import sys
import random

sample_rate = float(sys.argv[1]) if len(sys.argv) > 1 else 0.1
random.seed(42)

line_count = 0
for line in sys.stdin:
    line_count += 1
    if line_count == 1:
        print(line.strip())
    elif random.random() <= sample_rate:
        print(line.strip())
'''
    
    reducer_content = '''#!/usr/bin/env python3
import sys

for line in sys.stdin:
    print(line.strip())
'''
    
    with open("sampling_mapper.py", "w") as f:
        f.write(mapper_content)
    
    with open("sampling_reducer.py", "w") as f:
        f.write(reducer_content)
    
    run_command(f"aws s3 cp sampling_mapper.py s3://{BUCKET}/scripts/sampling/")
    run_command(f"aws s3 cp sampling_reducer.py s3://{BUCKET}/scripts/sampling/")
    
    os.remove("sampling_mapper.py")
    os.remove("sampling_reducer.py")
    
    print("Script di sampling caricati su S3")

def create_missing_samples_on_emr(missing_samples):
    """Crea i samples mancanti usando MapReduce su EMR (stesso del MapReduce)"""
    if not missing_samples:
        print("Tutti i samples esistono già!")
        return {}
    
    print(f"Creazione {len(missing_samples)} samples su EMR...")
    
    create_sampling_scripts()
    
    sampling_cluster = create_cluster(3)
    if not sampling_cluster:
        print("Impossibile creare cluster per sampling!")
        return {}
    
    created_samples = {}
    
    try:
        for sample_rate in missing_samples:
            sample_name = f"sample_{int(sample_rate*100)}pct.csv"
            print(f"Creando sample {sample_rate*100}% su EMR...")
            
            run_command(f"aws s3 rm s3://{BUCKET}/temp/sampling_{int(sample_rate*100)}pct/ --recursive")
            
            step_config = f'[{{"Name": "Create-Sample-{int(sample_rate*100)}pct", "ActionOnFailure": "CONTINUE", "Jar": "command-runner.jar", "Args": ["hadoop-streaming", "-files", "s3://{BUCKET}/scripts/sampling/sampling_mapper.py,s3://{BUCKET}/scripts/sampling/sampling_reducer.py", "-mapper", "python3 sampling_mapper.py {sample_rate}", "-reducer", "python3 sampling_reducer.py", "-input", "s3://{BUCKET}/data/{DATASET_FILE}", "-output", "s3://{BUCKET}/temp/sampling_{int(sample_rate*100)}pct/"]}}]'
            
            cmd = f'aws emr add-steps --cluster-id {sampling_cluster} --steps \'{step_config}\''
            result = run_command(cmd)
            
            if not result:
                print(f"Errore nella creazione del sample {sample_rate*100}%")
                continue
                
            step_data = json.loads(result)
            step_id = step_data['StepIds'][0]
            
            print(f"Monitoraggio sample {sample_rate*100}%...")
            while True:
                state_cmd = f'aws emr describe-step --cluster-id {sampling_cluster} --step-id {step_id} --query "Step.Status.State" --output text'
                state = run_command(state_cmd)
                
                if state == "COMPLETED":
                    break
                elif state == "FAILED":
                    print(f"Sample {sample_rate*100}% fallito!")
                    break
                
                time.sleep(30)
            
            if state == "COMPLETED":
                run_command(f"aws s3 cp s3://{BUCKET}/temp/sampling_{int(sample_rate*100)}pct/part-00000 s3://{BUCKET}/data/{sample_name}")
                run_command(f"aws s3 rm s3://{BUCKET}/temp/sampling_{int(sample_rate*100)}pct/ --recursive")
                
                created_samples[sample_rate] = sample_name
                print(f"✓ Sample {sample_rate*100}% creato")
    
    finally:
        print("Terminando cluster di sampling...")
        run_command(f"aws emr terminate-clusters --cluster-ids {sampling_cluster}")
    
    return created_samples

def get_file_info(filename):
    """Ottiene informazioni su un file S3"""
    cmd = f'aws s3api head-object --bucket {BUCKET} --key data/{filename}'
    result = run_command(cmd)
    
    if result:
        data = json.loads(result)
        size_bytes = data['ContentLength']
        size_mb = size_bytes / (1024 * 1024)
        return size_mb, size_bytes
    return 0, 0

def estimate_records(size_mb):
    """Stima il numero di record"""
    avg_bytes_per_record = 150
    total_bytes = size_mb * 1024 * 1024
    estimated_records = int(total_bytes / avg_bytes_per_record)
    return estimated_records

def run_spark_job_on_emr(cluster_id, job_name, input_file, output_folder, job_type):
    """Esegue un job Spark su EMR"""
    
    run_command(f"aws s3 rm s3://{BUCKET}/output/{output_folder}/ --recursive")
    
    if job_type == "job1":
        script_path = f"s3://{BUCKET}/scripts/job1/spark/job1_spark.py"
    else:
        script_path = f"s3://{BUCKET}/scripts/job2/spark/job2_spark.py"
    
    input_path = f"s3://{BUCKET}/data/{input_file}"
    output_path = f"s3://{BUCKET}/output/{output_folder}/"
    
    step_config = f'''[{{
      "Name": "{job_name}",
      "ActionOnFailure": "CONTINUE",
      "Jar": "command-runner.jar",
      "Args": [
        "spark-submit",
        "--deploy-mode", "cluster",
        "--conf", "spark.sql.adaptive.enabled=true",
        "--conf", "spark.sql.adaptive.coalescePartitions.enabled=true",
        "{script_path}",
        "{input_path}",
        "{output_path}"
      ]
    }}]'''
    
    print(f"Avvio {job_name} Spark con input {input_file}")
    start_time = time.time()
    
    cmd = f'aws emr add-steps --cluster-id {cluster_id} --steps \'{step_config}\''
    result = run_command(cmd)
    
    if not result:
        return None
    
    step_data = json.loads(result)
    step_id = step_data['StepIds'][0]
    
    while True:
        state_cmd = f'aws emr describe-step --cluster-id {cluster_id} --step-id {step_id} --query "Step.Status.State" --output text'
        state = run_command(state_cmd)
        
        if state == "COMPLETED":
            break
        elif state == "FAILED":
            print(f"Job {job_name} Spark fallito!")
            return None
        
        time.sleep(30)
    
    end_time = time.time()
    execution_time = end_time - start_time
    
    print(f"Job {job_name} Spark completato in {execution_time:.2f} secondi")
    return execution_time

def generate_spark_report(results):
    """Genera il report per Spark (con analisi scalabilità)"""
    report_filename = f"spark_benchmark_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    
    with open(report_filename, "w") as f:
        f.write("Spark Core Scalability Benchmark Report\n")
        f.write("=======================================\n\n")
        f.write(f"Date: {datetime.now()}\n")
        f.write(f"Input file: {DATASET_FILE}\n")
        f.write(f"Bucket: s3://{BUCKET}\n\n")
        
        nodes_groups = {}
        for result in results:
            nodes = result['nodes']
            if nodes not in nodes_groups:
                nodes_groups[nodes] = []
            nodes_groups[nodes].append(result)
        
        for nodes in sorted(nodes_groups.keys()):
            f.write(f"Summary for {nodes} nodes (Spark):\n")
            f.write("----------------------------------\n\n")
            
            f.write("Job: job1_spark\n")
            f.write("| Dataset Size | Input Size (MB) | Records | Time (s) | Records/sec | MB/sec |\n")
            f.write("|--------------|----------------|---------|----------|-------------|--------|\n")
            
            job1_results = [r for r in nodes_groups[nodes] if r['job1_time'] is not None]
            job1_results.sort(key=lambda x: x['sample_rate'])
            
            for result in job1_results:
                dataset_pct = f"{result['sample_rate']*100:8.2f}%"
                input_mb = f"{result['size_mb']:10.2f}"
                records = f"{result['records']:7d}"
                time_s = f"{result['job1_time']:8.2f}"
                records_sec = f"{result['records']/result['job1_time']:11.2f}" if result['job1_time'] > 0 else "0.00"
                mb_sec = f"{result['size_mb']/result['job1_time']:6.2f}" if result['job1_time'] > 0 else "0.00"
                
                f.write(f"|{dataset_pct} |{input_mb} |{records} |{time_s} |{records_sec} |{mb_sec} |\n")
            
            f.write("\n")
            
            f.write("Job: job2_spark\n")
            f.write("| Dataset Size | Input Size (MB) | Records | Time (s) | Records/sec | MB/sec |\n")
            f.write("|--------------|----------------|---------|----------|-------------|--------|\n")
            
            job2_results = [r for r in nodes_groups[nodes] if r['job2_time'] is not None]
            job2_results.sort(key=lambda x: x['sample_rate'])
            
            for result in job2_results:
                dataset_pct = f"{result['sample_rate']*100:8.2f}%"
                input_mb = f"{result['size_mb']:10.2f}"
                records = f"{result['records']:7d}"
                time_s = f"{result['job2_time']:8.2f}"
                records_sec = f"{result['records']/result['job2_time']:11.2f}" if result['job2_time'] > 0 else "0.00"
                mb_sec = f"{result['size_mb']/result['job2_time']:6.2f}" if result['job2_time'] > 0 else "0.00"
                
                f.write(f"|{dataset_pct} |{input_mb} |{records} |{time_s} |{records_sec} |{mb_sec} |\n")
            
            f.write("\n\n")
        
        # Analisi scalabilità (stesso del MapReduce)
        f.write("Scalability Analysis:\n")
        f.write("---------------------\n\n")
        
        for nodes in sorted(nodes_groups.keys()):
            f.write(f"Cluster with {nodes} nodes:\n")
            
            node_results = nodes_groups[nodes]
            node_results.sort(key=lambda x: x['sample_rate'])
            
            if len(node_results) >= 2:
                smallest = node_results[0]
                largest = node_results[-1]
                
                data_factor = largest['sample_rate'] / smallest['sample_rate']
                
                if smallest['job1_time'] and largest['job1_time']:
                    time1_factor = largest['job1_time'] / smallest['job1_time']
                    scaling1 = "SUB-linearly (good)" if time1_factor < data_factor else "SUPER-linearly (poor)"
                    f.write(f"Job1: Dataset increased {data_factor:.2f}x, time increased {time1_factor:.2f}x - scales {scaling1}\n")
                
                if smallest['job2_time'] and largest['job2_time']:
                    time2_factor = largest['job2_time'] / smallest['job2_time']
                    scaling2 = "SUB-linearly (good)" if time2_factor < data_factor else "SUPER-linearly (poor)"
                    f.write(f"Job2: Dataset increased {data_factor:.2f}x, time increased {time2_factor:.2f}x - scales {scaling2}\n")
            
            f.write("\n")
    
    print(f"Report Spark generato: {report_filename}")
    return report_filename

def main():
    print("=== BENCHMARK SPARK COMPLETO ===")
    print(f"Data: {datetime.now()}")
    print(f"Bucket: {BUCKET}")
    
    # TUTTI I CONTROLLI DEL MAPREDUCE:
    
    # 1. Termina cluster esistenti
    terminate_all_active_clusters()
    
    # 2. Controlla samples esistenti
    existing_samples, missing_samples = check_existing_samples()
    
    # 3. Crea samples mancanti usando EMR
    created_samples = create_missing_samples_on_emr(missing_samples)
    
    # 4. Combina samples esistenti e creati
    all_samples = {**existing_samples, **created_samples}
    all_samples[1.0] = DATASET_FILE
    print(f"✓ Dataset completo (100%): {DATASET_FILE}")
    
    if not all_samples:
        print("Nessun sample disponibile!")
        return
    
    node_counts = [3, 5, 7]
    results = []
    
    for node_count in node_counts:
        print(f"\n{'='*50}")
        print(f"TESTING SPARK CON {node_count} NODI")
        print(f"{'='*50}")

        # 5. Termina cluster prima di ogni nuovo test
        terminate_all_active_clusters()
        
        cluster_id = create_cluster(node_count)
        if not cluster_id:
            print(f"Impossibile creare cluster con {node_count} nodi")
            continue
        
        try:
            for sample_rate in sorted(all_samples.keys()):
                sample_file = all_samples[sample_rate]
                print(f"\n--- Testing Spark sample {sample_rate*100}% ---")
                
                size_mb, size_bytes = get_file_info(sample_file)
                records = estimate_records(size_mb)
                
                print(f"File: {sample_file} ({size_mb:.2f} MB, ~{records} records)")
                
                time1 = run_spark_job_on_emr(
                    cluster_id, 
                    f"Job1Spark-{sample_rate*100}pct-{node_count}nodes", 
                    sample_file, 
                    f"spark/job1-{sample_rate*100}pct-{node_count}nodes", 
                    "job1"
                )
                
                time2 = run_spark_job_on_emr(
                    cluster_id, 
                    f"Job2Spark-{sample_rate*100}pct-{node_count}nodes", 
                    sample_file, 
                    f"spark/job2-{sample_rate*100}pct-{node_count}nodes", 
                    "job2"
                )
                
                result = {
                    'nodes': node_count,
                    'sample_rate': sample_rate,
                    'sample_file': sample_file,
                    'size_mb': size_mb,
                    'size_bytes': size_bytes,
                    'records': records,
                    'job1_time': time1,
                    'job2_time': time2
                }
                results.append(result)
                
                print(f"Risultati Spark per {sample_file} su {node_count} nodi:")
                print(f"  Job1: {time1:.2f}s ({records/time1:.2f} rec/s)" if time1 else "  Job1: FAILED")
                print(f"  Job2: {time2:.2f}s ({records/time2:.2f} rec/s)" if time2 else "  Job2: FAILED")
        
        finally:
            # 6. Termina sempre il cluster alla fine
            print(f"Terminando cluster {cluster_id}...")
            run_command(f"aws emr terminate-clusters --cluster-ids {cluster_id}")
    
    report_file = generate_spark_report(results)
    
    print(f"\n{'='*50}")
    print("BENCHMARK SPARK COMPLETATO!")
    print(f"Report salvato: {report_file}")
    print(f"{'='*50}")

    s3_report_path = f"s3://{BUCKET}/reports/{report_file}"
    run_command(f"aws s3 cp {report_file} {s3_report_path}")
    print(f"Report caricato anche su: {s3_report_path}")

if __name__ == "__main__":
    main()
#EOF