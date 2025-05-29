#!/usr/bin/env python3

import argparse
import json
import os
import time
import psutil
import subprocess
import sys
from datetime import datetime

def create_sample(input_file, output_file, fraction):
    """
    Crea un campione del dataset di input
    """
    import random
    
    # Assicurati che la directory di output esista
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    # Conta il numero totale di righe per poter contare quelle campionate
    total_lines = 0
    sampled_lines = 0
    
    with open(input_file, 'r') as in_f, open(output_file, 'w') as out_f:
        # Copia l'intestazione
        header = next(in_f, None)
        if header:
            out_f.write(header)
        
        # Campiona le righe
        for line in in_f:
            total_lines += 1
            if random.random() <= fraction:
                out_f.write(line)
                sampled_lines += 1
    
    print(f"Campione {fraction*100:.0f}% creato in: {output_file}")
    print(f"  Righe originali: {total_lines}, Righe campionate: {sampled_lines}")
    return sampled_lines

def run_job(job_name, engine, input_file, dataset_size):
    """
    Esegue un job Spark specifico con i parametri forniti
    """
    job_script = f"src/spark/{job_name}_spark_{engine}.py"
    output_dir = f"results/spark_{engine}"
    
    # Crea la directory di output se non esiste
    os.makedirs(output_dir, exist_ok=True)
    
    print(f"Esecuzione di {job_script} con dimensione dataset {dataset_size}...")
    
    # ------- MODIFICA: Misura la memoria del sistema prima dell'esecuzione -------
    memory_info_before = psutil.virtual_memory()
    available_before = memory_info_before.available
    cached_before = memory_info_before.cached if hasattr(memory_info_before, 'cached') else 0
    
    start_time = time.time()
    
    # Esecuzione del job
    command = [
        "python3", 
        job_script, 
        "--input", input_file,
        "--output_dir", output_dir,
        "--dataset_size", str(dataset_size)
    ]
    
    try:
        proc = subprocess.run(command, check=True, capture_output=True, text=True)
        stdout = proc.stdout
        stderr = proc.stderr
    except subprocess.CalledProcessError as e:
        print(f"Errore nell'esecuzione di {job_script}: {e}")
        print(f"STDOUT: {e.stdout}")
        print(f"STDERR: {e.stderr}")
        return None
    
    end_time = time.time()
    
    # ------- MODIFICA: Misura la memoria del sistema dopo l'esecuzione -------
    time.sleep(1)  # Attendi un secondo per permettere al sistema di aggiornare le statistiche di memoria
    memory_info_after = psutil.virtual_memory()
    available_after = memory_info_after.available
    cached_after = memory_info_after.cached if hasattr(memory_info_after, 'cached') else 0
    
    # Calcola memoria utilizzata (disponibile prima - disponibile dopo + delta cache)
    # Questo è un modo migliore per stimare l'uso totale della memoria di un processo Spark
    delta_available = available_before - available_after
    delta_cached = cached_after - cached_before
    memory_usage = (delta_available + delta_cached) / (1024 * 1024)  # in MB
    
    # Imposta a un valore minimo positivo se la misurazione è negativa o troppo piccola
    memory_usage = max(1.0, memory_usage)
    
    # Calcola i record e dimensioni del file di input
    record_count = 0
    with open(input_file, 'r') as f:
        # Salta l'intestazione
        next(f, None)
        record_count = sum(1 for _ in f)
    
    # Calcola la dimensione dell'input in MB
    input_size_mb = os.path.getsize(input_file) / (1024 * 1024)
    
    # Calcola metriche aggiuntive
    execution_time = end_time - start_time
    records_per_sec = record_count / execution_time if execution_time > 0 else 0
    mb_per_sec = input_size_mb / execution_time if execution_time > 0 else 0
    
    print(f"  Completato in {execution_time:.2f} secondi")
    print(f"  Dimensione input: {input_size_mb:.2f} MB, Records: {record_count}")
    print(f"  Memoria utilizzata: {memory_usage:.2f} MB")
    
    return {
        "job": job_name,
        "engine": engine,
        "dataset_size": dataset_size,
        "execution_time": execution_time,
        "memory_usage_mb": memory_usage,
        "input_size_mb": input_size_mb,
        "record_count": record_count,
        "records_per_sec": records_per_sec,
        "mb_per_sec": mb_per_sec
    }

def run_benchmark(args):
    """
    Esegue tutti i job specificati con diverse dimensioni del dataset
    """
    results = []
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # ------- MODIFICA: Gestione migliorata dei campioni -------
    # Crea una directory per i campioni se non esiste
    samples_dir = "data/samples"
    os.makedirs(samples_dir, exist_ok=True)
    
    # Dizionario per tenere traccia dei campioni creati/trovati
    sample_files = {}
    
    # Prepara i campioni prima di iniziare i benchmark
    print("\nGestione dei campioni di dati:")
    
    # Sempre mappare il campione 100% al file di input originale
    sample_files[1.0] = args.input
    print(f"  • Dataset completo (100%): {args.input}")
    
    # Gestisci gli altri campioni (minori del 100%)
    for size in [s for s in args.sizes if s < 1.0]:
        # Nome del file campione standard: used_cars_10pct.csv, used_cars_25pct.csv, ecc.
        sample_filename = f"used_cars_{int(size*100)}pct.csv"
        sample_path = os.path.join(samples_dir, sample_filename)
        
        if os.path.exists(sample_path) and args.use_existing_samples:
            # Usa il campione esistente
            print(f"  • Campione {int(size*100)}% trovato: {sample_path}")
            sample_files[size] = sample_path
        elif os.path.exists(sample_path) and not args.use_existing_samples:
            # Rimuovi il campione esistente per ricrearlo fresco
            print(f"  • Ricreazione campione {int(size*100)}%...")
            os.remove(sample_path)
            create_sample(args.input, sample_path, size)
            sample_files[size] = sample_path
        elif not os.path.exists(sample_path):
            if not args.use_existing_samples:
                # Crea un nuovo campione se non esiste
                print(f"  • Creazione campione {int(size*100)}%...")
                create_sample(args.input, sample_path, size)
                sample_files[size] = sample_path
            else:
                # Vogliamo usare campioni esistenti ma questo non esiste
                print(f"  • AVVISO: Campione {int(size*100)}% richiesto ma non trovato.")
                print(f"    Usa --no-use-existing-samples per generare i campioni mancanti.")
                # Non impostiamo sample_files[size] qui, così cadrà nel fallback al dataset completo
    
    print("\nInizio esecuzione dei job di benchmark...")
                
    for job in args.jobs:
        for engine in args.engines:
            for size in args.sizes:
                # Determina il file di input corretto in base alla dimensione
                if size in sample_files:
                    # Abbiamo un campione valido per questa dimensione
                    input_file = sample_files[size]
                    actual_size = size
                else:
                    # Fallback al dataset completo con un avviso
                    input_file = args.input
                    actual_size = 1.0  # La dimensione effettiva è 100%
                    print(f"\nAVVISO: Campione {int(size*100)}% non disponibile, uso il dataset completo")
                
                print(f"\nEsecuzione {job} con engine {engine} e dimensione {size}")
                result = run_job(job, engine, input_file, actual_size)
                
                if result:
                    # Correggi il campo dataset_size per riflettere la dimensione richiesta originale
                    # anche se è stato usato un fallback
                    result["requested_dataset_size"] = size
                    results.append(result)
    
    # Salva i risultati in un file JSON
    results_file = os.path.join(args.output, f"spark_benchmark_results_{timestamp}.json")
    with open(results_file, 'w') as f:
        json.dump(results, f, indent=2)
    
    print(f"\nRisultati salvati in: {results_file}")
    
    # Genera un report in formato testo
    report_file = os.path.join(args.output, f"spark_benchmark_report_{timestamp}.txt")
    with open(report_file, 'w') as f:
        f.write(f"Spark Scalability Benchmark Report\n")
        f.write(f"================================\n\n")
        
        f.write(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Input file: {args.input}\n\n")
        
        f.write("Summary by Job, Engine and Dataset Size:\n")
        f.write("-------------------------------------\n\n")
        
        for job in args.jobs:
            for engine in args.engines:
                job_engine_results = [r for r in results if r["job"] == job and r["engine"] == engine]
                
                # Se non ci sono risultati per questa combinazione, salta
                if not job_engine_results:
                    continue
                
                f.write(f"Job: {job}, Engine: {engine}\n")
                f.write("| Dataset Size | Input Size (MB) | Records | Time (s) | Records/sec | MB/sec | Memory (MB) |\n")
                f.write("|--------------|----------------|---------|----------|-------------|--------|-------------|\n")
                
                # Ordina i risultati per dimensione del dataset
                job_engine_results.sort(key=lambda r: r["dataset_size"])
                
                for result in job_engine_results:
                    f.write(f"| {result['dataset_size']*100:>10.2f}% | {result['input_size_mb']:>14.2f} | {result['record_count']:>7d} | ")
                    f.write(f"{result['execution_time']:>8.2f} | {result['records_per_sec']:>11.2f} | {result['mb_per_sec']:>6.2f} | ")
                    f.write(f"{result['memory_usage_mb']:>11.2f} |\n")
                
                f.write("\n")
        
    print(f"Report salvato in: {report_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Benchmark per job Spark")
    parser.add_argument("--input", required=True, help="File CSV di input principale")
    parser.add_argument("--output", required=True, help="Directory per i risultati del benchmark")
    parser.add_argument("--jobs", nargs="+", default=["job1", "job2"], help="Job da eseguire")
    parser.add_argument("--engines", nargs="+", default=["core", "sql"], help="Engine Spark da testare")
    parser.add_argument("--sizes", nargs="+", type=float, default=[0.01, 0.05, 0.1, 0.25, 0.5, 1.0], 
                        help="Dimensioni del dataset da testare come frazione (es: 0.01 = 1%)")
    parser.add_argument("--use-existing-samples", action="store_true", 
                        help="Usa campioni esistenti del dataset invece di generarli")
    parser.add_argument("--no-use-existing-samples", action="store_false", dest="use_existing_samples",
                        help="Forza la rigenerazione dei campioni anche se esistono")
    
    args = parser.parse_args()
    
    # Crea la directory di output se non esiste
    os.makedirs(args.output, exist_ok=True)
    
    run_benchmark(args)
