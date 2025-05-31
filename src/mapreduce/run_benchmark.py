#!/usr/bin/env python3

import os
import time
import subprocess
import argparse
import json
from datetime import datetime
import multiprocessing
import os
import random
import shutil

def create_sample_datasets(input_csv, output_dir, sizes=[0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 1.0], random_seed=42):
    """
    Crea campioni del dataset di diverse dimensioni dal CSV originale senza usare pandas,
    utilizzando un campionamento probabilistico riga per riga.
    Il numero di righe nel campione sarà approssimativo.
    Simula il random_state di pandas usando random.seed().
    """
    print(f"Creating sample datasets in {output_dir}...")
    os.makedirs(output_dir, exist_ok=True)

    # Imposta il seed per la riproducibilità, simile a random_state in pandas
    random.seed(random_seed)

    # Passo 1: Leggere l'header e contare il numero totale di righe DATI
    header_line = None
    total_data_rows = 0
    try:
        with open(input_csv, 'r', encoding='utf-8') as f_count:
            header_line = f_count.readline() # Legge la prima riga (header)
            if not header_line:
                print(f"Error: Input CSV '{input_csv}' is empty or header could not be read.")
                return {}
            # Assicurarsi che l'header finisca con newline se non è l'unica riga
            # header_line = header_line.rstrip('\r\n') + '\n' # Opzionale, readline() di solito la include

            for _ in f_count: # Conta le righe rimanenti (righe dati)
                total_data_rows += 1
    except FileNotFoundError:
        print(f"Error: Input CSV '{input_csv}' not found.")
        return {}
    except Exception as e:
        print(f"Error reading or counting lines in '{input_csv}': {e}")
        return {}

    if total_data_rows == 0:
        print(f"Info: '{input_csv}' contains a header and {total_data_rows} data rows. "
              f"Sampled files for fractions < 1.0 will also contain only the header.")

    sample_paths = {}
    for size_fraction in sizes:
        output_file = os.path.join(output_dir, f"sample_{int(size_fraction*100)}pct.csv")
        
        actual_data_rows_written = 0
        log_message = ""

        if size_fraction >= 1.0:
            # Usa il dataset completo (copia il file originale)
            try:
                shutil.copy2(input_csv, output_file)
                actual_data_rows_written = total_data_rows # Numero di righe DATI
                log_message = (f"Created {output_file}: {actual_data_rows_written} data rows "
                               f"(full dataset from {total_data_rows} original data rows).")
            except Exception as e:
                print(f"Error copying '{input_csv}' to '{output_file}': {e}")
                continue # Salta al prossimo size_fraction
        else:
            # Crea campione casuale riga per riga
            try:
                with open(input_csv, 'r', encoding='utf-8') as in_f, \
                     open(output_file, 'w', encoding='utf-8') as out_f:
                    
                    # Scrivi l'header (già letto e memorizzato in header_line)
                    out_f.write(header_line)
                    
                    # Salta l'header nel file di input per questa iterazione di lettura
                    # (next(in_f) lo farebbe, ma dato che riapriamo il file, lo facciamo qui)
                    _ = in_f.readline() # Consuma la riga dell'header da in_f
                    
                    # Campiona le righe di dati
                    for line in in_f:
                        if random.random() <= size_fraction:
                            out_f.write(line)
                            actual_data_rows_written += 1
                
                log_message = (f"Created {output_file}: {actual_data_rows_written} data rows "
                               f"({size_fraction*100:.0f}% sample from {total_data_rows} original data rows).")

            except Exception as e:
                print(f"Error creating sample '{output_file}': {e}")
                continue # Salta al prossimo size_fraction
            
        sample_paths[size_fraction] = output_file
        print(log_message)
        
    return sample_paths

def run_job(job_name, input_file, output_dir, num_nodes=2):
    """Run a MapReduce job with timing and track resource usage"""
    job_output_dir = os.path.join(output_dir, f"{job_name}_{os.path.basename(input_file).split('.')[0]}")
    os.makedirs(job_output_dir, exist_ok=True)
    
    # Get the directory where the current script is located
    current_dir = os.path.dirname(os.path.abspath(__file__))
    mapper_path = os.path.join(current_dir, job_name, "mapper.py")
    reducer_path = os.path.join(current_dir, job_name, "reducer.py")
    
    # Make mapper and reducer executable
    os.chmod(mapper_path, 0o755)
    os.chmod(reducer_path, 0o755)
    
    # Execute with timing
    start_time = time.time()
    
    # Create temporary files for intermediate results
    temp_files = []
    for i in range(num_nodes):
        temp_files.append(os.path.join(job_output_dir, f"temp_mapper_output_{i}.txt"))
    
    # Split input file for parallel processing
    line_count = sum(1 for _ in open(input_file, 'r'))
    lines_per_node = line_count // num_nodes
    
    # Define function for parallel mapping
    def run_mapper(node_id, start_line, end_line, output_file):
        cmd = f"sed -n '{start_line},{end_line}p' {input_file} | {mapper_path} > {output_file}"
        return subprocess.call(cmd, shell=True)
    
    # Run mappers in parallel
    processes = []
    for i in range(num_nodes):
        start_line = i * lines_per_node + 1
        end_line = (i + 1) * lines_per_node if i < num_nodes - 1 else line_count
        p = multiprocessing.Process(
            target=run_mapper,
            args=(i, start_line, end_line, temp_files[i])
        )
        processes.append(p)
        p.start()
    
    # Wait for all mappers to complete
    for p in processes:
        p.join()
    
    # Combine and sort mapper outputs
    combined_mapper_output = os.path.join(job_output_dir, "combined_mapper_output.txt")
    cmd = f"cat {' '.join(temp_files)} | sort > {combined_mapper_output}"
    subprocess.call(cmd, shell=True)
    
    # Run the reducer
    output_file = os.path.join(job_output_dir, "output.txt")
    cmd = f"cat {combined_mapper_output} | {reducer_path} > {output_file}"
    subprocess.call(cmd, shell=True)
    
    end_time = time.time()
    execution_time = end_time - start_time
    
    # Clean up temporary files
    for f in temp_files:
        os.remove(f)
    os.remove(combined_mapper_output)
    
    # Determine input size
    input_size_bytes = os.path.getsize(input_file)
    
    result = {
        "job_name": job_name,
        "input_file": input_file,
        "input_size_mb": input_size_bytes / (1024 * 1024),
        "num_records": line_count,
        "num_nodes": num_nodes,
        "execution_time_sec": execution_time,
        "throughput_records_per_sec": line_count / execution_time,
        "throughput_mb_per_sec": input_size_bytes / (1024 * 1024) / execution_time,
        "output_path": output_file
    }
    
    print(f"Job {job_name} completed in {execution_time:.2f} seconds")
    return result

def main():
    parser = argparse.ArgumentParser(description='Run MapReduce benchmark with different dataset sizes')
    parser.add_argument('--input', required=True, help='Path to input CSV file')
    parser.add_argument('--output', required=True, help='Output directory for results')
    parser.add_argument('--jobs', nargs='+', default=['job1', 'job2'], help='Job names to run (job1, job2, etc.)')
    parser.add_argument('--sizes', nargs='+', type=float, default=[0.01, 0.1, 0.5, 1.0], 
                        help='Dataset size percentages to test (0.01 = 1%, 1.0 = 100%)')
    parser.add_argument('--nodes', type=int, default=multiprocessing.cpu_count(),
                        help='Number of nodes to simulate (default: number of CPU cores)')
    args = parser.parse_args()
    
    # Create output directory
    os.makedirs(args.output, exist_ok=True)
    
    # Create sample datasets
    samples = create_sample_datasets(args.input, os.path.join(args.output, "samples"), sizes=args.sizes)
    
    # Run benchmarks
    results = []
    
    for job_name in args.jobs:
        for size, sample_path in samples.items():
            try:
                result = run_job(job_name, sample_path, os.path.join(args.output, "results"), num_nodes=args.nodes)
                result["dataset_size_pct"] = size
                results.append(result)
            except Exception as e:
                print(f"Error running {job_name} on {sample_path}: {e}")
    
    # Save benchmark results
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_file = os.path.join(args.output, f"benchmark_report_{timestamp}.txt")
    json_file = os.path.join(args.output, f"benchmark_results_{timestamp}.json")
    
    # Save detailed results as JSON
    with open(json_file, 'w') as f:
        json.dump(results, f, indent=2)
    
    # Generate readable report
    with open(report_file, 'w') as f:
        f.write("MapReduce Scalability Benchmark Report\n")
        f.write("====================================\n\n")
        f.write(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Input file: {args.input}\n")
        f.write(f"Number of simulated nodes: {args.nodes}\n\n")
        
        f.write("Summary by Job and Dataset Size:\n")
        f.write("-------------------------------\n\n")
        
        for job_name in args.jobs:
            f.write(f"Job: {job_name}\n")
            f.write("| Dataset Size | Input Size (MB) | Records | Time (s) | Records/sec | MB/sec |\n")
            f.write("|--------------|----------------|---------|----------|-------------|--------|\n")
            
            job_results = [r for r in results if r["job_name"] == job_name]
            job_results.sort(key=lambda r: r["dataset_size_pct"])
            
            for r in job_results:
                f.write(f"| {r['dataset_size_pct']*100:>12.2f}% | {r['input_size_mb']:>14.2f} | {r['num_records']:>7d} | {r['execution_time_sec']:>8.2f} | {r['throughput_records_per_sec']:>11.2f} | {r['throughput_mb_per_sec']:>6.2f} |\n")
            
            f.write("\n")
        
        f.write("\nScalability Analysis:\n")
        f.write("---------------------\n")
        
        for job_name in args.jobs:
            job_results = [r for r in results if r["job_name"] == job_name]
            if len(job_results) > 1:
                smallest = min(job_results, key=lambda r: r["dataset_size_pct"])
                largest = max(job_results, key=lambda r: r["dataset_size_pct"])
                
                size_ratio = largest["dataset_size_pct"] / smallest["dataset_size_pct"]
                time_ratio = largest["execution_time_sec"] / smallest["execution_time_sec"]
                
                f.write(f"\nJob {job_name}:\n")
                f.write(f"- Dataset size increased by a factor of {size_ratio:.2f}x\n")
                f.write(f"- Execution time increased by a factor of {time_ratio:.2f}x\n")
                
                if time_ratio < size_ratio:
                    f.write("- The job scales SUB-linearly (good scaling properties)\n")
                elif time_ratio > size_ratio:
                    f.write("- The job scales SUPER-linearly (poor scaling properties)\n")
                else:
                    f.write("- The job scales linearly\n")
    
    print(f"\nBenchmark complete! Results saved to:")
    print(f"- Report: {report_file}")
    print(f"- Raw data: {json_file}")

if __name__ == "__main__":
    main()
