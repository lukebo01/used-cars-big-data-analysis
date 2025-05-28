#!/usr/bin/env python3

import os
import time
import subprocess
import argparse
import json
import psutil
from datetime import datetime
import pandas as pd
import shutil

def create_sample_datasets(input_csv, output_dir, sizes=[0.01, 0.05, 0.1, 0.25, 0.5, 0.75, 1.0]):
    """Create sample datasets of different sizes from the original CSV"""
    print(f"Creating sample datasets in {output_dir}...")
    os.makedirs(output_dir, exist_ok=True)
    
    df = pd.read_csv(input_csv)
    total_rows = len(df)
    
    sample_paths = {}
    for size in sizes:
        n_rows = int(total_rows * size)
        output_file = os.path.join(output_dir, f"sample_{int(size*100)}pct.csv")
        
        if size < 1.0:
            # Take random sample
            df.sample(n=n_rows, random_state=42).to_csv(output_file, index=False)
        else:
            # Use full dataset
            df.to_csv(output_file, index=False)
            
        sample_paths[size] = output_file
        print(f"Created {output_file} with {n_rows} rows")
    
    return sample_paths

def run_spark_job(job_name, engine, input_file, output_dir):
    """Run a Spark job with timing and track resource usage"""
    start_time = time.time()
    result = {
        "job_name": job_name,
        "engine": engine,  # 'core' or 'sql'
        "input_file": input_file,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "input_size_mb": os.path.getsize(input_file) / (1024 * 1024)
    }
    
    # Count number of records in input file
    with open(input_file, 'r') as f:
        result["num_records"] = sum(1 for _ in f) - 1  # Subtract 1 for header
    
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    # Determine which script to run
    if engine.lower() == 'core':
        script_path = f"src/spark/job{job_name[-1]}_spark_core.py"
    else:  # sql
        script_path = f"src/spark/job{job_name[-1]}_spark_sql.py"

    # Set environment variable for input and output paths
    os.environ['SPARK_JOB_INPUT'] = input_file
    os.environ['SPARK_JOB_OUTPUT'] = os.path.join(output_dir, f"{job_name}_{engine}")
    
    # Execute the command and capture start memory
    process_start = psutil.Process(os.getpid())
    mem_start = process_start.memory_info().rss / (1024 * 1024)  # MB
    
    print(f"Running {job_name} with {engine} engine on {input_file}...")
    cmd = f"spark-submit {script_path}"
    
    # Run the process and capture output
    try:
        output = subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT, universal_newlines=True)
        exit_code = 0
    except subprocess.CalledProcessError as e:
        output = e.output
        exit_code = e.returncode
    
    end_time = time.time()
    execution_time = end_time - start_time
    
    # Measure final memory
    process_end = psutil.Process(os.getpid())
    mem_end = process_end.memory_info().rss / (1024 * 1024)  # MB
    
    # Store results
    result["execution_time_sec"] = execution_time
    result["exit_code"] = exit_code
    result["memory_usage_mb"] = mem_end - mem_start
    result["throughput_records_per_sec"] = result["num_records"] / execution_time
    result["throughput_mb_per_sec"] = result["input_size_mb"] / execution_time
    
    # Save raw output
    output_log = os.path.join(output_dir, f"{job_name}_{engine}_log.txt")
    with open(output_log, 'w') as f:
        f.write(output)
    
    print(f"Job completed in {execution_time:.2f} seconds with exit code {exit_code}")
    print(f"Log saved to {output_log}")
    
    return result

def main():
    parser = argparse.ArgumentParser(description='Run Spark benchmark with different dataset sizes')
    parser.add_argument('--input', required=True, help='Path to input CSV file')
    parser.add_argument('--output', required=True, help='Output directory for results')
    parser.add_argument('--jobs', nargs='+', default=['job1', 'job2'], help='Job names to run (job1, job2, etc.)')
    parser.add_argument('--engines', nargs='+', default=['core', 'sql'], help='Spark engines to test (core, sql)')
    parser.add_argument('--sizes', nargs='+', type=float, default=[0.01, 0.1, 0.5, 1.0], 
                        help='Dataset size percentages to test (0.01 = 1%, 1.0 = 100%)')
    parser.add_argument('--use-existing-samples', action='store_true', 
                        help='Use existing sample files instead of creating new ones')
    args = parser.parse_args()
    
    # Create output directory
    os.makedirs(args.output, exist_ok=True)
    samples_dir = os.path.join(args.output, "samples")
    results_dir = os.path.join(args.output, "spark_results")
    os.makedirs(results_dir, exist_ok=True)
    
    # Create or use sample datasets
    if args.use_existing_samples and os.path.exists(samples_dir):
        print(f"Using existing sample datasets from {samples_dir}")
        samples = {
            size: os.path.join(samples_dir, f"sample_{int(size*100)}pct.csv")
            for size in args.sizes
        }
    else:
        samples = create_sample_datasets(args.input, samples_dir, sizes=args.sizes)
    
    # Run benchmarks
    results = []
    
    for job_name in args.jobs:
        for engine in args.engines:
            for size, sample_path in samples.items():
                try:
                    result = run_spark_job(
                        job_name, 
                        engine, 
                        sample_path, 
                        results_dir
                    )
                    result["dataset_size_pct"] = size
                    results.append(result)
                except Exception as e:
                    print(f"Error running {job_name} with {engine} on {sample_path}: {e}")
    
    # Save benchmark results
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_file = os.path.join(args.output, f"spark_benchmark_report_{timestamp}.txt")
    json_file = os.path.join(args.output, f"spark_benchmark_results_{timestamp}.json")
    
    # Save detailed results as JSON
    with open(json_file, 'w') as f:
        json.dump(results, f, indent=2)
    
    # Generate readable report
    with open(report_file, 'w') as f:
        f.write("Spark Scalability Benchmark Report\n")
        f.write("================================\n\n")
        f.write(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Input file: {args.input}\n\n")
        
        f.write("Summary by Job, Engine and Dataset Size:\n")
        f.write("-------------------------------------\n\n")
        
        for job_name in args.jobs:
            for engine in args.engines:
                f.write(f"Job: {job_name}, Engine: {engine}\n")
                f.write("| Dataset Size | Input Size (MB) | Records | Time (s) | Records/sec | MB/sec | Memory (MB) |\n")
                f.write("|--------------|----------------|---------|----------|-------------|--------|-------------|\n")
                
                job_engine_results = [r for r in results if r["job_name"] == job_name and r["engine"] == engine]
                job_engine_results.sort(key=lambda r: r["dataset_size_pct"])
                
                for r in job_engine_results:
                    f.write(f"| {r['dataset_size_pct']*100:>12.2f}% | {r['input_size_mb']:>14.2f} | {r['num_records']:>7d} | {r['execution_time_sec']:>8.2f} | {r['throughput_records_per_sec']:>11.2f} | {r['throughput_mb_per_sec']:>6.2f} | {r['memory_usage_mb']:>11.2f} |\n")
                
                f.write("\n")
        
        f.write("\nScalability Analysis:\n")
        f.write("---------------------\n")
        
        for job_name in args.jobs:
            for engine in args.engines:
                job_engine_results = [r for r in results if r["job_name"] == job_name and r["engine"] == engine]
                if len(job_engine_results) > 1:
                    smallest = min(job_engine_results, key=lambda r: r["dataset_size_pct"])
                    largest = max(job_engine_results, key=lambda r: r["dataset_size_pct"])
                    
                    size_ratio = largest["dataset_size_pct"] / smallest["dataset_size_pct"]
                    time_ratio = largest["execution_time_sec"] / smallest["execution_time_sec"]
                    
                    f.write(f"\nJob {job_name} with {engine.upper()} engine:\n")
                    f.write(f"- Dataset size increased by a factor of {size_ratio:.2f}x\n")
                    f.write(f"- Execution time increased by a factor of {time_ratio:.2f}x\n")
                    
                    if time_ratio < size_ratio:
                        f.write("- The job scales SUB-linearly (good scaling properties)\n")
                    elif time_ratio > size_ratio:
                        f.write("- The job scales SUPER-linearly (poor scaling properties)\n")
                    else:
                        f.write("- The job scales linearly\n")
                    
                    # Compare engines if both core and sql results are available
                    if engine == args.engines[-1] and all(e in args.engines for e in ['core', 'sql']):
                        core_results = [r for r in results if r["job_name"] == job_name and r["engine"] == 'core']
                        sql_results = [r for r in results if r["job_name"] == job_name and r["engine"] == 'sql']
                        
                        if core_results and sql_results:
                            largest_core = max(core_results, key=lambda r: r["dataset_size_pct"])
                            largest_sql = max(sql_results, key=lambda r: r["dataset_size_pct"])
                            
                            f.write(f"\nEngine comparison for Job {job_name} at {largest_core['dataset_size_pct']*100:.0f}% data size:\n")
                            f.write(f"- Spark Core: {largest_core['execution_time_sec']:.2f}s, {largest_core['throughput_records_per_sec']:.2f} records/sec\n")
                            f.write(f"- Spark SQL: {largest_sql['execution_time_sec']:.2f}s, {largest_sql['throughput_records_per_sec']:.2f} records/sec\n")
                            
                            ratio = largest_core['execution_time_sec'] / largest_sql['execution_time_sec']
                            if ratio > 1:
                                f.write(f"- Spark SQL is {ratio:.2f}x faster than Spark Core\n")
                            else:
                                f.write(f"- Spark Core is {1/ratio:.2f}x faster than Spark SQL\n")
    
    print(f"\nBenchmark complete! Results saved to:")
    print(f"- Report: {report_file}")
    print(f"- Raw data: {json_file}")

if __name__ == "__main__":
    main()
