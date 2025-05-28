#!/usr/bin/env python3

import os
import time
import subprocess
import argparse
import json
from datetime import datetime
import pandas as pd
import multiprocessing

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
