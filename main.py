import pandas as pd
import copy
from config import TASK_SIZES, EDGE_SIZES, BEAM_WIDTH, MAX_TIME
from modules import Cloud
from helper import read_arrival_data, generate_edges,generate_latency_matrix

from sched_for_compare import RCRSSAScheduler, DEWSAScheduler, HEFTLessScheduler, BeamSchedulerGreedyProfit
from hbsda import proposedBeamSchedulerEdgeFirst
def simulate_all(filename="datafile_100_10_with_homedges.csv"):
    
    results = []

    for task_size in TASK_SIZES:
        print(f"\nProcessing for task size: {task_size}")
        arrivals = read_arrival_data(filename, task_size)

        for edge_count in EDGE_SIZES:
            # Generate a consistent set of edges for fair comparison across schedulers
            base_edges = generate_edges(edge_count)
            latencies = generate_latency_matrix(base_edges)
            cloud_instance = None  # Initialize cloud instance once per edge count, reuse across schedulers

            schedulers_to_run = [
                (BeamSchedulerGreedyProfit,"Greedy-P"),
                (DEWSAScheduler,"DEWSA"),
                (RCRSSAScheduler,"RCRSSA"),
                (HEFTLessScheduler,"HEFTLess"),
                (proposedBeamSchedulerEdgeFirst, "HBS-DA"),
            ]

            for SchedulerClass, label in schedulers_to_run:
                print(f"Running Scheduler: {label}...")
                
                # Use deep copies to ensure each scheduler starts with a fresh, identical state
                edges = copy.deepcopy(base_edges)
                current_arrivals = copy.deepcopy(arrivals)
                
                # Conditionally pass the cloud instance only to the schedulers that use it.
                if label == "HBS-DA" or label == "BSPublic-DA" or label =='HEFTLess':
                    scheduler = SchedulerClass(current_arrivals, edges, latencies, beam_width=BEAM_WIDTH, cloud=cloud_instance)
                else:
                    scheduler = SchedulerClass(current_arrivals, edges, latencies, beam_width=BEAM_WIDTH)
                
                # Run the main simulation loop
                for t in range(MAX_TIME):
                    scheduler.step(t)
                
                # Collect and store the results
                res = scheduler.summary()
                res.update({
                    "Scheduler": label,
                    "task_size": task_size,
                    "edge_count": edge_count
                })
                results.append(res)

    # --- (The rest of the file is correct and remains the same) ---
    df = pd.DataFrame(results)
    
    output_filename = f"sim_{task_size}_{edge_count}.csv"
    df.to_csv(output_filename, index=False)
    
    print("\n" + "="*25)
    print("=== Simulation Complete ===")
    print(f"Results saved to: {output_filename}")
    print("="*25)
    
    print("\n=== Summary of Results ===")
    summary_cols = ['Scheduler', 'task_size', 'edge_count', 'fch_completed', 'fch_rejected', 'net_profit', 'cost']
    if 'fch_rejected' not in df.columns:
        summary_cols.remove('fch_rejected')
        
    print(df[summary_cols])


if __name__ == "__main__":

    input="input/medium_500_50.csv"

    simulate_all(input)
