# helper.py
import pandas as pd
import numpy as np
import random
from collections import defaultdict
from modules import Func, FuncChain, EdgeNode
from config import FUNCTION_TYPES
from config import TASK_SIZES, EDGE_SIZES

# --------------------- Online reader (arrivals) --------------------- #
def read_arrival_data(filename, task_size):
    
    df = pd.read_csv(filename)
    df = df[df['chain_id'] < task_size].copy()
    
    # Removed the line: df['homedge'] = df['homedge'].astype(int)
    df = df.sort_values(by=['arrival_time', 'chain_id', 'func_id'])

    arrivals = defaultdict(dict)  # {arrival_time: {cid: FuncChain}}
    for (arrival_time, cid), group in df.groupby(['arrival_time', 'chain_id']):
        chain_deadline = group['chain_deadline'].iloc[0]
        homedge = f"E{group['homedge'].iloc[0]}"
        func_chain = FuncChain(cid, arrival_time, chain_deadline, homedge)
        
        for _, row in group.iterrows():
            f = Func(row,cid)
            f.step = len(func_chain.functions)
            func_chain.functions.append(f)
        
        arrivals[arrival_time][cid] = func_chain

    return arrivals


# --------------------- Edge generator with neighbor connections --------------------- #


def generate_edges(count, 
                   cpu_range=(50, 150), 
                   mem_range=(100, 500), 
                   cache_range=(3, 4)):
    
    edges = []
    func_types = list(FUNCTION_TYPES.keys())

    for i in range(count):
        eid = f"E{i}"
        cpu_cap = random.randint(*cpu_range)
        mem_cap = random.randint(*mem_range)
        cache_size = random.randint(*cache_range)

        cached = []
        cpu_rem = cpu_cap
        mem_rem = mem_cap
        
        funcs_shuffled = func_types[:]
        random.shuffle(funcs_shuffled)
        
        for f_type in funcs_shuffled:
            if len(cached) >= cache_size:
                break
            
            res = FUNCTION_TYPES[f_type]
            if res["cpu"] <= cpu_rem and res["mem"] <= mem_rem:
                cached.append(f_type)
                cpu_rem -= res["cpu"]
                mem_rem -= res["mem"]

        node = EdgeNode(
            eid,
            cpu_cap,
            mem_cap,
            cached,
            cache_size
        )
        edges.append(node)

    # Create a list of all connection "stubs"
    stubs = []
    for i in range(count):
        # Each node wants 5 neighbors
        stubs.extend([i] * 5)

    # If the total number of stubs is odd, one node must have one less connection.
    if len(stubs) % 2 != 0:
        stubs.pop()

    # Randomly shuffle the stubs to prepare for pairing
    random.shuffle(stubs)

    # Pair up stubs to create symmetrical connections
    while stubs:
        node1_idx = stubs.pop()
        node2_idx = stubs.pop()

        # Avoid self-loops (a node being its own neighbor)
        if node1_idx == node2_idx:
            continue

        node1 = edges[node1_idx]
        node2 = edges[node2_idx]
        
        # Add the symmetrical connection
        node1.neighbors.add(node2.id)
        node2.neighbors.add(node1.id)

    return edges




#  after generating the edges and their neighbor connections:

def generate_latency_matrix(edges, neighbor_delay_range=(1, 3), public_delay_range=(4, 8)):
    """
    Creates a latency matrix where delay depends on whether nodes are neighbors.
    """
    latencies = defaultdict(dict)
    all_edge_ids = [edge.id for edge in edges]

    for source_node in edges:
        for dest_id in all_edge_ids:
            # 1. No delay to self
            if source_node.id == dest_id:
                latencies[source_node.id][dest_id] = 0
            # 2. Low delay for neighbors
            elif dest_id in source_node.neighbors:
                # Use existing value if already set by symmetric pair
                if dest_id not in latencies[source_node.id]:
                    delay = random.randint(*neighbor_delay_range)
                    latencies[source_node.id][dest_id] = delay
                    latencies[dest_id][source_node.id] = delay # Symmetric delay
            # 3. High delay for non-neighbors
            else:
                if dest_id not in latencies[source_node.id]:
                    delay = random.randint(*public_delay_range)
                    latencies[source_node.id][dest_id] = delay
                    latencies[dest_id][source_node.id] = delay

    return latencies