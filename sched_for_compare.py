
import copy
from collections import defaultdict
from config import COLD_START_DELAY, EDGE_COST_PER_UNIT

class RCRSSAScheduler:
    """
    Implements the two-stage RCRSSA from the paper but uses the classic
    priority-based function for final placement decisions.
    """
    def __init__(self, arrivals, edges, latencies, **kwargs):
        self.arrivals = arrivals
        self.edges = edges
        self.latencies = latencies
        self.edge_map = {e.id: e for e in edges}
        self.time = 0

        # State Tracking
        self.waiting_chains = {}
        self.completed_chains = []
        self.rejected_chains = []
        self.ready_times = defaultdict(int)
        self.simulation_end_time = 0
        self.all_completed_funcs = []
        
        self.vnf_queues = {edge.id: [] for edge in self.edges}

    def _predict_timeout(self, chain_to_check, homedge_id):
        temp_queue = sorted(self.vnf_queues[homedge_id] + chain_to_check.functions, key=lambda f: f.deadline)
        temp_edge = copy.deepcopy(self.edge_map[homedge_id])
        sim_time = self.time
        for func in temp_queue:
            can, cold = temp_edge.can_schedule(func)
            if can:
                finish_time = sim_time + func.exec_time + (COLD_START_DELAY if cold else 0)
                if func.chain_id == chain_to_check.chain_id and finish_time > chain_to_check.chain_deadline:
                    return True
                temp_edge.assign(func)
                sim_time = finish_time
            else:
                if func.chain_id == chain_to_check.chain_id:
                    return True
        return False

    def _find_best_region(self, chain_to_move):
        best_target_edge = None
        lowest_score = float('inf')
        homedge_id = chain_to_move.homedge
        for edge in self.edges:
            if edge.id == homedge_id:
                continue
            trans_delay = self.latencies[homedge_id][edge.id]
            total_processing_time = sum(f.exec_time for f in chain_to_move.functions)
            total_cold_starts = len(chain_to_move.functions) * COLD_START_DELAY
            projected_finish_time = self.time + trans_delay + total_processing_time + total_cold_starts
            if projected_finish_time <= chain_to_move.chain_deadline:
                score = sum(f.exec_time for f in self.vnf_queues[edge.id])
                if score < lowest_score:
                    lowest_score = score
                    best_target_edge = edge
        return best_target_edge

    # --- ADDED: Your classic edge selection function ---
    def _best_edge_for_func(self, func, homedge_id):
        homedge_node = self.edge_map.get(homedge_id)
        if not homedge_node:
            return None, False, 0 # Return 3 items for consistency

        neighbors = {self.edge_map[nid] for nid in homedge_node.neighbors if nid in self.edge_map}
        other_public_edges = [e for e in self.edges if e.id != homedge_id and e not in neighbors]

        # Priority 1: Homedge (Cache Hit)
        if func.func_type in homedge_node.cached_funcs and homedge_node.can_schedule(func)[0]:
            transmission_delay = self.latencies[homedge_id][homedge_node.id]
            return homedge_node, False, transmission_delay
        
        # Priority 2: Neighbors (Cache Hit)
        for edge in sorted(list(neighbors), key=lambda e: e.id):
            if func.func_type in edge.cached_funcs and edge.can_schedule(func)[0]:
                transmission_delay = self.latencies[homedge_id][edge.id] # Corrected
                return edge, False, transmission_delay

        # Priority 3: Other Public Edges (Cache Hit)
        for edge in sorted(other_public_edges, key=lambda e: e.id):
            if func.func_type in edge.cached_funcs and edge.can_schedule(func)[0]:
                transmission_delay = self.latencies[homedge_id][edge.id] # Corrected
                return edge, False, transmission_delay

        # Priority 4: Homedge (Cold Start)
        if homedge_node.can_schedule(func)[0]:
            transmission_delay = self.latencies[homedge_id][homedge_node.id]
            return homedge_node, True, transmission_delay

        
        return None, False, 0

    def step(self, t):
        self.time = t

        # (Sections 1 and 2 for releasing functions and handling arrivals are unchanged)
        for edge in self.edges:
            for func in list(edge.queue):
                if func.end_time and func.end_time <= t:
                    edge.release(func)
                    if func.end_time > self.simulation_end_time:
                        self.simulation_end_time = func.end_time

        if t in self.arrivals:
            for cid, chain in self.arrivals[t].items():
                needs_offload = self._predict_timeout(chain, chain.homedge)
                if needs_offload:
                    target_edge = self._find_best_region(chain)
                    if target_edge:
                        chain.homedge = target_edge.id
                        for func in chain.functions:
                            self.vnf_queues[target_edge.id].append(func)
                        self.waiting_chains[cid] = chain
                    else:
                        self.rejected_chains.append(chain)
                else:
                    for func in chain.functions:
                        self.vnf_queues[chain.homedge].append(func)
                    self.waiting_chains[cid] = chain

        # --- UPDATED: Scheduling logic now uses _best_edge_for_func ---
        for edge_id, queue in self.vnf_queues.items():
            if not queue:
                continue
            
            queue.sort(key=lambda f: f.deadline)
            func_to_schedule = queue[0]
            chain = self.waiting_chains.get(func_to_schedule.chain_id)
            if not chain: continue

            # Use the classic function to find the best placement
            edge, cold, trans_delay = self._best_edge_for_func(func_to_schedule, chain.homedge)

            # If a suitable edge was found, check deadline and assign
            if edge:
                finish_time = t + func_to_schedule.exec_time + (COLD_START_DELAY if cold else 0) + trans_delay
                if finish_time <= chain.chain_deadline:
                    func_to_schedule.start_time = t
                    func_to_schedule.end_time = finish_time
                    edge.assign(func_to_schedule)
                    
                    self.all_completed_funcs.append(func_to_schedule)
                    chain.advance()
                    queue.pop(0)

                    if not chain.ready:
                        chain.completion_time = finish_time
                        self.completed_chains.append(chain)
                        del self.waiting_chains[chain.chain_id]

    def summary(self):
        # Incomplete chains are also counted as rejected for final accounting
        final_rejected = self.rejected_chains + list(self.waiting_chains.values())

        Revenue = sum(chain.get_total_chain_price() for chain in self.completed_chains)
        cost = 0
        for chain in self.completed_chains:
            for func in chain.functions:
                cost += (func.cpu + func.mem) * EDGE_COST_PER_UNIT

        # 1. Edge Distribution (Raw Task Count)
        edge_dist = {edge.id: edge.tasks_serviced for edge in self.edges}

        # 2. Edge Utilization Score (Tasks relative to Capacity)
        # Because we don't track execution time per unit, we use Tasks per CPU Unit.
        # Higher score = the edge was utilized more heavily for its size.
        edge_utilization = {}
        for edge in self.edges:
            if edge.cpu_capacity > 0:
                # Calculate: Tasks serviced per unit of CPU capacity
                load_score = edge.tasks_serviced / edge.cpu_capacity
                edge_utilization[edge.id] = round(load_score, 3)
            else:
                edge_utilization[edge.id] = 0.0

        return {
            "scheduler": "RCRSSA",
            "func_completed": len(self.all_completed_funcs),
            "fch_completed": len(self.completed_chains),
            "fch_rejected": len(final_rejected),
            "Rev": round(Revenue, 2),
            "cost": round(cost, 2),
            "net_profit": round(Revenue - cost, 2),
            "total_simulation_time": self.simulation_end_time,
            "edge_distribution": edge_dist,
            "edge_utilization": edge_utilization  # Added metric
        }
    

"""
An online, deadline-aware scheduler based on the DEWSA cost model.

At each time step, it finds the next function from a ready chain and assigns
it to the available edge node that offers the minimum execution cost,
without violating the chain's deadline.
"""
class DEWSAScheduler:

    def __init__(self, arrivals, edges, latencies, **kwargs):
        self.arrivals = arrivals
        self.edges = edges
        self.latencies = latencies
        self.edge_map = {e.id: e for e in edges}
        self.time = 0

        # --- State Tracking ---
        self.waiting_chains = {}
        self.completed_chains = []
        self.rejected_chains = []
        self.ready_times = defaultdict(int)
        self.simulation_end_time = 0
        self.all_completed_funcs = []

    def _get_globally_cached_funcs(self):
        """Helper to get a set of all function types cached across all edges."""
        cached = set()
        for edge in self.edges:
            cached.update(edge.cached_funcs)
        return cached

    def step(self, t):
        self.time = t

        # 1. Release completed functions
        for edge in self.edges:
            for func in list(edge.queue):
                if func.end_time and func.end_time <= t:
                    edge.release(func)
                    if func.end_time > self.simulation_end_time:
                        self.simulation_end_time = func.end_time

        # 2. Add new arrivals to the waiting pool
        if t in self.arrivals:
            for cid, func_chain in self.arrivals[t].items():
                self.waiting_chains[cid] = func_chain
                self.ready_times[cid] = func_chain.arrival_time

        # 3. Admission Control for unstarted chains
        globally_cached = self._get_globally_cached_funcs()
        unstarted_cids = [cid for cid, chain in self.waiting_chains.items()
                          if chain.ready and t >= self.ready_times[cid] and chain.next_func_idx == 0]

        for cid in unstarted_cids:
            chain = self.waiting_chains[cid]
            projected_time = 0
            avg_transmission_delay = 3 # Matching the beam search assumption
            for f in chain.functions:
                projected_time += f.exec_time
                projected_time += avg_transmission_delay
                if f.func_type not in globally_cached:
                    projected_time += COLD_START_DELAY
            
            if t + projected_time > chain.chain_deadline:
                self.rejected_chains.append(chain)
                del self.waiting_chains[cid]

        # 4. Find all currently ready chains
        ready_chains = [chain for cid, chain in self.waiting_chains.items()
                        if chain.ready and t >= self.ready_times[cid]]

        # 5. Try to schedule the next function for each ready chain
        for chain in ready_chains:
            func = chain.get_next_func()
            if not func:
                continue

            best_edge = None
            best_finish_time = float('inf')
            min_cost = float('inf')
            
            # DEWSA Logic: Find the edge with the minimum cost
            for edge in self.edges:
                can_schedule, cold_delay = edge.can_schedule(func)
                trans_delay = self.latencies[chain.homedge][edge.id]
                finish_time = t + func.exec_time + cold_delay + trans_delay
                
                if can_schedule and finish_time <= chain.chain_deadline:
                    cost = (func.cpu + func.mem) * EDGE_COST_PER_UNIT + cold_delay
                    if cost < min_cost:
                        min_cost = cost
                        best_edge = edge
                        best_finish_time = finish_time

            # If a suitable edge was found, assign the function
            if best_edge:
                func.start_time = t
                func.end_time = best_finish_time
                best_edge.assign(func)
                
                self.all_completed_funcs.append(func)
                chain.advance()
                
                if chain.ready:
                    self.ready_times[chain.chain_id] = func.end_time
                else:
                    chain.completion_time = func.end_time
                    self.completed_chains.append(chain)
                    del self.waiting_chains[chain.chain_id]

    def summary(self):
        # Incomplete chains are also counted as rejected for final accounting
        final_rejected = self.rejected_chains + list(self.waiting_chains.values())

        Revenue = sum(chain.get_total_chain_price() for chain in self.completed_chains)
        cost = 0
        for chain in self.completed_chains:
            for func in chain.functions:
                cost += (func.cpu + func.mem) * EDGE_COST_PER_UNIT

        edge_dist = {edge.id: edge.tasks_serviced for edge in self.edges}

        # 1. Edge Distribution (Raw Task Count)
        edge_dist = {edge.id: edge.tasks_serviced for edge in self.edges}

        # 2. Edge Utilization Score (Tasks relative to Capacity)
        # Because we don't track execution time per unit, we use Tasks per CPU Unit.
        # Higher score = the edge was utilized more heavily for its size.
        edge_utilization = {}
        for edge in self.edges:
            if edge.cpu_capacity > 0:
                # Calculate: Tasks serviced per unit of CPU capacity
                load_score = edge.tasks_serviced / edge.cpu_capacity
                edge_utilization[edge.id] = round(load_score, 3)
            else:
                edge_utilization[edge.id] = 0.0

        total_funcs = sum(edge_dist.values())
        return {
            "scheduler": "DEWSA",
            "func_completed": len(self.all_completed_funcs),
            "fch_completed": len(self.completed_chains),
            "fch_rejected": len(final_rejected),
            "Rev": round(Revenue, 2),
            "cost": round(cost, 2),
            "net_profit": round(Revenue - cost, 2),
            "total_simulation_time": self.simulation_end_time,
            "edge_distribution": edge_dist,
            "edge_utilization": edge_utilization  # Added metric
    }
    
# HEFTLESS 

from collections import defaultdict
from config import COLD_START_DELAY, EDGE_COST_PER_UNIT, CLOUD_COST_PER_UNIT, ALPHA

class HEFTLessScheduler:
    """
    Implementation of the HEFTLess Algorithm adapted to the step-based simulation format.
    It calculates a HEFT rank for each function, sorts them, and assigns them to 
    the instance (Cloud, Fog, Edge) that minimizes a bi-objective function combining Cost and Time.
    """
    
    def __init__(self, arrivals, edges, latencies, beam_width=3, cloud=None, alpha=ALPHA, gamma=0.5):
        self.arrivals = arrivals
        self.edges = edges
        self.edge_map = {e.id: e for e in edges}
        self.beam_width = beam_width
        self.cloud = cloud
        self.alpha = alpha 
        self.gamma = gamma # Bi-objective weighting coefficient (HEFTLess default is typically 0.5)
        self.time = 0
        self.latencies = latencies

        self.completed_chains = []
        self.offloaded_chains = []
        self.rejected_chains = []
        self.waiting_chains = {}
        self.ready_times = defaultdict(int)
        self.simulation_end_time = 0

    def _calculate_objective(self, cost, completion_time, func):
        """
        Calculates the HEFTLess bi-objective formula:
        Obj = γ * (Cost / MaxCost) + (1 - γ) * (Time / MaxTime)
        """
        # Normalization constants (K* and TT* from the HEFTLess paper)
        # Using 1e-9 to prevent division by zero
        max_cost = (func.cpu + func.mem) * max(CLOUD_COST_PER_UNIT, EDGE_COST_PER_UNIT) + 1e-9
        max_time = func.exec_time + COLD_START_DELAY + 50  # 50 represents a generic max network latency bound
        
        norm_cost = cost / max_cost
        norm_time = completion_time / max_time
        
        return (self.gamma * norm_cost) + ((1 - self.gamma) * norm_time)

    def step(self, t):
        self.time = t
        
        # 1. Housekeeping: Release completed functions from edges
        for edge in self.edges:
            for func in list(edge.queue):
                if func.end_time and func.end_time <= t: 
                    edge.release(func)
                    self.simulation_end_time = max(self.simulation_end_time, t)

        # 2. Add newly arrived workflow chains
        if t in self.arrivals:
            for cid, func_chain in self.arrivals[t].items():
                self.waiting_chains[cid] = func_chain
                self.ready_times[cid] = func_chain.arrival_time

        # 3. HEFTLess Step 1 & 2: Ranking and Sorting
        # Collect all currently ready functions across all waiting workflows
        ready_tasks = []
        for cid, chain in list(self.waiting_chains.items()):
            if chain.ready and t >= self.ready_times[cid]:
                # Check if the entire chain is already expired
                if t > chain.chain_deadline:
                    self.rejected_chains.append(chain)
                    del self.waiting_chains[cid]
                    continue
                    
                func = chain.get_next_func()
                
                # HEFT Upward Rank: Average execution time of current node + successors
                # For a linear chain, this is the sum of execution times of the remaining functions.
                rank = sum(f.exec_time for f in chain.functions[chain.next_func_idx:])
                ready_tasks.append({'rank': rank, 'cid': cid, 'chain': chain, 'func': func})
        
        # Sort tasks based on rank descending (Highest rank gets scheduled first)
        ready_tasks.sort(key=lambda x: x['rank'], reverse=True)

        # 4. HEFTLess Step 3 & 4: Scheduling based on Bi-Objective optimization
        for task in ready_tasks:
            cid = task['cid']
            chain = task['chain']
            func = task['func']
            
            # If chain was offloaded or completed in a previous iteration of this loop, skip
            if cid not in self.waiting_chains:
                continue

            best_obj = float('inf')
            best_edge = None
            best_cold = False
            best_trans_delay = 0
            is_cloud_best = False
            
            # Evaluate Cloud Layer
            if self.cloud:
                # Simulating execution of remaining chain on the cloud
                cloud_exec_time, _ = self.cloud.execute_chain(chain) 
                cloud_finish_time = t + cloud_exec_time
                
                if cloud_finish_time <= chain.chain_deadline:
                    # Estimate total remaining cost for the cloud
                    cloud_cost = sum((f.cpu + f.mem) * CLOUD_COST_PER_UNIT for f in chain.functions[chain.next_func_idx:])
                    obj = self._calculate_objective(cloud_cost, cloud_finish_time, func)
                    
                    if obj < best_obj:
                        best_obj = obj
                        is_cloud_best = True

            # Evaluate all Edge/Fog instances in the computing continuum
            for edge in self.edges:
                # Constraint check: verify resources and concurrency limit
                if edge.can_schedule(func)[0]:
                    
                    # Determine deployment mode: Predeployed (cache hit) vs Undeployed (cold start)
                    mode_cold = func.func_type not in edge.cached_funcs
                    trans_delay = self.latencies.get(chain.homedge, {}).get(edge.id, 0)
                    
                    # Calculate Completion Time (TT^f)
                    edge_finish_time = t + func.exec_time + (COLD_START_DELAY if mode_cold else 0) + trans_delay
                    
                    # Check deadline constraint
                    if edge_finish_time <= chain.chain_deadline:
                        # Calculate Cost (K^f)
                        edge_cost = (func.cpu + func.mem) * EDGE_COST_PER_UNIT
                        
                        # Calculate Objective
                        obj = self._calculate_objective(edge_cost, edge_finish_time, func)
                        
                        if obj < best_obj:
                            best_obj = obj
                            best_edge = edge
                            best_cold = mode_cold
                            best_trans_delay = trans_delay
                            is_cloud_best = False

            # 5. HEFTLess Step 5: Execute Assignment
            if is_cloud_best:
                # Offload the remainder of the workflow to the cloud
                cloud_exec_time, _ = self.cloud.execute_chain(chain)
                chain.completion_time = t + cloud_exec_time
                self.offloaded_chains.append(chain)
                del self.waiting_chains[cid]
                
            elif best_edge:
                # Assign the function to the best edge instance based on the objective score
                finish_time = t + func.exec_time + (COLD_START_DELAY if best_cold else 0) + best_trans_delay
                func.start_time = t
                func.end_time = finish_time
                best_edge.assign(func)
                chain.advance()
                
                if not chain.ready:
                    # Chain is fully completed
                    chain.completion_time = func.end_time
                    self.completed_chains.append(chain)
                    del self.waiting_chains[cid]
                else:
                    # Chain has more functions, set ready time for the next one
                    self.ready_times[cid] = func.end_time
                    
            else:
                # No feasible instance found at this timestep that meets the deadline.
                # It stays in the waiting queue until resources free up or it misses its deadline.
                pass

    def summary(self):
        final_rejected = self.rejected_chains + list(self.waiting_chains.values())
        
        edge_profit = sum(chain.get_total_chain_price() for chain in self.completed_chains)
        cloud_profit = sum(chain.get_total_chain_price() for chain in self.offloaded_chains)
        profit = edge_profit + cloud_profit

        edge_cost = 0
        all_edge_funcs = []
        for chain in self.completed_chains:
            all_edge_funcs.extend(chain.functions)
            for func in chain.functions:
                edge_cost += (func.cpu + func.mem) * EDGE_COST_PER_UNIT
        
        cloud_cost = 0
        all_cloud_funcs = []
        for chain in self.offloaded_chains:
            all_cloud_funcs.extend(chain.functions)
            for func in chain.functions:
                cloud_cost += (func.cpu + func.mem) * CLOUD_COST_PER_UNIT

        cost = edge_cost + cloud_cost
        edge_dist = {edge.id: edge.tasks_serviced for edge in self.edges}
        
        # 1. Edge Distribution (Raw Task Count)
        edge_dist = {edge.id: edge.tasks_serviced for edge in self.edges}

        # 2. Edge Utilization Score (Tasks relative to Capacity)
        # Because we don't track execution time per unit, we use Tasks per CPU Unit.
        # Higher score = the edge was utilized more heavily for its size.
        edge_utilization = {}
        for edge in self.edges:
            if edge.cpu_capacity > 0:
                # Calculate: Tasks serviced per unit of CPU capacity
                load_score = edge.tasks_serviced / edge.cpu_capacity
                edge_utilization[edge.id] = round(load_score, 3)
            else:
                edge_utilization[edge.id] = 0.0

        total_funcs_completed = len(all_edge_funcs) + len(all_cloud_funcs)
        
        max_edge_time = self.simulation_end_time
        max_cloud_time = 0
        if self.offloaded_chains:
            max_cloud_time = max(c.completion_time for c in self.offloaded_chains)
        
        total_simulation_time = max(max_edge_time, max_cloud_time)

        return {
            "scheduler": "HEFTLess",
            "func_completed": total_funcs_completed,
            "fch_completed": len(self.completed_chains) + len(self.offloaded_chains),
            "fch_offloaded_cloud": len(self.offloaded_chains),
            "fch_rejected": len(final_rejected),
            "cost": round(cost, 2),
            "net_profit": round(profit - cost, 2),
            "total_simulation_time": total_simulation_time,
            "edge_distribution": edge_dist,
            "edge_utilization": edge_utilization  # Added metric
        }
    
#greedy scheduler that always picks the first available edge for the next function in a ready chain, without any ranking or optimization logic. This serves as a baseline for comparison against more sophisticated algorithms like HEFTLess and DEWSA.
#--------------------- pure greedy private scheduler ---------------------#
from collections import defaultdict
from config import COLD_START_DELAY, EDGE_COST_PER_UNIT

class BeamSchedulerGreedyProfit:
    
    def __init__(self, arrivals, edges, latencies, beam_width=3, cloud=None):
        self.arrivals = arrivals
        self.edges = edges
        self.edge_map = {e.id: e for e in edges}
        self.beam_width = beam_width
        self.time = 0
        self.latencies = latencies

        self.completed_funcs = []
        self.completed_chains = []
        self.rejected_chains = [] # For chains failing admission control
        self.waiting_chains = {}
        self.ready_times = defaultdict(int)
        self.simulation_end_time = 0

        self.committed_chain_id = None
        self.func_to_schedule = None

    def _get_globally_cached_funcs(self):
        """Helper to get a set of all function types cached across all edges."""
        cached = set()
        for edge in self.edges:
            cached.update(edge.cached_funcs)
        return cached

    def _best_edge_for_func(self, func, homedge_id):
        """
        Private Policy: Only the homedge is ever considered.
        """
        homedge_node = self.edge_map.get(homedge_id)
        if not homedge_node:
            transmission_delay = self.latencies[homedge_id][homedge_node.id]
            return None, False, transmission_delay

        # Priority 1: Homedge (Cache Hit)
        if func.func_type in homedge_node.cached_funcs:
            can, cold = homedge_node.can_schedule(func)
            if can:
                transmission_delay = self.latencies[homedge_id][homedge_node.id]
                return homedge_node, cold, transmission_delay

        # Priority 2: Homedge (Cold Start)
        can, cold = homedge_node.can_schedule(func)
        if can:
            transmission_delay = self.latencies[homedge_id][homedge_node.id]
            return homedge_node, cold, transmission_delay
        
        return None, False, 0

    def step(self, t):
        self.time = t

        # 1. Release completed functions
        for edge in self.edges:
            for func in list(edge.queue):
                if func.end_time and func.end_time <= t:
                    edge.release(func)
                    self.completed_funcs.append(func)
                    if func.end_time > self.simulation_end_time:
                        self.simulation_end_time = func.end_time

        # 2. Add new arrivals
        if t in self.arrivals:
            for cid, func_chain in self.arrivals[t].items():
                self.waiting_chains[cid] = func_chain
                self.ready_times[cid] = func_chain.arrival_time

        # 3. If no chain is committed, perform admission control and selection
        if not self.committed_chain_id:
            eligible_chains = {}
            globally_cached = self._get_globally_cached_funcs()

            # --- Admission Control: Feasibility Check ---
            # We still drop chains that are mathematically impossible to finish by their deadline
            unstarted_cids = [cid for cid, chain in self.waiting_chains.items()
                              if chain.ready and t >= self.ready_times[cid] and chain.next_func_idx == 0]

            for cid in unstarted_cids:
                chain = self.waiting_chains[cid]
                
                projected_time = 0
                avg_transmission_delay = 3 # Example: assume average neighbor delay
                for f in chain.functions:
                    projected_time += f.exec_time
                    projected_time += avg_transmission_delay
                    if f.func_type not in globally_cached:
                        projected_time += COLD_START_DELAY
                
                if t + projected_time <= chain.chain_deadline:
                    eligible_chains[cid] = chain
                else:
                    self.rejected_chains.append(chain)
                    del self.waiting_chains[cid]

            # --- Scoring and Selection from eligible chains ---
            if eligible_chains:
                chain_scores = []
                for cid, chain in eligible_chains.items():
                    # PURE GREEDY: Score is strictly expected profit (Revenue - Cost)
                    revenue = chain.get_total_chain_price()
                    expected_cost = sum((f.cpu + f.mem) * EDGE_COST_PER_UNIT for f in chain.functions)
                    
                    expected_profit = revenue - expected_cost
                    score = expected_profit
                    
                    chain_scores.append((score, cid))

                # Sort strictly by highest profit
                chain_scores.sort(key=lambda x: x[0], reverse=True)
                
                best_cid = chain_scores[0][1]
                self.committed_chain_id = best_cid
                self.func_to_schedule = self.waiting_chains[best_cid].get_next_func()

        # 4. Try to schedule the function for the committed chain
        if self.committed_chain_id and self.func_to_schedule:
            chain = self.waiting_chains[self.committed_chain_id]
            
            if t >= self.ready_times[chain.chain_id]:
                edge, cold, trans_delay = self._best_edge_for_func(self.func_to_schedule, chain.homedge)
                
                if edge:
                    func = self.func_to_schedule
                    func.start_time = t
                    func.end_time = t + func.exec_time + (COLD_START_DELAY if cold else 0) + trans_delay
                    edge.assign(func)

                    chain.advance()
                    self.func_to_schedule = None

                    if chain.ready:
                        self.ready_times[chain.chain_id] = func.end_time
                        self.func_to_schedule = chain.get_next_func()
                    else:
                        chain.completion_time = func.end_time
                        self.completed_chains.append(chain)
                        del self.waiting_chains[chain.chain_id]
                        self.committed_chain_id = None

        # 5. Best-effort scheduling for other chains (respecting private policy)
        other_ready_cids = [cid for cid, chain in self.waiting_chains.items()
                            if chain.ready and t >= self.ready_times[cid] and cid != self.committed_chain_id]

        # PURE GREEDY: Sort the best-effort queue by strict profit as well
        other_ready_cids.sort(
            key=lambda cid: self.waiting_chains[cid].get_total_chain_price() - sum((f.cpu + f.mem) * EDGE_COST_PER_UNIT for f in self.waiting_chains[cid].functions), 
            reverse=True
        )

        for cid in other_ready_cids:
            chain = self.waiting_chains[cid]
            func = chain.get_next_func()
            edge, cold, trans_delay = self._best_edge_for_func(func, chain.homedge)
            
            if edge:
                if t + func.exec_time <= chain.chain_deadline:
                    func.start_time = t
                    func.end_time = t + func.exec_time + (COLD_START_DELAY if cold else 0) + trans_delay
                    edge.assign(func)
                    chain.advance()

                    if not chain.ready:
                        chain.completion_time = func.end_time
                        self.completed_chains.append(chain)
                        del self.waiting_chains[cid]
                    else:
                        self.ready_times[cid] = func.end_time
    
    def summary(self):
        final_rejected = self.rejected_chains + list(self.waiting_chains.values())

        profit = sum(chain.get_total_chain_price() for chain in self.completed_chains)
        cost = 0
        for chain in self.completed_chains:
            for func in chain.functions:
                cost += (func.cpu + func.mem) * EDGE_COST_PER_UNIT

        edge_dist = {edge.id: edge.tasks_serviced for edge in self.edges}

        # 1. Edge Distribution (Raw Task Count)
        edge_dist = {edge.id: edge.tasks_serviced for edge in self.edges}

        # 2. Edge Utilization Score (Tasks relative to Capacity)
        # Because we don't track execution time per unit, we use Tasks per CPU Unit.
        # Higher score = the edge was utilized more heavily for its size.
        edge_utilization = {}
        for edge in self.edges:
            if edge.cpu_capacity > 0:
                # Calculate: Tasks serviced per unit of CPU capacity
                load_score = edge.tasks_serviced / edge.cpu_capacity
                edge_utilization[edge.id] = round(load_score, 3)
            else:
                edge_utilization[edge.id] = 0.0

        total_funcs = sum(edge_dist.values())
        return {
            "scheduler": "Greedy-P",
            "func_completed": len(self.completed_funcs),
            "fch_completed": len(self.completed_chains),
            "fch_rejected": len(final_rejected),
            "Rev": round(profit, 2),
            "cost": round(cost, 2),
            "net_profit": round(profit - cost, 2),
            "total_simulation_time": self.simulation_end_time,
            "edge_distribution": edge_dist,
            "edge_utilization": edge_utilization  # Added metric
        }