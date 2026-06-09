from collections import defaultdict
from config import COLD_START_DELAY, EDGE_COST_PER_UNIT, CLOUD_COST_PER_UNIT, ALPHA

class proposedBeamSchedulerEdgeFirst:
    """
    Implements a true Beam Search scheduler using an 'Edge First, Cloud Fallback'
    strategy. Selects a beam of top N chains, tries edge placement first,
    then cloud if edge fails or is infeasible. Uses hybrid placement.
    """
    def __init__(self, arrivals, edges, latencies, beam_width=3, cloud=None, alpha=ALPHA):
        self.arrivals = arrivals
        self.edges = edges
        self.edge_map = {e.id: e for e in edges}
        self.beam_width = beam_width
        self.cloud = cloud
        self.alpha = alpha
        self.time = 0
        self.latencies = latencies

        self.completed_chains = []
        self.offloaded_chains = []
        self.rejected_chains = []
        self.waiting_chains = {}
        self.ready_times = defaultdict(int)
        self.simulation_end_time = 0

    def _get_globally_cached_funcs(self):
        cached = set()
        for edge in self.edges:
            cached.update(edge.cached_funcs)
        return cached

    def _best_edge_for_func(self, func, homedge_id):
        # Hybrid placement policy
        homedge_node = self.edge_map.get(homedge_id)
        if not homedge_node: return None, False, 0

        neighbors = {self.edge_map[nid] for nid in homedge_node.neighbors if nid in self.edge_map}
        other_edges = [e for e in self.edges if e.id != homedge_id and e not in neighbors]

        # Check all cache hits first
        if func.func_type in homedge_node.cached_funcs and homedge_node.can_schedule(func)[0]:
            return homedge_node, False, self.latencies[homedge_id][homedge_node.id]
        for edge in sorted(list(neighbors), key=lambda e: e.id):
            if func.func_type in edge.cached_funcs and edge.can_schedule(func)[0]:
                return edge, False, self.latencies[homedge_id][edge.id]
        for edge in sorted(other_edges, key=lambda e: e.id):
            if func.func_type in edge.cached_funcs and edge.can_schedule(func)[0]:
                return edge, False, self.latencies[homedge_id][edge.id]

        # If no cache hits, try cold starts (Hybrid logic)
        if homedge_node.can_schedule(func)[0]:
            return homedge_node, True, self.latencies[homedge_id][homedge_node.id]

        all_other_edges = [e for e in self.edges if e.id != homedge_id]
        for edge in sorted(all_other_edges, key=lambda e: e.id):
            public_cpu = edge.cpu_available * (1 - self.alpha)
            public_mem = edge.memory_available * (1 - self.alpha)
            if func.cpu <= public_cpu and func.mem <= public_mem and edge.can_schedule(func)[0]:
                return edge, True, self.latencies[homedge_id][edge.id]
        
        return None, False, 0

    def _attempt_scheduling(self, t, chain, func):
        """
        Reusable helper for the 'Edge First, Cloud Fallback' logic.
        Returns True if the function was scheduled/chain finished, False otherwise.
        """
        # Try Edge placement FIRST
        edge, cold, trans_delay = self._best_edge_for_func(func, chain.homedge)
        edge_is_feasible_and_found = False
        edge_finish_time = float('inf')

        if edge:
            edge_finish_time = t + func.exec_time + (COLD_START_DELAY if cold else 0) + trans_delay
            if edge_finish_time <= chain.chain_deadline:
                edge_is_feasible_and_found = True

        if edge_is_feasible_and_found:
            # Assign to Edge
            func.start_time = t; func.end_time = edge_finish_time
            edge.assign(func)
            chain.advance()
            if chain.ready:
                self.ready_times[chain.chain_id] = func.end_time
            else: # Chain completed on edge
                chain.completion_time = func.end_time
                self.completed_chains.append(chain)
                if chain.chain_id in self.waiting_chains:
                    del self.waiting_chains[chain.chain_id]
            return True # Function scheduled or chain completed

        else:
            # Edge failed or was infeasible, try Cloud fallback
            cloud_is_feasible = False
            cloud_finish_time = float('inf')
            if self.cloud:
                # Check feasibility based on execution starting NOW (time t)
                cloud_exec_time, _ = self.cloud.execute_chain(chain)
                cloud_finish_time = t + cloud_exec_time
                if cloud_finish_time <= chain.chain_deadline:
                    cloud_is_feasible = True

            if cloud_is_feasible:
                # Offload to Cloud only because edge failed
                chain.completion_time = cloud_finish_time
                self.offloaded_chains.append(chain)
                if chain.chain_id in self.waiting_chains:
                    del self.waiting_chains[chain.chain_id]
                return True # Chain offloaded

        return False # Function could not be scheduled this step

    def step(self, t):
        self.time = t

        # --- Phase 1: free up resources ---
        for edge in self.edges:
            for func in list(edge.queue):
                if func.end_time and func.end_time <= t:
                    edge.release(func)
                    # Update simulation end time based on actual function completions
                    if func.end_time > self.simulation_end_time:
                         self.simulation_end_time = func.end_time
        
        if t in self.arrivals:
            for cid, func_chain in self.arrivals[t].items():
                self.waiting_chains[cid] = func_chain
                self.ready_times[cid] = func_chain.arrival_time

        # --- Phase 2: Admission Control (Edge First) ---
        globally_cached = self._get_globally_cached_funcs()
        # Check only chains that haven't been checked before (next_func_idx == 0 and ready)
        unstarted_cids = [cid for cid, chain in self.waiting_chains.items()
                          if chain.ready and t >= self.ready_times[cid] and chain.next_func_idx == 0 and not hasattr(chain, '_admission_checked')]

        for cid in unstarted_cids:
            if cid not in self.waiting_chains: continue # Might have been removed already
            chain = self.waiting_chains[cid]
            chain._admission_checked = True # Mark as checked

            # Check Edge feasibility FIRST (based on arrival time)
            projected_edge_time = sum(f.exec_time + (COLD_START_DELAY if f.func_type not in globally_cached else 0) for f in chain.functions)
            edge_is_feasible = chain.arrival_time + projected_edge_time <= chain.chain_deadline

            if not edge_is_feasible:
                # Edge failed admission, check Cloud fallback (based on arrival time)
                cloud_is_feasible = False
                cloud_exec_time = float('inf')
                if self.cloud:
                    _cloud_time, _ = self.cloud.execute_chain(chain) # Gets total time including latency
                    if chain.arrival_time + _cloud_time <= chain.chain_deadline:
                        cloud_is_feasible = True
                        cloud_exec_time = _cloud_time # Store time if feasible
                
                if cloud_is_feasible:
                    # Offload immediately if edge failed AND cloud works on arrival
                    # The completion time will be relative to when it starts, i.e., now 't'
                    chain.completion_time = t + cloud_exec_time
                    self.offloaded_chains.append(chain)
                    del self.waiting_chains[cid]
                else:
                    # Both failed admission, reject
                    self.rejected_chains.append(chain)
                    del self.waiting_chains[cid]
            # Else: Edge is feasible, leave in waiting_chains for scheduling attempt

        # --- Phase 3: Identify, Score, and Select Beam ---
        ready_cids = [cid for cid, chain in self.waiting_chains.items()
                      if chain.ready and t >= self.ready_times[cid]]

        if not ready_cids:
            return # Nothing to schedule

        chain_scores = []
        cids_to_reject_now = []
        for cid in ready_cids:
            chain = self.waiting_chains[cid]
            remaining_exec_time = sum(f.exec_time for f in chain.functions[chain.next_func_idx:])
            laxity = chain.chain_deadline - (t + remaining_exec_time) # Slack time from NOW

            # If laxity < 0, it's impossible even under ideal conditions from now on.
            if laxity < 0:
                cids_to_reject_now.append(cid)
                continue

            # Calculate score for feasible chains
            total_val = chain.get_total_chain_price()
            total_time = sum(f.exec_time for f in chain.functions)
            urgency_bonus = 1 / (laxity + 1) # Add 1 to prevent division by zero
            score = (total_val / (total_time + 1e-9)) * urgency_bonus
            chain_scores.append((score, cid))

        # Reject chains identified as impossible now
        for cid in cids_to_reject_now:
            if cid in self.waiting_chains:
                self.rejected_chains.append(self.waiting_chains[cid])
                del self.waiting_chains[cid]

        # Sort remaining candidates and select beam
        chain_scores.sort(key=lambda x: x[0], reverse=True) # Highest score first
        beam_cids = [cid for score, cid in chain_scores[:self.beam_width]]
        other_cids = [cid for score, cid in chain_scores[self.beam_width:]]

        # --- Phase 4: Attempt Scheduling for Chains in Beam ---
        scheduled_this_step = set() # Track chains scheduled to prevent double scheduling
        for cid in beam_cids:
            if cid not in self.waiting_chains or cid in scheduled_this_step: continue
            chain = self.waiting_chains[cid]
            func = chain.get_next_func()
            if self._attempt_scheduling(t, chain, func):
                scheduled_this_step.add(cid)

        # --- Phase 5: Attempt Best-Effort Scheduling for Others ---
        for cid in other_cids:
            if cid not in self.waiting_chains or cid in scheduled_this_step: continue
            chain = self.waiting_chains[cid]
            func = chain.get_next_func()
            if self._attempt_scheduling(t, chain, func):
                 scheduled_this_step.add(cid)

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
        
        # --- FIX: Calculate total completed functions ---
        total_funcs_completed = len(all_edge_funcs) + len(all_cloud_funcs)
        
        # --- FIX: Calculate correct simulation end time ---
        max_edge_time = self.simulation_end_time
        max_cloud_time = 0
        if self.offloaded_chains:
            max_cloud_time = max(c.completion_time for c in self.offloaded_chains)
        
        total_simulation_time = max(max_edge_time, max_cloud_time)

        return {
            "scheduler": "HBS-DA",
            "func_completed": total_funcs_completed, # ADDED THIS KEY
            "fch_completed": len(self.completed_chains) + len(self.offloaded_chains),
            "fch_offloaded_cloud": len(self.offloaded_chains),
            "fch_rejected": len(final_rejected),
            "cost": round(cost, 2),
            "net_profit": round(profit - cost, 2),
            "total_simulation_time": total_simulation_time, # ADDED THIS KEY
            "edge_distribution": edge_dist,
            "edge_utilization": edge_utilization  # Added metric
        }