# modules.py
# In modules.py

from collections import defaultdict
from config import COLD_START_DELAY, FUNCTION_TYPES, CLOUD_COST_PER_UNIT,CLOUD_LATENCY

class EdgeNode:
    # STEP 2: Add 'neighbors=None' to the constructor
    def __init__(self, eid, cpu_cap, mem_cap, cached_funcs, cache_size=5, neighbors=None):
        self.id = eid
        self.cpu_capacity = cpu_cap
        self.memory_capacity = mem_cap
        self.cpu_available = cpu_cap
        self.memory_available = mem_cap
        self.tasks_serviced = 0
        
        # STEP 3: Use the parameter, not a global variable
        self.neighbors = set(neighbors) if neighbors else set()

        # ... (the rest of the __init__ method is correct)

        # cache
        self.cache_size = cache_size
        self.cached_funcs = set(cached_funcs)
        self.usage_counter = {f: 0 for f in self.cached_funcs}

        # reduce available resources based on initial cached funcs
        for f_type in self.cached_funcs:
            res = FUNCTION_TYPES.get(f_type, {"cpu": 0, "mem": 0})
            if res['cpu'] != 0 and res['mem'] != 0:
                self.cpu_available -= res["cpu"]
                self.memory_available -= res["mem"]

        # scheduling state
        self.active_funcs = defaultdict(int)
        self.max_active = 7
        self.queue = []  # required by Beam

    # ===== Beam-compatible API (takes a Func) =====
    def can_schedule(self, func):
        cold = func.func_type not in self.cached_funcs
        delay = COLD_START_DELAY if cold else 0
        active_total = sum(self.active_funcs.values())
        if active_total >= self.max_active:
            return False, delay
        if func.cpu <= self.cpu_available and func.mem <= self.memory_available:
            return True, delay
        return False, delay

    def assign(self, func):
        self.cpu_available -= func.cpu
        self.memory_available -= func.mem
        self.active_funcs[func.func_type] += 1
        self.queue.append(func)
        self.tasks_serviced += 1
        self._update_cache(func.func_type)

    def release(self, func):
        self.cpu_available += func.cpu
        self.memory_available += func.mem
        if self.active_funcs[func.func_type] > 0:
            self.active_funcs[func.func_type] -= 1
        if func in self.queue:
            self.queue.remove(func)

    # ===== Policy-compatible API (takes raw resources + func_id) =====
    def can_schedule_req(self, cpu, mem, func_type, delay_allowed):
        cold = func_type not in self.cached_funcs
        delay = COLD_START_DELAY if cold else 0
        active_total = sum(self.active_funcs.values())
        if active_total >= self.max_active:
            return False, delay
        if cpu <= self.cpu_available and mem <= self.memory_available and delay <= delay_allowed:
            return True, delay
        return False, delay

    def assign_req(self, cpu, mem, func_type):
        self.cpu_available -= cpu
        self.memory_available -= mem
        self.active_funcs[func_type] += 1
        self.tasks_serviced += 1
        self._update_cache(func_type)

    def release_req(self, cpu, mem, func_type):
        self.cpu_available += cpu
        self.memory_available += mem
        if self.active_funcs[func_type] > 0:
            self.active_funcs[func_type] -= 1

    # ===== Cache policy =====
    def _update_cache(self, func_type):
        self.usage_counter[func_type] = self.usage_counter.get(func_type, 0) + 1

        if func_type in self.cached_funcs:
            return

        if len(self.cached_funcs) < self.cache_size:
            self.cached_funcs.add(func_type)
            res = FUNCTION_TYPES.get(func_type, {"cpu": 0, "mem": 0})
            self.cpu_available -= res["cpu"]
            self.memory_available -= res["mem"]
            return

        # eviction policy (LFU)
        least_used = min(self.cached_funcs, key=lambda f: self.usage_counter.get(f, 0))

        # release resources of evicted func
        res_old = FUNCTION_TYPES.get(least_used, {"cpu": 0, "mem": 0})
        self.cpu_available += res_old["cpu"]
        self.memory_available += res_old["mem"]

        # replace with new func
        self.cached_funcs.remove(least_used)
        self.cached_funcs.add(func_type)

        res_new = FUNCTION_TYPES.get(func_type, {"cpu": 0, "mem": 0})
        self.cpu_available -= res_new["cpu"]
        self.memory_available -= res_new["mem"]

#------------------------------Cloud definition------------------------------

class Cloud:
    """
    Represents a centralized cloud environment with unlimited resources.
    """
    def __init__(self, initial_cached_funcs=None):
        self.cached_funcs = set(initial_cached_funcs) if initial_cached_funcs else set()
        self.chains_serviced = 0

    def execute_chain(self, chain):
        total_execution_time = 0
        total_cost = 0

        # --- THIS IS THE FIX ---
        # Iterate ONLY over the remaining functions in the chain.
        for func in chain.functions[chain.next_func_idx:]:
            # Add the base execution time of the function
            total_execution_time = func.exec_time+func.exec_time + CLOUD_LATENCY  # Adding fixed cloud latency
            total_cost += (func.cpu + func.mem) * CLOUD_COST_PER_UNIT

            # Check for a cold start
            if func.func_type not in self.cached_funcs:
                total_execution_time = COLD_START_DELAY+func.exec_time+CLOUD_LATENCY  # Adding fixed cloud latency
                # After a cold start, the function is now cached
                self.cached_funcs.add(func.func_type)
        
        self.chains_serviced += 1
        return total_execution_time, total_cost

            

    

# ------------------------------Function Chain definition------------------------------
class FuncChain:
    def __init__(self, chain_id, arrival_time, chain_deadline, homedge):
        self.chain_id = chain_id
        self.arrival_time = arrival_time
        self.chain_deadline = chain_deadline
        self.homedge = homedge  # The homedge for the entire chain
        self.functions = []  # A list of Func objects
        self.next_func_idx = 0
        self.completion_time = None
    
    @property
    def ready(self):
        return self.next_func_idx < len(self.functions)

    def get_next_func(self):
        if self.ready:
            return self.functions[self.next_func_idx]
        return None

    def advance(self):
        self.next_func_idx += 1

    def get_total_chain_price(self):
        """Calculates the total revenue from all functions in the chain."""
        return sum(func.calc_price() for func in self.functions)
    

# ------------------------------Function definition------------------------------
class Func:
    def __init__(self, row,chain_id=None):
        self.func_id = int(row['func_id'])
        self.func_type = row['func_type']
        self.cpu = float(row['cpu_usg'])
        self.mem = float(row['mem_usg'])
        self.exec_time = float(row['func_delay'] + row['platform_delay'])
        self.deadline = float(row.get('deadline', float('inf')))
        self.chain_id=chain_id
        self.start_time = None
        self.end_time = None
        self.step = None

    def calc_price(self):
        urgency = max(1, 10 / max(1e-6, self.exec_time))
        return (self.cpu * 100 + self.mem * 2) * urgency

