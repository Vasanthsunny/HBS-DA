# config.py
# --------------------- Configuration --------------------- #


COLD_START_DELAY = 3
EDGE_COST_PER_UNIT = 3
CLOUD_COST_PER_UNIT = 5
CLOUD_LATENCY=10  # Latency to the cloud in time units
ALPHA = 0.4  # Weighting factor for profit vs. execution time in hybrid scheduling
TASK_SIZES = [500]
EDGE_SIZES = [50]
BEAM_WIDTH = 3
MAX_TIME = 1500
neighbors=None

# function types now have associated resource costs for caching
FUNCTION_TYPES = {
    "F0": {"cpu": 2, "mem": 10},
    "F1": {"cpu": 4, "mem": 15},
    "F2": {"cpu": 3, "mem": 20},
    "F3": {"cpu": 8, "mem": 25},
    "F4": {"cpu": 6, "mem": 30},
    "F5": {"cpu": 12, "mem": 35},
    "F6": {"cpu": 10, "mem": 40},
    "F7": {"cpu": 7, "mem": 45},
    "F8": {"cpu": 5, "mem": 50},
    "F9": {"cpu": 1, "mem": 55},
}

