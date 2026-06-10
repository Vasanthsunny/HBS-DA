# HBS-DA

## Profit-Aware Function Chain Scheduling in Collaborative Serverless Edge Cloud System (HBS-DA Scheduler)

This project implements the proposed **HBS-DA scheduling approach** for collaborative serverless edge-cloud systems. The execution process is driven by `main.py`, which reads task and edge information from formatted CSV files located in the `input/` directory.

---

# Project Structure

- `main.py`  
  Main driver script responsible for execution and workflow management.

- `hbsda.py`  
  Contains the core implementation of the proposed HBS-DA scheduling algorithm.

- `config.py`  
  Stores configuration parameters and tuning settings used during execution.

- `modules.py`  
  Defines the core classes such as `Edge`, `Cloud`, and related system components.

- `sched_for_compare.py`  
  Implements baseline and comparison scheduling approaches.

- `input/`  
  Directory containing all input dataset files.

---

# Input Data Format

Input CSV files must be placed inside the `input/` directory and follow the naming convention below:

```text
deadlinetype_tasksize_edgesize.csv
```

Example:

```text
tightdead_500_50.csv
```

---

# Usage Instructions

## 1. Prepare Input Dataset

Place the formatted CSV dataset file inside the `input/` directory.

Then, update the target dataset filename in `main.py`.

---

## 2. Update Configuration

Open `config.py` and modify the configuration parameters according to the selected dataset.

> **Important:**  
> Whenever you change the target input file in `main.py`, you must also manually update the corresponding `tasksize` and `edgesize` parameters in `config.py`.

---

## 3. Run the Scheduler

Execute the main program from the terminal:

```bash
python main.py
```

---

# Requirements

Make sure the Python interpretor is installed before running the project.

---

# Notes

- Ensure that dataset filenames follow the required naming convention.
- Incorrect `tasksize` or `edgesize` values in `config.py` may lead to execution errors or invalid scheduling results.
- All experiments and scheduling evaluations are performed through `main.py`.
