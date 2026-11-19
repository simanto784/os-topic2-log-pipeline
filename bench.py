#!/usr/bin/env python3
"""Benchmark single-process vs 3-process pipeline. 5 runs each, median wall time + total CPU time."""
import subprocess, time, resource, statistics as st, os, sys, csv
RUNS = 5; os.makedirs("results", exist_ok=True); os.makedirs("data", exist_ok=True)
def run(args):
    r0 = resource.getrusage(resource.RUSAGE_CHILDREN); t = time.perf_counter()
    subprocess.run(args, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
    w = time.perf_counter() - t; r1 = resource.getrusage(resource.RUSAGE_CHILDREN)
    return w, (r1.ru_utime - r0.ru_utime) + (r1.ru_stime - r0.ru_stime)
rows = []
cases = [(n, 0) for n in (10_000, 100_000, 1_000_000, 3_000_000)] + [(1_000_000, w) for w in (200, 1000)]
for n, work in cases:
    f = f"data/b{n}.log"
    if not os.path.exists(f): subprocess.run(["./gen_logs.py", str(n), f, "--bad", "0.01"], check=True)
    out = {}
    for mode in ("single", "pipeline"):
        a = ["./logpipe", "--mode", mode, "--work", str(work), "--ckpt", "data/bench.ckpt", f]
        run(a); res = [run(a) for _ in range(RUNS)]
        out[mode] = (st.median(r[0] for r in res), st.median(r[1] for r in res))
    rows.append([n, work, round(out["single"][0], 4), round(out["pipeline"][0], 4), round(out["single"][1], 4),
                 round(out["pipeline"][1], 4), round(out["single"][0] / out["pipeline"][0], 3)])
    print(rows[-1], flush=True)
with open("results/bench.csv", "w", newline="") as fh:
    w = csv.writer(fh); w.writerow(["lines", "work_per_record", "single_wall_s", "pipeline_wall_s", "single_cpu_s", "pipeline_cpu_s", "speedup"]); w.writerows(rows)
