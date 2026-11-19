#!/usr/bin/env python3
"""Independent oracle: recomputes the summary in Python (different language, different code path)."""
import re, sys
levels = ["WARN", "ERROR", "FATAL"]; args = sys.argv[1:]
if args[0] == "--levels": levels = args[1].split(","); args = args[2:]
rx = re.compile(rb"^\d{4}-\d\d-\d\d \d\d:\d\d:\d\d (DEBUG|INFO|WARN|ERROR|FATAL) .+$", re.S)
tot = bad = drop = sel = 0; cnt = {l: 0 for l in ["DEBUG", "INFO", "WARN", "ERROR", "FATAL"]}; first = last = "-"
with open(args[0], "rb") as f:
    for raw in f:
        tot += 1; line = raw.rstrip(b"\r\n")
        if len(raw.rstrip(b"\n")) > 4094 or len(line) < 22 or not rx.match(line): bad += 1; continue
        lv = rx.match(line).group(1).decode()
        if lv in levels:
            sel += 1; cnt[lv] += 1; ts = line[:19].decode(); first = ts if first == "-" else first; last = ts
        else: drop += 1
print(f"=== LOG SUMMARY ===\ntotal_lines={tot}\nmalformed={bad}\nvalid={sel+drop}\nfiltered_out={drop}\nselected={sel}")
for l in ["DEBUG", "INFO", "WARN", "ERROR", "FATAL"]:
    if l in levels: print(f"  {l:<5} {cnt[l]}")
print(f"first_ts={first}\nlast_ts={last}")
