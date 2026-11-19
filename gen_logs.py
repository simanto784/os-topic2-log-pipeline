#!/usr/bin/env python3
"""Generate a synthetic application log. Usage: gen_logs.py N OUT [--bad RATE] [--seed S]"""
import random, sys, argparse, datetime
ap = argparse.ArgumentParser(); ap.add_argument("n", type=int); ap.add_argument("out")
ap.add_argument("--bad", type=float, default=0.0); ap.add_argument("--seed", type=int, default=1)
a = ap.parse_args(); rnd = random.Random(a.seed)
LV = ["DEBUG"] * 40 + ["INFO"] * 40 + ["WARN"] * 8 + ["ERROR"] * 10 + ["FATAL"] * 2
EV = ["login_ok", "login_failed", "db_timeout", "cache_miss", "disk_full", "payment_declined", "retry", "shutdown"]
t = datetime.datetime(2026, 1, 1)
BAD = [lambda l: "", lambda l: "garbage ### no structure", lambda l: l[:10],            # empty / garbage / truncated
       lambda l: "2026-13-xx 99:99:99 " + l[20:], lambda l: l.replace(" ERROR ", " TRACE ").replace(" INFO ", " TRACE "),
       lambda l: l[:20], lambda l: "X" * 5000]                                           # level only / over-long line
with open(a.out, "w") as f:
    for i in range(a.n):
        t += datetime.timedelta(seconds=rnd.randint(0, 3))
        line = f"{t:%Y-%m-%d %H:%M:%S} {rnd.choice(LV)} svc={rnd.randint(1,9)} event={rnd.choice(EV)} id={i}"
        if a.bad and rnd.random() < a.bad: line = rnd.choice(BAD)(line)
        f.write(line + "\n")
