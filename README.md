# Topic 2 - Interprocess Communication Through a Log-Processing Pipeline (ID 2403002)

Three cooperating processes connected by anonymous pipes, supervised by a parent:

    reader --pipe1--> filter --pipe2--> sink (summary)        parent: fork / waitpid / restart

## Build and run
    make                                   # needs gcc, make (Linux/macOS), python3 for helpers
    ./gen_logs.py 20000 data/demo.log --bad 0.02      # synthetic log with 2% malformed lines
    ./logpipe data/demo.log                           # pipeline (report on stdout, lifecycle log on stderr)
    ./logpipe --mode single data/demo.log             # single-process version (same report)
    ./logpipe --levels ERROR,FATAL data/demo.log      # choose which severities are kept
    ./logpipe --every 1000 --inject filter:5000:kill data/demo.log   # crash a stage, watch recovery
    ./logpipe --timeout 2 --inject filter:3000:hang data/demo.log    # hung stage, watchdog restarts
    make test                                         # 13 tests (normal, boundary, failure)
    make bench                                        # timing -> results/bench.csv

Options: `--ckpt FILE --every N --restarts N --timeout SEC --work N --inject stage:seq:exit|kill|hang`.
Exit codes: 0 ok, 1 pipeline failed after all restarts, 2 unrecoverable (bad input path), 64 usage.

## Files
`logpipe.c` source | `gen_logs.py` log generator | `ref_summary.py` independent Python oracle |
`tests/run_tests.sh`, `tests/malformed.log` | `bench.py` | `results/` genuine outputs (tests, benchmark, sample run, crash recovery).

## Limitations
Text protocol costs CPU; a single global watchdog (not per-stage); the whole pipeline is restarted, not one stage;
benchmarks were taken on a 1-vCPU VM so no parallel speed-up could be observed; level set is fixed (5 levels).
