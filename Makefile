CC ?= gcc
CFLAGS = -O2 -Wall -Wextra -std=c11
all: logpipe
logpipe: logpipe.c
	$(CC) $(CFLAGS) -o $@ $<
test: logpipe
	./tests/run_tests.sh
bench: logpipe
	python3 bench.py
clean:
	rm -f logpipe *.ckpt *.ckpt.tmp
