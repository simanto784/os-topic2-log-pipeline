/*
 * logpipe.c - Log-processing pipeline built from cooperating processes.
 *
 *   reader --pipe1--> filter --pipe2--> sink (summary)      supervised by the parent
 *
 * OS concepts: fork(), pipe(), fd inheritance, waitpid(), EOF / EPIPE / SIGPIPE,
 * process supervision, atomic file replacement (rename) for checkpoints.
 *
 * Wire protocol (one text line per message, '\n' terminated):
 *   R|<seq>|<YYYY-MM-DD HH:MM:SS>|<level 0..4>|<message>   valid record
 *   M|<seq>                                                 malformed input line
 *   D|<seq>|<n>                                             n records were filtered out (last one = seq)
 *   E|<N>                                                   end marker: input had N lines
 * seq = 1-based input line number. Sink checks passed+dropped+malformed == N.
 */
#define _POSIX_C_SOURCE 200809L
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <signal.h>
#include <ctype.h>
#include <stdarg.h>
#include <time.h>
#include <sys/wait.h>

#define MAXLINE 4096
#define NLEV 5
static const char *LEVN[NLEV] = {"DEBUG", "INFO", "WARN", "ERROR", "FATAL"};
static unsigned g_mask = (1u << 2) | (1u << 3) | (1u << 4); /* default: WARN,ERROR,FATAL */
static long g_work = 0;                                     /* synthetic per-record CPU work */
static volatile unsigned g_sinkx;
static int g_attempt = 0;
static struct timespec g_t0;

typedef struct {
    long last_seq, passed, dropped, malformed, lev[NLEV];
    char first[20], last[20];
} State;

/* ---------- helpers ---------- */
static void logf_(const char *fmt, ...) __attribute__((format(printf, 1, 2)));
static void logf_(const char *fmt, ...) {
    struct timespec t; clock_gettime(CLOCK_MONOTONIC, &t);
    double s = (t.tv_sec - g_t0.tv_sec) + (t.tv_nsec - g_t0.tv_nsec) / 1e9;
    fprintf(stderr, "[%8.3f pid=%d] ", s, (int)getpid());
    va_list ap; va_start(ap, fmt); vfprintf(stderr, fmt, ap); va_end(ap);
}
static void burn(long n) { unsigned x = 12345; for (long i = 0; i < n; i++) x = x * 1664525u + 1013904223u; g_sinkx ^= x; }

/* Fault injection (attempt 0 only): --inject stage:seq:mode  mode = exit | kill | hang */
static char g_istage[16], g_imode[8]; static long g_iseq = -1;
static void maybe_fail(const char *stage, long seq) {
    if (g_iseq < 0 || g_attempt != 0 || seq < g_iseq || strcmp(stage, g_istage)) return;
    logf_("INJECTED FAULT in %s at seq %ld (%s)\n", stage, seq, g_imode);
    if (!strcmp(g_imode, "exit")) _exit(42);
    if (!strcmp(g_imode, "kill")) raise(SIGKILL);
    for (;;) pause();
}

/* Read one input line (bounded). Returns 0 at EOF. *toolong set if line exceeded MAXLINE-1. */
static int read_line(FILE *in, char *buf, int *toolong) {
    *toolong = 0;
    if (!fgets(buf, MAXLINE, in)) return 0;
    size_t n = strlen(buf);
    if (n && buf[n - 1] != '\n' && !feof(in)) { int c; *toolong = 1; while ((c = fgetc(in)) != '\n' && c != EOF); }
    return 1;
}

/* Validate "YYYY-MM-DD HH:MM:SS LEVEL message". Modifies the line in place. */
static int parse(char *l, char **ts, int *lev, char **msg) {
    size_t n = strlen(l);
    while (n && (l[n - 1] == '\n' || l[n - 1] == '\r')) l[--n] = 0;
    if (n < 22) return 0;
    static const char pat[] = "dddd-dd-dd dd:dd:dd";
    for (int i = 0; i < 19; i++)
        if (pat[i] == 'd' ? !isdigit((unsigned char)l[i]) : l[i] != pat[i]) return 0;
    if (l[19] != ' ') return 0;
    char *lv = l + 20, *sp = strchr(lv, ' ');
    if (!sp || !sp[1]) return 0;
    *sp = 0;
    int k; for (k = 0; k < NLEV; k++) if (!strcmp(lv, LEVN[k])) break;
    if (k == NLEV) return 0;
    l[19] = 0; *ts = l; *lev = k; *msg = sp + 1;
    return 1;
}

static void st_init(State *s) { memset(s, 0, sizeof *s); strcpy(s->first, "-"); strcpy(s->last, "-"); }
static void st_add(State *s, long seq, const char *ts, int lev) {
    s->last_seq = seq; s->passed++; s->lev[lev]++;
    if (!strcmp(s->first, "-")) strcpy(s->first, ts);
    strcpy(s->last, ts);
}
static void report(const State *s, FILE *o) {
    fprintf(o, "=== LOG SUMMARY ===\ntotal_lines=%ld\nmalformed=%ld\nvalid=%ld\nfiltered_out=%ld\nselected=%ld\n",
            s->passed + s->dropped + s->malformed, s->malformed, s->passed + s->dropped, s->dropped, s->passed);
    for (int i = 0; i < NLEV; i++) if (g_mask & (1u << i)) fprintf(o, "  %-5s %ld\n", LEVN[i], s->lev[i]);
    fprintf(o, "first_ts=%s\nlast_ts=%s\n", s->first, s->last);
}

/* ---------- checkpoint (atomic: write tmp, fsync, rename) ---------- */
static int ckpt_save(const char *p, const State *s) {
    char tmp[600]; snprintf(tmp, sizeof tmp, "%s.tmp", p);
    FILE *f = fopen(tmp, "w"); if (!f) return -1;
    fprintf(f, "%ld|%ld|%ld|%ld|%ld|%ld|%ld|%ld|%ld|%s|%s\n", s->last_seq, s->passed, s->dropped, s->malformed,
            s->lev[0], s->lev[1], s->lev[2], s->lev[3], s->lev[4], s->first, s->last);
    int bad = fflush(f) || fsync(fileno(f)); bad |= fclose(f);
    return bad ? -1 : rename(tmp, p);
}
static int ckpt_load(const char *p, State *s) {
    st_init(s); FILE *f = fopen(p, "r"); if (!f) return 0;
    int n = fscanf(f, "%ld|%ld|%ld|%ld|%ld|%ld|%ld|%ld|%ld|%19[^|]|%19[^\n]", &s->last_seq, &s->passed, &s->dropped,
                   &s->malformed, &s->lev[0], &s->lev[1], &s->lev[2], &s->lev[3], &s->lev[4], s->first, s->last);
    fclose(f);
    if (n != 11) { st_init(s); return -1; }
    return 1;
}

/* ---------- Stage 1: reader ---------- */
static int run_reader(const char *path, int wfd, long start) {
    FILE *in = fopen(path, "r");
    if (!in) { logf_("reader: cannot open '%s': %s\n", path, strerror(errno)); return 2; }
    FILE *out = fdopen(wfd, "w"); if (!out) return 2;
    setvbuf(out, NULL, _IOFBF, 1 << 16);
    char buf[MAXLINE + 2], *ts, *msg; int lev, tl; long seq = 0;
    while (read_line(in, buf, &tl)) {
        if (++seq <= start) continue;                 /* already committed by the sink */
        maybe_fail("reader", seq);
        if (tl || !parse(buf, &ts, &lev, &msg)) fprintf(out, "M|%ld\n", seq);
        else fprintf(out, "R|%ld|%s|%d|%s\n", seq, ts, lev, msg);
        if (ferror(out)) { logf_("reader: downstream closed pipe (EPIPE)\n"); return 3; }
    }
    fprintf(out, "E|%ld\n", seq);
    if (fflush(out) || ferror(out)) { logf_("reader: write failed: %s\n", strerror(errno)); return 3; }
    fclose(out); fclose(in);
    return 0;
}

/* ---------- Stage 2: filter ---------- */
static int run_filter(int rfd, int wfd) {
    FILE *in = fdopen(rfd, "r"), *out = fdopen(wfd, "w"); if (!in || !out) return 2;
    setvbuf(out, NULL, _IOFBF, 1 << 16);
    char line[8192]; long seq, dropped = 0, dlast = 0; int lev;
    while (fgets(line, sizeof line, in)) {
        if (!strchr(line, '\n')) { logf_("filter: truncated message (upstream died mid-write)\n"); return 4; }
        if (line[0] == 'R' && sscanf(line + 2, "%ld|%*[^|]|%d", &seq, &lev) == 2 && lev >= 0 && lev < NLEV) {
            maybe_fail("filter", seq);
            if (g_work) burn(g_work);
            if (g_mask & (1u << lev)) {
                if (dropped) { fprintf(out, "D|%ld|%ld\n", dlast, dropped); dropped = 0; }
                fputs(line, out);
            } else { dropped++; dlast = seq; }
        } else if (line[0] == 'M' && sscanf(line + 2, "%ld", &seq) == 1) {
            if (dropped) { fprintf(out, "D|%ld|%ld\n", dlast, dropped); dropped = 0; }
            fputs(line, out);
        } else if (line[0] == 'E') {
            if (dropped) { fprintf(out, "D|%ld|%ld\n", dlast, dropped); dropped = 0; }
            fputs(line, out);
            if (fflush(out) || ferror(out)) { logf_("filter: write failed (downstream closed)\n"); return 3; }
            return 0;
        } else { logf_("filter: protocol error: %.40s\n", line); return 4; }
        if (ferror(out)) { logf_("filter: downstream closed pipe (EPIPE)\n"); return 3; }
    }
    logf_("filter: EOF before end marker (reader failed?)\n");
    return 4;
}

/* ---------- Stage 3: sink / summariser ---------- */
static int run_sink(int rfd, const char *ckpt, long every) {
    FILE *in = fdopen(rfd, "r"); if (!in) return 2;
    State s; int r = ckpt_load(ckpt, &s);
    if (r == 1) logf_("sink: resumed from checkpoint at seq %ld\n", s.last_seq);
    char line[8192], ts[24]; long seq, n, endN = -1, since = 0, dups = 0; int lev;
    while (fgets(line, sizeof line, in)) {
        if (!strchr(line, '\n')) { logf_("sink: truncated message\n"); return 4; }
        switch (line[0]) {
        case 'R':
            if (sscanf(line + 2, "%ld|%19[^|]|%d", &seq, ts, &lev) != 3 || lev < 0 || lev >= NLEV) goto proto;
            maybe_fail("sink", seq);
            if (seq <= s.last_seq) { dups++; continue; }      /* duplicate guard */
            st_add(&s, seq, ts, lev); break;
        case 'M':
            if (sscanf(line + 2, "%ld", &seq) != 1) goto proto;
            if (seq <= s.last_seq) { dups++; continue; }
            s.malformed++; s.last_seq = seq; break;
        case 'D':
            if (sscanf(line + 2, "%ld|%ld", &seq, &n) != 2) goto proto;
            if (seq <= s.last_seq) { dups++; continue; }
            s.dropped += n; s.last_seq = seq; break;
        case 'E':
            if (sscanf(line + 2, "%ld", &endN) != 1) goto proto;
            goto done;
        default: goto proto;
        }
        if (++since >= every) { if (ckpt_save(ckpt, &s)) logf_("sink: checkpoint failed\n"); since = 0; }
    }
    logf_("sink: EOF before end marker - stream incomplete\n");
    return 4;
proto:
    logf_("sink: protocol error: %.40s\n", line); return 4;
done:
    if (s.passed + s.dropped + s.malformed != endN) {
        logf_("sink: INTEGRITY FAILURE accounted=%ld expected=%ld\n", s.passed + s.dropped + s.malformed, endN); return 5;
    }
    ckpt_save(ckpt, &s);
    if (dups) logf_("sink: ignored %ld duplicate messages\n", dups);
    report(&s, stdout); fflush(stdout);
    return 0;
}

/* ---------- Single-process reference implementation ---------- */
static int run_single(const char *path) {
    FILE *in = fopen(path, "r");
    if (!in) { logf_("cannot open '%s': %s\n", path, strerror(errno)); return 2; }
    State s; st_init(&s); char buf[MAXLINE + 2], *ts, *msg; int lev, tl; long seq = 0;
    while (read_line(in, buf, &tl)) {
        seq++;
        if (tl || !parse(buf, &ts, &lev, &msg)) { s.malformed++; continue; }
        if (g_work) burn(g_work);
        if (g_mask & (1u << lev)) st_add(&s, seq, ts, lev); else s.dropped++;
    }
    fclose(in); report(&s, stdout);
    return 0;
}

/* ---------- Supervisor ---------- */
static volatile sig_atomic_t g_alarm;
static void on_alarm(int sig) { (void)sig; g_alarm = 1; }
static void kill_all(pid_t *p, int sig) { for (int i = 0; i < 3; i++) if (p[i] > 0) kill(p[i], sig); }

static int run_pipeline(const char *path, const char *ckpt, long every, int max_restarts, int tmo) {
    static const char *names[3] = {"reader", "filter", "sink"};
    signal(SIGPIPE, SIG_IGN);                                 /* write() reports EPIPE instead of killing us */
    struct sigaction sa; memset(&sa, 0, sizeof sa); sa.sa_handler = on_alarm; sigaction(SIGALRM, &sa, NULL);
    unlink(ckpt);
    for (int att = 0; att <= max_restarts; att++) {
        g_attempt = att;
        State s; ckpt_load(ckpt, &s); long start = s.last_seq;
        int p1[2], p2[2];
        if (pipe(p1) || pipe(p2)) { perror("pipe"); return 1; }
        pid_t pid[3] = {0, 0, 0}; int alive = 0, failed = 0, fatal = 0;
        logf_("supervisor: attempt %d, resume after seq %ld\n", att, start);
        for (int i = 0; i < 3 && !failed; i++) {
            fflush(NULL);
            pid[i] = fork();
            if (pid[i] < 0) { perror("fork"); failed = 1; break; }
            if (pid[i] == 0) {                                  /* child: close every fd it does not own */
                int rc;
                if (i == 0)      { close(p1[0]); close(p2[0]); close(p2[1]); rc = run_reader(path, p1[1], start); }
                else if (i == 1) { close(p1[1]); close(p2[0]); rc = run_filter(p1[0], p2[1]); }
                else             { close(p1[0]); close(p1[1]); close(p2[1]); rc = run_sink(p2[0], ckpt, every); }
                fflush(NULL); _exit(rc);
            }
            alive++; logf_("supervisor: started %s pid=%d\n", names[i], (int)pid[i]);
        }
        close(p1[0]); close(p1[1]); close(p2[0]); close(p2[1]); /* parent keeps no pipe ends -> EOF can propagate */
        if (failed) kill_all(pid, SIGTERM);
        if (tmo > 0) alarm(tmo);
        while (alive > 0) {
            int st; pid_t w = waitpid(-1, &st, 0);
            if (w < 0) {
                if (errno == EINTR) {
                    if (g_alarm) { g_alarm = 0; logf_("supervisor: TIMEOUT %ds - killing pipeline\n", tmo); failed = 1; kill_all(pid, SIGKILL); }
                    continue;
                }
                break;
            }
            for (int i = 0; i < 3; i++) if (pid[i] == w) {
                pid[i] = 0; alive--;
                if (WIFEXITED(st)) logf_("supervisor: %s exited with status %d\n", names[i], WEXITSTATUS(st));
                else logf_("supervisor: %s killed by signal %d\n", names[i], WTERMSIG(st));
                if (WIFEXITED(st) && WEXITSTATUS(st) == 2) fatal = 1;   /* exit 2 = unrecoverable (e.g. bad input path) */
                if (!(WIFEXITED(st) && WEXITSTATUS(st) == 0)) {
                    if (!failed) { failed = 1; logf_("supervisor: %s failed - stopping remaining stages\n", names[i]); }
                    kill_all(pid, SIGTERM);                    /* only our own children, never other processes */
                }
            }
        }
        alarm(0);
        if (!failed) { logf_("supervisor: pipeline completed\n"); return 0; }
        if (fatal) { logf_("supervisor: unrecoverable error (exit 2) - not restarting\n"); return 2; }
        if (att < max_restarts) logf_("supervisor: restarting from checkpoint\n");
    }
    logf_("supervisor: giving up after %d restart(s)\n", max_restarts);
    return 1;
}

/* ---------- main ---------- */
int main(int argc, char **argv) {
    clock_gettime(CLOCK_MONOTONIC, &g_t0);
    const char *mode = "pipeline", *ckpt = "logpipe.ckpt", *file = NULL;
    long every = 10000; int restarts = 2, tmo = 0;
    for (int i = 1; i < argc; i++) {
        const char *a = argv[i]; int hv = i + 1 < argc;
        if (!strcmp(a, "--mode") && hv) mode = argv[++i];
        else if (!strcmp(a, "--ckpt") && hv) ckpt = argv[++i];
        else if (!strcmp(a, "--every") && hv) every = atol(argv[++i]);
        else if (!strcmp(a, "--restarts") && hv) restarts = atoi(argv[++i]);
        else if (!strcmp(a, "--timeout") && hv) tmo = atoi(argv[++i]);
        else if (!strcmp(a, "--work") && hv) g_work = atol(argv[++i]);
        else if (!strcmp(a, "--levels") && hv) {
            g_mask = 0; char *c = strdup(argv[++i]);
            for (char *t = strtok(c, ","); t; t = strtok(NULL, ","))
                for (int k = 0; k < NLEV; k++) if (!strcmp(t, LEVN[k])) g_mask |= 1u << k;
            free(c);
        } else if (!strcmp(a, "--inject") && hv) {
            if (sscanf(argv[++i], "%15[^:]:%ld:%7s", g_istage, &g_iseq, g_imode) != 3) { fprintf(stderr, "bad --inject\n"); return 64; }
        } else if (a[0] != '-') file = a;
        else { fprintf(stderr, "usage: %s [--mode single|pipeline] [--levels L,..] [--ckpt f] [--every N] [--restarts N]\n"
                               "          [--timeout S] [--work N] [--inject stage:seq:exit|kill|hang] logfile\n", argv[0]); return 64; }
    }
    if (!file) { fprintf(stderr, "error: no log file given\n"); return 64; }
    if (every < 1) every = 1;
    return !strcmp(mode, "single") ? run_single(file) : run_pipeline(file, ckpt, every, restarts, tmo);
}
