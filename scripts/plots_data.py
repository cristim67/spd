#!/usr/bin/env python3
"""Transformă results/*.csv în date pentru pgfplots (docs/data/*.csv) și în
macro-uri LaTeX cu valorile citate în text (docs/data/numbers.tex).

Folosire în LaTeX: \\R{e1/local/rps} -> „37.405" (format românesc)."""

import csv
import os
from collections import defaultdict

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RES = os.path.join(ROOT, "results")
OUT = os.path.join(ROOT, "docs", "data")
os.makedirs(OUT, exist_ok=True)
macros: dict[str, str] = {}


def read(name):
    path = os.path.join(RES, name)
    if not os.path.exists(path):
        return []
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def write(name, header, rows):
    with open(os.path.join(OUT, name), "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)


def ro(x, dec=0):
    """Format românesc: separator de mii „.", zecimale „,"."""
    s = f"{x:,.{dec}f}"
    return s.replace(",", "\0").replace(".", ",").replace("\0", ".")


def last(rows, **kv):
    """Ultimul rând care se potrivește (ultima rulare câștigă)."""
    hit = [r for r in rows if all(r[k] == v for k, v in kv.items())]
    return hit[-1] if hit else None


def f(r, k):
    return float(r[k]) if r else float("nan")


http = read("http.csv")

# E1 — backend-uri.
e1 = []
for i, b in enumerate(["none", "local", "cluster", "tiered", "redis", "pgmem"]):
    r = last(http, experiment="e1", label=b)
    if not r:
        continue
    e1.append([i, b, f(r, "throughput_rps"), f(r, "p50_ms"), f(r, "p95_ms"), f(r, "p99_ms"), f(r, "hit_ratio")])
    macros[f"e1/{b}/rps"] = ro(f(r, "throughput_rps"))
    macros[f"e1/{b}/p50"] = ro(f(r, "p50_ms"), 2)
    macros[f"e1/{b}/p99"] = ro(f(r, "p99_ms"), 1)
    macros[f"e1/{b}/hit"] = ro(100 * f(r, "hit_ratio"), 1)
write("e1.csv", ["idx", "backend", "rps", "p50", "p95", "p99", "hit"], e1)
if e1:
    base = next((x[2] for x in e1 if x[1] == "none"), None)
    if base:
        for x in e1:
            macros[f"e1/{x[1]}/speedup"] = ro(x[2] / base, 2)


def pivot(exp, keyfmt, backends, cols, key="label", by="backend"):
    rows = [r for r in http if r["experiment"] == exp]
    keys = sorted({r[key] for r in rows}, key=float)
    out = []
    for k in keys:
        line = [k]
        for b in backends:
            r = last(rows, **{key: k, by: b})
            line += [f(r, c) for c in cols]
            for c in cols:
                if r:
                    v = f(r, c)
                    txt = (ro(100 * v, 1) if c == "hit_ratio" else ro(v, 2) if c.endswith("_ms") else ro(v, 0))
                    macros[f"{exp}/{b}/{keyfmt(k)}/{c}"] = txt
        out.append(line)
    header = [key] + [f"{b}_{c}" for b in backends for c in cols]
    return header, out


cols = ["throughput_rps", "p50_ms", "p99_ms", "hit_ratio"]
def ideal(n, c, s):
    h = lambda m: sum(i ** -s for i in range(1, m + 1))  # noqa: E731
    return h(c) / h(n)


for exp, backends in [("e2", ["none", "local", "cluster"]), ("e4", ["none", "local", "cluster"]),
                      ("e5", ["none", "cluster"]), ("e6", ["cluster"])]:
    h, rows = pivot(exp, str, backends, cols)
    if exp == "e2":  # limita IRM pentru un cache cu 10% din chei
        h = h + ["ideal"]
        rows = [r + [ideal(100_000, 10_000, float(r[0]))] for r in rows]
    write(f"{exp}.csv", h, rows)

# E3 — strategii de scriere (pivot pe strategie).
h, rows = pivot("e3", str, ["invalidate", "write-through", "write-behind"],
                cols + ["db_writes", "requests", "duration_s"], by="strategy")
# Tranzacții DB per 1000 de scrieri cerute de clienți.
idx = {name: i for i, name in enumerate(h)}
h2 = h + [f"{s}_tx_per_1k" for s in ["invalidate", "write-through", "write-behind"]]
rows2 = []
for line in rows:
    rr = float(line[0])
    extra = []
    for s in ["invalidate", "write-through", "write-behind"]:
        writes = line[idx[f"{s}_requests"]] * (1 - rr)
        tx = line[idx[f"{s}_db_writes"]]
        v = 1000 * tx / writes if writes else float("nan")
        extra.append(v)
        macros[f"e3/{s}/{line[0]}/tx1k"] = ro(v, 0)
    rows2.append(line + extra)
write("e3.csv", h2, rows2)

# E7 — stampede.
e7 = []
for c in ["true", "false"]:
    r = last(http, experiment="e7", label=c)
    if r:
        e7.append([1 if c == "true" else 0, f(r, "db_reads"), f(r, "throughput_rps"), f(r, "p99_ms"), f(r, "coalesced")])
        for k in ["db_reads", "throughput_rps", "p99_ms", "coalesced"]:
            macros[f"e7/{c}/{k}"] = ro(f(r, k), 1 if k == "p99_ms" else 0)
write("e7.csv", ["coalesce", "db_reads", "rps", "p99", "coalesced"], e7)

# Simulare politici.
sim = read("sim_hit_ratio.csv")
groups = defaultdict(dict)
for r in sim:
    groups[(r["workload"], r["zipf"])].setdefault(float(r["capacity_pct"]), {})[r["policy"]] = float(r["hit_ratio"])
for (wl, s), caps in groups.items():
    name = f"sim_{wl.replace('+', '_')}_{s.replace('.', '')}.csv"
    rows = []
    for cap in sorted(caps):
        p = caps[cap]
        rows.append([cap] + [p.get(k, float("nan")) for k in ["lru", "fifo", "clock", "ideal"]])
        for k, v in p.items():
            macros[f"sim/{wl}/{s}/{int(cap)}/{k}"] = ro(100 * v, 1)
    write(name, ["cap", "lru", "fifo", "clock", "ideal"], rows)

# Scalabilitatea motorului.
eng = read("engine_scaling.csv")
by_t = defaultdict(dict)
series = []
for r in eng:
    # Serii: s<shards> pentru LRU/Zipf 0,99; s64clock și s64unif pentru variante.
    name = f"s{r['shards']}" + {("lru", "0.99"): "", ("clock", "0.99"): "clock", ("lru", "0"): "unif"}.get(
        (r["policy"], r["zipf"]), f"{r['policy']}{r['zipf']}")
    if name not in series:
        series.append(name)
    by_t[int(r["threads"])][name] = (float(r["mops"]), float(r["speedup"]))
rows = []
for t in sorted(by_t):
    line = [t]
    for s in series:
        m, sp = by_t[t].get(s, (float("nan"), float("nan")))
        line += [m, sp]
        macros[f"eng/{s}/{t}/mops"] = ro(m, 2)
        macros[f"eng/{s}/{t}/speedup"] = ro(sp, 2)
    rows.append(line)
write("engine.csv", ["threads"] + [f"{s}_{k}" for s in series for k in ["mops", "speedup"]], rows)

# E8 — CPU la debit fix (8.000 cereri/s), agregat pe roluri.
cpu = read("e8_cpu.csv")
RATE = 8000
roles = {"app": ("app1", "app2"), "db": ("postgres",), "cache": ("cache1", "cache2", "cache3", "redis", "pgmem"),
         "nginx": ("nginx",), "loadgen": ("loadgen",)}
e8 = []
for i, b in enumerate(["none", "local", "cluster", "tiered", "redis", "pgmem"]):
    rows = [r for r in cpu if r["backend"] == b]
    if not rows:
        continue
    v = {k: sum(float(r["cpu"]) for r in rows if r["container"] in names) for k, names in roles.items()}
    server = v["app"] + v["db"] + v["cache"]
    us_per_req = server / 100 * 1e6 / RATE
    e8.append([i, b, v["app"], v["db"], v["cache"], v["nginx"], v["loadgen"], server, us_per_req])
    macros[f"e8/{b}/server"] = ro(server, 0)
    macros[f"e8/{b}/db"] = ro(v["db"], 0)
    macros[f"e8/{b}/usreq"] = ro(us_per_req, 0)
write("e8.csv", ["idx", "backend", "app", "db", "cache", "nginx", "loadgen", "server", "us_per_req"], e8)

# Optimizări (tabel generat).
opt = read(os.path.join("opt", "opt.csv"))
steps = [
    ("baseline-none", "Inițial, fără cache"),
    ("baseline", "Inițial, cache local"),
    ("db-tuned", "+ tuning PostgreSQL (fără paralelism, indecși acoperitori)"),
    ("app-opt", "+ calea fierbinte (mimalloc, metrici, fără tracing)"),
    ("notify-opt-local", "+ NOTIFY condiționat — cache local"),
    ("final-none", "\\quad același pas, fără cache"),
    ("mat-local", "+ agregate materializate, RETURNING — cache local"),
    ("mat-cluster", "\\quad același pas — cluster"),
    ("mat-none", "\\quad același pas — fără cache"),
]
lines = []
for key, desc in steps:
    r = last(opt, label=key)
    if r:
        lines.append(f"{desc} & {ro(f(r, 'throughput_rps'))} & {ro(f(r, 'p50_ms'), 2)} & "
                     f"{ro(f(r, 'p99_ms'), 1)} & {ro(100 * f(r, 'hit_ratio'), 1)}\\,\\% \\\\")
        macros[f"opt/{key}/rps"] = ro(f(r, "throughput_rps"))
        macros[f"opt/{key}/p99"] = ro(f(r, "p99_ms"), 1)

with open(os.path.join(OUT, "numbers.tex"), "w") as fh:
    fh.write("% Generat de scripts/plots_data.py — nu edita manual.\n")
    fh.write("\\providecommand{\\R}[1]{\\ifcsname r@#1\\endcsname\\csname r@#1\\endcsname\\else\\textbf{??}\\fi}\n")
    for k, v in sorted(macros.items()):
        fh.write(f"\\expandafter\\def\\csname r@{k}\\endcsname{{{v}}}\n")
    if lines:
        fh.write("\\def\\OptTable{\\begin{table}[H]\\centering\\small\n"
                 "\\begin{tabularx}{\\linewidth}{Xrrrr}\\toprule\n"
                 "Pas & cereri/s & p50 (ms) & p99 (ms) & hit \\\\ \\midrule\n")
        fh.write("\n".join(lines))
        fh.write("\n\\bottomrule\\end{tabularx}\n\\caption{Efectul optimizărilor (64 clienți, 95\\,\\% citiri, "
                 "Zipf 0,99, rulări de 15 s).}\\label{tab:opt}\\end{table}}\n")
print(f"wrote {len(macros)} macros and data files to {OUT}")
