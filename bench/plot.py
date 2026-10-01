#!/usr/bin/env python3
"""Make the benchmark chart (SVG) and tables (Markdown) from bench/results/*.csv.

No extra packages needed: the SVG is written by hand.

Output:
  bench/results/pg_wal.svg   one panel per run, same axes: pg_wal size and total WAL written
  bench/results/tables.md    summary table + time-series table (every 30 s)
"""
import csv
import math
from collections import defaultdict
from pathlib import Path

RESULTS = Path(__file__).resolve().parent / "results"
GB = 1024 ** 3

RUNS = [  # run id, panel title, subtitle, light color, dark color
    ("no-hb", "No heartbeat", "cdc_no_hb · pub_no_hb = {customers}", "#2a78d6", "#3987e5"),
    ("interval-only", "Heartbeat messages only", "cdc_interval_only · heartbeat.interval.ms, no table", "#eb6834", "#d95926"),
    ("hb", "Heartbeat table", "cdc_hb · pub_hb = {customers, debezium_heartbeat} + UPDATE every 10 s", "#1baf7a", "#199e70"),
]


def load():
    samples = defaultdict(list)
    with open(RESULTS / "samples.csv") as f:
        for r in csv.DictReader(f):
            samples[r["run"]].append({
                "t": int(r["t_s"]),
                "phase": r["phase"],
                "pg_wal": int(r["pg_wal_bytes"]) / GB,
                "written": int(r["wal_written_bytes"]) / GB,
                "retained": int(r["slot_retained_bytes"]) / GB,
            })
    events = defaultdict(dict)
    with open(RESULTS / "events.csv") as f:
        for r in csv.DictReader(f):
            events[r["run"]][r["event"]] = int(r["t_s"])
    return samples, events


def nice_step(span, target_ticks=5):
    raw = span / target_ticks
    mag = 10 ** math.floor(math.log10(raw))
    for m in (1, 2, 2.5, 5, 10):
        if raw <= m * mag:
            return m * mag
    return 10 * mag


def fmt_gb(v):
    return f"{v:.1f} GB" if v >= 1 else f"{v * 1024:.0f} MB"


def svg_chart(samples, events):
    W, left, right = 880, 76, 24
    legend_h, panel_h, panel_gap, axis_h = 44, 190, 58, 50
    title_h = 38
    runs = [r for r in RUNS if r[0] in samples]
    H = legend_h + len(runs) * (title_h + panel_h) + (len(runs) - 1) * panel_gap + axis_h
    plot_w = W - left - right

    t_max = max(s["t"] for run in samples.values() for s in run)
    t_max = math.ceil(t_max / 60) * 60
    y_top = max(max(s["written"], s["pg_wal"]) for run in samples.values() for s in run)
    y_step = nice_step(y_top)
    y_max = math.ceil(y_top / y_step) * y_step

    def x(t):
        return left + plot_w * t / t_max

    out = []
    a = out.append
    a(f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" width="{W}" height="{H}" '
      f'role="img" aria-labelledby="chart-title">')
    a('<title id="chart-title">pg_wal size over time for each connector setup (same load in every run)</title>')
    a("<style>")
    a(":root{--surface:#fcfcfb;--ink:#0b0b0b;--ink2:#52514e;--muted:#898781;--grid:#e1e0d9;--axis:#c3c2b7;"
      "--band:rgba(137,135,129,0.13);--written:#b5b3ab;" +
      "".join(f"--s{i}:{c};" for i, (_, _, _, c, _) in enumerate(runs)) + "}")
    a("@media (prefers-color-scheme: dark){:root{--surface:#1a1a19;--ink:#ffffff;--ink2:#c3c2b7;--grid:#2c2c2a;"
      "--axis:#383835;--band:rgba(195,194,183,0.10);--written:#6b6a64;" +
      "".join(f"--s{i}:{c};" for i, (_, _, _, _, c) in enumerate(runs)) + "}}")
    a("text{font-family:system-ui,-apple-system,'Segoe UI',sans-serif;fill:var(--ink2);font-size:12px}"
      ".title{fill:var(--ink);font-size:14px;font-weight:600}.sub{font-size:12px}"
      ".tick{fill:var(--muted);font-size:11px;font-variant-numeric:tabular-nums}"
      ".lbl{fill:var(--ink);font-size:12px;font-weight:600}.note{fill:var(--muted);font-size:11px}"
      ".axis{fill:var(--ink2);font-size:12px}"
      ".hit{fill:transparent}.hit:hover{fill:var(--ink);fill-opacity:.25}")
    a("</style>")
    a(f'<rect width="{W}" height="{H}" fill="var(--surface)"/>')

    # Legend. The panel title names the setup, so each colored line means "pg_wal on disk".
    ly, lx = 22, left
    for i in range(len(runs)):
        a(f'<line x1="{lx}" y1="{ly}" x2="{lx + 18}" y2="{ly}" stroke="var(--s{i})" stroke-width="2.5" stroke-linecap="round"/>')
        lx += 22
    a(f'<text x="{lx + 4}" y="{ly + 4}">pg_wal size on disk</text>')
    lx += 150
    a(f'<line x1="{lx}" y1="{ly}" x2="{lx + 26}" y2="{ly}" stroke="var(--written)" stroke-width="2" stroke-linecap="round"/>')
    a(f'<text x="{lx + 34}" y="{ly + 4}">total WAL written since run start</text>')
    lx += 250
    a(f'<rect x="{lx}" y="{ly - 7}" width="22" height="14" fill="var(--band)"/>')
    a(f'<text x="{lx + 30}" y="{ly + 4}">heavy load</text>')
    lx += 124
    a(f'<line x1="{lx}" y1="{ly - 8}" x2="{lx}" y2="{ly + 8}" stroke="var(--ink2)" stroke-width="1.5"/>')
    a(f'<text x="{lx + 8}" y="{ly + 4}">heartbeat table added</text>')

    y0 = legend_h
    for i, (run, title, sub, _, _) in enumerate(runs):
        pts = samples[run]
        ev = events[run]
        top = y0 + title_h
        bottom = top + panel_h

        def y(v):
            return bottom - panel_h * v / y_max

        a(f'<text class="title" x="{left}" y="{y0 + 16}">{title}</text>')
        a(f'<text class="sub" x="{left}" y="{y0 + 32}">{sub}</text>')

        # Load band + label.
        if "load_start" in ev and "load_end" in ev:
            bx0, bx1 = x(ev["load_start"]), x(ev["load_end"])
            a(f'<rect x="{bx0:.1f}" y="{top}" width="{bx1 - bx0:.1f}" height="{panel_h}" fill="var(--band)"/>')
            a(f'<text class="note" x="{bx0 + 6:.1f}" y="{top + 14}">heavy load</text>')

        # Grid + y ticks.
        v = 0.0
        while v <= y_max + 1e-9:
            gy = y(v)
            stroke = "var(--axis)" if v == 0 else "var(--grid)"
            a(f'<line x1="{left}" y1="{gy:.1f}" x2="{W - right}" y2="{gy:.1f}" stroke="{stroke}" stroke-width="1"/>')
            a(f'<text class="tick" x="{left - 8}" y="{gy + 4:.1f}" text-anchor="end">{v:g}</text>')
            v += y_step

        # Moment we added the heartbeat table (`sim.sh fix`).
        if "fix" in ev:
            fx = x(ev["fix"])
            a(f'<line x1="{fx:.1f}" y1="{top}" x2="{fx:.1f}" y2="{bottom}" stroke="var(--ink2)" stroke-width="1.5"/>')
            a(f'<text class="note" x="{fx + 6:.1f}" y="{top + 14}">heartbeat table added</text>')

        # WAL written (reference line) and pg_wal (area wash + line).
        written = " ".join(f"{x(p['t']):.1f},{y(p['written']):.1f}" for p in pts)
        a(f'<polyline points="{written}" fill="none" stroke="var(--written)" stroke-width="2" stroke-linejoin="round" stroke-linecap="round"/>')
        line = " ".join(f"{x(p['t']):.1f},{y(p['pg_wal']):.1f}" for p in pts)
        area = f"{x(pts[0]['t']):.1f},{bottom} {line} {x(pts[-1]['t']):.1f},{bottom}"
        a(f'<polygon points="{area}" fill="var(--s{i})" fill-opacity="0.10"/>')
        a(f'<polyline points="{line}" fill="none" stroke="var(--s{i})" stroke-width="2" stroke-linejoin="round" stroke-linecap="round"/>')

        # Direct labels: the peak and the last value only.
        peak = max(pts, key=lambda p: p["pg_wal"])
        last = pts[-1]
        labels = [(last, fmt_gb(last["pg_wal"]), "end", -12)]
        if x(last["t"]) - x(peak["t"]) < 110:  # too close: one label says both
            labels = [(last, f"peak {fmt_gb(peak['pg_wal'])}", "end", -12)]
        else:
            labels.append((peak, f"peak {fmt_gb(peak['pg_wal'])}", "middle", -12))
        for p, text, anchor, dy in labels:
            px, py = x(p["t"]), y(p["pg_wal"])
            a(f'<circle cx="{px:.1f}" cy="{py:.1f}" r="4" fill="var(--s{i})" stroke="var(--surface)" stroke-width="2"/>')
            tx = min(max(px, left + 40), W - right) if anchor == "middle" else px
            a(f'<text class="lbl" x="{tx:.1f}" y="{max(py + dy, top + 28):.1f}" text-anchor="{anchor}">{text}</text>')

        # Hover targets (native tooltips when the SVG is opened in a browser).
        for p in pts:
            tip = (f"{title} · t={p['t'] // 60}:{p['t'] % 60:02d} ({p['phase']}) · "
                   f"pg_wal {fmt_gb(p['pg_wal'])} · written {fmt_gb(p['written'])}")
            a(f'<circle class="hit" cx="{x(p["t"]):.1f}" cy="{y(p["pg_wal"]):.1f}" r="7"><title>{tip}</title></circle>')

        # Axis titles on every panel: y on the left (rotated), x under the minute ticks.
        a(f'<text class="axis" transform="translate(18,{top + panel_h / 2:.1f}) rotate(-90)" '
          f'text-anchor="middle">Size (GB)</text>')
        for m in range(0, t_max // 60 + 1):
            a(f'<text class="tick" x="{x(m * 60):.1f}" y="{bottom + 16}" text-anchor="middle">{m}</text>')
        a(f'<text class="axis" x="{left + plot_w / 2:.1f}" y="{bottom + 34}" text-anchor="middle">'
          f'Time since run start (minutes)</text>')
        y0 = bottom + panel_gap

    a("</svg>")
    return "\n".join(out)


def at(pts, t):
    return min(pts, key=lambda p: abs(p["t"] - t))


def tables(samples, events):
    runs = [r for r in RUNS if "end" in events.get(r[0], {})]  # finished runs only
    lines = ["| Setup | WAL written | pg_wal peak | pg_wal at load end | pg_wal before adding heartbeat table | pg_wal at end | Back under 0.5 GB |",
             "|---|---|---|---|---|---|---|"]
    for run, title, *_ in runs:
        pts, ev = samples[run], events[run]
        before = at(pts, ev.get("fix", ev["load_end"]) - 1)
        ref = ev.get("fix", ev["load_end"])
        low = next((p for p in pts if p["t"] >= ref and p["pg_wal"] < 0.5), None)
        back = f"{low['t'] - ref} s after {'heartbeat table added' if 'fix' in ev else 'load end'}" if low else "no"
        lines.append(f"| {title} | {fmt_gb(pts[-1]['written'])} | {fmt_gb(max(p['pg_wal'] for p in pts))} | "
                     f"{fmt_gb(at(pts, ev['load_end'])['pg_wal'])} | "
                     f"{fmt_gb(before['pg_wal']) if 'fix' in ev else '- (has it from the start)'} | "
                     f"{fmt_gb(pts[-1]['pg_wal'])} | {back} |")

    if not runs:
        return "\n".join(lines) + "\n"
    first = samples[runs[0][0]]
    t_end = min(samples[run][-1]["t"] for run, *_ in runs)
    ts = ["| Time | Phase | " + " | ".join(t for _, t, *_ in runs) + " |",
          "|---|---|" + "---|" * len(runs)]
    for t in range(0, t_end + 1, 30):
        phase = at(first, t)["phase"]
        for run, *_ in runs:
            ev = events[run]
            if "fix" in ev and abs(ev["fix"] - t) <= 15:
                phase = "after (heartbeat table added)"
        cells = [fmt_gb(at(samples[run], t)["pg_wal"]) for run, *_ in runs]
        ts.append(f"| {t // 60}:{t % 60:02d} | {phase} | " + " | ".join(cells) + " |")
    return "\n".join(lines) + "\n\n" + "\n".join(ts) + "\n"


def main():
    samples, events = load()
    (RESULTS / "pg_wal.svg").write_text(svg_chart(samples, events))
    (RESULTS / "tables.md").write_text(tables(samples, events))
    print(f"wrote {RESULTS / 'pg_wal.svg'} and {RESULTS / 'tables.md'}")


if __name__ == "__main__":
    main()
