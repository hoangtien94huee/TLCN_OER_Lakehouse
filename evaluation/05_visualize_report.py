"""
Visualise evaluation results from evaluation_report.json.

Generates a single PNG dashboard with 6 panels:
  1. Global metric gauges
  2. Hit Rate by question type
  3. Per-question hit rate strip (all 80 in-scope)
  4. Latency distribution (box + swarm)
  5. Confidence distribution
  6. OOS latency vs detected

Run:
  python3 05_visualize_report.py
"""

import json
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np

ROOT = Path(__file__).parent
REPORT_PATH = ROOT / "evaluation_report.json"
OUT_PATH    = ROOT / "evaluation_dashboard.png"

# ── colour palette ────────────────────────────────────────────────────────────
C_GREEN  = "#27AE60"
C_YELLOW = "#F39C12"
C_RED    = "#E74C3C"
C_BLUE   = "#2980B9"
C_PURPLE = "#8E44AD"
C_GREY   = "#95A5A6"
C_DARK   = "#2C3E50"
C_BG     = "#F8F9FA"

TYPE_COLORS = {
    "definition":    "#3498DB",
    "comparison":    "#9B59B6",
    "multi_step":    "#E67E22",
    "find_material": "#1ABC9C",
    "out_of_scope":  "#95A5A6",
}

def load_report():
    data = json.loads(REPORT_PATH.read_text())
    g   = data["global"]
    by_type = data["by_type"]
    per_q   = data["per_question"]
    return g, by_type, per_q


def _rating_color(value: float, metric: str) -> str:
    thresholds = {
        "hit_rate":           (0.50, 0.70, 0.85),
        "tier1_recall_at_k":  (0.60, 0.80, 0.90),
        "grounded_rate":      (0.50, 0.70, 0.85),
        "oos_detection_rate": (0.50, 0.70, 0.85),
    }
    t = thresholds.get(metric)
    if t is None:
        return C_BLUE
    fair, good, exc = t
    if value >= exc:   return C_GREEN
    if value >= good:  return C_YELLOW
    if value >= fair:  return C_GREY
    return C_RED


# ── Panel 1 — Global metric gauges ───────────────────────────────────────────
def draw_gauges(ax, g: dict):
    metrics = [
        ("Hit Rate",       "hit_rate",           g["hit_rate"]),
        ("Tier-1 Recall",  "tier1_recall_at_k",  g["tier1_recall_at_k"]),
        ("Grounded",       "grounded_rate",       g["grounded_rate"]),
        ("OOS Detection",  "oos_detection_rate",  g["oos_detection_rate"]),
    ]
    targets = {
        "hit_rate": 0.80,
        "tier1_recall_at_k": 0.80,
        "grounded_rate": 0.85,
        "oos_detection_rate": 0.80,
    }
    n = len(metrics)
    xs = np.linspace(0.12, 0.88, n)
    ax.set_xlim(0, 1)
    ax.set_ylim(0, 1)
    ax.axis("off")
    ax.set_facecolor(C_BG)
    ax.set_title("Global Metrics", fontsize=13, fontweight="bold",
                 color=C_DARK, pad=8)

    for x, (label, key, val) in zip(xs, metrics):
        color = _rating_color(val, key)
        # circle background
        circle = plt.Circle((x, 0.55), 0.10, color=color, alpha=0.15,
                             transform=ax.transAxes, zorder=2)
        ax.add_patch(circle)
        # score text
        ax.text(x, 0.55, f"{val:.2%}", ha="center", va="center",
                fontsize=14, fontweight="bold", color=color,
                transform=ax.transAxes)
        # label
        ax.text(x, 0.28, label, ha="center", va="center",
                fontsize=9, color=C_DARK, transform=ax.transAxes)
        # target line indicator
        tgt = targets.get(key, 0.80)
        status = "✓" if val >= tgt else "✗"
        status_c = C_GREEN if val >= tgt else C_RED
        ax.text(x, 0.12, f"target ≥{tgt:.0%} {status}", ha="center",
                va="center", fontsize=8, color=status_c,
                transform=ax.transAxes)

    # latency note
    lat_avg = g.get("latency_avg_ms", 0)
    lat_p50 = g.get("latency_p50_ms", 0)
    lat_p95 = g.get("latency_p95_ms", 0)
    ax.text(0.5, 0.90,
            f"Latency  avg={lat_avg/1000:.1f}s  P50={lat_p50/1000:.1f}s  P95={lat_p95/1000:.1f}s",
            ha="center", va="center", fontsize=9, color=C_GREY,
            transform=ax.transAxes)


# ── Panel 2 — Hit Rate by question type ──────────────────────────────────────
def draw_by_type(ax, by_type: dict):
    order = ["definition", "comparison", "multi_step", "find_material"]
    labels, values, colors = [], [], []
    for t in order:
        info = by_type.get(t, {})
        hr = info.get("hit_rate", 0) or 0
        labels.append(t.replace("_", "\n"))
        values.append(hr)
        colors.append(TYPE_COLORS.get(t, C_BLUE))

    bars = ax.bar(labels, values, color=colors, edgecolor="white",
                  linewidth=1.2, zorder=3)
    ax.axhline(0.80, color=C_RED, linewidth=1.5, linestyle="--",
               label="Target 0.80", zorder=4)
    ax.set_ylim(0, 1.05)
    ax.set_ylabel("Hit Rate (evidence)", fontsize=9, color=C_DARK)
    ax.set_title("Hit Rate by Question Type", fontsize=11,
                 fontweight="bold", color=C_DARK)
    ax.set_facecolor(C_BG)
    ax.tick_params(colors=C_DARK, labelsize=8)
    ax.spines[["top", "right"]].set_visible(False)
    ax.yaxis.grid(True, linestyle=":", alpha=0.5)
    ax.set_axisbelow(True)
    ax.legend(fontsize=8)

    for bar, val in zip(bars, values):
        ax.text(bar.get_x() + bar.get_width() / 2,
                val + 0.02, f"{val:.2f}",
                ha="center", va="bottom", fontsize=9,
                fontweight="bold", color=C_DARK)


# ── Panel 3 — Per-question hit rate strip ────────────────────────────────────
def draw_per_question(ax, per_q: list):
    in_scope = [q for q in per_q if q["question_type"] != "out_of_scope"
                and not q.get("has_error")]
    # sort by question_type then hit desc
    order_map = {"definition": 0, "comparison": 1,
                 "multi_step": 2, "find_material": 3}
    in_scope.sort(key=lambda q: (order_map.get(q["question_type"], 9),
                                 -q.get("hit", 0)))

    xs = list(range(len(in_scope)))
    ys = [q.get("hit", 0) for q in in_scope]
    cs = [TYPE_COLORS.get(q["question_type"], C_BLUE) for q in in_scope]

    ax.bar(xs, ys, color=cs, edgecolor="none", width=0.85, zorder=3)
    ax.axhline(0.80, color=C_RED, linewidth=1.3, linestyle="--",
               label="Target 0.80", zorder=4)
    ax.set_ylim(0, 1.05)
    ax.set_xlim(-0.5, len(in_scope) - 0.5)
    ax.set_xlabel("Questions (sorted by type)", fontsize=9, color=C_DARK)
    ax.set_ylabel("Hit Rate", fontsize=9, color=C_DARK)
    ax.set_title("Per-Question Hit Rate (80 in-scope)", fontsize=11,
                 fontweight="bold", color=C_DARK)
    ax.set_facecolor(C_BG)
    ax.tick_params(colors=C_DARK, labelsize=8)
    ax.spines[["top", "right"]].set_visible(False)
    ax.yaxis.grid(True, linestyle=":", alpha=0.5)
    ax.set_axisbelow(True)
    ax.set_xticks([])

    # type separators + labels
    type_groups: dict[str, list[int]] = {}
    for i, q in enumerate(in_scope):
        type_groups.setdefault(q["question_type"], []).append(i)
    for t, idxs in type_groups.items():
        mid = (min(idxs) + max(idxs)) / 2
        ax.text(mid, 1.02, t.replace("_", "\n"), ha="center", va="bottom",
                fontsize=7, color=TYPE_COLORS.get(t, C_DARK))
        if min(idxs) > 0:
            ax.axvline(min(idxs) - 0.5, color=C_GREY, linewidth=0.8,
                       linestyle=":", zorder=2)

    mean_hit = np.mean(ys)
    ax.text(len(in_scope) - 0.5, mean_hit + 0.03,
            f"avg={mean_hit:.3f}", ha="right", va="bottom",
            fontsize=8, color=C_DARK)


# ── Panel 4 — Latency distribution by type ───────────────────────────────────
def draw_latency(ax, per_q: list):
    order = ["definition", "comparison", "multi_step", "find_material"]
    data_by_type = {t: [] for t in order}
    for q in per_q:
        t = q.get("question_type")
        lat = q.get("latency_ms", 0)
        if t in data_by_type and lat > 0 and not q.get("has_error"):
            data_by_type[t].append(lat / 1000)

    positions = list(range(1, len(order) + 1))
    bp = ax.boxplot(
        [data_by_type[t] for t in order],
        positions=positions,
        patch_artist=True,
        widths=0.5,
        medianprops=dict(color="white", linewidth=2),
        whiskerprops=dict(linewidth=1),
        capprops=dict(linewidth=1.5),
        flierprops=dict(marker=".", markersize=4, alpha=0.5),
    )
    for patch, t in zip(bp["boxes"], order):
        patch.set_facecolor(TYPE_COLORS.get(t, C_BLUE))
        patch.set_alpha(0.8)

    ax.axhline(4, color=C_RED, linewidth=1.3, linestyle="--",
               label="4 s target", zorder=4)
    ax.set_xticks(positions)
    ax.set_xticklabels([t.replace("_", "\n") for t in order], fontsize=8)
    ax.set_ylabel("Latency (s)", fontsize=9, color=C_DARK)
    ax.set_title("Response Latency by Type", fontsize=11,
                 fontweight="bold", color=C_DARK)
    ax.set_facecolor(C_BG)
    ax.tick_params(colors=C_DARK, labelsize=8)
    ax.spines[["top", "right"]].set_visible(False)
    ax.yaxis.grid(True, linestyle=":", alpha=0.5)
    ax.set_axisbelow(True)
    ax.legend(fontsize=8)


# ── Panel 5 — Confidence distribution ────────────────────────────────────────
def draw_confidence(ax, per_q: list):
    conf_counts: dict[str, int] = {}
    for q in per_q:
        c = q.get("confidence", "unknown") or "unknown"
        conf_counts[c] = conf_counts.get(c, 0) + 1

    conf_colors = {
        "high":    C_GREEN,
        "medium":  C_BLUE,
        "low":     C_YELLOW,
        "error":   C_RED,
        "unknown": C_GREY,
    }
    order = [k for k in ["high", "medium", "low", "error", "unknown"]
             if k in conf_counts]
    sizes  = [conf_counts[k] for k in order]
    colors = [conf_colors.get(k, C_GREY) for k in order]

    wedges, texts, autotexts = ax.pie(
        sizes, labels=None, colors=colors,
        autopct="%1.0f%%", startangle=90,
        wedgeprops=dict(edgecolor="white", linewidth=1.5),
        pctdistance=0.75,
    )
    for at in autotexts:
        at.set_fontsize(9)
        at.set_color("white")
        at.set_fontweight("bold")

    ax.legend(wedges, [f"{k} ({conf_counts[k]})" for k in order],
              loc="lower center", fontsize=8, ncol=2,
              bbox_to_anchor=(0.5, -0.18))
    ax.set_title("Confidence Distribution\n(100 questions)", fontsize=11,
                 fontweight="bold", color=C_DARK)


# ── Panel 6 — Recall & Grounded by type ──────────────────────────────────────
def draw_recall_grounded(ax, by_type: dict):
    order = ["definition", "comparison", "multi_step", "find_material"]
    x = np.arange(len(order))
    w = 0.35

    recall   = [by_type.get(t, {}).get("tier1_recall_at_k", 0) or 0 for t in order]
    grounded = [by_type.get(t, {}).get("grounded_rate", 0) or 0 for t in order]

    ax.bar(x - w/2, recall,   width=w, label="Tier-1 Recall@K",
           color=C_BLUE,   alpha=0.85, edgecolor="white")
    ax.bar(x + w/2, grounded, width=w, label="Grounded Answer Rate",
           color=C_PURPLE, alpha=0.85, edgecolor="white")

    ax.set_ylim(0, 1.12)
    ax.set_xticks(x)
    ax.set_xticklabels([t.replace("_", "\n") for t in order], fontsize=8)
    ax.set_ylabel("Score", fontsize=9, color=C_DARK)
    ax.set_title("Retrieval Quality by Type", fontsize=11,
                 fontweight="bold", color=C_DARK)
    ax.set_facecolor(C_BG)
    ax.tick_params(colors=C_DARK, labelsize=8)
    ax.spines[["top", "right"]].set_visible(False)
    ax.yaxis.grid(True, linestyle=":", alpha=0.5)
    ax.set_axisbelow(True)
    ax.legend(fontsize=8)

    for xi, (r, g_) in enumerate(zip(recall, grounded)):
        ax.text(xi - w/2, r + 0.02, f"{r:.2f}", ha="center", va="bottom",
                fontsize=8, color=C_BLUE, fontweight="bold")
        ax.text(xi + w/2, g_ + 0.02, f"{g_:.2f}", ha="center", va="bottom",
                fontsize=8, color=C_PURPLE, fontweight="bold")


# ── Compose dashboard ─────────────────────────────────────────────────────────
def main():
    if not REPORT_PATH.exists():
        print(f"ERROR: {REPORT_PATH} not found. Run 03_compute_metrics.py first.")
        sys.exit(1)

    g, by_type, per_q = load_report()

    fig = plt.figure(figsize=(18, 14), facecolor="white")
    fig.suptitle(
        "OER Chatbot — PageIndex RAG Evaluation Dashboard",
        fontsize=16, fontweight="bold", color=C_DARK, y=0.98,
    )

    # grid: 3 rows × 3 cols, merge top row across all 3 cols
    gs = fig.add_gridspec(3, 3, hspace=0.45, wspace=0.35,
                          left=0.06, right=0.97, top=0.93, bottom=0.06)

    ax_gauges = fig.add_subplot(gs[0, :])        # row 0 — full width
    ax_type   = fig.add_subplot(gs[1, 0])        # row 1 col 0
    ax_strip  = fig.add_subplot(gs[1, 1:])       # row 1 col 1-2
    ax_lat    = fig.add_subplot(gs[2, 0])        # row 2 col 0
    ax_conf   = fig.add_subplot(gs[2, 1])        # row 2 col 1
    ax_recall = fig.add_subplot(gs[2, 2])        # row 2 col 2

    for ax in [ax_type, ax_strip, ax_lat, ax_recall]:
        ax.set_facecolor(C_BG)

    draw_gauges(ax_gauges, g)
    draw_by_type(ax_type, by_type)
    draw_per_question(ax_strip, per_q)
    draw_latency(ax_lat, per_q)
    draw_confidence(ax_conf, per_q)
    draw_recall_grounded(ax_recall, by_type)

    fig.savefig(OUT_PATH, dpi=150, bbox_inches="tight", facecolor="white")
    print(f"Dashboard saved → {OUT_PATH}")


if __name__ == "__main__":
    main()
