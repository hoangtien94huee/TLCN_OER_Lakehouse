"""Dashboard gọn (1 panel) — chỉ số tổng quát, dễ giải thích cho khóa luận."""
import json, sys
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import Patch

REPORT = sys.argv[1] if len(sys.argv) > 1 else "evaluation_report.json"
OUT = sys.argv[2] if len(sys.argv) > 2 else "evaluation_dashboard.png"
r = json.load(open(REPORT))
g = r["global"]

plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 12})

def color(v):
    if v is None: return "#bdc3c7"
    if v >= 0.85: return "#2ecc71"
    if v >= 0.70: return "#3498db"
    if v >= 0.50: return "#f1c40f"
    return "#e74c3c"

labels = ["Hit Rate\n(bằng chứng khớp)",
          "Tier-1 Recall@K\n(chọn đúng tài liệu)",
          "Grounded Rate\n(bám nguồn)",
          "OOS Detection\n(từ chối ngoài phạm vi)"]
vals = [g.get("hit_rate"), g.get("tier1_recall_at_k"), g.get("grounded_rate"), g.get("oos_detection_rate")]

fig, ax = plt.subplots(figsize=(10, 5.6))
y = range(len(vals))
ax.barh(y, vals, color=[color(v) for v in vals], height=0.62)
ax.set_yticks(y); ax.set_yticklabels(labels)
ax.invert_yaxis(); ax.set_xlim(0, 1.08)
ax.axvline(0.85, ls="--", color="#7f8c8d", lw=1)
for i, v in enumerate(vals):
    ax.text(v + 0.015, i, f"{v:.2f}", va="center", fontweight="bold", fontsize=13)
ax.set_xlabel("Giá trị (0–1)   ·   đường nét đứt = ngưỡng Rất tốt 0.85", fontsize=10, color="#34495e")

ax.set_title("ĐÁNH GIÁ HỆ THỐNG HỎI–ĐÁP OER — CHỈ SỐ TỔNG QUÁT\n"
             f"(200 câu: 100 tiếng Anh + 100 tiếng Việt)",
             fontweight="bold", fontsize=13, pad=14)

sub = (f"Tổng: {g.get('total_questions')} câu    |    Lỗi: {g.get('error_rate',0)*100:.0f}%    |    "
       f"Độ trễ TB: {g.get('latency_avg_ms'):.0f} ms (P95 {g.get('latency_p95_ms'):.0f} ms)")
fig.text(0.5, 0.085, sub, ha="center", fontsize=10.5, color="#34495e")

leg = [Patch(facecolor="#2ecc71", label="Rất tốt ≥0.85"),
       Patch(facecolor="#3498db", label="Tốt ≥0.70"),
       Patch(facecolor="#f1c40f", label="Trung bình ≥0.50"),
       Patch(facecolor="#e74c3c", label="Kém <0.50")]
fig.legend(handles=leg, loc="lower center", bbox_to_anchor=(0.5, 0.0),
           ncol=4, fontsize=9, frameon=False)

plt.tight_layout(rect=[0, 0.14, 1, 1])
plt.savefig(OUT, dpi=150, bbox_inches="tight")
print("Saved", OUT)