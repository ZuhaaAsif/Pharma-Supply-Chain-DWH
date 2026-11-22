"""Renders deliverables/Milestone.1.SDI.DWH.Video.mp4 (1920x1080, 30 fps): slides + burnt-in captions + narration."""
from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import textwrap
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from PIL import Image, ImageDraw, ImageFont

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts" / "video"))
from narration import SCENES  # noqa: E402

W, H = 1920, 1080
WORK = Path("/home/claude/vid")
(WORK / "slides").mkdir(parents=True, exist_ok=True)
(WORK / "parts").mkdir(parents=True, exist_ok=True)
(WORK / "aux").mkdir(parents=True, exist_ok=True)
DUR = json.loads((WORK / "durations.json").read_text())
PAD = 1.2
FONT = "/usr/share/fonts/opentype/inter/Inter-Regular.otf"
FONTB = "/usr/share/fonts/opentype/inter/Inter-Bold.otf"
SURF, INK, INK2, NAVY = (252, 252, 251), (11, 11, 11), (82, 81, 78), (31, 58, 95)
S = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#6250d6", "#e34948"]
f = lambda size, bold=False: ImageFont.truetype(FONTB if bold else FONT, size)
plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 15, "axes.spines.top": False, "axes.spines.right": False,
                     "axes.edgecolor": "#e1e0d9", "figure.facecolor": "#fcfcfb", "axes.facecolor": "#fcfcfb",
                     "axes.grid": True, "grid.color": "#e1e0d9", "xtick.color": "#52514e", "ytick.color": "#52514e"})
EV = ROOT / "docs" / "evidence"
QR = EV / "query_results"


# ---------------------------------------------------------------- auxiliary figures
def fig_airflow():
    df = pd.read_csv(EV / "airflow_task_states_final_run.csv", header=None,
                     names=["dag", "run", "rstate", "task", "map", "state", "dur", "try"])
    runs = df.drop_duplicates("run")[["dag", "run"]].reset_index(drop=True)
    labels = ["batch\n(base)", "oversample\nR1", "batch\nR1", "stream\nR1", "oversample\nR2", "batch\nR2", "stream\nR2"]
    tasks = list(dict.fromkeys(df.task))
    fig, ax = plt.subplots(figsize=(18.4, 7.4), dpi=100)
    ax.grid(False)
    for j, run in enumerate(runs.run):
        sub = df[df.run == run]
        for i, t in enumerate(tasks):
            n = (sub.task == t).sum()
            if n:
                ok = (sub[sub.task == t].state == "success").all()
                ax.add_patch(plt.Rectangle((j - 0.32, i - 0.36), 0.64, 0.72, color="#0ca30c" if ok else "#d03b3b"))
                if n > 1:
                    ax.text(j, i, f"×{n}", ha="center", va="center", color="white", fontsize=13, fontweight="bold")
    ax.set_xlim(-0.6, len(runs) - 0.4); ax.set_ylim(len(tasks) - 0.4, -0.6)
    ax.set_xticks(range(len(runs)), labels[:len(runs)], fontsize=13)
    ax.set_yticks(range(len(tasks)), tasks, fontsize=12.5)
    ax.xaxis.tick_top()
    for sp in ax.spines.values():
        sp.set_visible(False)
    ax.text(len(runs) - 0.4, len(tasks) - 0.2, "green = success · ×N = dynamically mapped task instances", ha="right", fontsize=12, color="#52514e")
    fig.tight_layout(); fig.savefig(WORK / "aux" / "airflow.png"); plt.close(fig)


def fig_validation():
    rej = pd.read_csv(EV / "staging_rejects.csv")
    rej["rule"] = rej.reason.str.split().str[0] + " " + rej.reason.map(lambda r: {"R2": "null SKU", "R4": "non-positive price/qty",
                                                                                  "R5": "invalid date", "R7": "duplicate key"}.get(r.split()[0], r))
    ct = pd.crosstab(rej.rule, rej.batch_label)
    fig, axes = plt.subplots(1, 2, figsize=(18.4, 7.2), dpi=100, gridspec_kw={"width_ratios": [1.5, 1]})
    ax = axes[0]
    y = np.arange(len(ct)); h = 0.38
    for k, (col, c) in enumerate(zip(ct.columns, S)):
        ax.barh(y + (k - 0.5) * h, ct[col], h, color=c, label=col.replace("refresh_0", "refresh ").replace("_2026", " 2026"))
        for yi, v in zip(y, ct[col]):
            ax.text(v, yi + (k - 0.5) * h, f" {v}", va="center", fontsize=14)
    ax.set_yticks(y, ct.index, fontsize=15); ax.invert_yaxis(); ax.grid(axis="y", visible=False)
    ax.legend(loc="lower right", frameon=False); ax.set_title("Rows rejected by rule", loc="left", fontsize=18, fontweight="bold")
    ax = axes[1]; ax.axis("off")
    tb = ax.table([["Refresh 1", "119", "119", "100%"], ["Refresh 2", "90", "90", "100%"]],
                  colLabels=["", "Injected", "Rejected", "Caught"], loc="center", cellLoc="center", bbox=[0, 0.35, 1, 0.4])
    tb.auto_set_font_size(False); tb.set_fontsize(20)
    for (r, c), cell in tb.get_celld().items():
        cell.set_edgecolor("#d9d7cf"); cell.set_facecolor("#f3f2ee" if r == 0 else "white")
    ax.set_title("Fault-injection test", loc="left", fontsize=18, fontweight="bold")
    fig.tight_layout(); fig.savefig(WORK / "aux" / "validation.png"); plt.close(fig)


def fig_oversampling():
    st = {r: json.loads((ROOT / "data/oversampled" / r / "generation_stats.json").read_text()) for r in ("refresh_01_2026Q1", "refresh_02_2026Q2")}
    keys = ["sales", "iot", "warehouse", "hospital_demand", "delivery", "manufacturing", "master"]
    v1 = [next(x for x in st["refresh_01_2026Q1"]["files"] if x["source_key"] == k)["rows_generated"] for k in keys]
    v2 = [next(x for x in st["refresh_02_2026Q2"]["files"] if x["source_key"] == k)["rows_generated"] for k in keys]
    fig, ax = plt.subplots(figsize=(10.5, 7.2), dpi=100)
    y = np.arange(len(keys)); h = 0.38
    ax.barh(y - h / 2, v1, h, color=S[0], label="refresh 1 · 2026 Q1"); ax.barh(y + h / 2, v2, h, color=S[1], label="refresh 2 · 2026 Q2")
    for yi, a, b in zip(y, v1, v2):
        ax.text(a, yi - h / 2, f" {a:,}", va="center", fontsize=13); ax.text(b, yi + h / 2, f" {b:,}", va="center", fontsize=13)
    ax.set_yticks(y, keys, fontsize=15); ax.invert_yaxis(); ax.set_xscale("log"); ax.grid(axis="y", visible=False)
    ax.set_xlim(right=max(v1) * 5); ax.legend(loc="lower right", frameon=False)
    ax.set_title("Rows generated per file (log scale)", loc="left", fontsize=18, fontweight="bold")
    fig.tight_layout(); fig.savefig(WORK / "aux" / "oversampling.png"); plt.close(fig)


def fig_results():
    fig, axes = plt.subplots(1, 2, figsize=(18.4, 7.4), dpi=100, gridspec_kw={"width_ratios": [1, 1.25]})
    ax = axes[0]
    loads = ["base", "refresh_01_2026Q1", "refresh_02_2026Q2"]
    k = {b: pd.read_csv(QR / b / "B1_channel_kpis.csv").set_index("channel_group").net_sales for b in loads}
    x = np.arange(3); w = 0.26
    for i, ch in enumerate(["Retail", "Hospital", "E-commerce"]):
        vals = [k[b][ch] / 1e6 for b in loads]
        ax.bar(x + (i - 1) * w, vals, w, color=S[i], label=ch)
        for xi, v in zip(x, vals):
            ax.text(xi + (i - 1) * w, v, f"{v:.2f}", ha="center", va="bottom", fontsize=12)
    ax.set_xticks(x, ["after base", "after refresh 1", "after refresh 2"]); ax.grid(axis="x", visible=False)
    ax.set_ylabel("cumulative net sales ($M)"); ax.legend(frameon=False, loc="upper left", ncols=3)
    ax.set_ylim(0, 4.6); ax.set_title("Channel revenue keeps growing", loc="left", fontsize=18, fontweight="bold")
    ax = axes[1]; ax.axis("off")
    cols = [pd.read_csv(QR / b / "C1_top10_fast_movers.csv").head(5).generic_name.tolist() for b in loads]
    rows = [[str(i + 1)] + [c[i] for c in cols] for i in range(5)]
    tb = ax.table(rows, colLabels=["#", "2025 Q4 (base)", "2026 Q1", "2026 Q2"], loc="center", cellLoc="left", bbox=[0, 0.08, 1, 0.8],
                  colWidths=[0.07, 0.31, 0.31, 0.31])
    tb.auto_set_font_size(False); tb.set_fontsize(16)
    for (r, c), cell in tb.get_celld().items():
        cell.set_edgecolor("#d9d7cf"); cell.set_facecolor("#f3f2ee" if r == 0 else "white")
    ax.set_title("Top-5 fast movers (last 90 days) change each quarter", loc="left", fontsize=18, fontweight="bold")
    fig.tight_layout(); fig.savefig(WORK / "aux" / "results.png"); plt.close(fig)


# ---------------------------------------------------------------- slide composition
def wrap_caption(text: str, n_words=17) -> list[str]:
    parts = re.split(r"(?<=[.:,])\s+", text)
    chunks, cur = [], ""
    for p in parts:
        if cur and len((cur + " " + p).split()) > n_words:
            chunks.append(cur); cur = p
        else:
            cur = (cur + " " + p).strip()
    if cur:
        chunks.append(cur)
    return chunks


def base_slide(title: str, idx: int) -> Image.Image:
    im = Image.new("RGB", (W, H), SURF)
    d = ImageDraw.Draw(im)
    d.rectangle([0, 0, W, 8], fill=NAVY)
    d.text((60, 42), title, font=f(46, True), fill=NAVY)
    d.text((W - 60, 56), f"SDI · Milestone 1   {idx}/{len(SCENES)}", font=f(22), fill=INK2, anchor="ra")
    d.line([60, 122, W - 60, 122], fill=(225, 224, 217), width=2)
    return im


def paste_fit(im: Image.Image, path: Path, box=(60, 140, W - 60, 900)):
    pic = Image.open(path).convert("RGB")
    bw, bh = box[2] - box[0], box[3] - box[1]
    s = min(bw / pic.width, bh / pic.height)
    pic = pic.resize((int(pic.width * s), int(pic.height * s)), Image.LANCZOS)
    im.paste(pic, (box[0] + (bw - pic.width) // 2, box[1] + (bh - pic.height) // 2))


def text_block(im, lines, x=90, y=170, size=36, gap=22, bullet=True):
    d = ImageDraw.Draw(im)
    for ln in lines:
        wrapped = textwrap.wrap(ln, 52)
        for k, w_ in enumerate(wrapped):
            if bullet and k == 0:
                d.ellipse([x, y + size * 0.42, x + 12, y + size * 0.42 + 12], fill=S[0])
            d.text((x + 32, y), w_, font=f(size), fill=INK)
            y += int(size * 1.3)
        y += gap


def caption(im, text):
    d = ImageDraw.Draw(im)
    d.rectangle([0, 918, W, H], fill=(24, 34, 48))
    lines = textwrap.wrap(text, 92)[:2]
    y = 948 if len(lines) == 2 else 975
    for ln in lines:
        d.text((W // 2, y), ln, font=f(34), fill=(245, 245, 240), anchor="ma")
        y += 48


def content(key: str, title: str, idx: int) -> Image.Image:
    im = base_slide(title, idx)
    d = ImageDraw.Draw(im)
    dg = ROOT / "docs"
    if key == "title":
        d.text((W // 2, 300), "Pharma Cold-Chain DWH + BI", font=f(40), fill=INK2, anchor="ma")
        d.text((W // 2, 380), "Phase 1 · Sales & Demand Intelligence Mart", font=f(74, True), fill=NAVY, anchor="ma")
        d.text((W // 2, 500), "Airflow 3 · FastAPI · PostgreSQL · DuckDB · Grafana", font=f(38), fill=INK, anchor="ma")
        d.text((W // 2, 620), "Zuhaa Asif (29169) · Kashaf Saleem (29175)", font=f(34), fill=INK2, anchor="ma")
        d.text((W // 2, 680), "github.com/ZuhaaAsif/Pharma-Supply-Chain-DWH", font=f(30), fill=INK2, anchor="ma")
    elif key == "arch":
        paste_fit(im, dg / "diagrams/airflow_pipeline_compact.png")
    elif key == "oversampling":
        text_block(im, ["Template = same quarter last year (seasonality kept)", "Whole rows / shipments / lots resampled together",
                        "Volume = template × 12-month growth (0.90–1.25)", "Keys continue · lineage re-pointed · prices re-derived",
                        "6 SKUs re-priced per refresh → SCD Type 2", "0.3% bad rows injected to test validation"],
                   x=70, y=175, size=31, gap=20)
        paste_fit(im, WORK / "aux/oversampling.png", box=(1000, 150, W - 50, 900))
    elif key == "airflow":
        paste_fit(im, WORK / "aux/airflow.png")
    elif key == "validation":
        paste_fit(im, WORK / "aux/validation.png")
    elif key == "dash1":
        paste_fit(im, dg / "dashboards/1_batch_ingestion.png")
    elif key == "dash2":
        paste_fit(im, dg / "dashboards/2_streaming_ingestion.png")
    elif key == "star":
        paste_fit(im, dg / "diagrams/star_schema.png")
    elif key == "dash3":
        paste_fit(im, dg / "dashboards/3_storage.png")
    elif key == "dash4":
        paste_fit(im, dg / "dashboards/4_query_performance.png")
    elif key == "results":
        paste_fit(im, WORK / "aux/results.png")
    elif key == "close":
        text_block(im, ["Oversampling DAG: 2 refreshes, 126,371 new rows", "Ingestion API + SHA-256 file watcher (batch) and micro-batch stream",
                        "Postgres bronze → silver with rules R1–R8 (100% of faults caught)", "DuckDB star schema: SCD-2, junk, role-playing, 0 orphans",
                        "15 timed widget queries: 0% errors, max 24 ms, up to 6.9× faster", "4 Grafana data-engineering dashboards"],
                   x=120, y=190, size=38, gap=24)
    return im


def render():
    fig_airflow(); fig_validation(); fig_oversampling(); fig_results()
    parts = []
    for idx, (key, title, text) in enumerate(SCENES, start=1):
        total = DUR[key] + PAD
        chunks = wrap_caption(text)
        weights = np.array([len(c) for c in chunks], float)
        durs = total * weights / weights.sum()
        slide = content(key, title, idx)
        for k, (c, dsec) in enumerate(zip(chunks, durs)):
            im = slide.copy(); caption(im, c)
            png = WORK / "slides" / f"{idx:02d}_{k:02d}.png"; im.save(png)
            out = WORK / "parts" / f"{idx:02d}_{k:02d}.mp4"; tmp = out.with_name(out.stem + ".tmp.mp4")
            subprocess.run(["ffmpeg", "-y", "-loglevel", "error", "-framerate", "30", "-i", str(png), "-vf",
                            f"format=yuv420p,loop=-1:1,trim=duration={dsec:.3f}", "-c:v", "libx264", "-crf", "20",
                            "-preset", "veryfast", "-pix_fmt", "yuv420p", "-r", "30", str(tmp)], check=True)
            os.replace(tmp, out); parts.append(out)
        # audio: narration + pad of silence
        subprocess.run(["ffmpeg", "-y", "-loglevel", "error", "-i", str(WORK / "audio" / f"{key}.wav"), "-af",
                        f"apad=whole_dur={total:.3f},aresample=44100", "-ac", "1", str(WORK / "audio" / f"{key}_pad.wav")], check=True)
    (WORK / "parts.txt").write_text("".join(f"file '{p}'\n" for p in parts))
    (WORK / "audio.txt").write_text("".join(f"file '{WORK / 'audio' / (k + '_pad.wav')}'\n" for k, _, _ in SCENES))
    subprocess.run(["ffmpeg", "-y", "-loglevel", "error", "-f", "concat", "-safe", "0", "-i", str(WORK / "parts.txt"), "-c", "copy",
                    str(WORK / "video_only.mp4")], check=True)
    subprocess.run(["ffmpeg", "-y", "-loglevel", "error", "-f", "concat", "-safe", "0", "-i", str(WORK / "audio.txt"),
                    str(WORK / "narration.wav")], check=True)
    out = ROOT / "deliverables" / "Milestone.1.SDI.DWH.Video.mp4"
    tmp = out.with_name("video.tmp.mp4")
    subprocess.run(["ffmpeg", "-y", "-loglevel", "error", "-i", str(WORK / "video_only.mp4"), "-i", str(WORK / "narration.wav"),
                    "-map", "0:v", "-map", "1:a", "-c:v", "copy", "-c:a", "aac", "-b:a", "128k", "-shortest", str(tmp)], check=True)
    os.replace(tmp, out)
    print("video:", out)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "preview":
        fig_airflow(); fig_validation(); fig_oversampling(); fig_results()
        for idx, (key, title, text) in enumerate(SCENES, start=1):
            if key in sys.argv[2:]:
                im = content(key, title, idx); caption(im, wrap_caption(text)[0]); im.save(WORK / f"preview_{key}.png")
    else:
        render()
