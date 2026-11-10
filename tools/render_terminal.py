"""
Renders console text as a terminal-style PNG ("screenshot") for the report.
Can be used as a module (render(...)) or CLI:
  python tools/render_terminal.py LOGFILE OUT.png --cmd "python x.py" --lines 0:30 --title "My step"

These images are rendered from REAL console output of this project's scripts. They do NOT replace real
screenshots of MongoDB Compass / the HBase shell / HBase web UI - add those yourself (see STEP_BY_STEP_GUIDE.md §9).
"""
import argparse
import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt


def render(lines, out, cmd="", title="Terminal", note="", width=118):
    body = [l.rstrip("\n").replace("\t", "    ") for l in lines]
    body = [l[:width] + ("…" if len(l) > width else "") for l in body]
    lines = ([f"$ {cmd}"] if cmd else []) + body
    fs, lh = 9.2, 0.205
    h = 0.6 + lh * len(lines) + 0.2
    w = max(7.5, 0.5 + 0.0775 * max([len(l) for l in lines] + [20]))
    fig = plt.figure(figsize=(w, h), dpi=150, facecolor="#1e1f26")
    ax = fig.add_axes([0, 0, 1, 1]); ax.set_xlim(0, w); ax.set_ylim(0, h); ax.axis("off")
    ax.add_patch(plt.Rectangle((0, h - 0.4), w, 0.4, color="#33343d"))
    for i, c in enumerate(["#ff5f57", "#febc2e", "#28c840"]):
        ax.add_patch(plt.Circle((0.25 + i * 0.25, h - 0.2), 0.07, color=c))
    ax.text(w / 2 + 0.4, h - 0.2, title + (f"  |  {note}" if note else ""), color="#c9cad3", ha="center", va="center", fontsize=8.5)
    y = h - 0.62
    for i, l in enumerate(lines):
        col = "#7ee787" if (cmd and i == 0) else "#d6d7de"
        ax.text(0.25, y, l, family="DejaVu Sans Mono", fontsize=fs, color=col, va="center")
        y -= lh
    fig.savefig(out, facecolor=fig.get_facecolor()); plt.close(fig)
    return out


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("log"); ap.add_argument("out"); ap.add_argument("--cmd", default="")
    ap.add_argument("--title", default="Terminal"); ap.add_argument("--lines", default="0:30")
    ap.add_argument("--width", type=int, default=118); ap.add_argument("--note", default="")
    a = ap.parse_args()
    s, e = (int(x) for x in a.lines.split(":"))
    render(open(a.log, errors="replace").read().splitlines()[s:e], a.out, a.cmd, a.title, a.note, a.width)
