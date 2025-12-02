"""Plot median latency vs node count for Pompe-HS experiments."""

from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Dict, Iterable, List, Sequence

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

BASE_DIR = Path(__file__).resolve().parent

# Relative log file locations for each protocol keyed by total node count.
LOG_SOURCES: Dict[str, Dict[int, str]] = {
    "pompe": {
        4: "ec2/axiv/4nodes/ec2-logs-8vcpu-pompe-4000tps/logs-client/load_test-4000tps.log",
        10: "ec2/axiv/10nodes/ec2-logs-pompe/logs-client/load_test.log",
        34: "ec2/axiv/34nodes/ec2-logs-pompe/logs-client/load_test.log",
        100: "ec2/axiv/100nodes/ec2-logs-pompe/logs-client/load_test.log",
    },
    "hotstuff": {
        4: "ec2/axiv/4nodes/ec2-logs-8vcpu-4000tps/logs-client/load_test-4000tps.log",
        10: "ec2/axiv/10nodes/ec2-logs-smrol/logs-client/load_test.log",
        34: "ec2/axiv/34nodes/ec2-logs-smrol/logs-client/load_test.log",
        100: "ec2/axiv/100nodes/ec2-logs-smrol/logs-client/load_test.log",
    },
}

SERIES_STYLE = [
    {
        "source": "pompe",
        "stage": "ordering",
        "label": "Pompe-HS (light, ordering)",
        "color": "#7a7f00",
        "marker": "o",
    },
    {
        "source": "pompe",
        "stage": "consensus",
        "label": "Pompe-HS (light, consensus)",
        "color": "#123d8d",
        "marker": "s",
    },
    {
        "source": "hotstuff",
        "stage": "consensus",
        "label": "HotStuff",
        "color": "#b40020",
        "marker": "v",
    },
]

STAGE_HEADER_RE = re.compile(r"\\[(?P<stage>[^\\]]+)\\]\\s+latency statistics", re.IGNORECASE)
P50_LINE_RE = re.compile(r"P50:\s+(?P<value>[0-9]+(?:\\.[0-9]+)?)\\s*ms", re.IGNORECASE)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Plot median latency vs total nodes.")
    parser.add_argument(
        "--output",
        type=Path,
        default=BASE_DIR / "latency_vs_nodes.png",
        help="Path for the generated plot (default: %(default)s)",
    )
    parser.add_argument(
        "--show",
        action="store_true",
        help="Display the matplotlib window in addition to saving the image.",
    )
    return parser.parse_args()


def extract_p50_values(log_path: Path) -> Dict[str, float]:
    """Return the last seen P50 values for each latency stage in the log."""

    if not log_path.exists():
        raise FileNotFoundError(f"Log file not found: {log_path}")

    stage: str | None = None
    values: Dict[str, float] = {}

    with log_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            header_match = STAGE_HEADER_RE.search(line)
            if header_match:
                stage = header_match.group("stage").strip().lower()
                continue
            if stage:
                p50_match = P50_LINE_RE.search(line)
                if p50_match:
                    values[stage] = float(p50_match.group("value"))
                    stage = None
    if not values:
        raise ValueError(f"No P50 values parsed from {log_path}")
    return values


def collect_latency_data(log_map: Dict[int, str]) -> Dict[int, Dict[str, float]]:
    data: Dict[int, Dict[str, float]] = {}
    for nodes, relative_path in sorted(log_map.items()):
        log_path = (BASE_DIR / relative_path).resolve()
        data[nodes] = extract_p50_values(log_path)
    return data


def prepare_series(parsed: Dict[str, Dict[int, Dict[str, float]]]) -> List[dict]:
    series_data: List[dict] = []
    for config in SERIES_STYLE:
        dataset = parsed.get(config["source"], {})
        xs: List[int] = []
        ys: List[float] = []
        for nodes in sorted(dataset):
            value = dataset[nodes].get(config["stage"])
            if value is None:
                continue
            xs.append(nodes)
            ys.append(value)
        if not xs:
            raise ValueError(
                f"Missing data for stage '{config['stage']}' in source '{config['source']}'"
            )
        series_data.append({**config, "x": xs, "y": ys})
    return series_data


def apply_matplotlib_style() -> None:
    plt.rcParams.update(
        {
            "font.family": "DejaVu Serif",
            "font.size": 14,
            "axes.labelsize": 15,
            "axes.titlesize": 15,
            "figure.dpi": 150,
        }
    )


def plot_series(series: Sequence[dict], output_path: Path) -> plt.Figure:
    apply_matplotlib_style()

    fig, ax = plt.subplots(figsize=(8.6, 3.2))

    for item in series:
        ax.plot(
            item["x"],
            item["y"],
            label=item["label"],
            color=item["color"],
            marker=item["marker"],
            markersize=8,
            linewidth=2.2,
        )

    max_nodes = max(max(item["x"]) for item in series)
    max_latency = max(max(item["y"]) for item in series)

    tick_step = 20 if max_nodes >= 40 else 10
    last_tick = ((max_nodes + tick_step - 1) // tick_step) * tick_step
    xticks = list(range(0, last_tick + tick_step, tick_step))
    if max_nodes not in xticks:
        xticks.append(max_nodes)
        xticks = sorted(set(xticks))

    ax.set_xlim(0, max_nodes + tick_step * 0.25)
    ax.set_ylim(0, max_latency * 1.1)
    ax.set_xticks(xticks)
    ax.set_xlabel("Total # of nodes")
    ax.set_ylabel("Median latency (ms)")
    ax.grid(axis="y", linestyle="--", linewidth=0.6, alpha=0.4)
    ax.tick_params(direction="in", length=5, width=1, colors="#222222")

    legend = ax.legend(loc="upper left", frameon=True)
    legend.get_frame().set_facecolor("white")
    legend.get_frame().set_edgecolor("#cccccc")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, bbox_inches="tight")
    return fig


def print_latency_table(title: str, dataset: Dict[int, Dict[str, float]], stages: Iterable[str]) -> None:
    print(f"\n{title} median latencies (ms):")
    for nodes in sorted(dataset):
        stage_parts = []
        for stage in stages:
            value = dataset[nodes].get(stage)
            if value is not None:
                stage_parts.append(f"{stage}: {value:.1f}")
        summary = ", ".join(stage_parts) if stage_parts else "no data"
        print(f"  {nodes:>3} nodes -> {summary}")


def main() -> None:
    args = parse_args()

    parsed = {name: collect_latency_data(paths) for name, paths in LOG_SOURCES.items()}

    print_latency_table("Pompe-HS", parsed["pompe"], ["ordering", "consensus"])
    print_latency_table("HotStuff", parsed["hotstuff"], ["consensus"])

    series = prepare_series(parsed)
    fig = plot_series(series, args.output)

    print(f"\nSaved plot to {args.output}")
    if args.show:
        plt.show()
    plt.close(fig)


if __name__ == "__main__":
    main()
