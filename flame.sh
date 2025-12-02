#!/usr/bin/env bash
set -euo pipefail

FOLDED="${1:-logs/node0.folded}"
OUTDIR="flame"
WIDTH="${2:-3200}"
MINW="${3:-0.3}"

if inferno-flamegraph --help 2>&1 | grep -q -- '--min-width'; then
  MINFLAG="--min-width"
else
  MINFLAG="--minwidth"
fi

mkdir -p "$OUTDIR"

THREADS=$(grep -o 'ThreadId([0-9]\+)' "$FOLDED" | sort -u)

echo "🔥 Generating per-thread flamegraphs to: $OUTDIR/"
for t in $THREADS; do
  tid="${t#ThreadId(}"; tid="${tid%)}"
  base="$OUTDIR/t${tid}"
  tmp="$base.folded"

  # Loose match (tolerate spaces or indentation)
  awk -v tid="ThreadId(${tid})-" '
    index($0, tid)==1 && $0 ~ /;/ {
      sub(/^[^;]*;/, "", $0)  # Remove the ThreadId prefix
      gsub(/[[:space:]]+$/, "")
      print
    }' "$FOLDED" > "$tmp"

  if [[ ! -s "$tmp" ]]; then
    echo "  ⚠️  Thread ${tid}: no stacks found"
    rm -f "$tmp"
    continue
  fi

  COUNT=$(wc -l < "$tmp" | tr -d ' ')
  firstline=$(grep -m1 "^ThreadId(${tid})-" "$FOLDED" | cut -d'-' -f2 | cut -d' ' -f1)
  echo "  🧩 Thread ${tid} (${firstline}): ${COUNT} stacks"

  inferno-flamegraph \
    --width "$WIDTH" \
    "$MINFLAG" "$MINW" \
    --title "smrol node0 | ThreadId(${tid})-${firstline}" \
    --countname "µs" \
    "$tmp" > "${base}.svg"

  echo "  ✅ Generated ${base}.svg"
  rm -f "$tmp"
done

INDEX="$OUTDIR/index.html"
{
  echo "<html><body><h2>🔥 smrol node0 Per-Thread Flamegraphs</h2><ul>"
  for svg in "$OUTDIR"/*.svg; do
    f=$(basename "$svg")
    echo "<li><a href=\"$f\">$f</a></li>"
  done
  echo "</ul></body></html>"
} > "$INDEX"

echo "✅ Done! Open $INDEX to view all flamegraphs."
