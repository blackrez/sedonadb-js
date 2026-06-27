#!/usr/bin/env bash
set -euo pipefail

# ── download.sh ──────────────────────────────────────────────────────────
#  Download / generate the SpatialBench dataset needed by
#  `yarn bench:spatial` (DATA_DIR=./spatial-data).
#
#  Uses the `spatialbench-cli` data generator (v0.2.0) installed
#  in the project's Python virtual environment.
#
#  Usage:  ./download.sh [scale-factor]
#          scale-factor  –  TPC-H-inspired scale factor (default: 1)
#
#  The generated Parquet files are written under ./spatial-data/.
# ──────────────────────────────────────────────────────────────────────────

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUTPUT_DIR="$SCRIPT_DIR/spatial-data"
SCALE="${1:-1}"
CLI="$SCRIPT_DIR/.venv/bin/spatialbench-cli"

# Ensure the CLI tool exists
if [ ! -x "$CLI" ]; then
  echo "  ✗  spatialbench-cli not found at $CLI"
  echo "     Run  uv sync  or re-create the virtual environment."
  exit 1
fi

# Check whether the data already exists
TABLES=(building customer driver trip vehicle zone)
EXISTING=0
for t in "${TABLES[@]}"; do
  if [ -f "$OUTPUT_DIR/$t.parquet" ]; then
    ((EXISTING++))
  fi
done

if [ "$EXISTING" -eq "${#TABLES[@]}" ]; then
  echo "  ✓  SpatialBench dataset (SF=$SCALE) already present in $OUTPUT_DIR"
  exit 0
elif [ "$EXISTING" -gt 0 ]; then
  echo "  ⚠  Partial SpatialBench data found in $OUTPUT_DIR (${EXISTING}/${#TABLES[@]} tables)."
  echo "     Re-generating to ensure consistency …"
fi

echo "  Generating SpatialBench dataset (SF=$SCALE) → $OUTPUT_DIR"
echo ""

"$CLI" \
  --scale-factor "$SCALE" \
  --output-dir "$OUTPUT_DIR" \
  --format parquet \
  --verbose

echo ""
echo "  ✓  SpatialBench dataset (SF=$SCALE) generated in $OUTPUT_DIR"
