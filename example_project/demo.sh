#!/bin/bash
# Demo script for dbt-multi-adapter-utils

set -e

echo "=================================================="
echo "dbt-multi-adapter-utils Demo"
echo "=================================================="
echo ""

# Make sure we're in the right directory
cd "$(dirname "$0")"

echo "📂 Current directory: $(pwd)"
echo ""

echo "Step 1: Scanning project for non-portable functions..."
echo "-------------------------------------------------------"
uv run --directory .. dbt-multi-adapter-utils scan --config .dbt-multi-adapter.yml
echo ""
echo "Press Enter to continue..."
read

echo "Step 2: Generating portable macros..."
echo "-------------------------------------------------------"
uv run --directory .. dbt-multi-adapter-utils generate --config .dbt-multi-adapter.yml
echo ""
echo "✅ Macros generated at: macros/portable_functions.sql"
echo ""
echo "Press Enter to see the generated macros..."
read

echo "Generated macros:"
echo "-------------------------------------------------------"
head -60 macros/portable_functions.sql
echo ""
echo "... (more macros below)"
echo ""
echo "Press Enter to continue..."
read

echo "Step 3: Rewrite models (dry-run first)..."
echo "-------------------------------------------------------"
uv run --directory .. dbt-multi-adapter-utils rewrite --dry-run --config .dbt-multi-adapter.yml
echo ""
echo "Press Enter to actually rewrite the models..."
read

echo "Step 4: Rewriting models..."
echo "-------------------------------------------------------"
uv run --directory .. dbt-multi-adapter-utils rewrite --config .dbt-multi-adapter.yml
echo ""

echo "=================================================="
echo "✅ Demo complete!"
echo "=================================================="
echo ""
echo "Check out the changes:"
echo "  - macros/portable_functions.sql    (generated macros)"
echo "  - models/*.sql                     (rewritten models)"
echo ""
echo "To run the full workflow in one command:"
echo "  uv run --directory .. dbt-multi-adapter-utils migrate --config .dbt-multi-adapter.yml"
echo ""
