#!/bin/bash
#
# CrUX Cache Download Script
# Downloads CrUX datasets directly from GitHub
#
# Usage:
#   bash <(curl -sSL https://raw.githubusercontent.com/lonetis/crux/main/download.sh) <dataset>
#   bash <(curl -sSL https://raw.githubusercontent.com/lonetis/crux/main/download.sh) <dataset> <YYYYMM>
#
# Examples:
#   bash <(curl -sSL https://raw.githubusercontent.com/lonetis/crux/main/download.sh) global
#   bash <(curl -sSL https://raw.githubusercontent.com/lonetis/crux/main/download.sh) global 202510
#

set -e

# Configuration
REPO_BASE="https://raw.githubusercontent.com/lonetis/crux/main"
DATASET="${1:-global}"
REQUESTED_MONTH="${2:-}"

# Validate input
if [ -z "$DATASET" ]; then
    echo "Usage: $0 <dataset> [YYYYMM]"
    echo ""
    echo "Examples:"
    echo "  $0 global          # Download latest global dataset"
    echo "  $0 global 202510   # Download October 2025 global dataset"
    echo "  $0 <country>       # Download latest country-specific dataset"
    echo ""
    echo "Visit https://lonetis.github.io/crux or check data/datasets.json"
    echo "for the list of available datasets."
    exit 1
fi

# Validate month format if provided
if [ -n "$REQUESTED_MONTH" ]; then
    if ! [[ "$REQUESTED_MONTH" =~ ^[0-9]{6}$ ]]; then
        echo "✗ Error: Month must be in YYYYMM format (e.g., 202510)"
        exit 1
    fi
fi

# Convert to lowercase
DATASET=$(echo "$DATASET" | tr '[:upper:]' '[:lower:]')

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "CrUX Cache Download"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "Dataset: $DATASET"
echo ""

# Download manifest
echo "→ Downloading manifest..."
MANIFEST_URL="${REPO_BASE}/data/${DATASET}/manifest.json"
MANIFEST=$(curl -sSL "$MANIFEST_URL" 2>/dev/null)

if [ $? -ne 0 ] || [ -z "$MANIFEST" ]; then
    echo "✗ Error: Dataset '$DATASET' not found"
    echo ""
    echo "Visit https://lonetis.github.io/crux for the list of available datasets"
    exit 1
fi

# Determine which month to download
if [ -n "$REQUESTED_MONTH" ]; then
    # Use requested month
    TARGET_MONTH="$REQUESTED_MONTH"

    # Verify month exists in manifest
    if ! echo "$MANIFEST" | grep -q "\"$TARGET_MONTH\""; then
        echo "✗ Error: Month $TARGET_MONTH not found in dataset '$DATASET'"
        echo ""
        echo "Available months:"
        echo "$MANIFEST" | grep -o '"[0-9]\{6\}"' | tr -d '"' | sort -u
        exit 1
    fi

    echo "  Requested month: $TARGET_MONTH"
else
    # Use latest month
    TARGET_MONTH=$(echo "$MANIFEST" | grep -o '"latest_month": *"[^"]*"' | cut -d'"' -f4)

    if [ -z "$TARGET_MONTH" ]; then
        echo "✗ Error: No data available for dataset '$DATASET'"
        exit 1
    fi

    echo "  Latest month: $TARGET_MONTH"
fi

# Get chunk information
CHUNKS=$(echo "$MANIFEST" | grep -A 1000 "\"$TARGET_MONTH\"" | grep -o '"filename": *"[^"]*"' | cut -d'"' -f4)
CHUNK_COUNT=$(echo "$CHUNKS" | wc -l | tr -d ' ')

if [ -z "$CHUNKS" ] || [ "$CHUNK_COUNT" -eq 0 ]; then
    echo "✗ Error: No chunks found for month $TARGET_MONTH"
    exit 1
fi

echo "  Chunks: $CHUNK_COUNT"
echo ""

# Output filename
OUTPUT_FILE="${TARGET_MONTH}.csv"

# Remove existing file if present
if [ -f "$OUTPUT_FILE" ]; then
    rm "$OUTPUT_FILE"
fi

# Download and concatenate chunks
echo "→ Downloading chunks..."
CHUNK_NUM=0

for CHUNK_FILE in $CHUNKS; do
    CHUNK_NUM=$((CHUNK_NUM + 1))
    CHUNK_URL="${REPO_BASE}/data/${DATASET}/${CHUNK_FILE}"

    echo -n "  [$CHUNK_NUM/$CHUNK_COUNT] $CHUNK_FILE ... "

    if curl -sSL "$CHUNK_URL" >> "$OUTPUT_FILE" 2>/dev/null; then
        echo "✓"
    else
        echo "✗"
        echo ""
        echo "✗ Error: Failed to download $CHUNK_FILE"
        rm -f "$OUTPUT_FILE"
        exit 1
    fi
done

# Get file size
FILE_SIZE=$(du -h "$OUTPUT_FILE" | cut -f1)
LINE_COUNT=$(wc -l < "$OUTPUT_FILE" | tr -d ' ')

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "✓ Download complete!"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "File: $OUTPUT_FILE"
echo "Size: $FILE_SIZE"
echo "Lines: $LINE_COUNT"
echo ""
