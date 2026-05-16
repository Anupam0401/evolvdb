#!/usr/bin/env bash
set -euo pipefail

# Use 'auto' to let request_review.py detect the correct base branch
# For feature branches: compares against origin/master (PR target)
# For master/main: compares against upstream tracking

# Use fixed temp file location for AI to read directly
TEMP_REVIEW_FILE="/tmp/cascade_code_review_latest.txt"

# Clean up any previous file
rm -f "$TEMP_REVIEW_FILE"

# Generate review to file (handles unlimited size)
python3 .cursor/commands/code_review/request_review.py \
  --base auto \
  --mode worktree \
  --include-untracked \
  --max-bytes 50000000 \
  --output file \
  --out "$TEMP_REVIEW_FILE" \
  "$@"

echo ""
echo "=========================================="
echo "✅ Review generated successfully!"
echo "📄 Complete diff saved to: $TEMP_REVIEW_FILE"
echo "📊 File size: $(wc -c < "$TEMP_REVIEW_FILE" | awk '{printf "%.2f MB", $1/1024/1024}')"
echo "📝 Line count: $(wc -l < "$TEMP_REVIEW_FILE")"
echo ""
echo "🤖 AI: Use read_file tool to read the complete diff:"
echo "   read_file('$TEMP_REVIEW_FILE')"
echo ""
echo "🧹 To clean up: rm $TEMP_REVIEW_FILE"
echo "=========================================="
