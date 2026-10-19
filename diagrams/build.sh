#!/usr/bin/env bash
# Render docs/diagrams/src/<name>.mmd to docs/diagrams/<name>.svg, then check that
# every diagram link in the docs resolves.
#
# The .mmd files are the source of truth; the .md files embed only the SVG:
#
#   ![System context](diagrams/01-system-context.svg)
#
#   *Source: [diagrams/src/01-system-context.mmd](diagrams/src/01-system-context.mmd)*
#
# Requires Node.js (npx). Set MMDC to use an already-installed mermaid-cli binary
# instead of downloading one.
set -euo pipefail

DIAGRAMS_DIR="$(cd "$(dirname "$0")" && pwd)"
DOCS_DIR="$(cd "$DIAGRAMS_DIR/.." && pwd)"
MMDC_VERSION="${MMDC_VERSION:-11.12.0}"
if [ -n "${MMDC:-}" ]; then
    MMDC_CMD=("$MMDC")
else
    MMDC_CMD=(npx --yes -p "@mermaid-js/mermaid-cli@$MMDC_VERSION" mmdc)
fi

LOG="$(mktemp)"
trap 'rm -f "$LOG"' EXIT

built=0
failed=0
for source in "$DIAGRAMS_DIR"/src/*.mmd; do
    [ -e "$source" ] || continue
    name="$(basename "$source" .mmd)"
    svg="$DIAGRAMS_DIR/$name.svg"
    if "${MMDC_CMD[@]}" -q -b white -i "$source" -o "$svg" >"$LOG" 2>&1; then
        # Mermaid emits width="100%", which stretches small diagrams to the full
        # page width when shown as <img>. Pin the width to the natural size.
        sed -i -E '0,/<svg/ s/<svg([^>]*) width="100%"([^>]*max-width: ([0-9.]+)px)/<svg\1 width="\3"\2/' "$svg"
        echo "built   diagrams/$name.svg"
        built=$((built + 1))
    else
        echo "FAILED  diagrams/src/$name.mmd" >&2
        sed 's/^/    /' "$LOG" >&2
        failed=$((failed + 1))
    fi
done

# Every diagram link (SVG or .mmd source) in the docs must point at an existing file.
broken=0
while IFS= read -r -d '' md; do
    while IFS= read -r target; do
        if [ ! -f "$(dirname "$md")/$target" ]; then
            echo "BROKEN  ${md#"$DOCS_DIR"/} -> $target" >&2
            broken=$((broken + 1))
        fi
    done < <(grep -oE '\]\([^)<>]*diagrams/[^)<>]*\.(svg|mmd)\)' "$md" | sed -E 's/^\]\(//; s/\)$//')
done < <(find "$DOCS_DIR" -path "$DOCS_DIR/archive" -prune -o -name '*.md' -print0)

echo "$built diagram(s) built, $failed failed, $broken broken link(s)."
[ "$failed" -eq 0 ] && [ "$broken" -eq 0 ]
