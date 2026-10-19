#!/usr/bin/env bash
# Regenerate docs/diagrams/*.svg from the Mermaid blocks in the docs.
#
# Convention: every ```mermaid block is preceded by an image link that names its
# output file, for example:
#
#   ![System context](diagrams/01-system-context.svg)
#
#   ```mermaid
#   flowchart LR
#   ...
#
# The Markdown block is the source of truth; the SVG is a rendered copy for
# viewers without Mermaid support. Requires Node.js (npx). Set MMDC to use an
# already-installed mermaid-cli binary instead of downloading one.
set -euo pipefail

DOCS_DIR="$(cd "$(dirname "$0")/.." && pwd)"
MMDC_VERSION="${MMDC_VERSION:-11.12.0}"
if [ -n "${MMDC:-}" ]; then
    MMDC_CMD=("$MMDC")
else
    MMDC_CMD=(npx --yes -p "@mermaid-js/mermaid-cli@$MMDC_VERSION" mmdc)
fi

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

built=0
failed=0
while IFS= read -r -d '' md; do
    # Emit "<svg path>\t<temp .mmd path>" for each linked Mermaid block.
    awk -v work="$WORK" -v md="$md" '
        /^!\[[^]]*\]\([^)]*\.svg\)[[:space:]]*$/ {
            target = $0; sub(/^!\[[^]]*\]\(/, "", target); sub(/\).*$/, "", target)
            pending = target; next
        }
        /^```mermaid/ {
            if (pending == "") { print "WARN\t" md ": mermaid block without an SVG image link" > "/dev/stderr"; skip = 1; next }
            n++; out = work "/" n ".mmd"
            print pending "\t" out; inblock = 1; pending = ""; next
        }
        /^```/ && inblock { inblock = 0; close(out); next }
        /^```/ && skip { skip = 0; next }
        inblock { print > out; next }
        /[^[:space:]]/ { pending = "" }
    ' "$md" > "$WORK/list"
    while IFS=$'\t' read -r target source; do
        svg="$(cd "$(dirname "$md")" && cd "$(dirname "$target")" && pwd)/$(basename "$target")"
        if "${MMDC_CMD[@]}" -q -b white -i "$source" -o "$svg" >"$WORK/log" 2>&1; then
            # Mermaid emits width="100%", which stretches small diagrams to the full
            # page width when shown as <img>. Pin the width to the natural size.
            sed -i -E '0,/<svg/ s/<svg([^>]*) width="100%"([^>]*max-width: ([0-9.]+)px)/<svg\1 width="\3"\2/' "$svg"
            echo "built  ${svg#"$DOCS_DIR"/}"
            built=$((built + 1))
        else
            echo "FAILED ${svg#"$DOCS_DIR"/} (from ${md#"$DOCS_DIR"/})" >&2
            sed 's/^/    /' "$WORK/log" >&2
            failed=$((failed + 1))
        fi
    done < "$WORK/list"
done < <(find "$DOCS_DIR" -path "$DOCS_DIR/archive" -prune -o -name '*.md' -print0 | sort -z)

echo "$built diagram(s) built, $failed failed."
[ "$failed" -eq 0 ]
