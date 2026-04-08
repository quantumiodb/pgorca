#!/bin/bash
# Strip EXPLAIN plan blocks and sort unordered result sets, then diff.
# Usage: diff_no_plan.sh expected.out results.out

normalize() {
  awk '
    # Track SQL statements to detect ORDER BY
    /^[A-Za-z]/ { sql = $0 }

    # Strip EXPLAIN plan blocks
    /^ *QUERY PLAN *$/ { in_plan=1; next }
    in_plan && /^-+$/ { next }
    in_plan && /^\(.* rows?\)$/ { in_plan=0; next }
    in_plan { next }

    # Detect table header separator (e.g. "----+----+----")
    /^-[-+]+-$/ {
      if (!in_table) {
        in_table = 1
        has_order = (tolower(sql) ~ /order[[:space:]]+by/)
        delete rows
        nrows = 0
      }
      print; next
    }

    # Collect table data rows (start with " ")
    in_table && /^ / {
      rows[nrows++] = $0
      next
    }

    # End of table: row count line like "(N rows)"
    in_table && /^\([0-9]+ rows?\)$/ {
      if (!has_order && nrows > 0) {
        # Sort rows for unordered results
        asort(rows)
      }
      for (i = 0; i < nrows; i++) print rows[i]
      in_table = 0
      print; next
    }

    { print }
  ' "$1"
}

diff -U3 <(normalize "$1") <(normalize "$2")
