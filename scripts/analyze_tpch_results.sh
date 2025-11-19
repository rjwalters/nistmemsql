#!/bin/bash
# Analyze TPC-H benchmark results from Criterion output

LOG_FILE="${1:-/tmp/tpch_bench.log}"

if [ ! -f "$LOG_FILE" ]; then
    echo "Error: Log file not found: $LOG_FILE"
    exit 1
fi

echo "==================================================================="
echo "TPC-H Benchmark Results Summary"
echo "==================================================================="
echo ""

# Extract all benchmark results
grep -E "tpch_q[0-9]+/(vibesql|sqlite|duckdb)/SF" "$LOG_FILE" | \
    grep "time:" | \
    awk '{
        # Extract query number, database, and time
        split($1, a, "/");
        query = a[1];
        db = a[2];

        # Extract time value and unit
        time_val = $4;
        time_unit = $5;

        # Convert to milliseconds for consistent comparison
        if (time_unit == "ms]") {
            time_ms = time_val;
        } else if (time_unit == "µs]") {
            time_ms = time_val / 1000;
        } else if (time_unit == "s]") {
            time_ms = time_val * 1000;
        } else {
            # Assume ms if unit unclear
            time_ms = time_val;
        }

        # Store results
        results[query, db] = time_ms;
        queries[query] = 1;
        dbs[db] = 1;
    }
    END {
        # Print header
        printf "%-10s %15s %15s %15s %12s %12s\n", "Query", "VibeSQL", "SQLite", "DuckDB", "vs SQLite", "vs DuckDB";
        printf "%-10s %15s %15s %15s %12s %12s\n", "------", "-------", "------", "------", "---------", "---------";

        total_vibesql = 0;
        total_sqlite = 0;
        total_duckdb = 0;
        count = 0;

        # Sort and print results
        n = asorti(queries, sorted_queries);
        for (i = 1; i <= n; i++) {
            q = sorted_queries[i];
            v = results[q, "vibesql"];
            s = results[q, "sqlite"];
            d = results[q, "duckdb"];

            if (v > 0) {
                vibesql_str = sprintf("%.2f ms", v);
                total_vibesql += v;
                count++;
            } else {
                vibesql_str = "N/A";
            }

            if (s > 0) {
                sqlite_str = sprintf("%.2f ms", s);
                total_sqlite += s;
            } else {
                sqlite_str = "N/A";
            }

            if (d > 0) {
                duckdb_str = sprintf("%.2f ms", d);
                total_duckdb += d;
            } else {
                duckdb_str = "N/A";
            }

            # Calculate ratios
            if (v > 0 && s > 0) {
                ratio_sqlite = v / s;
                ratio_str = sprintf("%.2fx", ratio_sqlite);
            } else {
                ratio_str = "N/A";
            }

            if (v > 0 && d > 0) {
                ratio_duckdb = v / d;
                ratio_duckdb_str = sprintf("%.2fx", ratio_duckdb);
            } else {
                ratio_duckdb_str = "N/A";
            }

            printf "%-10s %15s %15s %15s %12s %12s\n", q, vibesql_str, sqlite_str, duckdb_str, ratio_str, ratio_duckdb_str;
        }

        # Print totals
        printf "\n%-10s %15s %15s %15s\n", "TOTAL", sprintf("%.2f ms", total_vibesql), sprintf("%.2f ms", total_sqlite), sprintf("%.2f ms", total_duckdb);

        if (total_sqlite > 0) {
            avg_ratio_sqlite = total_vibesql / total_sqlite;
            printf "%-10s %15s %15s %15s %12s\n", "AVG RATIO", "", "", "", sprintf("%.2fx", avg_ratio_sqlite);
        }

        if (total_duckdb > 0) {
            avg_ratio_duckdb = total_vibesql / total_duckdb;
            printf "%-10s %15s %15s %15s %12s %12s\n", "", "", "", "", "", sprintf("%.2fx", avg_ratio_duckdb);
        }
    }' | column -t

echo ""
echo "==================================================================="
echo "Performance Assessment"
echo "==================================================================="
echo ""
echo "According to docs/performance/BENCHMARK_STRATEGY.md:"
echo "  < 1.5x slower than SQLite  = Competitive ✅"
echo "  1.5x - 2.5x slower         = Acceptable ⚠️"
echo "  2.5x - 5.0x slower         = Needs improvement ⚠️"
echo "  > 5.0x slower              = Performance issue ❌"
echo ""
