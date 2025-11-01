/**
 * Benchmark results page
 *
 * Loads and displays performance benchmark data comparing nistmemsql to SQLite.
 */

import './styles/main.css';
import { initTheme } from './theme';
import { NavigationComponent } from './components/Navigation';

// Chart.js is loaded via CDN in benchmarks.html
declare const Chart: any;

interface BenchmarkStats {
  mean: number;
  stddev: number;
  min: number;
  max: number;
  rounds: number;
}

interface Benchmark {
  name: string;
  stats: BenchmarkStats;
}

interface BenchmarkResults {
  benchmarks: Benchmark[];
  datetime: string;
  machine_info?: {
    system?: string;
    python_version?: string;
  };
}

/**
 * Format time in appropriate units
 */
function formatTime(seconds: number): string {
  if (seconds < 0.001) {
    return `${(seconds * 1_000_000).toFixed(2)} µs`;
  } else if (seconds < 1) {
    return `${(seconds * 1000).toFixed(2)} ms`;
  } else {
    return `${seconds.toFixed(2)} s`;
  }
}

/**
 * Parse benchmark name to extract database and operation info
 */
function parseBenchmarkName(name: string): { operation: string; database: string } {
  // Example names: "test_simple_select_1k_nistmemsql", "test_simple_select_1k_sqlite", "test_simple_select_1k_duckdb"
  const parts = name.split('_');
  const database = parts[parts.length - 1]; // Last part is database name

  // Remove "test_" prefix and database suffix
  const operation = parts.slice(1, -1).join('_');

  return { operation, database };
}

/**
 * Group benchmarks by operation
 */
function groupBenchmarksByOperation(benchmarks: Benchmark[]): Map<string, Map<string, Benchmark>> {
  const grouped = new Map<string, Map<string, Benchmark>>();

  for (const bench of benchmarks) {
    const { operation, database } = parseBenchmarkName(bench.name);

    if (!grouped.has(operation)) {
      grouped.set(operation, new Map());
    }

    grouped.get(operation)!.set(database, bench);
  }

  return grouped;
}

/**
 * Calculate speedup factor
 */
function calculateSpeedup(nistmemsql: number, sqlite: number): number {
  return sqlite / nistmemsql;
}

/**
 * Render results table
 */
function renderResultsTable(data: BenchmarkResults) {
  const tbody = document.getElementById('results-tbody');
  if (!tbody) return;

  const grouped = groupBenchmarksByOperation(data.benchmarks);

  tbody.innerHTML = '';

  let totalSpeedup = 0;
  let comparisonCount = 0;

  for (const [operation, databases] of grouped.entries()) {
    const nistmemsql = databases.get('nistmemsql');
    const sqlite = databases.get('sqlite');
    const duckdb = databases.get('duckdb');

    if (!nistmemsql && !sqlite && !duckdb) continue;

    const row = document.createElement('tr');
    row.className = 'hover:bg-card/50 transition-colors';

    // Operation name
    const opCell = document.createElement('td');
    opCell.className = 'px-4 py-3 font-medium text-foreground';
    opCell.textContent = operation.replace(/_/g, ' ').toUpperCase();
    row.appendChild(opCell);

    // nistmemsql time
    const nistCell = document.createElement('td');
    nistCell.className = 'px-4 py-3 text-right text-muted';
    nistCell.textContent = nistmemsql ? formatTime(nistmemsql.stats.mean) : 'N/A';
    row.appendChild(nistCell);

    // SQLite time
    const sqliteCell = document.createElement('td');
    sqliteCell.className = 'px-4 py-3 text-right text-muted';
    sqliteCell.textContent = sqlite ? formatTime(sqlite.stats.mean) : 'N/A';
    row.appendChild(sqliteCell);

    // DuckDB time
    const duckdbCell = document.createElement('td');
    duckdbCell.className = 'px-4 py-3 text-right text-muted';
    duckdbCell.textContent = duckdb ? formatTime(duckdb.stats.mean) : 'N/A';
    row.appendChild(duckdbCell);

    // Speedup vs SQLite
    const speedupCell = document.createElement('td');
    speedupCell.className = 'px-4 py-3 text-right font-semibold';

    if (nistmemsql && sqlite) {
      const speedup = calculateSpeedup(nistmemsql.stats.mean, sqlite.stats.mean);
      speedupCell.textContent = `${speedup.toFixed(2)}x`;

      if (speedup > 1) {
        speedupCell.className += ' text-green-600 dark:text-green-400';
      } else if (speedup < 1) {
        speedupCell.className += ' text-red-600 dark:text-red-400';
      } else {
        speedupCell.className += ' text-muted';
      }

      totalSpeedup += speedup;
      comparisonCount++;
    } else {
      speedupCell.textContent = 'N/A';
      speedupCell.className += ' text-muted';
    }

    row.appendChild(speedupCell);

    // Winner
    const winnerCell = document.createElement('td');
    winnerCell.className = 'px-4 py-3 text-center text-2xl';

    if (nistmemsql && sqlite) {
      const speedup = calculateSpeedup(nistmemsql.stats.mean, sqlite.stats.mean);
      winnerCell.textContent = speedup > 1 ? '🚀' : speedup < 1 ? '🐌' : '🤝';
    } else {
      winnerCell.textContent = '-';
    }

    row.appendChild(winnerCell);
    tbody.appendChild(row);
  }

  // Update summary cards
  if (comparisonCount > 0) {
    const avgSpeedup = totalSpeedup / comparisonCount;
    const avgSpeedupEl = document.getElementById('avg-speedup');
    if (avgSpeedupEl) {
      if (avgSpeedup > 1) {
        avgSpeedupEl.textContent = `${avgSpeedup.toFixed(2)}x faster`;
        avgSpeedupEl.className = avgSpeedupEl.className.replace(
          'text-primary-light dark:text-primary-dark',
          'text-green-600 dark:text-green-400'
        );
      } else if (avgSpeedup < 1) {
        // Invert the ratio: if speedup = 0.11, we're 1/0.11 = 9.09x slower
        const slowerBy = 1 / avgSpeedup;
        avgSpeedupEl.textContent = `${slowerBy.toFixed(2)}x slower`;
        avgSpeedupEl.className = avgSpeedupEl.className.replace(
          'text-primary-light dark:text-primary-dark',
          'text-red-600 dark:text-red-400'
        );
      } else {
        avgSpeedupEl.textContent = `${avgSpeedup.toFixed(2)}x`;
      }
    }
  }

  const opsTestedEl = document.getElementById('ops-tested');
  if (opsTestedEl) {
    opsTestedEl.textContent = grouped.size.toString();
  }
}

/**
 * Render performance chart
 */
function renderChart(data: BenchmarkResults) {
  const canvas = document.getElementById('performance-chart') as HTMLCanvasElement;
  if (!canvas) return;

  const grouped = groupBenchmarksByOperation(data.benchmarks);

  const labels: string[] = [];
  const nistmemsqlData: number[] = [];
  const sqliteData: number[] = [];
  const duckdbData: number[] = [];

  for (const [operation, databases] of grouped.entries()) {
    const nistmemsql = databases.get('nistmemsql');
    const sqlite = databases.get('sqlite');
    const duckdb = databases.get('duckdb');

    if (nistmemsql || sqlite || duckdb) {
      labels.push(operation.replace(/_/g, ' ').toUpperCase());
      nistmemsqlData.push(nistmemsql ? nistmemsql.stats.mean * 1000 : 0); // Convert to ms
      sqliteData.push(sqlite ? sqlite.stats.mean * 1000 : 0);
      duckdbData.push(duckdb ? duckdb.stats.mean * 1000 : 0);
    }
  }

  new Chart(canvas, {
    type: 'bar',
    data: {
      labels,
      datasets: [
        {
          label: 'nistmemsql',
          data: nistmemsqlData,
          backgroundColor: 'rgba(34, 197, 94, 0.5)',
          borderColor: 'rgba(34, 197, 94, 1)',
          borderWidth: 1,
        },
        {
          label: 'SQLite',
          data: sqliteData,
          backgroundColor: 'rgba(239, 68, 68, 0.5)',
          borderColor: 'rgba(239, 68, 68, 1)',
          borderWidth: 1,
        },
        {
          label: 'DuckDB',
          data: duckdbData,
          backgroundColor: 'rgba(59, 130, 246, 0.5)',
          borderColor: 'rgba(59, 130, 246, 1)',
          borderWidth: 1,
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      scales: {
        y: {
          type: 'logarithmic',
          beginAtZero: false,
          title: {
            display: true,
            text: 'Time (ms) - Log Scale',
          },
          ticks: {
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            callback: function (value: any) {
              // Only show specific tick marks: 0.01, 0.1, 1, 10, 100, 1000
              const allowedTicks = [0.01, 0.1, 1, 10, 100, 1000];
              if (allowedTicks.includes(value)) {
                return value;
              }
              return null;
            },
          },
        },
      },
      plugins: {
        legend: {
          display: true,
          position: 'top',
        },
        tooltip: {
          callbacks: {
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            label: function (context: any) {
              return `${context.dataset.label}: ${context.parsed.y.toFixed(2)} ms`;
            },
          },
        },
      },
    },
  });
}

/**
 * Load and display benchmark data
 */
async function loadBenchmarkData() {
  try {
    const response = await fetch('/nistmemsql/benchmarks/benchmark_results.json');

    if (!response.ok) {
      throw new Error(`Failed to load benchmark data: ${response.status}`);
    }

    const data: BenchmarkResults = await response.json();

    // Update last updated timestamp
    const lastUpdatedEl = document.getElementById('last-updated');
    if (lastUpdatedEl && data.datetime) {
      const date = new Date(data.datetime);
      lastUpdatedEl.textContent = date.toLocaleDateString();
      lastUpdatedEl.className = 'text-xl font-bold text-primary-light dark:text-primary-dark';
    }

    renderResultsTable(data);
    renderChart(data);
  } catch (error) {
    console.error('Error loading benchmark data:', error);

    const tbody = document.getElementById('results-tbody');
    if (tbody) {
      tbody.innerHTML = `
        <tr>
          <td colspan="6" class="px-4 py-8 text-center text-red-500">
            ⚠️ Failed to load benchmark results. Please check back later.
          </td>
        </tr>
      `;
    }

    const avgSpeedupEl = document.getElementById('avg-speedup');
    if (avgSpeedupEl) {
      avgSpeedupEl.textContent = 'N/A';
      avgSpeedupEl.className = 'text-xl font-bold text-muted';
    }
  }
}

// Initialize page
document.addEventListener('DOMContentLoaded', () => {
  // Initialize theme system
  const theme = initTheme();

  // Initialize navigation component
  new NavigationComponent('benchmarks', theme);

  // Load benchmark data
  loadBenchmarkData();
});
