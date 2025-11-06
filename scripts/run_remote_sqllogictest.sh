#!/usr/bin/env bash
#
# Remote SQLLogicTest Suite Runner
#
# Automates running the full SQLLogicTest suite on a remote sandbox machine.
# Uses SSH to connect, setup environment, run tests, and retrieve results.
#
# Usage:
#   ./scripts/run_remote_sqllogictest.sh [OPTIONS]
#
# Options:
#   --workers N         Number of parallel workers (default: 320 for 5x oversubscription)
#   --time-budget S     Time budget per worker in seconds (default: 3600)
#   --sandbox HOST      SSH host (default: rwalters-sandbox-1)
#   --setup-only        Only setup environment, don't run tests
#   --retrieve-only     Only retrieve existing results
#   --interactive       Run in interactive tmux session
#
# Examples:
#   # Full run with defaults (320 workers, 1 hour)
#   ./scripts/run_remote_sqllogictest.sh
#
#   # Quick validation (64 workers, 10 minutes)
#   ./scripts/run_remote_sqllogictest.sh --workers 64 --time-budget 600
#
#   # Setup environment only
#   ./scripts/run_remote_sqllogictest.sh --setup-only
#
#   # Just retrieve existing results
#   ./scripts/run_remote_sqllogictest.sh --retrieve-only

set -euo pipefail

# Default configuration
WORKERS=320
TIME_BUDGET=3600
SANDBOX_HOST="rwalters-sandbox-1"
SETUP_ONLY=false
RETRIEVE_ONLY=false
INTERACTIVE=false

# Parse arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --workers)
      WORKERS="$2"
      shift 2
      ;;
    --time-budget)
      TIME_BUDGET="$2"
      shift 2
      ;;
    --sandbox)
      SANDBOX_HOST="$2"
      shift 2
      ;;
    --setup-only)
      SETUP_ONLY=true
      shift
      ;;
    --retrieve-only)
      RETRIEVE_ONLY=true
      shift
      ;;
    --interactive)
      INTERACTIVE=true
      shift
      ;;
    --help)
      head -n 30 "$0" | grep "^#" | sed 's/^# //'
      exit 0
      ;;
    *)
      echo "Unknown option: $1"
      echo "Run with --help for usage information"
      exit 1
      ;;
  esac
done

# Validate configuration
if ! command -v ssh &> /dev/null; then
  echo "Error: ssh command not found"
  exit 1
fi

echo "=== Remote SQLLogicTest Suite Runner ==="
echo "Sandbox: $SANDBOX_HOST"
echo "Workers: $WORKERS"
echo "Time budget: ${TIME_BUDGET}s"
echo ""

# Check SSH connectivity
echo "Checking SSH connection..."
if ! ssh -o ConnectTimeout=10 "$SANDBOX_HOST" "echo '✓ Connected'" 2>/dev/null; then
  echo "Error: Cannot connect to $SANDBOX_HOST"
  echo "Check that:"
  echo "  1. SSH config exists: ~/.sky/generated/ssh/$SANDBOX_HOST"
  echo "  2. Sandbox is running: sky status"
  echo "  3. Network connectivity is available"
  exit 1
fi

# Retrieve results only
if $RETRIEVE_ONLY; then
  echo "=== Retrieving Results ==="

  echo "Downloading cumulative results..."
  scp "$SANDBOX_HOST:~/vibesql/target/sqllogictest_cumulative.json" ./target/ || {
    echo "Warning: Could not download cumulative results"
    echo "Check if tests have completed on sandbox"
    exit 1
  }

  echo "✓ Results downloaded to target/sqllogictest_cumulative.json"
  echo ""
  echo "Summary:"
  jq '.summary' target/sqllogictest_cumulative.json || echo "Could not parse summary"

  exit 0
fi

# Setup environment
echo "=== Setting Up Environment ==="

ssh "$SANDBOX_HOST" bash << 'SETUP_EOF'
set -e

echo "Checking repository..."
if [ ! -d vibesql ]; then
  echo "Cloning repository..."
  git clone https://github.com/rjwalters/vibesql.git
fi

cd vibesql
echo "Updating repository..."
git fetch origin
git checkout main
git pull origin main
git submodule update --init --recursive

echo "Checking Rust installation..."
if ! command -v cargo &> /dev/null; then
  echo "Installing Rust..."
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
  source "$HOME/.cargo/env"
else
  echo "✓ Rust already installed"
  source "$HOME/.cargo/env" 2>/dev/null || true
fi

rustc --version
cargo --version

echo "Pre-compiling test binary..."
cargo test --test sqllogictest_suite --release --no-run

echo "✓ Environment ready"
SETUP_EOF

if $SETUP_ONLY; then
  echo "✓ Setup complete"
  exit 0
fi

# Run tests
echo ""
echo "=== Running Tests ==="

if $INTERACTIVE; then
  echo "Starting interactive tmux session..."
  echo "To detach: Ctrl+B, then D"
  echo "To reattach: ssh $SANDBOX_HOST -t 'tmux attach -t sqllogictest'"
  echo ""

  ssh -t "$SANDBOX_HOST" << INTERACTIVE_EOF
cd vibesql
source "\$HOME/.cargo/env" 2>/dev/null || true

# Kill any existing session
tmux kill-session -t sqllogictest 2>/dev/null || true

# Start new session
tmux new -s sqllogictest bash -c "
  python3 scripts/run_parallel_tests.py \
    --workers $WORKERS \
    --time-budget $TIME_BUDGET \
    2>&1 | tee sqllogictest_run_\$(date +%Y%m%d_%H%M%S).log

  echo ''
  echo 'Tests complete! Press Ctrl+D to exit or Ctrl+B,D to detach'
  bash
"
INTERACTIVE_EOF

else
  echo "Running tests in background..."
  echo "Monitor progress: ssh $SANDBOX_HOST 'tail -f vibesql/sqllogictest_*.log'"
  echo ""

  # Generate timestamp for log file
  TIMESTAMP=$(date +%Y%m%d_%H%M%S)
  LOG_FILE="sqllogictest_run_${TIMESTAMP}.log"

  ssh "$SANDBOX_HOST" << RUN_EOF
cd vibesql
source "\$HOME/.cargo/env" 2>/dev/null || true

nohup python3 scripts/run_parallel_tests.py \
  --workers $WORKERS \
  --time-budget $TIME_BUDGET \
  > "$LOG_FILE" 2>&1 &

PID=\$!
echo "✓ Tests started with PID \$PID"
echo "✓ Logging to $LOG_FILE"
echo ""
echo "Monitor progress:"
echo "  ssh $SANDBOX_HOST 'tail -f vibesql/$LOG_FILE'"
echo ""
echo "Check status:"
echo "  ssh $SANDBOX_HOST 'ps aux | grep python.*run_parallel_tests'"
RUN_EOF

  echo ""
  echo "=== Next Steps ==="
  echo "1. Monitor progress: ssh $SANDBOX_HOST 'tail -f vibesql/$LOG_FILE'"
  echo "2. Wait for completion (estimated: 1-2 hours)"
  echo "3. Retrieve results: $0 --retrieve-only"
  echo "4. Commit to repo: git add target/sqllogictest_cumulative.json && git commit && git push"
fi

echo ""
echo "✓ Done"
