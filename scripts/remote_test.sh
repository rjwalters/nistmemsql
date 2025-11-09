#!/usr/bin/env bash
#
# Remote SQLLogicTest Execution Script
#
# Runs the SQLLogicTest suite on a remote machine (e.g., rwalters-sandbox-1)
# with many cores for maximum parallelization.
#
# Usage:
#   ./scripts/remote_test.sh [options]
#
# Options:
#   --host HOST          Remote hostname (default: rwalters-sandbox-1)
#   --workers N          Number of parallel workers (default: auto-detect CPUs)
#   --time N             Time budget in seconds per worker (default: 3600)
#   --repo-path PATH     Path to repo on remote (default: ~/vibesql)
#   --setup              Initial setup: clone repo, install deps
#   --sync               Sync local code to remote before running
#   --fetch-results      Fetch results back to local machine
#   --clean              Clean up remote workspace
#
# Examples:
#   # Initial setup on new machine
#   ./scripts/remote_test.sh --setup
#
#   # Sync code and run tests with all cores, 1 hour per worker
#   ./scripts/remote_test.sh --sync --time 3600
#
#   # Quick test with 8 workers, 5 minutes each
#   ./scripts/remote_test.sh --sync --workers 8 --time 300
#
#   # Fetch results from previous run
#   ./scripts/remote_test.sh --fetch-results

set -euo pipefail

# Default configuration
REMOTE_HOST="rwalters-sandbox-1"
REMOTE_REPO_PATH="vibesql"
WORKERS=""
TIME_BUDGET="3600"
DO_SETUP=false
DO_SYNC=false
DO_FETCH=false
DO_CLEAN=false

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

print_error() { echo -e "${RED}❌ $1${NC}" >&2; }
print_success() { echo -e "${GREEN}✓ $1${NC}"; }
print_warning() { echo -e "${YELLOW}⚠️  $1${NC}"; }
print_info() { echo -e "${BLUE}ℹ $1${NC}"; }

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --host)
            REMOTE_HOST="$2"
            shift 2
            ;;
        --workers)
            WORKERS="$2"
            shift 2
            ;;
        --time)
            TIME_BUDGET="$2"
            shift 2
            ;;
        --repo-path)
            REMOTE_REPO_PATH="$2"
            shift 2
            ;;
        --setup)
            DO_SETUP=true
            shift
            ;;
        --sync)
            DO_SYNC=true
            shift
            ;;
        --fetch-results)
            DO_FETCH=true
            shift
            ;;
        --clean)
            DO_CLEAN=true
            shift
            ;;
        --help)
            head -n 30 "$0" | grep "^#" | sed 's/^# \?//'
            exit 0
            ;;
        *)
            print_error "Unknown option: $1"
            echo "Run with --help for usage"
            exit 1
            ;;
    esac
done

# Get local repo root
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Initial setup on remote machine
if $DO_SETUP; then
    print_info "Setting up remote machine: $REMOTE_HOST"

    ssh "$REMOTE_HOST" bash << 'EOF'
set -euo pipefail

# Check if repo already exists
if [ -d "vibesql" ]; then
    echo "Repository already exists at ~/vibesql"
    cd vibesql
    git status
else
    echo "Cloning repository..."
    git clone https://github.com/rjwalters/vibesql.git
    cd vibesql
fi

# Install Rust if not present
if ! command -v cargo &> /dev/null; then
    echo "Installing Rust..."
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
fi

# Ensure cargo is in PATH
if [ -f "$HOME/.cargo/env" ]; then
    source "$HOME/.cargo/env"
fi

# Install dependencies
echo "Installing system dependencies..."
# Use sudo only if not root
if [ "$(id -u)" -eq 0 ]; then
    apt-get update
    apt-get install -y build-essential python3 python3-pip jq
else
    sudo apt-get update
    sudo apt-get install -y build-essential python3 python3-pip jq
fi

# Initialize submodules
echo "Initializing SQLLogicTest submodule..."
git submodule update --init --recursive

# Build in release mode (for faster test execution)
echo "Building in release mode..."
cargo build --release --test sqllogictest_suite

echo "Setup complete!"
EOF

    print_success "Remote setup complete"
    exit 0
fi

# Sync local changes to remote
if $DO_SYNC; then
    print_info "Syncing local code to $REMOTE_HOST:~/$REMOTE_REPO_PATH"

    # Use rsync to efficiently sync code
    rsync -avz --delete \
        --exclude 'target/' \
        --exclude '.git/' \
        --exclude 'node_modules/' \
        --exclude '.loom/worktrees/' \
        "$REPO_ROOT/" \
        "$REMOTE_HOST:~/$REMOTE_REPO_PATH/"

    # Rebuild on remote
    print_info "Rebuilding on remote..."
    ssh "$REMOTE_HOST" "cd ~/$REMOTE_REPO_PATH && cargo build --release --test sqllogictest_suite"

    print_success "Sync complete"
fi

# Clean remote workspace
if $DO_CLEAN; then
    print_info "Cleaning remote workspace"

    ssh "$REMOTE_HOST" << EOF
cd ~/$REMOTE_REPO_PATH
cargo clean
rm -rf target/worker_results/
rm -f target/sqllogictest_*.json
echo "Workspace cleaned"
EOF

    print_success "Clean complete"
    exit 0
fi

# Fetch results from remote
if $DO_FETCH; then
    print_info "Fetching results from $REMOTE_HOST"

    # Create local results directory
    mkdir -p "$REPO_ROOT/target/remote_results"

    # Fetch results files
    scp "$REMOTE_HOST:~/$REMOTE_REPO_PATH/target/sqllogictest_cumulative.json" \
        "$REPO_ROOT/target/sqllogictest_cumulative.json" 2>/dev/null || print_warning "No cumulative results found"

    scp "$REMOTE_HOST:~/$REMOTE_REPO_PATH/target/sqllogictest_results.sql" \
        "$REPO_ROOT/target/sqllogictest_results.sql" 2>/dev/null || print_warning "No database results found"

    scp -r "$REMOTE_HOST:~/$REMOTE_REPO_PATH/target/worker_results/" \
        "$REPO_ROOT/target/remote_results/" 2>/dev/null || print_warning "No worker results found"

    print_success "Results fetched to target/"

    # Show quick summary
    if [ -f "$REPO_ROOT/target/sqllogictest_cumulative.json" ]; then
        print_info "Test results summary:"
        "$REPO_ROOT/scripts/sqllogictest" status
    fi

    exit 0
fi

# Run tests on remote
print_info "Running SQLLogicTest suite on $REMOTE_HOST"

# Detect workers if not specified
if [ -z "$WORKERS" ]; then
    WORKERS=$(ssh "$REMOTE_HOST" "nproc")
    print_info "Auto-detected $WORKERS CPUs on remote"
fi

echo ""
echo "Configuration:"
echo "  Host:         $REMOTE_HOST"
echo "  Repo:         ~/$REMOTE_REPO_PATH"
echo "  Workers:      $WORKERS"
echo "  Time budget:  ${TIME_BUDGET}s per worker"
echo "  Total time:   ~${TIME_BUDGET}s (parallel)"
echo ""

# Run the test suite
ssh "$REMOTE_HOST" bash << EOF
set -euo pipefail

cd ~/$REMOTE_REPO_PATH

# Show git status
echo "=== Git Status ==="
git log -1 --oneline
git status --short
echo ""

# Run the test suite
echo "=== Starting Test Suite ==="
./scripts/sqllogictest run --parallel --workers $WORKERS --time $TIME_BUDGET

# Show quick results summary
echo ""
echo "=== Results Summary ==="
./scripts/sqllogictest status

echo ""
echo "Fetch results with:"
echo "  ./scripts/remote_test.sh --fetch-results"
EOF

print_success "Remote test run complete"
echo ""
echo "Next steps:"
echo "  1. Fetch results: ./scripts/remote_test.sh --fetch-results"
echo "  2. View punchlist: ./scripts/sqllogictest punchlist"
echo "  3. Query results: ./scripts/sqllogictest query --preset failed-files"
