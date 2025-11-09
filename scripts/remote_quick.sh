#!/usr/bin/env bash
#
# Quick Remote Testing - Simplified Workflow
#
# This is a simplified wrapper around remote_test.sh for common operations.
# For more control, use remote_test.sh directly.
#
# Usage:
#   ./scripts/remote_quick.sh [command]
#
# Commands:
#   setup        Initial setup (install Rust, clone repo, build)
#   sync         Sync local code to remote and rebuild
#   run          Run full test suite (64 workers, 1 hour)
#   quick        Quick test (8 workers, 5 minutes)
#   fetch        Fetch results from remote
#   status       Show test results summary
#   clean        Clean remote workspace
#
# Examples:
#   # First time setup
#   ./scripts/remote_quick.sh setup
#
#   # Typical workflow: sync, run, fetch
#   ./scripts/remote_quick.sh sync
#   ./scripts/remote_quick.sh run
#   ./scripts/remote_quick.sh fetch
#   ./scripts/remote_quick.sh status

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REMOTE_TEST="$SCRIPT_DIR/remote_test.sh"

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m'

print_info() { echo -e "${BLUE}ℹ $1${NC}"; }
print_success() { echo -e "${GREEN}✓ $1${NC}"; }

if [ $# -eq 0 ]; then
    head -n 20 "$0" | grep "^#" | sed 's/^# \?//'
    exit 0
fi

COMMAND="$1"

case "$COMMAND" in
    setup)
        print_info "Setting up remote machine..."
        "$REMOTE_TEST" --setup
        ;;

    sync)
        print_info "Syncing code to remote..."
        "$REMOTE_TEST" --sync
        print_success "Code synced and rebuilt"
        ;;

    run)
        print_info "Running full test suite (64 workers, 1 hour per worker)..."
        "$REMOTE_TEST" --workers 64 --time 3600
        echo ""
        print_success "Test run complete - fetch results with: ./scripts/remote_quick.sh fetch"
        ;;

    quick)
        print_info "Running quick test (8 workers, 5 minutes)..."
        "$REMOTE_TEST" --workers 8 --time 300
        echo ""
        print_success "Quick test complete - fetch results with: ./scripts/remote_quick.sh fetch"
        ;;

    fetch)
        print_info "Fetching results from remote..."
        "$REMOTE_TEST" --fetch-results
        ;;

    status)
        if [ -f "$SCRIPT_DIR/../target/sqllogictest_cumulative.json" ]; then
            "$SCRIPT_DIR/sqllogictest" status
        else
            echo "No results found locally. Fetch them first:"
            echo "  ./scripts/remote_quick.sh fetch"
            exit 1
        fi
        ;;

    clean)
        print_info "Cleaning remote workspace..."
        "$REMOTE_TEST" --clean
        ;;

    *)
        echo "Unknown command: $COMMAND"
        echo ""
        head -n 20 "$0" | grep "^#" | sed 's/^# \?//'
        exit 1
        ;;
esac
