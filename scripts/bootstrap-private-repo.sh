#!/usr/bin/env bash
# bootstrap-private-repo.sh
#
# Normalize a private-repo checkout for shell Git usage in fresh containers.
# This script makes shell Git expectations explicit:
#   1. ensure/set the remote URL
#   2. diagnose whether shell Git has usable credentials
#   3. fetch the requested branch
#   4. preserve the current branch if switching away from it
#   5. check out the target branch and set upstream tracking
#
# Usage:
#   ./scripts/bootstrap-private-repo.sh [OPTIONS]
#
# Options:
#   --repo-url <url>       Remote URL to set on the chosen remote (default: keep existing)
#   --remote   <name>      Remote name to use (default: origin)
#   --branch   <name>      Branch to fetch/check out (default: main)
#   --diagnose-only        Print shell Git auth/branch diagnostics without changing branches
#   --allow-dirty          Allow switching branches even with local working tree changes
#   --help / -h            Show this help

set -euo pipefail

REMOTE_NAME="origin"
TARGET_BRANCH="main"
REPO_URL=""
DIAGNOSE_ONLY=0
ALLOW_DIRTY=0

while [[ $# -gt 0 ]]; do
    case "$1" in
        --repo-url)
            REPO_URL="${2:?--repo-url requires a URL argument}"
            shift 2
            ;;
        --remote)
            REMOTE_NAME="${2:?--remote requires a name argument}"
            shift 2
            ;;
        --branch)
            TARGET_BRANCH="${2:?--branch requires a branch name}"
            shift 2
            ;;
        --diagnose-only)
            DIAGNOSE_ONLY=1
            shift
            ;;
        --allow-dirty)
            ALLOW_DIRTY=1
            shift
            ;;
        --help|-h)
            cat <<'EOF'
bootstrap-private-repo.sh

Usage:
  ./scripts/bootstrap-private-repo.sh [OPTIONS]

Options:
  --repo-url <url>       Remote URL to set on the chosen remote (default: keep existing)
  --remote   <name>      Remote name to use (default: origin)
  --branch   <name>      Branch to fetch/check out (default: main)
  --diagnose-only        Print shell Git auth/branch diagnostics without changing branches
  --allow-dirty          Allow switching branches even with local working tree changes
  --help / -h            Show this help
EOF
            exit 0
            ;;
        --*)
            echo "ERROR: Unknown option \"$1\"." >&2
            exit 1
            ;;
        *)
            echo "ERROR: Unexpected positional argument \"$1\"." >&2
            exit 1
            ;;
    esac
done

if ! git rev-parse --show-toplevel >/dev/null 2>&1; then
    echo "ERROR: Not inside a Git repository." >&2
    exit 1
fi

REPO_ROOT="$(git rev-parse --show-toplevel)"
cd "$REPO_ROOT"

current_branch() {
    git rev-parse --abbrev-ref HEAD 2>/dev/null || echo "HEAD"
}

print_auth_diagnostics() {
    local has_github_token="no"
    local has_gh_token="no"
    local has_git_askpass="no"
    local has_gh="no"
    local has_ssh_dir="no"
    local cred_helpers=""

    if [ -n "${GITHUB_TOKEN:-}" ]; then
        has_github_token="yes"
    fi
    if [ -n "${GH_TOKEN:-}" ]; then
        has_gh_token="yes"
    fi
    if [ -n "${GIT_ASKPASS:-}" ] || [ -n "${SSH_ASKPASS:-}" ]; then
        has_git_askpass="yes"
    fi
    if command -v gh >/dev/null 2>&1; then
        has_gh="yes"
    fi
    if [ -d "${HOME}/.ssh" ]; then
        has_ssh_dir="yes"
    fi

    cred_helpers="$(git config --get-all credential.helper || true)"
    if [ -z "$cred_helpers" ]; then
        cred_helpers="(none)"
    fi

    echo "Shell Git diagnostics:"
    echo "  repo root          : $REPO_ROOT"
    echo "  current branch     : $(current_branch)"
    echo "  remote name        : $REMOTE_NAME"
    if git remote get-url "$REMOTE_NAME" >/dev/null 2>&1; then
        echo "  remote url         : $(git remote get-url "$REMOTE_NAME")"
    else
        echo "  remote url         : (missing)"
    fi
    echo "  credential.helper  : $cred_helpers"
    echo "  GITHUB_TOKEN set   : $has_github_token"
    echo "  GH_TOKEN set       : $has_gh_token"
    echo "  GIT/SSH_ASKPASS    : $has_git_askpass"
    echo "  gh installed       : $has_gh"
    echo "  ~/.ssh present     : $has_ssh_dir"
    echo ""
    echo "Note: Codex/UI repo access can exist even when shell Git has no credentials."
}

ensure_remote_url() {
    if [ -n "$REPO_URL" ]; then
        if git remote get-url "$REMOTE_NAME" >/dev/null 2>&1; then
            git remote set-url "$REMOTE_NAME" "$REPO_URL"
        else
            git remote add "$REMOTE_NAME" "$REPO_URL"
        fi
    fi
}

ensure_clean_if_needed() {
    if [ "$ALLOW_DIRTY" -eq 1 ]; then
        return 0
    fi
    if ! git diff --quiet || ! git diff --cached --quiet; then
        echo "ERROR: Working tree has uncommitted changes." >&2
        echo "Re-run with --allow-dirty if you intentionally want to switch anyway." >&2
        exit 1
    fi
}

preserve_current_branch_if_switching() {
    local current_branch_name="$1"
    if [ "$current_branch_name" = "$TARGET_BRANCH" ] || [ "$current_branch_name" = "HEAD" ]; then
        return 0
    fi
    local backup_branch="preserve/${current_branch_name}-$(date +%Y%m%d-%H%M%S)"
    git branch "$backup_branch" "$current_branch_name"
    echo "Preserved current branch as $backup_branch"
}

verify_remote_access() {
    local remote_url
    if ! remote_url="$(git remote get-url "$REMOTE_NAME" 2>/dev/null)"; then
        echo "ERROR: Remote \"$REMOTE_NAME\" is not configured." >&2
        echo "Pass --repo-url <url> or configure the remote before running this script." >&2
        exit 1
    fi

    if ! git ls-remote "$remote_url" "refs/heads/$TARGET_BRANCH" >/dev/null 2>&1; then
        echo "ERROR: Shell Git could not access $remote_url or branch $TARGET_BRANCH." >&2
        echo ""
        print_auth_diagnostics
        echo ""
        echo "Fix one of these before retrying:"
        echo "  - configure an HTTPS credential helper or GITHUB_TOKEN/GH_TOKEN"
        echo "  - install/authenticate gh and run: gh auth setup-git"
        echo "  - provide SSH credentials and network access to github.com:22"
        exit 2
    fi
}

echo "=== Bootstrap Private Repo ==="
ensure_remote_url
print_auth_diagnostics

if [ "$DIAGNOSE_ONLY" -eq 1 ]; then
    echo "Diagnose-only mode: no Git state changes made."
    exit 0
fi

verify_remote_access

echo "Fetching $REMOTE_NAME..."
git fetch "$REMOTE_NAME" --prune

CURRENT_BRANCH="$(current_branch)"
ensure_clean_if_needed
preserve_current_branch_if_switching "$CURRENT_BRANCH"

echo "Checking out $TARGET_BRANCH from $REMOTE_NAME/$TARGET_BRANCH..."
git checkout -B "$TARGET_BRANCH" "$REMOTE_NAME/$TARGET_BRANCH"
git branch --set-upstream-to="$REMOTE_NAME/$TARGET_BRANCH" "$TARGET_BRANCH"

echo ""
echo "Final branch state:"
git status --short --branch
