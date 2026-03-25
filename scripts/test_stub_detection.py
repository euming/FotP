"""Unit tests for _is_stub_node() JIT semantic stub detection.

Tests the stub-detection logic independently of the full orchestrator runtime.
The implementation under test is in run-swarm-plan.py (Orchestrator._is_stub_node).

Run with:
    python -m pytest scripts/test_stub_detection.py -v
or:
    python scripts/test_stub_detection.py
"""

import unittest

# ---------------------------------------------------------------------------
# Reference implementation — mirrors Orchestrator._is_stub_node exactly.
# Keep in sync with run-swarm-plan.py when updating the heuristic.
# ---------------------------------------------------------------------------

_STUB_SEMANTIC_PREFIXES: tuple[str, ...] = (
    "build a",
    "build an",
    "implement a",
    "implement an",
    "create a",
    "create an",
    "design a",
    "design an",
    "analyze and fix",
    "analyse and fix",
    "investigate and fix",
    "add a system",
    "build a system",
    "develop a",
    "develop an",
    "write a system",
    "create a command that",
    "build a command that",
    "implement a command that",
)


def _is_stub(node: dict) -> bool:
    """Mirror of Orchestrator._is_stub_node for unit-test isolation."""
    already_flagged = node.get("may_decompose", "false") == "true"
    if already_flagged:
        return False

    title = (node.get("title") or "").strip()
    obs_count = int(node.get("observations_count") or "0")
    description = (node.get("description") or "").strip()

    # Structural heuristic: short title + zero observations + blank/missing description.
    # Any non-trivial description (> 10 chars) means this is a concrete task, not a placeholder.
    if len(title) < 60 and obs_count == 0 and len(description) <= 10:
        return True

    # Semantic heuristic: open-ended goal phrases in description
    for prefix in _STUB_SEMANTIC_PREFIXES:
        if description.lower().startswith(prefix):
            return True

    return False


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestIsStubNode(unittest.TestCase):

    # ── 1. Structural detection ──────────────────────────────────────────────

    def test_structural_short_title_no_observations(self) -> None:
        """Short title + zero observations → structural stub."""
        node = {"title": "my-feature", "observations_count": "0"}
        self.assertTrue(_is_stub(node))

    def test_structural_short_title_with_observations_not_stub(self) -> None:
        """Short title but WITH observations + concrete desc → not a stub."""
        node = {
            "title": "my-feature",
            "observations_count": "3",
            "description": "Fix the login form validation.",
        }
        self.assertFalse(_is_stub(node))

    def test_structural_long_title_concrete_desc_not_stub(self) -> None:
        """Long title, no obs, concrete description → not a structural stub."""
        node = {
            "title": "fix-login-form-validation-for-empty-password-edge-case",
            "observations_count": "0",
            "description": "Fix the login form so empty passwords return a 400 error.",
        }
        self.assertFalse(_is_stub(node))

    # ── 2. Semantic detection ────────────────────────────────────────────────

    def test_semantic_build_a_flagged(self) -> None:
        """'Build a ...' description flags node as stub regardless of title length."""
        node = {
            "title": "orchestrator-self-healing-resilience-layer-for-swarm-plan",
            "observations_count": "0",
            "description": "Build a self-healing resilience layer for the swarm-plan orchestrator.",
        }
        self.assertTrue(_is_stub(node))

    def test_semantic_implement_a_flagged(self) -> None:
        """'Implement a ...' description with long title + observations → still flagged."""
        node = {
            "title": "some-long-title-that-exceeds-the-short-threshold-by-quite-a-lot",
            "observations_count": "5",
            "description": "Implement a batch verification command that converts sessions.",
        }
        self.assertTrue(_is_stub(node))

    def test_semantic_create_a_command_that_flagged(self) -> None:
        """'Create a command that ...' → flagged."""
        node = {
            "title": "batch-verify-command-very-long-title-to-bypass-structural-heuristic",
            "observations_count": "2",
            "description": "Create a command that spawns parallel subagents per completed node.",
        }
        self.assertTrue(_is_stub(node))

    def test_semantic_analyze_and_fix_flagged(self) -> None:
        """'Analyze and fix ...' → flagged."""
        node = {
            "title": "fix-factory-contamination-across-all-code-paths-in-the-project",
            "observations_count": "0",
            "description": "Analyze and fix all write paths that contaminate the factory DB.",
        }
        self.assertTrue(_is_stub(node))

    def test_semantic_design_a_system_flagged(self) -> None:
        """'Design a system ...' → flagged."""
        node = {
            "title": "some-architectural-node-with-a-very-long-and-descriptive-title",
            "observations_count": "1",
            "description": "Design a system that routes memory queries through a semantic index.",
        }
        self.assertTrue(_is_stub(node))

    # ── 3. Explicit may_decompose override ───────────────────────────────────

    def test_explicit_may_decompose_returns_false(self) -> None:
        """may_decompose=true nodes are handled at level-1; _is_stub_node must return False
        so they are not double-counted as level-3 stubs."""
        node = {
            "title": "short",
            "observations_count": "0",
            "may_decompose": "true",
            "description": "Build a thing.",
        }
        self.assertFalse(_is_stub(node))

    # ── 4. Concrete leaf tasks: no false positives ───────────────────────────

    def test_concrete_leaf_task_not_falsely_flagged(self) -> None:
        """Concrete task with specific instruction should not be flagged."""
        node = {
            "title": "update-cargo-lock-after-bumping-serde-version-to-1-0-200",
            "observations_count": "0",
            "description": "Run cargo update -p serde --precise 1.0.200 and commit the lock file.",
        }
        self.assertFalse(_is_stub(node))

    def test_concrete_fix_task_not_flagged(self) -> None:
        """Specific fix with file+line reference should not be flagged."""
        node = {
            "title": "fix-off-by-one-in-token-window-slice-for-long-prompts",
            "observations_count": "0",
            "description": "Fix the off-by-one error at line 342 of tokenizer.py causing truncation.",
        }
        self.assertFalse(_is_stub(node))

    # ── Edge cases ───────────────────────────────────────────────────────────

    def test_empty_description_structural_fallback(self) -> None:
        """None description + short title → structural stub still fires."""
        node = {"title": "short", "observations_count": "0", "description": None}
        self.assertTrue(_is_stub(node))

    def test_long_title_with_obs_and_empty_desc_not_flagged(self) -> None:
        """Long title + observations + empty description → clean node."""
        node = {
            "title": "a-very-long-task-title-that-clearly-exceeds-sixty-characters-in-total",
            "observations_count": "1",
            "description": "",
        }
        self.assertFalse(_is_stub(node))

    def test_missing_obs_count_treated_as_zero(self) -> None:
        """Missing observations_count key is treated as 0 (structural check applies)."""
        node = {"title": "short"}
        self.assertTrue(_is_stub(node))


if __name__ == "__main__":
    unittest.main()
