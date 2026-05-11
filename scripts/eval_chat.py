"""
eval_chat.py — Automated evaluation of the Birdology Graph-RAG chat system.

Sends a battery of test questions to the running web server, checks tool routing,
forbidden patterns, and optionally uses Claude Haiku as an LLM judge.

Usage:
    # Server must be running on localhost:5000
    uv run python scripts/eval_chat.py
    uv run python scripts/eval_chat.py --judge          # enable LLM-as-judge (needs ANTHROPIC_API_KEY)
    uv run python scripts/eval_chat.py --url http://localhost:8080
    uv run python scripts/eval_chat.py --output eval_results.json
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from dataclasses import dataclass, field
from typing import Optional

import requests

# ---------------------------------------------------------------------------
# Test case definitions
# ---------------------------------------------------------------------------

@dataclass
class TestCase:
    id: str
    category: str
    question: str
    expected_tools: list[str] = field(default_factory=list)   # at least one of these must be called
    forbidden_tools: list[str] = field(default_factory=list)  # none of these should be called
    must_contain: list[str] = field(default_factory=list)     # substrings that must appear
    must_not_contain: list[str] = field(default_factory=list) # substrings that must NOT appear
    notes: str = ""                                            # human notes for the judge


TESTS: list[TestCase] = [
    # --- Species description ---
    TestCase(
        id="species_01",
        category="species",
        question="Parle-moi du Rouge-gorge familier",
        expected_tools=["find_species", "search_wikipedia"],
        must_not_contain=["Observation récente"],
        notes="find_species must be called before search_wikipedia; Wikipedia query should use 'European Robin'",
    ),
    TestCase(
        id="species_02",
        category="species",
        question="C'est quoi un Balbuzard pêcheur ?",
        expected_tools=["find_species", "search_wikipedia"],
        notes="Wikipedia query should use 'Osprey', not 'Balbuzard pêcheur'",
    ),
    TestCase(
        id="species_03",
        category="species",
        question="Qu'est-ce que mange le Pic épeiche ?",
        expected_tools=["find_species", "search_wikipedia"],
        notes="Diet question → search_wikipedia with English name (Great Spotted Woodpecker)",
    ),
    TestCase(
        id="species_04",
        category="species",
        question="Parle-moi de la Mouette rieuse",
        expected_tools=["find_species", "search_wikipedia"],
        must_not_contain=["Laughing Gull"],
        notes="Wikipedia query must use 'Black-headed Gull', not literal translation 'Laughing Gull'",
    ),

    # --- Recent observations ---
    TestCase(
        id="obs_01",
        category="observations",
        question="Quelles observations récentes au Danemark ?",
        expected_tools=["recent_observations"],
        notes="Should use source=all by default",
    ),
    TestCase(
        id="obs_02",
        category="observations",
        question="A-t-on vu des Cigognes blanches récemment ?",
        expected_tools=["find_species", "recent_observations"],
        notes="find_species first to get scientific name, then recent_observations",
    ),
    TestCase(
        id="obs_03",
        category="observations",
        question="Observations à Utterslev Mose ce mois-ci",
        expected_tools=["recent_observations"],
        notes="locality=Utterslev Mose, date_from=first day of current month",
    ),
    TestCase(
        id="obs_04",
        category="observations",
        question="Quels oiseaux ont été observés à Skagen en avril ?",
        expected_tools=["recent_observations"],
        notes="locality=Skagen, date_from=YYYY-04-01, date_to=YYYY-04-30",
    ),

    # --- Live / current ---
    TestCase(
        id="live_01",
        category="live",
        question="Qu'est-ce qu'on peut voir en ce moment au Danemark ?",
        expected_tools=["compare_seasonal"],
        forbidden_tools=["live_observations"],
        notes="'en ce moment' → compare_seasonal, not live_observations",
    ),
    TestCase(
        id="live_02",
        category="live",
        question="Y a-t-il des raretés signalées cette semaine ?",
        expected_tools=["compare_seasonal"],
        notes="Should use compare_seasonal to detect unexpected species",
    ),

    # --- Where to watch ---
    TestCase(
        id="where_01",
        category="where_to_watch",
        question="Où observer des oiseaux ce week-end près de Copenhague ?",
        expected_tools=["where_to_watch"],
        must_not_contain=["Observation récente :"],
        notes="Should only list locations from tool results, not from memory",
    ),
    TestCase(
        id="where_02",
        category="where_to_watch",
        question="Meilleurs endroits pour voir des canards autour d'Aarhus",
        expected_tools=["where_to_watch"],
        notes="Should pass Aarhus coordinates, not default to Copenhagen",
    ),
    TestCase(
        id="where_03",
        category="where_to_watch",
        question="Où observer des espèces rares près de Copenhague ?",
        expected_tools=["nearby_birds"],
        forbidden_tools=["live_observations"],
        notes="'espèces rares' → nearby_birds (IUCN sort), not live_observations",
    ),
    TestCase(
        id="where_04",
        category="where_to_watch",
        question="Où observer des oiseaux près de chez moi ?",
        expected_tools=[],
        must_contain=["où", "location", "localisation", "ville", "coordonnées",
                      "où êtes-vous", "quelle ville", "quel endroit"],
        notes="No location given → must ask user for location, not default to Copenhagen",
    ),

    # --- Seasonal ---
    TestCase(
        id="seasonal_01",
        category="seasonal",
        question="Quels oiseaux voit-on en mars au Danemark ?",
        expected_tools=["observations_by_month"],
        notes="Month question → observations_by_month(month=3)",
    ),
    TestCase(
        id="seasonal_02",
        category="seasonal",
        question="Quelles espèces sont présentes seulement en hiver ?",
        expected_tools=["observations_by_month"],
        notes="Winter → months 12, 1, 2",
    ),

    # --- Taxonomy ---
    TestCase(
        id="taxonomy_01",
        category="taxonomy",
        question="Combien d'espèces sont dans le graphe ?",
        expected_tools=["taxonomy_summary"],
        notes="Simple stats query",
    ),
    TestCase(
        id="taxonomy_02",
        category="taxonomy",
        question="Quelles espèces appartiennent à la famille des Turdidae ?",
        expected_tools=["species_by_family"],
        notes="Family query → species_by_family",
    ),

    # --- Edge cases / traps ---
    TestCase(
        id="edge_01",
        category="edge_cases",
        question="Observations près de chez moi",
        must_contain=["où", "location", "ville", "endroit", "coordonnées",
                      "où êtes-vous", "quelle ville"],
        notes="No location → must ask, not default to Copenhagen or Assistens Kirkegård",
    ),
    TestCase(
        id="edge_02",
        category="edge_cases",
        question="Parle-moi du Condor de Californie",
        expected_tools=["find_species"],
        notes="Not in the graph → should say so, not hallucinate Danish observations",
    ),
    TestCase(
        id="edge_03",
        category="edge_cases",
        question="Quels oiseaux voit-on à Vejle ce mois-ci ?",
        expected_tools=["recent_observations"],
        notes="City with possibly sparse data → should report honestly if no results",
    ),
]

# ---------------------------------------------------------------------------
# Forbidden patterns (server-side strips should have removed these)
# ---------------------------------------------------------------------------

FORBIDDEN_PATTERNS = [
    (r"[\U0001F300-\U0001F9FF\U00002702-\U000027B0]", "emoji present"),
    (r"Observation[s]?\s+r[eé]cente?s?\s*:", "hallucinated 'Observation récente' line"),
    (r"\*{0,2}(?:Remarque|Remarque finale|À noter|À surveiller)\*{0,2}", "forbidden padding section"),
    (r"\*{0,2}(?:Conseils?|À\s+éviter|Équipement)\*{0,2}\s*:", "forbidden advice section"),
    (r"Si vous (souhaitez|voulez)[^\n]{0,80}(approfondir|détails)", "trailing offer sentence"),
    (r"N'hésitez pas[^\n]{0,80}", "trailing offer sentence"),
]

# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------

@dataclass
class TestResult:
    test: TestCase
    passed: bool
    tool_calls: list[str]
    answer: str
    failures: list[str]
    judge_verdict: Optional[str] = None
    judge_score: Optional[int] = None   # 1-5


# ---------------------------------------------------------------------------
# Core runner
# ---------------------------------------------------------------------------

def run_test(test: TestCase, base_url: str) -> TestResult:
    failures: list[str] = []
    tool_calls: list[str] = []
    answer = ""

    try:
        resp = requests.post(
            f"{base_url}/api/chat",
            json={"question": test.question, "session_id": f"eval_{test.id}"},
            timeout=60,
        )
        resp.raise_for_status()
        data = resp.json()
        answer = data.get("answer", "")
        tool_calls = [tc["name"] for tc in data.get("tool_calls", [])]
    except Exception as e:
        failures.append(f"HTTP error: {e}")
        return TestResult(test=test, passed=False, tool_calls=[], answer="", failures=failures)

    # 1. Check expected tools
    if test.expected_tools:
        if not any(t in tool_calls for t in test.expected_tools):
            failures.append(
                f"Expected one of {test.expected_tools}, got {tool_calls}"
            )

    # 2. Check forbidden tools
    for t in test.forbidden_tools:
        if t in tool_calls:
            failures.append(f"Forbidden tool called: {t}")

    # 3. Check must_contain
    answer_lower = answer.lower()
    for phrase in test.must_contain:
        if phrase.lower() not in answer_lower:
            failures.append(f"Expected phrase not found: '{phrase}'")

    # 4. Check must_not_contain
    for phrase in test.must_not_contain:
        if phrase.lower() in answer_lower:
            failures.append(f"Forbidden phrase found: '{phrase}'")

    # 5. Global forbidden patterns
    for pattern, label in FORBIDDEN_PATTERNS:
        if re.search(pattern, answer, re.IGNORECASE):
            failures.append(f"Forbidden pattern ({label})")

    return TestResult(
        test=test,
        passed=len(failures) == 0,
        tool_calls=tool_calls,
        answer=answer,
        failures=failures,
    )


# ---------------------------------------------------------------------------
# LLM-as-judge (Claude Haiku)
# ---------------------------------------------------------------------------

JUDGE_SYSTEM = """\
You are evaluating a birdwatching chatbot response. Given the question, tool calls made, \
and the response, identify any issues. Score 1-5 (5=perfect) and briefly list problems found.

Issues to look for:
- Locations mentioned that were NOT returned by tool calls (hallucination)
- Species mentioned that were NOT returned by tool calls (hallucination)
- Wrong tool order (find_species must come before search_wikipedia)
- Wikipedia query in French instead of English
- Padding sections (Conseils, Équipement, Remarque, etc.)
- Offer sentences (Si vous souhaitez, N'hésitez pas, etc.)
- Defaulting to Copenhagen when no location was given
- Emojis

Respond in JSON: {"score": 1-5, "issues": ["issue1", "issue2"], "summary": "one sentence"}
"""

def judge_response(test: TestCase, result: TestResult, api_key: str) -> tuple[int, str]:
    try:
        import anthropic
        client = anthropic.Anthropic(api_key=api_key)
        content = f"""Question: {test.question}
Notes: {test.notes}
Tool calls made: {result.tool_calls}
Response:
{result.answer[:2000]}"""

        msg = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=300,
            system=JUDGE_SYSTEM,
            messages=[{"role": "user", "content": content}],
        )
        raw = msg.content[0].text.strip()
        # Extract JSON even if wrapped in markdown
        m = re.search(r'\{.*\}', raw, re.DOTALL)
        if m:
            parsed = json.loads(m.group(0))
            return parsed.get("score", 0), parsed.get("summary", "") + " | " + "; ".join(parsed.get("issues", []))
    except Exception as e:
        return 0, f"Judge error: {e}"
    return 0, "Could not parse judge response"


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------

def print_report(results: list[TestResult], use_judge: bool) -> None:
    passed = sum(1 for r in results if r.passed)
    total = len(results)
    print(f"\n{'='*60}")
    print(f"BIRDOLOGY EVAL — {passed}/{total} passed")
    print(f"{'='*60}\n")

    by_category: dict[str, list[TestResult]] = {}
    for r in results:
        by_category.setdefault(r.test.category, []).append(r)

    for cat, cat_results in by_category.items():
        cat_pass = sum(1 for r in cat_results if r.passed)
        print(f"  [{cat}] {cat_pass}/{len(cat_results)}")
        for r in cat_results:
            icon = "✓" if r.passed else "✗"
            tools_str = ", ".join(r.tool_calls) if r.tool_calls else "none"
            print(f"    {icon} {r.test.id}: {r.test.question[:60]}")
            print(f"         tools: {tools_str}")
            for f in r.failures:
                print(f"         FAIL: {f}")
            if use_judge and r.judge_verdict:
                score_str = f"[{r.judge_score}/5]" if r.judge_score else ""
                print(f"         JUDGE {score_str}: {r.judge_verdict}")
        print()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(description="Evaluate Birdology chat system")
    ap.add_argument("--url", default="http://localhost:5000", help="Server base URL")
    ap.add_argument("--judge", action="store_true", help="Enable LLM-as-judge via Claude Haiku")
    ap.add_argument("--output", help="Save results to JSON file")
    ap.add_argument("--category", help="Only run tests in this category")
    ap.add_argument("--id", help="Only run test with this id")
    ap.add_argument("--delay", type=float, default=1.5, help="Seconds between requests")
    args = ap.parse_args()

    anthropic_key = os.getenv("ANTHROPIC_API_KEY", "") if args.judge else ""
    if args.judge and not anthropic_key:
        print("WARNING: --judge requires ANTHROPIC_API_KEY — judge disabled")
        args.judge = False

    tests = TESTS
    if args.category:
        tests = [t for t in tests if t.category == args.category]
    if args.id:
        tests = [t for t in tests if t.id == args.id]

    # Check server is up
    try:
        requests.get(args.url, timeout=5)
    except Exception:
        print(f"ERROR: server not reachable at {args.url}")
        sys.exit(1)

    print(f"Running {len(tests)} tests against {args.url}...")
    results: list[TestResult] = []

    for i, test in enumerate(tests, 1):
        print(f"  [{i}/{len(tests)}] {test.id}: {test.question[:55]}...", end=" ", flush=True)
        result = run_test(test, args.url)

        if args.judge and anthropic_key:
            score, verdict = judge_response(test, result, anthropic_key)
            result.judge_score = score
            result.judge_verdict = verdict

        results.append(result)
        status = "PASS" if result.passed else f"FAIL ({len(result.failures)} issues)"
        print(status)

        if i < len(tests):
            time.sleep(args.delay)

    print_report(results, args.judge)

    if args.output:
        with open(args.output, "w") as f:
            json.dump(
                [
                    {
                        "id": r.test.id,
                        "category": r.test.category,
                        "question": r.test.question,
                        "passed": r.passed,
                        "tool_calls": r.tool_calls,
                        "failures": r.failures,
                        "judge_score": r.judge_score,
                        "judge_verdict": r.judge_verdict,
                    }
                    for r in results
                ],
                f,
                ensure_ascii=False,
                indent=2,
            )
        print(f"\nResults saved to {args.output}")


if __name__ == "__main__":
    main()
