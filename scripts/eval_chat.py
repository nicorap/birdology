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
    expected_tools: list[str] = field(default_factory=list)     # at least one of these must be called
    forbidden_tools: list[str] = field(default_factory=list)    # none of these should be called
    must_contain: list[str] = field(default_factory=list)       # ALL of these substrings must appear
    must_contain_any: list[str] = field(default_factory=list)   # AT LEAST ONE must appear (OR)
    must_not_contain: list[str] = field(default_factory=list)   # substrings that must NOT appear
    notes: str = ""                                              # human notes for the judge


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
        notes="Should call recent_observations (source='all' by default)",
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
        expected_tools=["recent_observations", "observations_by_month"],
        forbidden_tools=["compare_seasonal"],
        notes="'peut voir en ce moment' → recent_observations(source='live') + observations_by_month (possibility, not comparison)",
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
        notes="'espèces rares près de Copenhague' → nearby_birds (IUCN sort, historical graph data)",
    ),
    TestCase(
        id="where_04",
        category="where_to_watch",
        question="Où observer des oiseaux près de chez moi ?",
        forbidden_tools=["where_to_watch", "nearby_birds", "recent_observations"],
        must_contain_any=["localisation", "location", "où vous êtes", "où êtes-vous",
                          "quelle ville", "quel endroit", "coordonnées", "dites-moi où"],
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
        forbidden_tools=["recent_observations", "where_to_watch"],
        must_contain_any=["localisation", "location", "où vous êtes", "où êtes-vous",
                          "quelle ville", "quel endroit", "coordonnées", "dites-moi où"],
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
        expected_tools=["recent_observations", "observations_by_month", "where_to_watch"],
        notes="City query + current month → recent_observations or observations_by_month both valid",
    ),
]

# ---------------------------------------------------------------------------
# Robustness tests — same intents, different phrasings
# ---------------------------------------------------------------------------

ROBUSTNESS_TESTS: list[TestCase] = [
    # Species — alternate phrasings
    TestCase(
        id="r_species_01",
        category="robustness",
        question="Que sais-tu sur le Rouge-gorge ?",
        expected_tools=["find_species", "search_wikipedia"],
        notes="Short name without 'familier' — same routing as species_01",
    ),
    TestCase(
        id="r_species_02",
        category="robustness",
        question="Info sur le Balbuzard",
        expected_tools=["find_species"],
        notes="Informal 'info sur' phrasing — should still call find_species",
    ),
    TestCase(
        id="r_species_03",
        category="robustness",
        question="Comment se nourrit le Pic épeiche ?",
        expected_tools=["find_species", "search_wikipedia"],
        notes="Diet question, different phrasing — same routing as species_03",
    ),
    TestCase(
        id="r_species_04",
        category="robustness",
        question="Mouette rieuse — habitat et comportement",
        expected_tools=["find_species", "search_wikipedia"],
        must_not_contain=["Laughing Gull"],
        notes="Dash-separated topic — same routing as species_04, no Laughing Gull",
    ),

    # Observations — alternate phrasings
    TestCase(
        id="r_obs_01",
        category="robustness",
        question="Quoi de neuf dans les observations au Danemark ?",
        expected_tools=["recent_observations"],
        notes="Informal 'quoi de neuf' — should call recent_observations",
    ),
    TestCase(
        id="r_obs_02",
        category="robustness",
        question="Est-ce qu'on a vu des Cigognes cette année ?",
        expected_tools=["find_species", "recent_observations"],
        notes="Implicit time ('cette année') — should filter by current year",
    ),
    TestCase(
        id="r_obs_03",
        category="robustness",
        question="Ce qu'on a vu à Utterslev Mose en mai",
        expected_tools=["recent_observations"],
        notes="Implicit current month — locality + month filter",
    ),

    # Live / current — alternate phrasings
    TestCase(
        id="r_live_01",
        category="robustness",
        question="Qu'est-ce qu'il y a à voir en ce moment ?",
        expected_tools=["recent_observations", "observations_by_month"],
        forbidden_tools=["compare_seasonal"],
        notes="'il y a à voir en ce moment' = possibility → recent_observations(source='live') + observations_by_month",
    ),
    TestCase(
        id="r_live_02",
        category="robustness",
        question="Des espèces inhabituelles cette semaine ?",
        expected_tools=["compare_seasonal"],
        notes="'Inhabituelles cette semaine' → compare_seasonal",
    ),

    # Where to watch — alternate phrasings
    TestCase(
        id="r_where_01",
        category="robustness",
        question="Bons spots pour observer ce samedi près de Copenhague ?",
        expected_tools=["where_to_watch"],
        must_not_contain=["Observation récente :"],
        notes="'spots' and 'samedi' — same routing as where_01",
    ),
    TestCase(
        id="r_where_02",
        category="robustness",
        question="Je veux voir des canards, où aller autour d'Aarhus ?",
        expected_tools=["where_to_watch"],
        notes="Species goal + location — should pass Aarhus coordinates",
    ),
    TestCase(
        id="r_where_03",
        category="robustness",
        question="Oiseaux menacés autour de Copenhague ?",
        expected_tools=["nearby_birds"],
        notes="'menacés' → nearby_birds (IUCN sort, historical graph data), same as where_03",
    ),
    TestCase(
        id="r_where_04",
        category="robustness",
        question="Je suis chez moi, où observer des oiseaux ?",
        forbidden_tools=["where_to_watch", "nearby_birds", "recent_observations"],
        must_contain_any=["localisation", "location", "où vous êtes", "où êtes-vous",
                          "quelle ville", "quel endroit", "coordonnées", "dites-moi où"],
        notes="'chez moi' without location — must ask, same as where_04",
    ),

    # Seasonal — alternate phrasings
    TestCase(
        id="r_seasonal_01",
        category="robustness",
        question="Oiseaux de mars au Danemark ?",
        expected_tools=["observations_by_month"],
        notes="Telegraphic phrasing — same as seasonal_01",
    ),
    TestCase(
        id="r_seasonal_02",
        category="robustness",
        question="Quels oiseaux hivernent au Danemark ?",
        expected_tools=["observations_by_month"],
        notes="'hivernent' → winter months (12, 1, 2)",
    ),

    # Edge cases — alternate phrasings
    TestCase(
        id="r_edge_01",
        category="robustness",
        question="Je veux observer depuis chez moi",
        forbidden_tools=["recent_observations", "where_to_watch"],
        must_contain_any=["localisation", "location", "où vous êtes", "où êtes-vous",
                          "quelle ville", "quel endroit", "coordonnées", "dites-moi où"],
        notes="No location — must ask, same as edge_01",
    ),
    TestCase(
        id="r_edge_02",
        category="robustness",
        question="Parle-moi du Gypaète barbu",
        expected_tools=["find_species"],
        notes="Rare/absent species — different species than edge_02, same pattern",
    ),
    TestCase(
        id="r_edge_03",
        category="robustness",
        question="Qu'est-ce qu'on observe à Vejle en ce moment ?",
        expected_tools=["recent_observations", "observations_by_month", "where_to_watch",
                        "compare_seasonal"],
        notes="'en ce moment' + city — recent_observations or observations_by_month both valid",
    ),
]

# ---------------------------------------------------------------------------
# Forbidden patterns (server-side strips should have removed these)
# ---------------------------------------------------------------------------

FORBIDDEN_PATTERNS = [
    (r"[\U0001F300-\U0001F9FF\U00002702-\U000027B0]", "emoji present"),
    # Must be on its own line (not inside a table header like | Remarque |)
    (r"(?:^|\n)[ \t]*\*{0,2}(?:Remarque|Remarque finale|À noter|À surveiller)\*{0,2}[ \t]*(?:\n|$)",
     "forbidden padding section"),
    (r"(?:^|\n)[ \t]*\*{0,2}(?:Conseils?|À\s+éviter|Équipement)\*{0,2}[ \t]*:",
     "forbidden advice section"),
    (r"(?:^|\n)[ \t]*Observation[s]?\s+r[eé]cente?s?\s*:[^\n]+", "hallucinated 'Observation récente' line"),
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


@dataclass
class Turn:
    """One question in a conversation. Every assertion field is optional: a turn
    that only exists to set up context asserts nothing."""
    question: str
    expected_tools: list[str] = field(default_factory=list)
    forbidden_tools: list[str] = field(default_factory=list)
    must_contain: list[str] = field(default_factory=list)
    must_contain_any: list[str] = field(default_factory=list)
    must_not_contain: list[str] = field(default_factory=list)
    expected_tool_args: dict = field(default_factory=dict)
    notes: str = ""


@dataclass
class Conversation:
    id: str
    category: str
    turns: list[Turn]


@dataclass
class TurnResult:
    index: int              # 1-based, so failures read as "turn 3"
    turn: Turn
    passed: bool
    tool_calls: list[str]
    answer: str
    failures: list[str]
    tool_call_details: list[dict] = field(default_factory=list)


@dataclass
class ConversationResult:
    conversation: Conversation
    turns: list[TurnResult]

    @property
    def passed(self) -> bool:
        return all(t.passed for t in self.turns)


# ---------------------------------------------------------------------------
# Multi-turn conversations
#
# Every bug found on 2026-07-13 was multi-turn, and the single-turn suite scored
# 21/21 throughout. These cases run all turns against one session so the model
# sees its own history — which is where it actually breaks.
# ---------------------------------------------------------------------------

CONVERSATIONS: list[Conversation] = [
    # --- Regressions: these bugs really happened ---
    Conversation(
        id="conv_photo_after_nearby",
        category="conversation_regression",
        turns=[
            Turn("Y a-t-il des rapaces près de Nørrebro ?",
                 expected_tools=["nearby_birds", "where_to_watch"]),
            Turn("montre moi une photo !",
                 expected_tools=["find_species"],
                 must_contain_any=["!["],
                 notes="nearby_birds used to return no thumbnail, so the model claimed the "
                       "species had no photo (false) instead of calling find_species."),
        ],
    ),
    Conversation(
        id="conv_pronoun_where_seen",
        category="conversation_regression",
        turns=[
            Turn("Parle-moi du Chardonneret élégant",
                 expected_tools=["find_species"]),
            Turn("ou a t il ete vu la dernière fois ?",
                 expected_tools=["where_seen"],
                 must_not_contain=["pas d'information", "pas cette information"],
                 notes="Pronoun follow-up, deliberately typo'd as the user typed it. "
                       "The species has 425 observations — a refusal here is always a bug."),
        ],
    ),
    Conversation(
        id="conv_refusal_inertia",
        category="conversation_regression",
        turns=[
            Turn("Parle-moi du Condor de Californie",
                 expected_tools=["find_species"]),
            Turn("où a-t-il été vu ?",
                 notes="No Danish observations — a refusal here is CORRECT and expected."),
            Turn("Et le Chardonneret élégant ?",
                 expected_tools=["find_species"]),
            Turn("où a-t-il été vu ?",
                 expected_tools=["where_seen"],
                 must_not_contain=["pas d'information", "pas cette information"],
                 notes="The legitimate refusal two turns ago must NOT be inherited here."),
        ],
    ),
    Conversation(
        id="conv_no_invented_species",
        category="conversation_regression",
        turns=[
            Turn("Parle-moi de l'Épervier d'Europe",
                 expected_tools=["find_species"]),
            Turn("montre moi une photo !",
                 expected_tools=["find_species"],
                 must_contain_any=["!["]),
            Turn("il n'y a pas de photo !",
                 must_not_contain=["albicillatus"],
                 notes="Under pressure the model invented 'Haliaeetus albicillatus' "
                       "(the real taxon is Haliaeetus albicilla) to have something to offer."),
            Turn("montre moi une photo d'un autre rapace alors !",
                 must_not_contain=["albicillatus"]),
        ],
    ),

    # --- Robustness: general multi-turn behaviour ---
    Conversation(
        id="conv_location_carryover",
        category="conversation_robustness",
        turns=[
            Turn("Où observer des oiseaux près de Copenhague ?",
                 expected_tools=["where_to_watch", "nearby_birds"]),
            Turn("et à Aarhus ?",
                 expected_tools=["where_to_watch", "nearby_birds"],
                 expected_tool_args={"lat": 56.16, "lon": 10.20},
                 notes="Must re-run the tool for the NEW location, not reuse Copenhagen."),
        ],
    ),
    Conversation(
        id="conv_month_carryover",
        category="conversation_robustness",
        turns=[
            Turn("Quels oiseaux voit-on en mars au Danemark ?",
                 expected_tools=["observations_by_month"]),
            Turn("et en juin ?",
                 expected_tools=["observations_by_month"],
                 expected_tool_args={"month": 6},
                 notes="Must re-run for June, not answer from the March result."),
        ],
    ),
    Conversation(
        id="conv_topic_switch_and_back",
        category="conversation_robustness",
        turns=[
            Turn("Parle-moi du Rouge-gorge familier",
                 expected_tools=["find_species"]),
            Turn("Quels oiseaux voit-on en mars au Danemark ?",
                 expected_tools=["observations_by_month"]),
            Turn("Revenons au premier oiseau : est-il menacé ?",
                 expected_tools=["find_species"],
                 expected_tool_args={"name": "Rouge-gorge"},
                 notes="Must resolve 'le premier oiseau' back to the Robin."),
        ],
    ),
    Conversation(
        id="conv_user_correction",
        category="conversation_robustness",
        turns=[
            Turn("Parle-moi du Rouge-gorge familier",
                 expected_tools=["find_species"]),
            Turn("non, je voulais dire le Rouge-queue noir",
                 expected_tools=["find_species"],
                 expected_tool_args={"name": "Rouge-queue"},
                 notes="Must look up the corrected species, not keep answering about the Robin."),
        ],
    ),
]


# ---------------------------------------------------------------------------
# Core runner
# ---------------------------------------------------------------------------

# A coordinate this far from the pinned value is the same place, reported with more
# decimals (Aarhus 56.16 vs a model's 56.1629). Anything further is a different place.
# It has to be small: at 0.5 the check was useless for latitude, where Aarhus (56.16)
# and Copenhagen (55.69) are only 0.47 apart — a wrong-city answer sailed through and
# only the longitude could ever fail the assertion.
_COORD_TOLERANCE = 0.05  # ~5.5 km of latitude


def _arg_matches(expected, actual) -> bool:
    """One expected-arg value matches one actual-arg value.

    Expected int (a month, a radius — categorical, not measured) → exact match, so
    `month: 6` rejects 3 and 6.4 alike.
    Expected float (a coordinate) → within _COORD_TOLERANCE, which accepts a model's
    higher-precision 56.1629 for a pinned 56.16 but rejects another city.
    Otherwise → case-insensitive substring match.
    """
    if isinstance(expected, bool):
        return expected is actual
    if isinstance(expected, int):
        try:
            return float(actual) == float(expected)
        except (TypeError, ValueError):
            return False
    if isinstance(expected, float):
        try:
            return abs(float(actual) - float(expected)) <= _COORD_TOLERANCE
        except (TypeError, ValueError):
            return False
    return str(expected).lower() in str(actual).lower()


def _tool_call_matches_expected_args(
    expected_tool_args: dict,
    tool_call_details: list[dict],
    expected_tools: list[str] | None = None,
) -> bool:
    """At least one recorded call TO AN EXPECTED TOOL must match ALL expected args.

    Restricting to expected_tools is the whole point. Several tools share a parameter
    name — `month` belongs to both observations_by_month and where_to_watch, `name` to
    both find_species and search_wikipedia. Scanning every call regardless of its name
    let a turn pass on the wrong one: on conv_month_carryover the model could call
    observations_by_month(month=3) — precisely the regression under test — and still go
    green because it also called where_to_watch(month=6).
    """
    candidates = tool_call_details
    if expected_tools:
        candidates = [c for c in tool_call_details if c.get("name") in expected_tools]
    for call in candidates:
        args = call.get("args", {})
        if all(key in args and _arg_matches(value, args[key]) for key, value in expected_tool_args.items()):
            return True
    return False


def check_answer(spec, tool_calls: list[str], answer: str, tool_call_details: list[dict] = None) -> list[str]:
    """Evaluate one answer against a spec's assertions.

    `spec` is anything exposing expected_tools / forbidden_tools / must_contain /
    must_contain_any / must_not_contain — both TestCase and Turn satisfy this.
    `expected_tool_args` is optional (only Turn defines it; TestCase does not), read
    via getattr so single-turn TestCase callers are unaffected.
    Returns the list of failures; empty means the answer passed.
    """
    # Infrastructure failures are not assertion failures — report them alone.
    if answer.startswith("Erreur serveur:") and "api/embeddings" in answer:
        return ["INFRA: Ollama embeddings crashed (nomic-embed-text unavailable)"]

    failures: list[str] = []

    if spec.expected_tools and not any(t in tool_calls for t in spec.expected_tools):
        failures.append(f"Expected one of {spec.expected_tools}, got {tool_calls}")

    expected_tool_args = getattr(spec, "expected_tool_args", {})
    if expected_tool_args:
        if not _tool_call_matches_expected_args(
            expected_tool_args, tool_call_details or [], spec.expected_tools
        ):
            failures.append(
                f"Expected {spec.expected_tools or 'a tool'} to be called with args "
                f"{expected_tool_args}, got {tool_call_details or []}"
            )

    for t in spec.forbidden_tools:
        if t in tool_calls:
            failures.append(f"Forbidden tool called: {t}")

    answer_lower = answer.lower()
    for phrase in spec.must_contain:
        if phrase.lower() not in answer_lower:
            failures.append(f"Expected phrase not found: '{phrase}'")

    if spec.must_contain_any and not any(p.lower() in answer_lower for p in spec.must_contain_any):
        failures.append(f"Expected at least one of {spec.must_contain_any}")

    for phrase in spec.must_not_contain:
        if phrase.lower() in answer_lower:
            failures.append(f"Forbidden phrase found: '{phrase}'")

    for pattern, label in FORBIDDEN_PATTERNS:
        if re.search(pattern, answer, re.IGNORECASE):
            failures.append(f"Forbidden pattern ({label})")

    return failures


def run_test(test: TestCase, base_url: str) -> TestResult:
    failures: list[str] = []
    tool_calls: list[str] = []
    answer = ""

    try:
        # Use timestamp in session_id to avoid contamination across eval runs
        session_id = f"eval_{test.id}_{int(time.time())}"
        resp = requests.post(
            f"{base_url}/api/chat",
            json={"message": test.question, "session_id": session_id},
            timeout=300,
        )
        resp.raise_for_status()
        data = resp.json()
        answer = data.get("answer", "")
        tool_calls = [tc["name"] for tc in data.get("tool_calls", [])]
    except Exception as e:
        failures.append(f"HTTP error: {e}")
        return TestResult(test=test, passed=False, tool_calls=[], answer="", failures=failures)

    failures = check_answer(test, tool_calls, answer)
    return TestResult(
        test=test,
        passed=len(failures) == 0,
        tool_calls=tool_calls,
        answer=answer,
        failures=failures,
    )


def _post_message(base_url: str, session_id: str, question: str) -> tuple[str, list[dict]]:
    """POST one message to an existing session.

    Returns (answer, tool_calls), where tool_calls is the list of dicts the server
    returns as-is — each {"name": str, "args": dict} (see scripts/web_chat.py's
    tool_calls_log). Argument capture matters: checking tool NAMES alone lets a
    conversation pass even when the model calls the right tool with stale/wrong args.
    """
    resp = requests.post(
        f"{base_url}/api/chat",
        json={"message": question, "session_id": session_id},
        timeout=300,
    )
    resp.raise_for_status()
    data = resp.json()
    return data.get("answer", ""), data.get("tool_calls", [])


def run_conversation(
    conv: Conversation,
    base_url: str,
    delay: float = 0.0,
    post=_post_message,
    sleep=time.sleep,
) -> ConversationResult:
    """Run every turn against ONE session, so the model sees its own history.

    A failing turn does not abort the conversation: whether the assistant recovers
    on a later turn is exactly what we want to measure.
    """
    session_id = f"eval_{conv.id}_{int(time.time())}"
    results: list[TurnResult] = []

    for i, turn in enumerate(conv.turns, 1):
        try:
            answer, tool_call_details = post(base_url, session_id, turn.question)
        except Exception as e:
            results.append(TurnResult(
                index=i, turn=turn, passed=False,
                tool_calls=[], answer="", failures=[f"HTTP error: {e}"],
                tool_call_details=[],
            ))
        else:
            tool_calls = [tc["name"] for tc in tool_call_details]
            failures = check_answer(turn, tool_calls, answer, tool_call_details=tool_call_details)
            results.append(TurnResult(
                index=i, turn=turn, passed=len(failures) == 0,
                tool_calls=tool_calls, answer=answer, failures=failures,
                tool_call_details=tool_call_details,
            ))
        if i < len(conv.turns):
            sleep(delay)

    return ConversationResult(conversation=conv, turns=results)


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


def print_conversation_report(results: list[ConversationResult]) -> None:
    passed = sum(1 for r in results if r.passed)
    total = len(results)
    print(f"\n{'='*60}")
    print(f"BIRDOLOGY CONVERSATION EVAL — {passed}/{total} conversations passed")
    print(f"{'='*60}\n")

    by_category: dict[str, list[ConversationResult]] = {}
    for r in results:
        by_category.setdefault(r.conversation.category, []).append(r)

    for cat, cat_results in by_category.items():
        cat_pass = sum(1 for r in cat_results if r.passed)
        print(f"  [{cat}] {cat_pass}/{len(cat_results)}")
        for r in cat_results:
            icon = "✓" if r.passed else "✗"
            print(f"    {icon} {r.conversation.id}")
            for t in r.turns:
                t_icon = "✓" if t.passed else "✗"
                tools_str = ", ".join(t.tool_calls) if t.tool_calls else "none"
                print(f"        {t_icon} turn {t.index}: {t.turn.question[:55]}")
                print(f"             tools: {tools_str}")
                for f in t.failures:
                    print(f"             FAIL: {f}")
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
    ap.add_argument(
        "--suite",
        choices=["core", "robustness", "conversations", "all"],
        default="core",
        help="Test suite: core (default), robustness (phrasings), "
             "conversations (multi-turn), all",
    )
    args = ap.parse_args()

    anthropic_key = os.getenv("ANTHROPIC_API_KEY", "") if args.judge else ""
    if args.judge and not anthropic_key:
        print("WARNING: --judge requires ANTHROPIC_API_KEY — judge disabled")
        args.judge = False

    if args.suite == "robustness":
        tests = ROBUSTNESS_TESTS
    elif args.suite == "all":
        tests = TESTS + ROBUSTNESS_TESTS
    else:
        tests = TESTS
    if args.category:
        tests = [t for t in tests if t.category == args.category]
    if args.id:
        tests = [t for t in tests if t.id == args.id]

    conversations: list[Conversation] = []
    if args.suite in ("conversations", "all"):
        conversations = CONVERSATIONS
    if args.suite == "conversations":
        tests = []
    if args.category:
        conversations = [c for c in conversations if c.category == args.category]
    if args.id:
        conversations = [c for c in conversations if c.id == args.id]

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

    conv_results: list[ConversationResult] = []
    for i, conv in enumerate(conversations, 1):
        print(f"  [conv {i}/{len(conversations)}] {conv.id} ({len(conv.turns)} turns)...",
              end=" ", flush=True)
        cr = run_conversation(conv, args.url, delay=args.delay)
        conv_results.append(cr)
        failed_turns = [t.index for t in cr.turns if not t.passed]
        print("PASS" if cr.passed else f"FAIL (turns {failed_turns})")

    if conv_results:
        print_conversation_report(conv_results)

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
                        "answer": r.answer,
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
