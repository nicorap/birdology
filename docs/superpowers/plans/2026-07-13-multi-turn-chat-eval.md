# Multi-turn Chat Eval Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add multi-turn conversation cases to `scripts/eval_chat.py`, so the eval catches the cross-turn failures (stale refusals, photo-after-`nearby_birds`) that the 21 single-turn cases missed entirely — they scored 21/21 while the app was visibly broken in conversation.

**Architecture:** A `Turn` reuses the assertion fields `TestCase` already has, so the existing check logic is extracted once (`check_answer`) and shared by both suites. A `Conversation` is a list of Turns run against **one** session id — that shared session is what exercises history. HTTP is injected into `run_conversation` as a `post` callable, so the runner is unit-testable offline with a fake.

**Tech Stack:** Python 3.13, `pytest`, `requests`, `dataclasses`. Run everything with `uv run`.

## Global Constraints

- Spec: `docs/superpowers/specs/2026-07-13-multi-turn-chat-eval-design.md`
- Unit tests MUST NOT require a live server, a live LLM, or network. CI stays offline.
- The eval stays **black-box over HTTP**. It must not read or write `output/sessions.db`.
- These cases are a diagnostic, **not a CI gate** — multi-turn LLM behaviour is nondeterministic.
- Do not change existing single-turn assertion semantics. `check_answer` must behave exactly as the current inline checks in `run_test`.
- `scripts/` is not a package; tests import it via `sys.path.insert(0, <repo>/scripts)`, as `tests/test_web_session.py` already does.

## File Structure

| File | Responsibility |
|------|----------------|
| `scripts/eval_chat.py` (modify) | `check_answer` (shared), `Turn`/`Conversation`/`TurnResult`/`ConversationResult`, `_post_message`, `run_conversation`, conversation report, CLI wiring, the cases |
| `tests/test_eval_conversations.py` (create) | Offline unit tests for `check_answer` and `run_conversation` using a fake `post` |

---

### Task 1: Extract the shared assertion checker

Pull the five assertion checks + the infra-error short-circuit + global forbidden patterns out of `run_test` into one function that both suites call. No behaviour change.

**Files:**
- Modify: `scripts/eval_chat.py:402-469` (`run_test`)
- Test: `tests/test_eval_conversations.py`

**Interfaces:**
- Produces: `check_answer(spec, tool_calls: list[str], answer: str) -> list[str]` — returns a list of failure strings, empty if all checks pass. `spec` is any object exposing `expected_tools`, `forbidden_tools`, `must_contain`, `must_contain_any`, `must_not_contain` (satisfied by both `TestCase` and `Turn`).

- [ ] **Step 1: Write the failing test**

Create `tests/test_eval_conversations.py`:

```python
"""Offline unit tests for the eval harness. No live server, LLM, or network."""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

import eval_chat
from eval_chat import TestCase, check_answer


def _case(**kw) -> TestCase:
    kw.setdefault("id", "t")
    kw.setdefault("category", "c")
    kw.setdefault("question", "q")
    return TestCase(**kw)


def test_check_answer_passes_when_expected_tool_called():
    spec = _case(expected_tools=["find_species"])
    assert check_answer(spec, ["find_species"], "Le Rouge-gorge.") == []


def test_check_answer_flags_missing_expected_tool():
    spec = _case(expected_tools=["where_seen"])
    failures = check_answer(spec, ["find_species"], "answer")
    assert any("where_seen" in f for f in failures)


def test_check_answer_flags_forbidden_tool():
    spec = _case(forbidden_tools=["compare_seasonal"])
    failures = check_answer(spec, ["compare_seasonal"], "answer")
    assert any("Forbidden tool" in f for f in failures)


def test_check_answer_must_contain_is_case_insensitive():
    spec = _case(must_contain=["ROUGE-GORGE"])
    assert check_answer(spec, [], "le rouge-gorge familier") == []


def test_check_answer_must_contain_any_needs_only_one():
    spec = _case(must_contain_any=["![", "photo"])
    assert check_answer(spec, [], "voici une photo") == []


def test_check_answer_flags_must_not_contain():
    spec = _case(must_not_contain=["pas d'information"])
    failures = check_answer(spec, [], "Je n'ai pas d'information sur la localité")
    assert any("Forbidden phrase" in f for f in failures)


def test_check_answer_short_circuits_on_infra_error():
    spec = _case(expected_tools=["find_species"])
    failures = check_answer(spec, [], "Erreur serveur: /api/embeddings failed")
    assert failures == ["INFRA: Ollama embeddings crashed (nomic-embed-text unavailable)"]
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run pytest tests/test_eval_conversations.py -q`
Expected: FAIL — `ImportError: cannot import name 'check_answer' from 'eval_chat'`

- [ ] **Step 3: Add `check_answer` and make `run_test` call it**

In `scripts/eval_chat.py`, insert above `run_test`:

```python
def check_answer(spec, tool_calls: list[str], answer: str) -> list[str]:
    """Evaluate one answer against a spec's assertions.

    `spec` is anything exposing expected_tools / forbidden_tools / must_contain /
    must_contain_any / must_not_contain — both TestCase and Turn satisfy this.
    Returns the list of failures; empty means the answer passed.
    """
    # Infrastructure failures are not assertion failures — report them alone.
    if answer.startswith("Erreur serveur:") and "api/embeddings" in answer:
        return ["INFRA: Ollama embeddings crashed (nomic-embed-text unavailable)"]

    failures: list[str] = []

    if spec.expected_tools and not any(t in tool_calls for t in spec.expected_tools):
        failures.append(f"Expected one of {spec.expected_tools}, got {tool_calls}")

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
```

Now replace the body of `run_test` from the `# Detect infrastructure errors` comment through the final `return` (currently `scripts/eval_chat.py:423-469`) with:

```python
    failures = check_answer(test, tool_calls, answer)
    return TestResult(
        test=test,
        passed=len(failures) == 0,
        tool_calls=tool_calls,
        answer=answer,
        failures=failures,
    )
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/test_eval_conversations.py -q && uv run pytest -q`
Expected: new tests PASS; full suite still PASS (360 passed, 41 skipped before this change).

- [ ] **Step 5: Commit**

```bash
git add scripts/eval_chat.py tests/test_eval_conversations.py
git commit -m "Extract check_answer so single-turn and conversation cases share one checker"
```

---

### Task 2: `Turn`, `Conversation`, and the `run_conversation` runner

**Files:**
- Modify: `scripts/eval_chat.py`
- Test: `tests/test_eval_conversations.py`

**Interfaces:**
- Consumes: `check_answer` (Task 1).
- Produces:
  - `Turn(question, expected_tools=[], forbidden_tools=[], must_contain=[], must_contain_any=[], must_not_contain=[], notes="")`
  - `Conversation(id: str, category: str, turns: list[Turn])`
  - `TurnResult(index: int, turn: Turn, passed: bool, tool_calls: list[str], answer: str, failures: list[str])` — `index` is 1-based
  - `ConversationResult(conversation: Conversation, turns: list[TurnResult])` with property `passed -> bool`
  - `_post_message(base_url: str, session_id: str, question: str) -> tuple[str, list[str]]` returning `(answer, tool_calls)`
  - `run_conversation(conv, base_url, delay=0.0, post=_post_message, sleep=time.sleep) -> ConversationResult`

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_eval_conversations.py`:

```python
from eval_chat import Conversation, Turn, run_conversation


class FakePost:
    """Stands in for HTTP. Records calls; replays scripted (answer, tool_calls)."""

    def __init__(self, scripted):
        self.scripted = list(scripted)
        self.calls = []          # (session_id, question)

    def __call__(self, base_url, session_id, question):
        self.calls.append((session_id, question))
        return self.scripted.pop(0)


def test_all_turns_share_one_session():
    """The shared session is the whole point — it is what exercises history."""
    conv = Conversation(id="c", category="conversation_regression", turns=[
        Turn("premiere question"),
        Turn("deuxieme question"),
        Turn("troisieme question"),
    ])
    post = FakePost([("a", []), ("b", []), ("c", [])])
    run_conversation(conv, "http://x", delay=0, post=post, sleep=lambda s: None)

    session_ids = {sid for sid, _ in post.calls}
    assert len(session_ids) == 1, f"turns used {len(session_ids)} sessions, must share one"
    assert [q for _, q in post.calls] == [
        "premiere question", "deuxieme question", "troisieme question",
    ]


def test_per_turn_assertions_are_evaluated():
    conv = Conversation(id="c", category="conversation_regression", turns=[
        Turn("setup"),                                     # asserts nothing
        Turn("ou a-t-il ete vu ?", expected_tools=["where_seen"]),
    ])
    post = FakePost([("bla", ["find_species"]), ("Egå Engsø", ["where_seen"])])
    result = run_conversation(conv, "http://x", delay=0, post=post, sleep=lambda s: None)

    assert result.turns[0].passed          # setup turn has no assertions
    assert result.turns[1].passed
    assert result.passed


def test_failing_turn_does_not_abort_the_conversation():
    """Whether the assistant RECOVERS is exactly what we want to measure."""
    conv = Conversation(id="c", category="conversation_regression", turns=[
        Turn("t1", expected_tools=["where_seen"]),   # will fail
        Turn("t2", expected_tools=["find_species"]),  # must still run, and pass
    ])
    post = FakePost([("nope", []), ("photo", ["find_species"])])
    result = run_conversation(conv, "http://x", delay=0, post=post, sleep=lambda s: None)

    assert len(result.turns) == 2, "later turns must still run after a failure"
    assert not result.turns[0].passed
    assert result.turns[1].passed
    assert not result.passed, "conversation fails if any turn fails"


def test_turn_results_are_numbered_from_one():
    conv = Conversation(id="c", category="x", turns=[Turn("a"), Turn("b")])
    post = FakePost([("1", []), ("2", [])])
    result = run_conversation(conv, "http://x", delay=0, post=post, sleep=lambda s: None)
    assert [t.index for t in result.turns] == [1, 2]


def test_http_error_fails_that_turn_but_later_turns_still_run():
    def boom(base_url, session_id, question):
        if question == "t1":
            raise RuntimeError("connection reset")
        return ("ok", ["find_species"])

    conv = Conversation(id="c", category="x", turns=[
        Turn("t1"), Turn("t2", expected_tools=["find_species"]),
    ])
    result = run_conversation(conv, "http://x", delay=0, post=boom, sleep=lambda s: None)

    assert not result.turns[0].passed
    assert any("connection reset" in f for f in result.turns[0].failures)
    assert result.turns[1].passed
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/test_eval_conversations.py -q`
Expected: FAIL — `ImportError: cannot import name 'Conversation' from 'eval_chat'`

- [ ] **Step 3: Implement the dataclasses and runner**

In `scripts/eval_chat.py`, add after the `TestResult` dataclass:

```python
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


@dataclass
class ConversationResult:
    conversation: Conversation
    turns: list[TurnResult]

    @property
    def passed(self) -> bool:
        return all(t.passed for t in self.turns)
```

Then add the runner, after `run_test`:

```python
def _post_message(base_url: str, session_id: str, question: str) -> tuple[str, list[str]]:
    """POST one message to an existing session. Returns (answer, tool_call_names)."""
    resp = requests.post(
        f"{base_url}/api/chat",
        json={"message": question, "session_id": session_id},
        timeout=300,
    )
    resp.raise_for_status()
    data = resp.json()
    return data.get("answer", ""), [tc["name"] for tc in data.get("tool_calls", [])]


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
            answer, tool_calls = post(base_url, session_id, turn.question)
        except Exception as e:
            results.append(TurnResult(
                index=i, turn=turn, passed=False,
                tool_calls=[], answer="", failures=[f"HTTP error: {e}"],
            ))
        else:
            failures = check_answer(turn, tool_calls, answer)
            results.append(TurnResult(
                index=i, turn=turn, passed=len(failures) == 0,
                tool_calls=tool_calls, answer=answer, failures=failures,
            ))
        if i < len(conv.turns):
            sleep(delay)

    return ConversationResult(conversation=conv, turns=results)
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/test_eval_conversations.py -q`
Expected: PASS (all 12 tests).

- [ ] **Step 5: Commit**

```bash
git add scripts/eval_chat.py tests/test_eval_conversations.py
git commit -m "Add Turn/Conversation model and run_conversation (one session across turns)"
```

---

### Task 3: The conversation cases

**Files:**
- Modify: `scripts/eval_chat.py`
- Test: `tests/test_eval_conversations.py`

**Interfaces:**
- Consumes: `Turn`, `Conversation` (Task 2).
- Produces: `CONVERSATIONS: list[Conversation]` with categories `conversation_regression` and `conversation_robustness`.

Note on the fixtures used below: **Condor de Californie** is in the eBird taxonomy but has no Danish observations, so `where_seen` legitimately returns nothing — that gives us a *genuine* refusal to test inertia against. **Chardonneret élégant** has 425 observations, so a refusal about it is always a bug.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_eval_conversations.py`:

```python
from eval_chat import CONVERSATIONS


def test_conversations_have_unique_ids():
    ids = [c.id for c in CONVERSATIONS]
    assert len(ids) == len(set(ids))


def test_both_conversation_categories_present():
    cats = {c.category for c in CONVERSATIONS}
    assert "conversation_regression" in cats
    assert "conversation_robustness" in cats


def test_every_conversation_is_multi_turn_and_within_budget():
    for c in CONVERSATIONS:
        assert 2 <= len(c.turns) <= 5, f"{c.id} has {len(c.turns)} turns"


def test_every_conversation_asserts_something():
    """A conversation with no assertions anywhere would silently always pass."""
    for c in CONVERSATIONS:
        asserted = any(
            t.expected_tools or t.forbidden_tools or t.must_contain
            or t.must_contain_any or t.must_not_contain
            for t in c.turns
        )
        assert asserted, f"{c.id} asserts nothing"


def test_regression_conversations_cover_the_known_bugs():
    ids = {c.id for c in CONVERSATIONS}
    assert {"conv_photo_after_nearby", "conv_pronoun_where_seen",
            "conv_refusal_inertia", "conv_no_invented_species"} <= ids
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run pytest tests/test_eval_conversations.py -q -k conversation`
Expected: FAIL — `ImportError: cannot import name 'CONVERSATIONS' from 'eval_chat'`

- [ ] **Step 3: Add the cases**

In `scripts/eval_chat.py`, after `ROBUSTNESS_TESTS`:

```python
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
                 notes="Must look up the corrected species, not keep answering about the Robin."),
        ],
    ),
]
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/test_eval_conversations.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add scripts/eval_chat.py tests/test_eval_conversations.py
git commit -m "Add conversation_regression and conversation_robustness eval cases"
```

---

### Task 4: Per-turn reporting and CLI wiring

A failure must read as `conv_photo_after_nearby / turn 2`, never as an unexplained final-turn failure.

**Files:**
- Modify: `scripts/eval_chat.py` — add `print_conversation_report`, extend `main` (`scripts/eval_chat.py:555-640`)
- Test: `tests/test_eval_conversations.py`

**Interfaces:**
- Consumes: `ConversationResult`, `CONVERSATIONS`, `run_conversation`.
- Produces: `print_conversation_report(results: list[ConversationResult]) -> None`; `--suite conversations` and `--suite all` now include conversations.

- [ ] **Step 1: Write the failing test**

Append to `tests/test_eval_conversations.py`:

```python
from eval_chat import ConversationResult, TurnResult, print_conversation_report


def test_report_names_the_failing_turn(capsys):
    conv = Conversation(id="conv_photo_after_nearby", category="conversation_regression",
                        turns=[Turn("t1"), Turn("montre moi une photo !")])
    result = ConversationResult(conversation=conv, turns=[
        TurnResult(1, conv.turns[0], True, ["nearby_birds"], "ok", []),
        TurnResult(2, conv.turns[1], False, [], "je n'ai pas de photo",
                   ["Expected one of ['find_species'], got []"]),
    ])
    print_conversation_report([result])
    out = capsys.readouterr().out

    assert "conv_photo_after_nearby" in out
    assert "turn 2" in out
    assert "montre moi une photo !" in out          # the question, so the failure is legible
    assert "find_species" in out                     # the actual failure text
    assert "0/1" in out                              # conversation counted as failed
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run pytest tests/test_eval_conversations.py -q -k report`
Expected: FAIL — `ImportError: cannot import name 'print_conversation_report'`

- [ ] **Step 3: Implement the report and wire up the CLI**

Add after `print_report` in `scripts/eval_chat.py`:

```python
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
```

In `main`, add the CLI choice — change the `--suite` argument (`scripts/eval_chat.py:563-568`) to:

```python
    ap.add_argument(
        "--suite",
        choices=["core", "robustness", "conversations", "all"],
        default="core",
        help="Test suite: core (default), robustness (phrasings), "
             "conversations (multi-turn), all",
    )
```

Then, immediately after the existing `tests = ...` selection block and before the server reachability check, insert:

```python
    conversations: list[Conversation] = []
    if args.suite in ("conversations", "all"):
        conversations = CONVERSATIONS
    if args.suite == "conversations":
        tests = []
    if args.category:
        conversations = [c for c in conversations if c.category == args.category]
    if args.id:
        conversations = [c for c in conversations if c.id == args.id]
```

Finally, after the existing `print_report(results, args.judge)` call, insert:

```python
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
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/test_eval_conversations.py -q && uv run pytest -q`
Expected: all PASS.

- [ ] **Step 5: Verify the CLI wiring against the real server**

The server must be running on port 8080 (5000 is held by AirPlay Receiver on macOS):

```bash
uv run python scripts/eval_chat.py --url http://localhost:8080 \
    --suite conversations --category conversation_regression
```

Expected: 4 conversations run; each turn prints its tools; failures name the turn number.
**Do not claim success without reading this output.** These cases are nondeterministic — record what actually passed.

- [ ] **Step 6: Commit**

```bash
git add scripts/eval_chat.py tests/test_eval_conversations.py
git commit -m "Report conversation failures per turn; add --suite conversations"
```

---

### Task 5: Document the new suite

**Files:**
- Modify: `CLAUDE.md` (the `## Tests` section, and the command list)

- [ ] **Step 1: Add the commands to `CLAUDE.md`**

Under the existing command list, add:

```bash
# Evaluate the chat system (server must be running)
python scripts/eval_chat.py --url http://localhost:8080                      # 21 single-turn cases
python scripts/eval_chat.py --url http://localhost:8080 --suite conversations # multi-turn cases
python scripts/eval_chat.py --url http://localhost:8080 --suite all --judge   # everything + LLM judge
```

And in the `## Tests` table, add:

```
tests/test_eval_conversations.py — eval harness: check_answer + run_conversation (offline, mocked HTTP)
```

- [ ] **Step 2: Commit**

```bash
git add CLAUDE.md
git commit -m "Document the multi-turn eval suite"
```

---

## Self-Review

**Spec coverage:** Data model → Task 2. Reuse of existing checks → Task 1. Runner (one session, per-turn checks, no abort on failure) → Task 2. Reporting → Task 4. Cases (4 regression + 4 robustness) → Task 3. Cost/`--category` → Task 4. Offline unit tests → Tasks 1–4. Non-goal "not a CI gate" → honoured: nothing is added to `.github/workflows/tests.yml`; the offline unit tests run there, the live cases do not.

**Type consistency:** `check_answer(spec, tool_calls, answer) -> list[str]` is defined in Task 1 and consumed in Task 2. `TurnResult(index, turn, passed, tool_calls, answer, failures)` is positional in the Task 4 test and matches the Task 2 dataclass field order. `ConversationResult.passed` is a property, used in Task 4's report and Task 2's tests. `CONVERSATIONS` is defined in Task 3 and consumed in Task 4.

**Known risk, carried from the spec:** these cases are nondeterministic and may pass 4 runs in 5. That is information, not noise — but it is why Task 4 Step 5 says to read the real output rather than assume, and why nothing here gates CI.
