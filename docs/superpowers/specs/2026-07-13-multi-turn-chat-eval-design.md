# Multi-turn conversation cases for `eval_chat.py`

**Date:** 2026-07-13
**Status:** approved, ready for implementation plan

## Problem

`scripts/eval_chat.py` runs 21 single-turn cases: one question, one fresh session, one set of
assertions. Every real bug found in the 2026-07-13 debugging session was **multi-turn**, and none
of them could have been caught by a single-turn case:

- The assistant refused to give observation localities ("je n'ai pas d'information sur la
  localité") because its *own earlier refusals*, replayed from session history, outweighed the
  instruction to re-check with a tool. A single-turn case cannot reproduce this: there is no
  history.
- Asked for a photo after a `nearby_birds` answer, the assistant claimed the species had no photo
  (false — it was in the graph) and then invented a species, `Haliaeetus albicillatus`, to offer
  a photo of. The failure depended entirely on *which tool ran in the previous turn*: `nearby_birds`
  did not return a `thumbnail`, so no photo was in context.

The eval suite passed throughout. It was measuring the wrong shape of interaction.

## Goals

Catch regressions that only appear across turns, in the two flavours that matter:

1. **`conversation_regression`** — the failure modes we actually hit. Proven real.
2. **`conversation_robustness`** — general multi-turn behaviour (coreference, topic switching,
   carried-over constraints). Speculative but cheap to add once the machinery exists.

## Non-goals

- **Not** a CI gate. Multi-turn LLM behaviour is nondeterministic; these cases will be flaky in a
  way single-turn cases are not. They are a diagnostic, run on demand.
- **Not** a test of the session-invalidation mechanism. The capability fingerprint
  (`web_chat._capability_fingerprint`) is already covered by unit tests in `tests/test_web_session.py`.
  The eval stays black-box over HTTP and does not touch `sessions.db`.

## Design

### Data model

```python
@dataclass
class Turn:
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
```

A `Turn` is structurally a `TestCase` without the `id`/`category`. All assertion fields are
optional, so a turn that only exists to set up context asserts nothing.

### Reuse of existing checks

`check_result()` currently takes a whole `TestCase`. Refactor it to accept the assertion fields
(or a protocol satisfied by both `TestCase` and `Turn`) so the *same* checking logic serves both
suites. No new assertion semantics are introduced.

### Runner

`run_conversation(conv, base_url, delay)`:

- Mint **one** `session_id` for the whole conversation (timestamped, as `run_case` does today, to
  avoid contamination across eval runs). Reusing one session across the turns is the entire point:
  it is what exercises history.
- POST each turn's question to that session in order; check each turn's assertions as its response
  lands.
- **A failing turn does not abort the conversation.** Later turns still run — whether the assistant
  *recovers* is exactly what we want to measure.

### Reporting

A failure is reported as `conv_photo_after_nearby / turn 4` together with the question text. A break
at turn 3 must read as turn 3, not as an unexplained final-turn failure — that ambiguity was the
debugging pain that motivated this work.

### Cases

**`conversation_regression`** (3–4):

- `conv_photo_after_nearby` — ask for birds near Nørrebro (routes through `nearby_birds`), then
  "montre moi une photo!". Must call `find_species`; answer must contain a markdown image.
- `conv_pronoun_where_seen` — describe a species, then "ou a t il ete vu ?". Must call `where_seen`;
  must not contain "pas d'information".
- `conv_refusal_inertia` — ask about a species with no observations (a legitimate "no data"), then
  ask an *answerable* question about a common species. The earlier refusal must not be inherited:
  the tool must be called and the answer must not refuse.
- `conv_no_invented_species` — after a photo request that cannot be satisfied, the answer must not
  contain a fabricated binomial. **Start narrow**: assert the specific fabrications observed
  (`Haliaeetus albicillatus`) do not recur. A general "every binomial in the answer must appear in
  a tool result" check is attractive but fiddly (the model legitimately writes names in prose);
  generalise only if the narrow version proves insufficient.

**`conversation_robustness`** (4–6): topic switch between locations; location carried across turns;
month carried across turns; a correction/contradiction from the user mid-conversation.

### Cost

8–10 conversations × up to 5 turns ≈ 40–50 extra LLM calls, roughly tripling eval runtime. The new
cases live in their own categories, so the existing `--category` flag gives a fast targeted run,
and `--delay` still applies.

### Testing

The conversation runner gets unit tests with **mocked HTTP** so CI stays offline and fast: turn
sequencing, one-session-for-all-turns, per-turn assertion evaluation, and the
failing-turn-does-not-abort rule. The cases themselves only run against a live server, as today.

## Risks

- **Flakiness.** Multi-turn behaviour is nondeterministic; a case may pass 4 runs in 5. This is
  information, not noise — but it is why these must not gate CI.
- **The `no_invented_species` assertion may not generalise.** Deliberately scoped narrow to start.
