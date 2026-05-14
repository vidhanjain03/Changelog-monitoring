#!/usr/bin/env python3
"""
Changelog Monitoring Pipeline
Stages: INIT -> SOURCES_LOADED -> CHANGELOGS_FETCHED -> ENTRIES_PARSED ->
        RECENT_ENTRIES_FILTERED -> CHANGES_CLASSIFIED -> HIGH_RISK_STRIPE_CHANGES_SELECTED ->
        CODEBASE_IMPACT_ANALYSED -> MIGRATION_GUIDES_GENERATED -> MIGRATION_CODE_VALIDATED ->
        IMPACT_REPORT_WRITTEN -> OPTIONAL_OUTPUTS_GENERATED -> VALIDATION_COMPLETE -> RESULTS_FINALISED
"""

import ast
import hashlib
import json
import os
import re
import sys
import traceback
from datetime import datetime, timezone, timedelta
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from dateutil import parser as dateparser

# ─────────────────────────────────────────────
# TAXONOMY CONSTANTS
# ─────────────────────────────────────────────
VALID_CHANGE_TYPES  = {"deprecation", "breaking", "enhancement", "bugfix", "security"}
VALID_RISK_LEVELS   = {"critical", "high", "medium", "low", "none"}
NINETY_DAYS_AGO     = datetime.now(timezone.utc) - timedelta(days=90)
BATCH_SIZE = 50   # max entries per LLM call
# ─────────────────────────────────────────────
# PIPELINE STATE
# ─────────────────────────────────────────────
STAGES = [
    "INIT", "SOURCES_LOADED", "CHANGELOGS_FETCHED", "ENTRIES_PARSED",
    "RECENT_ENTRIES_FILTERED", "CHANGES_CLASSIFIED", "HIGH_RISK_STRIPE_CHANGES_SELECTED",
    "CODEBASE_IMPACT_ANALYSED", "MIGRATION_GUIDES_GENERATED", "MIGRATION_CODE_VALIDATED",
    "IMPACT_REPORT_WRITTEN", "OPTIONAL_OUTPUTS_GENERATED", "VALIDATION_COMPLETE",
    "RESULTS_FINALISED",
]

pipeline_state = {"current_stage": "INIT", "errors": []}

def advance_stage(stage: str):
    assert stage in STAGES, f"Unknown stage: {stage}"
    pipeline_state["current_stage"] = stage
    print(f"\n{'='*60}\n[PIPELINE] {stage}\n{'='*60}")

# ─────────────────────────────────────────────
# DIRECTORY SETUP
# ─────────────────────────────────────────────
def init_dirs():
    Path("parsed_changelogs").mkdir(exist_ok=True)
    Path("llm_calls.jsonl").touch(exist_ok=True)

# ─────────────────────────────────────────────
# LLM CALL LOGGER
# ─────────────────────────────────────────────
def log_llm_call(stage, source_id, entry_ids, prompt, model, input_artifacts, output_artifact):
    record = {
        "stage":            stage,
        "source_id":        source_id,
        "entry_ids":        entry_ids,
        "timestamp":        datetime.now(timezone.utc).isoformat(),
        "provider":         "anthropic",
        "model":            model,
        "prompt_hash":      hashlib.sha256(prompt.encode()).hexdigest(),
        "input_artifacts":  input_artifacts,
        "output_artifact":  output_artifact,
    }
    with open("llm_calls.jsonl", "a") as f:
        f.write(json.dumps(record) + "\n")

# ─────────────────────────────────────────────
# ANTHROPIC CALL HELPER
# ─────────────────────────────────────────────
def call_llm(prompt: str, stage: str, source_id: str, entry_ids: list,
             input_artifacts: list, output_artifact: str) -> str:
    """Call Anthropic API and log the call."""
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise EnvironmentError("ANTHROPIC_API_KEY environment variable not set.")

    # model = "claude-sonnet-4-20250514"
    model = "claude-3-5-sonnet-20241022"
    headers = {
        "x-api-key":         api_key,
        "anthropic-version": "2023-06-01",
        "content-type":      "application/json",
    }
    body = {
        "model":      model,
        "max_tokens": 4096,
        "messages":   [{"role": "user", "content": prompt}],
    }

    log_llm_call(stage, source_id, entry_ids, prompt, model, input_artifacts, output_artifact)

    resp = requests.post("https://api.anthropic.com/v1/messages", headers=headers, json=body, timeout=120)
    if not resp.ok:
        print(f"[LLM ERROR] Status {resp.status_code}: {resp.text}")   # ← add this
    resp.raise_for_status()
    data = resp.json()
    return data["content"][0]["text"]

# ─────────────────────────────────────────────
# HELPER: SAFE JSON EXTRACT
# ─────────────────────────────────────────────
def extract_json(text: str):
    """Try to extract a JSON object or array from LLM response text."""
    # Try raw parse first
    try:
        return json.loads(text.strip())
    except json.JSONDecodeError:
        pass
    # Try extracting from markdown code fences
    match = re.search(r"```(?:json)?\s*([\s\S]+?)```", text)
    if match:
        try:
            return json.loads(match.group(1).strip())
        except json.JSONDecodeError:
            pass
    # Try finding first [ or { to end
    for start_char, end_char in [("[", "]"), ("{", "}")]:
        idx = text.find(start_char)
        if idx != -1:
            try:
                return json.loads(text[idx:])
            except json.JSONDecodeError:
                pass
    raise ValueError(f"Could not extract JSON from LLM response:\n{text[:500]}")

# ─────────────────────────────────────────────
# STAGE: SOURCES_LOADED
# ─────────────────────────────────────────────
def load_sources() -> list:
    advance_stage("SOURCES_LOADED")
    with open("changelog_sources.json") as f:
        data = json.load(f)
    sources = data["sources"]
    print(f"[SOURCES] Loaded {len(sources)} sources: {[s['source_id'] for s in sources]}")
    return sources

# ─────────────────────────────────────────────
# STAGE: CHANGELOGS_FETCHED
# ─────────────────────────────────────────────
def fetch_changelogs(sources: list) -> dict:
    advance_stage("CHANGELOGS_FETCHED")
    raw = {}
    for src in sources:
        sid = src["source_id"]
        try:
            resp = requests.get(src["url"], timeout=30,
                                headers={"User-Agent": "changelog-pipeline/1.0"})
            resp.raise_for_status()
            raw[sid] = {"content": resp.text, "format": src["format"], "source": src}
            print(f"[FETCH] {sid}: {len(resp.text)} chars")
        except Exception as e:
            raw[sid] = {"content": None, "format": src["format"], "source": src,
                        "error": str(e)}
            print(f"[FETCH] {sid}: FAILED — {e}")
    return raw

# ─────────────────────────────────────────────
# PARSERS
# ─────────────────────────────────────────────
def _try_parse_date(text: str):
    """Return timezone-aware datetime or None."""
    if not text:
        return None
    try:
        dt = dateparser.parse(text, fuzzy=True)
        if dt:
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
    except Exception:
        pass
    return None


def parse_markdown_changelog(content: str, source: dict) -> list:
    """Parse markdown changelog into entry dicts."""
    sid      = source["source_id"]
    sname    = source["name"]
    entries  = []
    counter  = 0

    # Split on lines that look like version headers: ## [x.y.z] or ## x.y.z
    sections = re.split(r'\n(?=#{1,3} )', content)

    for section in sections:
        lines = section.strip().splitlines()
        if not lines:
            continue
        header = lines[0].strip()
        # Must look like a version/date header
        if not re.match(r'^#{1,3} ', header):
            continue

        # Extract version string
        ver_match = re.search(
            r'(\d{4}-\d{2}-\d{2}|\bv?\d+\.\d+[\.\d]*\b)',
            header, re.IGNORECASE
        )
        version_or_date = ver_match.group(1) if ver_match else header.lstrip("# ").strip()

        # Try to find a date in the header
        date_match = re.search(r'\d{4}-\d{2}-\d{2}', header)
        published_at = None
        if date_match:
            published_at = _try_parse_date(date_match.group(0))

        body = "\n".join(lines[1:]).strip()
        if not body:
            continue

        # Split body into individual bullet points as separate entries
        bullets = re.findall(r'(?:^|\n)[*\-] (.+?)(?=\n[*\-] |\Z)', body, re.DOTALL)
        if not bullets:
            bullets = [body]

        for bullet in bullets:
            bullet = bullet.strip()
            if not bullet:
                continue
            counter += 1
            entry_id = f"{sid}-{counter:04d}"

            # Try to detect raw change type from keywords
            change_type_raw = None
            lower = bullet.lower()
            if any(w in lower for w in ["deprecat"]):
                change_type_raw = "deprecation"
            elif any(w in lower for w in ["breaking", "removed", "dropped"]):
                change_type_raw = "breaking"
            elif any(w in lower for w in ["fix", "bug", "patch", "resolved"]):
                change_type_raw = "bugfix"
            elif any(w in lower for w in ["security", "vulnerability", "cve", "auth"]):
                change_type_raw = "security"
            elif any(w in lower for w in ["add", "new", "feat", "support", "enhanc", "improve"]):
                change_type_raw = "enhancement"

            entries.append({
                "entry_id":        entry_id,
                "source_id":       sid,
                "source":          sname,
                "version_or_date": version_or_date,
                "published_at":    published_at.isoformat() if published_at else None,
                "change_title":    bullet[:120],
                "change_body":     bullet,
                "change_type_raw": change_type_raw,
            })

    return entries


def parse_html_changelog(content: str, source: dict) -> list:
    """Parse HTML changelog (Twilio style) into entry dicts."""
    sid     = source["source_id"]
    sname   = source["name"]
    entries = []
    counter = 0
    soup    = BeautifulSoup(content, "html.parser")

    # Try common patterns: article tags, changelog-item divs, li items in changelog sections
    items = (
        soup.find_all("article")
        or soup.find_all(class_=re.compile(r'changelog|entry|item|post', re.I))
        or soup.find_all("li", class_=re.compile(r'changelog|change', re.I))
    )

    # Fallback: grab h2/h3 headings + following paragraph
    if not items:
        for heading in soup.find_all(["h2", "h3"]):
            title_text = heading.get_text(strip=True)
            if not title_text:
                continue
            sibling = heading.find_next_sibling(["p", "div", "ul"])
            body_text = sibling.get_text(" ", strip=True) if sibling else title_text

            date_match = re.search(r'\d{4}-\d{2}-\d{2}|\w+ \d{1,2},? \d{4}', title_text)
            published_at = _try_parse_date(date_match.group(0)) if date_match else None

            counter += 1
            entries.append({
                "entry_id":        f"{sid}-{counter:04d}",
                "source_id":       sid,
                "source":          sname,
                "version_or_date": title_text[:80],
                "published_at":    published_at.isoformat() if published_at else None,
                "change_title":    title_text[:120],
                "change_body":     body_text[:1000],
                "change_type_raw": None,
            })
        return entries

    for item in items:
        title_el = item.find(["h2", "h3", "h4", "strong", "b"])
        title_text = title_el.get_text(strip=True) if title_el else item.get_text(" ", strip=True)[:80]

        date_el = item.find(["time", "span"], class_=re.compile(r'date|time|when', re.I))
        date_text = None
        if date_el:
            date_text = date_el.get("datetime") or date_el.get_text(strip=True)
        else:
            date_match = re.search(r'\d{4}-\d{2}-\d{2}|\w+ \d{1,2},? \d{4}', item.get_text())
            date_text = date_match.group(0) if date_match else None

        published_at = _try_parse_date(date_text)
        body_text    = item.get_text(" ", strip=True)[:1000]

        counter += 1
        entries.append({
            "entry_id":        f"{sid}-{counter:04d}",
            "source_id":       sid,
            "source":          sname,
            "version_or_date": title_text,
            "published_at":    published_at.isoformat() if published_at else None,
            "change_title":    title_text[:120],
            "change_body":     body_text,
            "change_type_raw": None,
        })

    return entries

# ─────────────────────────────────────────────
# STAGE: ENTRIES_PARSED + RECENT_ENTRIES_FILTERED
# ─────────────────────────────────────────────
def parse_and_filter(raw: dict) -> dict:
    advance_stage("ENTRIES_PARSED")
    parsed_all = {}

    for sid, payload in raw.items():
        if payload.get("error") or not payload.get("content"):
            result = {"source_id": sid, "entries": [], "reason": payload.get("error", "fetch_failed")}
            Path(f"parsed_changelogs/{sid}.json").write_text(json.dumps(result, indent=2))
            parsed_all[sid] = result
            continue

        src    = payload["source"]
        fmt    = payload["format"]
        content = payload["content"]

        if fmt == "markdown":
            entries = parse_markdown_changelog(content, src)
        else:
            entries = parse_html_changelog(content, src)

        print(f"[PARSE] {sid}: {len(entries)} total entries before filter")

    advance_stage("RECENT_ENTRIES_FILTERED")

    for sid, payload in raw.items():
        if payload.get("error"):
            continue
        src     = payload["source"]
        entries = parsed_all.get(sid, {}).get("entries")
        if entries is None:
            # Re-parse
            if payload["format"] == "markdown":
                entries = parse_markdown_changelog(payload["content"], src)
            else:
                entries = parse_html_changelog(payload["content"], src)

        # 90-day filter
        recent = []
        for e in entries:
            if e["published_at"]:
                try:
                    dt = dateparser.parse(e["published_at"])
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    if dt >= NINETY_DAYS_AGO:
                        recent.append(e)
                except Exception:
                    # No parseable date — include with a note
                    e["date_parse_warning"] = "could_not_verify_age"
                    recent.append(e)
            else:
                # No date info — include cautiously
                e["date_parse_warning"] = "no_date_found"
                recent.append(e)

        print(f"[FILTER] {sid}: {len(recent)} entries after 90-day filter")

        if not recent:
            result = {"source_id": sid, "entries": [], "reason": "no_entries_in_last_90_days"}
        else:
            result = {"source_id": sid, "entries": recent, "reason": None}

        Path(f"parsed_changelogs/{sid}.json").write_text(json.dumps(result, indent=2))
        parsed_all[sid] = result

    return parsed_all

# ─────────────────────────────────────────────
# STAGE: CHANGES_CLASSIFIED
# ─────────────────────────────────────────────
CLASSIFICATION_PROMPT_TEMPLATE = """
You are a changelog classification engine. Classify each change entry using ONLY the controlled taxonomy below.

TAXONOMY:
change_type values (pick exactly one): deprecation, breaking, enhancement, bugfix, security
breaking_risk values (pick exactly one): critical, high, medium, low, none
affects_auth: true or false
affects_billing: true or false
affects_data_model: true or false

RULES:
- Do NOT invent new categories.
- Do NOT leave any field empty.
- Output ONLY a JSON array. No prose, no explanation outside the JSON.

ENTRIES TO CLASSIFY (source: {source_name}):
{entries_json}

OUTPUT FORMAT (JSON array, one object per entry):
[
  {{
    "entry_id": "string",
    "change_type": "breaking|deprecation|enhancement|bugfix|security",
    "breaking_risk": "critical|high|medium|low|none",
    "affects_auth": true or false,
    "affects_billing": true or false,
    "affects_data_model": true or false,
    "rationale": "one sentence explanation"
  }}
]
"""

def validate_classification(entry: dict) -> dict:
    """Clamp any out-of-taxonomy values to safe defaults."""
    entry["change_type"]   = entry.get("change_type",   "enhancement") if entry.get("change_type") in VALID_CHANGE_TYPES else "enhancement"
    entry["breaking_risk"] = entry.get("breaking_risk", "none")        if entry.get("breaking_risk") in VALID_RISK_LEVELS  else "none"
    entry["affects_auth"]         = bool(entry.get("affects_auth", False))
    entry["affects_billing"]      = bool(entry.get("affects_billing", False))
    entry["affects_data_model"]   = bool(entry.get("affects_data_model", False))
    entry["rationale"]            = entry.get("rationale", "")
    return entry


def classify_changes(parsed_all: dict) -> list:
    advance_stage("CHANGES_CLASSIFIED")
    all_classified = []

    for sid, data in parsed_all.items():
        entries = data.get("entries", [])
        if not entries:
            print(f"[CLASSIFY] {sid}: no entries — skipping LLM call")
            continue

        source_name = entries[0]["source"] if entries else sid
        # Trim entries for prompt (title + body only, to save tokens)
        slim = [{"entry_id": e["entry_id"],
                 "change_title": e["change_title"],
                 "change_body":  e["change_body"][:300]} for e in entries]

        prompt = CLASSIFICATION_PROMPT_TEMPLATE.format(
            source_name=source_name,
            entries_json=json.dumps(slim, indent=2)
        )

        print(f"[CLASSIFY] Calling LLM for {sid} ({len(entries)} entries)...")
        try:
            response = call_llm(
                prompt=prompt,
                stage="CHANGES_CLASSIFIED",
                source_id=sid,
                entry_ids=[e["entry_id"] for e in entries],
                input_artifacts=[f"parsed_changelogs/{sid}.json"],
                output_artifact="classified_changes.json",
            )
            classified = extract_json(response)
            if not isinstance(classified, list):
                classified = [classified]
            classified = [validate_classification(c) for c in classified]
            all_classified.extend(classified)
            print(f"[CLASSIFY] {sid}: {len(classified)} entries classified")
        except Exception as e:
            print(f"[CLASSIFY] {sid}: LLM call FAILED — {e}")
            pipeline_state["errors"].append({"stage": "CLASSIFY", "source_id": sid, "error": str(e)})
            # Fallback: mark all as enhancement/none
            for e2 in entries:
                all_classified.append({
                    "entry_id":       e2["entry_id"],
                    "change_type":    "enhancement",
                    "breaking_risk":  "none",
                    "affects_auth":   False,
                    "affects_billing": False,
                    "affects_data_model": False,
                    "rationale":      "classification_failed_fallback",
                })

    Path("classified_changes.json").write_text(json.dumps(all_classified, indent=2))
    print(f"[CLASSIFY] Total classified: {len(all_classified)}")
    return all_classified

# ─────────────────────────────────────────────
# STAGE: HIGH_RISK_STRIPE_CHANGES_SELECTED
# ─────────────────────────────────────────────
def select_high_risk_stripe(classified: list, parsed_all: dict) -> list:
    advance_stage("HIGH_RISK_STRIPE_CHANGES_SELECTED")

    # Build entry_id -> full entry lookup
    id_to_entry = {}
    for sid, data in parsed_all.items():
        for e in data.get("entries", []):
            id_to_entry[e["entry_id"]] = e

    high_risk = [
        c for c in classified
        if c["entry_id"].startswith("stripe_node")
        and c["breaking_risk"] in {"critical", "high"}
    ]

    # Enrich with original entry text
    enriched = []
    for c in high_risk:
        orig = id_to_entry.get(c["entry_id"], {})
        enriched.append({**c, **{k: orig.get(k) for k in
                                  ["change_title", "change_body", "version_or_date", "published_at"]}})

    print(f"[SELECT] High-risk Stripe entries: {len(enriched)}")
    return enriched

# ─────────────────────────────────────────────
# STAGE: CODEBASE_IMPACT_ANALYSED
# ─────────────────────────────────────────────
IMPACT_PROMPT_TEMPLATE = """
You are a senior software engineer performing a breaking-change impact analysis.

BREAKING CHANGES (Stripe Node.js SDK, classified as critical or high risk):
{changes_json}

CODEBASE SNIPPET (file: codebase_snippet.py):
```python
{codebase}
```

TASK:
Analyse each function in the codebase snippet. For each function, determine whether any of the
breaking changes above would affect it. Reason at the function level.

OUTPUT FORMAT (JSON array, one object per function analysed):
[
  {{
    "function_name": "string",
    "affected": true or false,
    "breaking_detail": "string — what exactly breaks (empty string if not affected)",
    "suggested_fix_summary": "string — one-sentence fix description (empty string if not affected)",
    "related_entry_ids": ["entry_id_1"]
  }}
]

Output ONLY valid JSON. No prose outside the array.
"""

def analyse_codebase_impact(high_risk: list) -> list:
    advance_stage("CODEBASE_IMPACT_ANALYSED")

    if not high_risk:
        result = {
            "functions": [],
            "reason": "No critical or high-risk Stripe changes found in the last 90 days.",
        }
        Path("codebase_impact.json").write_text(json.dumps(result, indent=2))
        print("[IMPACT] No high-risk Stripe changes — skipping impact analysis")
        return []

    codebase = Path("codebase_snippet.py").read_text()

    prompt = IMPACT_PROMPT_TEMPLATE.format(
        changes_json=json.dumps(high_risk, indent=2),
        codebase=codebase,
    )

    print(f"[IMPACT] Calling LLM for codebase impact analysis...")
    try:
        response = call_llm(
            prompt=prompt,
            stage="CODEBASE_IMPACT_ANALYSED",
            source_id="stripe_node",
            entry_ids=[c["entry_id"] for c in high_risk],
            input_artifacts=["classified_changes.json", "codebase_snippet.py"],
            output_artifact="codebase_impact.json",
        )
        functions = extract_json(response)
        if not isinstance(functions, list):
            functions = [functions]
    except Exception as e:
        print(f"[IMPACT] LLM call FAILED — {e}")
        pipeline_state["errors"].append({"stage": "IMPACT", "error": str(e)})
        functions = []

    result = {"functions": functions, "reason": None}
    Path("codebase_impact.json").write_text(json.dumps(result, indent=2))

    affected = [f for f in functions if f.get("affected")]
    print(f"[IMPACT] Functions analysed: {len(functions)}, affected: {len(affected)}")
    return functions

# ─────────────────────────────────────────────
# STAGE: MIGRATION_GUIDES_GENERATED
# ─────────────────────────────────────────────
MIGRATION_PROMPT_TEMPLATE = """
You are a Python migration guide author. Generate a migration guide for each affected function.

AFFECTED FUNCTIONS:
{affected_json}

ORIGINAL CODEBASE:
```python
{codebase}
```

For each affected function, output a migration guide with:
1. The original "before" code block
2. The corrected "after" code block (must be syntactically valid Python)
3. A one-sentence explanation of why the change is necessary

FORMAT YOUR RESPONSE as Markdown with this structure for EACH function:

## Migration: <function_name>

**Why:** <one sentence explanation>

**Before:**
```python
<original code>
```

**After:**
```python
<corrected code>
```

---
"""

def generate_migration_guides(functions: list) -> str:
    advance_stage("MIGRATION_GUIDES_GENERATED")

    affected = [f for f in functions if f.get("affected")]

    if not affected:
        content = "# Migration Guides\n\nNo functions were identified as affected by high-risk changes.\n"
        Path("migration_guides.md").write_text(content)
        print("[MIGRATION] No affected functions — empty guide written")
        return content

    codebase = Path("codebase_snippet.py").read_text()

    prompt = MIGRATION_PROMPT_TEMPLATE.format(
        affected_json=json.dumps(affected, indent=2),
        codebase=codebase,
    )

    print(f"[MIGRATION] Calling LLM for {len(affected)} affected function(s)...")
    try:
        response = call_llm(
            prompt=prompt,
            stage="MIGRATION_GUIDES_GENERATED",
            source_id="stripe_node",
            entry_ids=[eid for f in affected for eid in f.get("related_entry_ids", [])],
            input_artifacts=["codebase_impact.json", "codebase_snippet.py"],
            output_artifact="migration_guides.md",
        )
        content = f"# Migration Guides\n\nGenerated: {datetime.now().isoformat()}\n\n" + response
    except Exception as e:
        print(f"[MIGRATION] LLM call FAILED — {e}")
        pipeline_state["errors"].append({"stage": "MIGRATION", "error": str(e)})
        content = f"# Migration Guides\n\nGeneration failed: {e}\n"

    Path("migration_guides.md").write_text(content)
    print("[MIGRATION] migration_guides.md written")
    return content

# ─────────────────────────────────────────────
# STAGE: MIGRATION_CODE_VALIDATED
# ─────────────────────────────────────────────
def validate_migration_code() -> list:
    advance_stage("MIGRATION_CODE_VALIDATED")

    content = Path("migration_guides.md").read_text()
    blocks  = re.findall(r'```python\s*([\s\S]+?)```', content)

    results = []
    for i, block in enumerate(blocks):
        block_id = f"block_{i+1}"
        try:
            ast.parse(block)
            results.append({"block_id": block_id, "valid": True, "error": None,
                             "preview": block[:80].replace("\n", " ")})
        except SyntaxError as e:
            results.append({"block_id": block_id, "valid": False,
                             "error": f"SyntaxError at line {e.lineno}: {e.msg}",
                             "preview": block[:80].replace("\n", " ")})
            print(f"[VALIDATE] {block_id}: INVALID — {e.msg}")

    valid_count   = sum(1 for r in results if r["valid"])
    invalid_count = len(results) - valid_count
    print(f"[VALIDATE] {valid_count} valid, {invalid_count} invalid code blocks")

    Path("migration_validation.json").write_text(json.dumps(results, indent=2))
    return results

# ─────────────────────────────────────────────
# STAGE: IMPACT_REPORT_WRITTEN
# ─────────────────────────────────────────────
def write_impact_report(parsed_all: dict, classified: list,
                        functions: list, validation: list):
    advance_stage("IMPACT_REPORT_WRITTEN")

    total_ingested = sum(len(d.get("entries", [])) for d in parsed_all.values())
    recent_count   = total_ingested  # already filtered
    breaking_count = sum(1 for c in classified
                         if c["breaking_risk"] in {"critical", "high"})
    affected_funcs = [f for f in functions if f.get("affected")]

    # Group classified by source
    by_source = {}
    for c in classified:
        src = c["entry_id"].rsplit("-", 1)[0]
        by_source.setdefault(src, []).append(c)

    # Unaffected sources
    affected_sources = {c["entry_id"].rsplit("-", 1)[0]
                        for c in classified if c["breaking_risk"] in {"critical", "high"}}
    unaffected = [sid for sid in parsed_all if sid not in affected_sources]

    # Security alerts
    security = [c for c in classified if c["change_type"] == "security"]

    lines = [
        "# Developer Impact Report",
        f"\nGenerated: {datetime.now().isoformat()}\n",
        "---",
        "",
        "## Executive Summary",
        "",
        f"| Metric | Value |",
        f"|--------|-------|",
        f"| Total changes ingested | {total_ingested} |",
        f"| Recent entries (90-day window) | {recent_count} |",
        f"| Breaking / high-risk changes | {breaking_count} |",
        f"| Affected functions in codebase | {len(affected_funcs)} |",
        "",
        "---",
        "",
        "## Breaking Changes by Source",
        "",
    ]

    for sid, changes in by_source.items():
        high = [c for c in changes if c["breaking_risk"] in {"critical", "high"}]
        lines.append(f"### {sid} ({len(high)} high-risk)")
        if high:
            for c in high:
                lines.append(f"- **[{c['breaking_risk'].upper()}]** `{c['entry_id']}` — "
                              f"{c['change_type']} | {c.get('rationale', '')[:120]}")
        else:
            lines.append("_No high-risk changes._")
        lines.append("")

    lines += [
        "---",
        "",
        "## Codebase Impact",
        "",
    ]

    if not affected_funcs:
        lines.append("No functions in `codebase_snippet.py` are affected by high-risk changes.\n")
    else:
        for f in affected_funcs:
            lines.append(f"### `{f['function_name']}`")
            lines.append(f"- **Breaking detail:** {f.get('breaking_detail', 'N/A')}")
            lines.append(f"- **Suggested fix:** {f.get('suggested_fix_summary', 'N/A')}")
            lines.append(f"- **Related entries:** {', '.join(f.get('related_entry_ids', []))}")
            lines.append("")

    # Embed migration guides
    lines += ["---", "", "## Migration Guides", ""]
    try:
        guide_content = Path("migration_guides.md").read_text()
        # Strip the top heading we added
        guide_body = re.sub(r'^# Migration Guides.*?\n', '', guide_content, flags=re.DOTALL, count=1)
        lines.append(guide_body)
    except FileNotFoundError:
        lines.append("_Migration guides not generated._\n")

    # Unaffected sources
    lines += ["---", "", "## Unaffected Sources", ""]
    if unaffected:
        for sid in unaffected:
            lines.append(f"- `{sid}`")
    else:
        lines.append("_All sources had high-risk changes._")
    lines.append("")

    # Security alerts
    lines += ["---", "", "## Security Alerts", ""]
    if security:
        for s in security:
            lines.append(f"- **{s['entry_id']}**: {s.get('rationale', '')[:200]}")
    else:
        lines.append("_No security changes detected._")
    lines.append("")

    # Version pinning recommendation
    lines += ["---", "", "## Version Pinning Recommendation", ""]
    if breaking_count > 0:
        lines.append("High-risk breaking changes detected. See `version_pinning.md` for pinning details.")
    else:
        lines.append("No breaking changes detected. No immediate pinning required.")
    lines.append("")

    # Validation summary
    lines += ["---", "", "## Migration Code Validation", ""]
    for v in validation:
        status = "✅ VALID" if v["valid"] else "❌ INVALID"
        lines.append(f"- `{v['block_id']}`: {status} — {v['preview']}")
        if not v["valid"]:
            lines.append(f"  - Error: {v['error']}")
    lines.append("")

    Path("impact_report.md").write_text("\n".join(lines))
    print("[REPORT] impact_report.md written")

# ─────────────────────────────────────────────
# OPTIONAL: SECURITY ALERTS
# ─────────────────────────────────────────────
def generate_security_alerts(classified: list):
    security = [c for c in classified if c["change_type"] == "security"]
    alerts = []
    for c in security:
        alerts.append({
            "entry_id":   c["entry_id"],
            "source_id":  c["entry_id"].rsplit("-", 1)[0],
            "severity":   c["breaking_risk"],
            "summary":    c.get("rationale", "Security change detected."),
            "draft_slack_notification": (
                f":rotating_light: *Security Change Alert* — `{c['entry_id']}`\n"
                f"Risk: {c['breaking_risk'].upper()} | {c.get('rationale', '')[:200]}\n"
                f"Please review and update your integration."
            ),
        })

    Path("security_alerts.json").write_text(json.dumps(alerts, indent=2))
    print(f"[SECURITY] {len(alerts)} security alerts written")
    return alerts

# ─────────────────────────────────────────────
# OPTIONAL: VERSION PINNING
# ─────────────────────────────────────────────
def generate_version_pinning(parsed_all: dict, classified: list):
    breaking = [c for c in classified if c["breaking_risk"] in {"critical", "high"}]
    sources_with_breaks = {c["entry_id"].rsplit("-", 1)[0] for c in breaking}

    lines = ["# Version Pinning Recommendations", "",
             f"Generated: {datetime.now().isoformat()}", "",
             "Pin these dependencies to avoid unexpected breaking changes.", ""]

    has_stripe = "stripe_node" in sources_with_breaks or "stripe" in sources_with_breaks
    has_openai = "openai_python" in sources_with_breaks

    if has_stripe:
        lines += [
            "## Stripe Python",
            "```",
            "# requirements.txt",
            "stripe>=5.0.0,<6.0.0",
            "```",
            "**Reason:** Breaking changes detected in recent changelog.",
            "**When to unpin:** After reviewing and applying the migration guides in `migration_guides.md`.",
            "",
            "## Stripe Node.js",
            "```json",
            '// package.json',
            '"stripe": "^12.0.0"',
            "```",
            "**Reason:** High-risk changes in stripe-node SDK.",
            "**When to unpin:** After validating TypeScript migration if applicable.",
            "",
        ]

    if has_openai:
        lines += [
            "## OpenAI Python",
            "```",
            "# requirements.txt",
            "openai>=1.0.0,<2.0.0",
            "```",
            "**Reason:** Breaking changes detected in OpenAI Python SDK changelog.",
            "**When to unpin:** After verifying API compatibility with your prompt code.",
            "",
        ]

    if not has_stripe and not has_openai:
        lines.append("_No breaking changes detected. No pinning currently required._\n")

    Path("version_pinning.md").write_text("\n".join(lines))
    print("[PINNING] version_pinning.md written")

# ─────────────────────────────────────────────
# STRETCH: DELTA PROCESSING
# ─────────────────────────────────────────────
def run_delta_simulation(parsed_all: dict):
    """Snapshot current Stripe entries, add 2 fake entries, re-classify delta only."""
    sid = "stripe_node"
    snapshot = parsed_all.get(sid, {}).get("entries", [])

    snapshot_path = f"parsed_changelogs/{sid}_snapshot.json"
    Path(snapshot_path).write_text(json.dumps(snapshot, indent=2))

    # Two fabricated entries
    fake_entries = [
        {
            "entry_id":        f"{sid}-DELTA-001",
            "source_id":       sid,
            "source":          "Stripe Node.js SDK",
            "version_or_date": "99.0.0",
            "published_at":    datetime.now(timezone.utc).isoformat(),
            "change_title":    "Removed PaymentIntent.confirm() shorthand method",
            "change_body":     "The confirm() shorthand has been removed. Call stripe.paymentIntents.confirm(id) explicitly.",
            "change_type_raw": "breaking",
        },
        {
            "entry_id":        f"{sid}-DELTA-002",
            "source_id":       sid,
            "source":          "Stripe Node.js SDK",
            "version_or_date": "99.0.0",
            "published_at":    datetime.now(timezone.utc).isoformat(),
            "change_title":    "Deprecate Charge.list() in favour of PaymentIntent.list()",
            "change_body":     "Charge.list() is now deprecated. Migrate to PaymentIntent.list() for all charge lookups.",
            "change_type_raw": "deprecation",
        },
    ]

    existing_ids = {e["entry_id"] for e in snapshot}
    delta = [e for e in fake_entries if e["entry_id"] not in existing_ids]

    print(f"[DELTA] Simulated delta: {len(delta)} new entries")

    report = {
        "snapshot_entries": len(snapshot),
        "fabricated_entries": len(fake_entries),
        "delta_entries": len(delta),
        "delta_entry_ids": [e["entry_id"] for e in delta],
        "classification_results": [],
    }

    if delta:
        slim = [{"entry_id": e["entry_id"], "change_title": e["change_title"],
                 "change_body": e["change_body"]} for e in delta]
        prompt = CLASSIFICATION_PROMPT_TEMPLATE.format(
            source_name="Stripe Node.js SDK (delta)",
            entries_json=json.dumps(slim, indent=2),
        )
        try:
            resp = call_llm(
                prompt=prompt,
                stage="DELTA_CLASSIFICATION",
                source_id=sid,
                entry_ids=[e["entry_id"] for e in delta],
                input_artifacts=[snapshot_path],
                output_artifact="delta_processing_report.json",
            )
            classified_delta = extract_json(resp)
            if not isinstance(classified_delta, list):
                classified_delta = [classified_delta]
            report["classification_results"] = [validate_classification(c) for c in classified_delta]
        except Exception as e:
            report["classification_results"] = []
            report["delta_error"] = str(e)

    Path("delta_processing_report.json").write_text(json.dumps(report, indent=2))
    print("[DELTA] delta_processing_report.json written")

# ─────────────────────────────────────────────
# STRETCH: TYPESCRIPT MIGRATION
# ─────────────────────────────────────────────
TS_MIGRATION_PROMPT = """
You are a TypeScript migration guide author.

Below is a Python migration guide for the Stripe Python SDK.
Produce an EQUIVALENT migration guide for the Stripe Node.js SDK (TypeScript).

The TypeScript version must be functionally equivalent to the Python fix.
Use async/await syntax and the official stripe-node SDK.

PYTHON MIGRATION GUIDE:
{python_guide}

OUTPUT: A Markdown document titled "TypeScript Migration Guide" with the same structure
(Why / Before / After) for each function, but using TypeScript/Node.js Stripe SDK equivalents.
"""

def generate_typescript_migration():
    python_guide = Path("migration_guides.md").read_text()
    if "No functions were identified" in python_guide:
        Path("typescript_migration.md").write_text(
            "# TypeScript Migration Guide\n\nNo Python migrations were generated.\n"
        )
        print("[TS] No Python migrations to translate")
        return

    prompt = TS_MIGRATION_PROMPT.format(python_guide=python_guide)
    try:
        response = call_llm(
            prompt=prompt,
            stage="TYPESCRIPT_MIGRATION",
            source_id="stripe_node",
            entry_ids=[],
            input_artifacts=["migration_guides.md"],
            output_artifact="typescript_migration.md",
        )
        Path("typescript_migration.md").write_text(response)
        print("[TS] typescript_migration.md written")
    except Exception as e:
        Path("typescript_migration.md").write_text(
            f"# TypeScript Migration Guide\n\nGeneration failed: {e}\n"
        )
        print(f"[TS] FAILED — {e}")

# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
def main():
    print("\n🚀 Starting Changelog Monitoring Pipeline")
    print(f"   90-day cutoff: {NINETY_DAYS_AGO.date()}\n")

    advance_stage("INIT")
    init_dirs()

    # ── Core pipeline ──────────────────────────
    sources     = load_sources()
    raw         = fetch_changelogs(sources)
    parsed_all  = parse_and_filter(raw)
    classified  = classify_changes(parsed_all)
    high_risk   = select_high_risk_stripe(classified, parsed_all)
    functions   = analyse_codebase_impact(high_risk)
    _           = generate_migration_guides(functions)
    validation  = validate_migration_code()
    write_impact_report(parsed_all, classified, functions, validation)

    # ── Optional outputs ───────────────────────
    advance_stage("OPTIONAL_OUTPUTS_GENERATED")
    generate_security_alerts(classified)
    generate_version_pinning(parsed_all, classified)
    run_delta_simulation(parsed_all)
    generate_typescript_migration()

    advance_stage("VALIDATION_COMPLETE")
    advance_stage("RESULTS_FINALISED")

    # Print summary
    affected = [f for f in functions if f.get("affected")]
    breaking = [c for c in classified if c["breaking_risk"] in {"critical", "high"}]
    total    = sum(len(d.get("entries", [])) for d in parsed_all.values())

    print("\n" + "="*60)
    print("✅ Pipeline complete")
    print(f"   Entries ingested : {total}")
    print(f"   High-risk changes: {len(breaking)}")
    print(f"   Affected functions: {len(affected)}")
    print(f"   Pipeline errors  : {len(pipeline_state['errors'])}")
    if pipeline_state["errors"]:
        for err in pipeline_state["errors"]:
            print(f"   ⚠️  {err}")
    print("="*60 + "\n")


if __name__ == "__main__":
    main()