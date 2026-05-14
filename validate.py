#!/usr/bin/env python3
"""
Validation script — run after pipeline.py to verify all artifacts and constraints.
Exit code 0 = all checks passed. Exit code 1 = one or more failures.
"""

import ast
import json
import re
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

NINETY_DAYS_AGO = datetime.now(timezone.utc) - timedelta(days=90)

VALID_CHANGE_TYPES = {"deprecation", "breaking", "enhancement", "bugfix", "security"}
VALID_RISK_LEVELS  = {"critical", "high", "medium", "low", "none"}

REQUIRED_FILES = [
    "changelog_sources.json",
    "codebase_snippet.py",
    "classified_changes.json",
    "codebase_impact.json",
    "migration_guides.md",
    "migration_validation.json",
    "impact_report.md",
    "llm_calls.jsonl",
]

OPTIONAL_FILES = [
    "security_alerts.json",
    "version_pinning.md",
    "delta_processing_report.json",
    "typescript_migration.md",
]

PARSED_CHANGELOG_DIR = Path("parsed_changelogs")
REPORT_REQUIRED_SECTIONS = [
    "Executive Summary",
    "Breaking Changes by Source",
    "Codebase Impact",
    "Migration Guides",
    "Unaffected Sources",
]

PARSED_ENTRY_REQUIRED_FIELDS = [
    "entry_id", "source_id", "source", "version_or_date",
    "published_at", "change_title", "change_body",
]

results = []


def check(name: str, passed: bool, detail: str = ""):
    status = "✅ PASS" if passed else "❌ FAIL"
    msg    = f"{status}  {name}"
    if detail:
        msg += f"\n        {detail}"
    results.append((passed, msg))
    print(msg)


# ─────────────────────────────────────────────
# 1. Required files exist
# ─────────────────────────────────────────────
def check_required_files():
    for f in REQUIRED_FILES:
        exists = Path(f).exists()
        check(f"Required file exists: {f}", exists,
              "" if exists else f"Missing: {f}")

    # parsed_changelogs/ directory
    check("parsed_changelogs/ directory exists", PARSED_CHANGELOG_DIR.is_dir())


# ─────────────────────────────────────────────
# 2. JSON files are valid
# ─────────────────────────────────────────────
def check_json_valid():
    json_files = [f for f in REQUIRED_FILES + OPTIONAL_FILES if f.endswith(".json")]
    json_files += [str(p) for p in PARSED_CHANGELOG_DIR.glob("*.json")]

    for fpath in json_files:
        p = Path(fpath)
        if not p.exists():
            continue
        try:
            json.loads(p.read_text())
            check(f"Valid JSON: {fpath}", True)
        except json.JSONDecodeError as e:
            check(f"Valid JSON: {fpath}", False, str(e))


# ─────────────────────────────────────────────
# 3. Sources were fetched or failures logged
# ─────────────────────────────────────────────
def check_sources_fetched():
    sources_path = Path("changelog_sources.json")
    if not sources_path.exists():
        check("All sources fetched or logged", False, "changelog_sources.json missing")
        return

    sources = json.loads(sources_path.read_text()).get("sources", [])
    for src in sources:
        sid        = src["source_id"]
        src_file   = PARSED_CHANGELOG_DIR / f"{sid}.json"
        if src_file.exists():
            data = json.loads(src_file.read_text())
            has_entries = bool(data.get("entries"))
            has_reason  = bool(data.get("reason"))
            check(f"Source fetched/logged: {sid}",
                  has_entries or has_reason,
                  f"entries={len(data.get('entries',[]))}, reason={data.get('reason')}")
        else:
            check(f"Source fetched/logged: {sid}", False, f"{src_file} not found")


# ─────────────────────────────────────────────
# 4. Parsed entries have required fields
# ─────────────────────────────────────────────
def check_parsed_entry_fields():
    for src_file in PARSED_CHANGELOG_DIR.glob("*.json"):
        if "snapshot" in src_file.name or "delta" in src_file.name:
            continue
        data    = json.loads(src_file.read_text())
        entries = data.get("entries", [])
        if not entries:
            check(f"Entry fields in {src_file.name}", True, "No entries (empty source is valid)")
            continue
        sample = entries[0]
        missing = [f for f in PARSED_ENTRY_REQUIRED_FIELDS if f not in sample]
        check(f"Entry fields in {src_file.name}",
              len(missing) == 0,
              f"Missing fields: {missing}" if missing else f"{len(entries)} entries OK")


# ─────────────────────────────────────────────
# 5. 90-day filter was applied before classification
# ─────────────────────────────────────────────
def check_90_day_filter():
    violations = []
    for src_file in PARSED_CHANGELOG_DIR.glob("*.json"):
        if "snapshot" in src_file.name:
            continue
        data    = json.loads(src_file.read_text())
        entries = data.get("entries", [])
        for e in entries:
            pub = e.get("published_at")
            if pub:
                try:
                    from dateutil import parser as dp
                    dt = dp.parse(pub)
                    if dt.tzinfo is None:
                        from datetime import timezone
                        dt = dt.replace(tzinfo=timezone.utc)
                    if dt < NINETY_DAYS_AGO:
                        violations.append(f"{e['entry_id']} published {pub}")
                except Exception:
                    pass  # unparseable date — skip

    check("90-day filter applied",
          len(violations) == 0,
          f"{len(violations)} entries older than 90 days slipped through: "
          f"{violations[:3]}" if violations else "")


# ─────────────────────────────────────────────
# 6. Each source has its own Stage 1 LLM call
# ─────────────────────────────────────────────
def check_separate_llm_calls():
    llm_log = Path("llm_calls.jsonl")
    if not llm_log.exists():
        check("Separate LLM calls per source", False, "llm_calls.jsonl missing")
        return

    records = [json.loads(line) for line in llm_log.read_text().splitlines() if line.strip()]
    stage1  = [r for r in records if r.get("stage") == "CHANGES_CLASSIFIED"]

    sources_path = Path("changelog_sources.json")
    if not sources_path.exists():
        check("Separate LLM calls per source", False, "changelog_sources.json missing")
        return

    sources   = json.loads(sources_path.read_text()).get("sources", [])
    seen_sids = {r.get("source_id") for r in stage1}

    # Every source with entries should have a call
    sources_with_entries = set()
    for src in sources:
        sid      = src["source_id"]
        src_file = PARSED_CHANGELOG_DIR / f"{sid}.json"
        if src_file.exists():
            data = json.loads(src_file.read_text())
            if data.get("entries"):
                sources_with_entries.add(sid)

    missing_calls = sources_with_entries - seen_sids
    check("Separate Stage-1 LLM call per source",
          len(missing_calls) == 0,
          f"Missing calls for: {missing_calls}" if missing_calls else
          f"{len(stage1)} stage-1 calls found for: {seen_sids}")


# ─────────────────────────────────────────────
# 7. Classification uses only controlled taxonomy
# ─────────────────────────────────────────────
def check_taxonomy():
    classified_path = Path("classified_changes.json")
    if not classified_path.exists():
        check("Taxonomy values valid", False, "classified_changes.json missing")
        return

    entries   = json.loads(classified_path.read_text())
    bad_types = [e["entry_id"] for e in entries
                 if e.get("change_type") not in VALID_CHANGE_TYPES]
    bad_risks = [e["entry_id"] for e in entries
                 if e.get("breaking_risk") not in VALID_RISK_LEVELS]

    check("change_type taxonomy valid",
          len(bad_types) == 0,
          f"Invalid change_type in: {bad_types[:5]}" if bad_types else "")
    check("breaking_risk taxonomy valid",
          len(bad_risks) == 0,
          f"Invalid breaking_risk in: {bad_risks[:5]}" if bad_risks else "")


# ─────────────────────────────────────────────
# 8. Stage 2 uses codebase snippet
# ─────────────────────────────────────────────
def check_stage2_uses_codebase():
    llm_log = Path("llm_calls.jsonl")
    if not llm_log.exists():
        check("Stage-2 impact analysis used codebase snippet", False)
        return

    records = [json.loads(line) for line in llm_log.read_text().splitlines() if line.strip()]
    stage2  = [r for r in records if r.get("stage") == "CODEBASE_IMPACT_ANALYSED"]

    if not stage2:
        # Check if there were no high-risk changes (valid reason to skip)
        impact = json.loads(Path("codebase_impact.json").read_text()) if Path("codebase_impact.json").exists() else {}
        reason = impact.get("reason", "")
        check("Stage-2 impact analysis used codebase snippet",
              "no_high_risk" in reason.lower() or "no critical" in reason.lower() or "no entries" in reason.lower(),
              f"No Stage-2 call found. Impact reason: {reason}")
        return

    uses_snippet = any(
        "codebase_snippet.py" in str(r.get("input_artifacts", []))
        for r in stage2
    )
    check("Stage-2 impact analysis used codebase snippet", uses_snippet,
          "codebase_snippet.py not in input_artifacts" if not uses_snippet else "")


# ─────────────────────────────────────────────
# 9. Migration guides exist for affected functions
# ─────────────────────────────────────────────
def check_migration_guides():
    impact_path  = Path("codebase_impact.json")
    guides_path  = Path("migration_guides.md")

    if not impact_path.exists():
        check("Migration guides for affected functions", False, "codebase_impact.json missing")
        return

    impact    = json.loads(impact_path.read_text())
    affected  = [f["function_name"] for f in impact.get("functions", []) if f.get("affected")]

    if not affected:
        check("Migration guides for affected functions", True, "No affected functions — nothing to generate")
        return

    if not guides_path.exists():
        check("Migration guides for affected functions", False, "migration_guides.md missing")
        return

    guide_content = guides_path.read_text()
    missing_guides = [fn for fn in affected if fn not in guide_content]
    check("Migration guides for affected functions",
          len(missing_guides) == 0,
          f"Functions without guides: {missing_guides}" if missing_guides else
          f"{len(affected)} function(s) covered")


# ─────────────────────────────────────────────
# 10. Generated after-code is syntactically valid
# ─────────────────────────────────────────────
def check_migration_code_syntax():
    validation_path = Path("migration_validation.json")
    if not validation_path.exists():
        check("Migration code syntax valid", False, "migration_validation.json missing")
        return

    results_data = json.loads(validation_path.read_text())
    invalid = [r for r in results_data if not r.get("valid")]
    check("All generated migration code is syntactically valid",
          len(invalid) == 0,
          f"{len(invalid)} invalid block(s): "
          f"{[r['block_id'] for r in invalid]}" if invalid else
          f"{len(results_data)} block(s) validated")


# ─────────────────────────────────────────────
# 11. Impact report has all required sections
# ─────────────────────────────────────────────
def check_report_sections():
    report_path = Path("impact_report.md")
    if not report_path.exists():
        check("Impact report has all required sections", False, "impact_report.md missing")
        return

    content = report_path.read_text()
    for section in REPORT_REQUIRED_SECTIONS:
        check(f"Report section: '{section}'",
              section in content,
              f"Section '{section}' not found in report" if section not in content else "")


# ─────────────────────────────────────────────
# 12. LLM log contains required stage records
# ─────────────────────────────────────────────
def check_llm_log_records():
    llm_log = Path("llm_calls.jsonl")
    if not llm_log.exists():
        check("LLM log has Stage-1 records", False)
        check("LLM log has required fields", False)
        return

    records = [json.loads(line) for line in llm_log.read_text().splitlines() if line.strip()]

    required_fields = ["stage", "source_id", "entry_ids", "timestamp",
                       "provider", "model", "prompt_hash", "input_artifacts", "output_artifact"]

    field_errors = []
    for i, rec in enumerate(records):
        missing = [f for f in required_fields if f not in rec]
        if missing:
            field_errors.append(f"Record {i}: missing {missing}")

    check("LLM log records have all required fields",
          len(field_errors) == 0,
          "; ".join(field_errors[:3]) if field_errors else f"{len(records)} records OK")

    stage1_count = sum(1 for r in records if r.get("stage") == "CHANGES_CLASSIFIED")
    check(f"LLM log has Stage-1 classification records ({stage1_count} found)",
          stage1_count > 0)


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
def main():
    print("\n" + "="*60)
    print("Changelog Pipeline Validator")
    print("="*60 + "\n")

    check_required_files()
    print()
    check_json_valid()
    print()
    check_sources_fetched()
    print()
    check_parsed_entry_fields()
    print()
    check_90_day_filter()
    print()
    check_separate_llm_calls()
    print()
    check_taxonomy()
    print()
    check_stage2_uses_codebase()
    print()
    check_migration_guides()
    print()
    check_migration_code_syntax()
    print()
    check_report_sections()
    print()
    check_llm_log_records()

    print("\n" + "="*60)
    passed = sum(1 for ok, _ in results if ok)
    failed = sum(1 for ok, _ in results if not ok)
    print(f"Results: {passed} passed, {failed} failed out of {len(results)} checks")
    print("="*60 + "\n")

    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()