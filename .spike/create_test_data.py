"""
SurveyMonkey CRUD script to create, read, update, and delete test data,
and fetch data for all tap streams into a file.

Streams covered:
    surveys               – paginated list of all surveys
    survey_details        – full detail record for each survey
    responses             – bulk responses per survey (raw)
    simplified_responses  – bulk responses per survey (simple=true)

Usage:
    python .spike/create_test_data.py --access-token YOUR_TOKEN [options]

Flags:
    --access-token  SurveyMonkey OAuth2 access token (required)
    --cleanup       Delete all created resources at the end (default: False)
    --config        Path to tap JSON config file (alternative to --access-token)
    --survey-id     Scope fetching to a single survey ID (skips listing all surveys)
    --output        Path for the stream-data JSON file (default: tmp/stream_data.json)
    --show-raw      Print every raw API response to stdout
"""

import argparse
import json
import sys
import time
from pathlib import Path

import requests

BASE_URL = "https://api.surveymonkey.com/v3"


# ---------------------------------------------------------------------------
# Low-level helpers
# ---------------------------------------------------------------------------

def _headers(access_token: str) -> dict:
    return {
        "Authorization": f"bearer {access_token}",
        "Content-Type": "application/json",
    }


def api(method: str, path: str, access_token: str, **kwargs):
    """Make a SurveyMonkey API call and return the parsed JSON body."""
    url = f"{BASE_URL}/{path.lstrip('/')}"
    resp = requests.request(method, url, headers=_headers(access_token), timeout=30, **kwargs)
    if not resp.ok:
        print(f"  ERROR {resp.status_code}: {resp.text}")
        resp.raise_for_status()
    return resp.json()


def pretty(label: str, data: dict):
    print(f"\n--- {label} ---")
    print(json.dumps(data, indent=2))


# ---------------------------------------------------------------------------
# CREATE helpers
# ---------------------------------------------------------------------------

def create_survey(access_token: str) -> dict:
    print("\n[CREATE] Survey")
    payload = {
        "title": "TAP Test Survey",
        "language": "en",
        "nickname": "tap-test",
    }
    result = api("POST", "/surveys", access_token, json=payload)
    print(f"  Created survey id={result['id']!r} title={result['title']!r}")
    return result


def create_page(access_token: str, survey_id: str) -> dict:
    print("\n[CREATE] Survey page")
    payload = {"title": "Page 1"}
    result = api("POST", f"/surveys/{survey_id}/pages", access_token, json=payload)
    print(f"  Created page id={result['id']!r}")
    return result


def create_questions(access_token: str, survey_id: str, page_id: str) -> list:
    print("\n[CREATE] Questions")
    questions = []

    # Q1 – single-choice
    q1_payload = {
        "headings": [{"heading": "How satisfied are you with our service?"}],
        "family": "single_choice",
        "subtype": "vertical",
        "answers": {
            "choices": [
                {"text": "Very satisfied"},
                {"text": "Satisfied"},
                {"text": "Neutral"},
                {"text": "Dissatisfied"},
                {"text": "Very dissatisfied"},
            ]
        },
        "position": 1,
    }
    q1 = api("POST", f"/surveys/{survey_id}/pages/{page_id}/questions", access_token, json=q1_payload)
    print(f"  Created single_choice question id={q1['id']!r}")
    questions.append(q1)

    # Q2 – open-ended
    q2_payload = {
        "headings": [{"heading": "Any additional comments?"}],
        "family": "open_ended",
        "subtype": "essay",
        "position": 2,
    }
    q2 = api("POST", f"/surveys/{survey_id}/pages/{page_id}/questions", access_token, json=q2_payload)
    print(f"  Created open_ended question id={q2['id']!r}")
    questions.append(q2)

    # Q3 – multiple-choice (checkboxes)
    q3_payload = {
        "headings": [{"heading": "Which features do you use? (select all that apply)"}],
        "family": "multiple_choice",
        "subtype": "vertical",
        "answers": {
            "choices": [
                {"text": "Surveys"},
                {"text": "Reports"},
                {"text": "Integrations"},
                {"text": "Team collaboration"},
            ]
        },
        "position": 3,
    }
    q3 = api("POST", f"/surveys/{survey_id}/pages/{page_id}/questions", access_token, json=q3_payload)
    print(f"  Created multiple_choice question id={q3['id']!r}")
    questions.append(q3)

    return questions


def activate_survey(access_token: str, survey_id: str) -> bool:
    """Set survey_state to 'open' so collectors can be created on it."""
    print(f"\n[ACTIVATE] Publishing survey {survey_id}")
    try:
        result = api("PATCH", f"/surveys/{survey_id}", access_token,
                     json={"survey_state": "open"})
        print(f"  survey_state → {result.get('survey_state', 'unknown')!r}")
        return True
    except requests.HTTPError as exc:
        print(f"  Warning – could not activate survey: {exc}")
        return False


def create_collector(access_token: str, survey_id: str) -> dict | None:
    """
    Try collector types in order: weblink → manual.
    weblink requires verified email (403 if unverified).
    manual requires the survey to be published/open (500 on DRAFT).
    """
    last_error_msg = ""
    for ctype in ("weblink", "manual"):
        print(f"\n[CREATE] Collector ({ctype})")
        payload = {"type": ctype, "name": f"TAP Test Collector ({ctype})"}
        try:
            result = api("POST", f"/surveys/{survey_id}/collectors", access_token, json=payload)
            print(f"  Created collector id={result['id']!r}  type={ctype!r}  url={result.get('url', 'n/a')!r}")
            return result
        except requests.HTTPError as exc:
            code = exc.response.status_code if exc.response is not None else 0
            try:
                last_error_msg = exc.response.json().get("error", {}).get("message", str(exc))
            except Exception:
                last_error_msg = str(exc)
            print(f"  {code} – {last_error_msg}  (skipping type={ctype!r})")
            if code in (400, 403, 500):
                continue
            raise
    print(f"\n  SKIP – all collector types failed. Last error: {last_error_msg}")
    return None


def create_response(access_token: str, collector_id: str, questions: list) -> dict:
    """Submit a synthetic response through the collector."""
    print("\n[CREATE] Synthetic response")

    # Build answer rows from question definitions
    pages_answers = []
    for q in questions:
        family = q.get("family")
        q_id = q["id"]

        if family == "single_choice":
            choices = q.get("answers", {}).get("choices", [])
            if choices:
                pages_answers.append({
                    "question_id": q_id,
                    "answers": [{"choice_id": choices[0]["id"]}],
                })

        elif family == "open_ended":
            pages_answers.append({
                "question_id": q_id,
                "answers": [{"text": "This is automated test data created by the CRUD script."}],
            })

        elif family == "multiple_choice":
            choices = q.get("answers", {}).get("choices", [])
            if choices:
                # pick first two options
                pages_answers.append({
                    "question_id": q_id,
                    "answers": [{"choice_id": c["id"]} for c in choices[:2]],
                })

    # We need the page_id – retrieve question detail to get it
    # The collector/responses endpoint expects pages array
    page_id = questions[0].get("page_id") if questions else None

    payload = {
        "pages": [
            {
                "id": page_id,
                "questions": pages_answers,
            }
        ]
    }
    # Remove None page id gracefully
    if page_id is None:
        payload["pages"][0].pop("id", None)

    result = api("POST", f"/collectors/{collector_id}/responses", access_token, json=payload)
    print(f"  Created response id={result.get('id', 'unknown')!r}")
    return result


# ---------------------------------------------------------------------------
# READ helpers
# ---------------------------------------------------------------------------

def read_survey_details(access_token: str, survey_id: str) -> dict:
    print("\n[READ] Survey details")
    result = api("GET", f"/surveys/{survey_id}/details", access_token)
    print(f"  Survey title={result['title']!r}  pages={len(result.get('pages', []))}")
    return result


def read_responses(access_token: str, survey_id: str) -> dict:
    print("\n[READ] Survey responses (bulk)")
    result = api("GET", f"/surveys/{survey_id}/responses/bulk", access_token,
                 params={"per_page": 10})
    total = result.get("total", 0)
    count = len(result.get("data", []))
    print(f"  Total responses: {total}  (fetched {count})")
    return result


def read_collectors(access_token: str, survey_id: str) -> dict:
    print("\n[READ] Collectors")
    result = api("GET", f"/surveys/{survey_id}/collectors", access_token)
    print(f"  Collector count: {len(result.get('data', []))}")
    return result


# ---------------------------------------------------------------------------
# UPDATE helpers
# ---------------------------------------------------------------------------

def update_survey_title(access_token: str, survey_id: str) -> dict:
    print("\n[UPDATE] Survey title")
    new_title = "TAP Test Survey (updated)"
    result = api("PATCH", f"/surveys/{survey_id}", access_token,
                 json={"title": new_title})
    print(f"  Updated title → {result['title']!r}")
    return result


def update_collector_name(access_token: str, collector_id: str) -> dict:
    print("\n[UPDATE] Collector name")
    result = api("PATCH", f"/collectors/{collector_id}", access_token,
                 json={"name": "TAP Test Collector (updated)"})
    print(f"  Updated collector name → {result['name']!r}")
    return result


# ---------------------------------------------------------------------------
# DELETE helpers
# ---------------------------------------------------------------------------

def delete_collector(access_token: str, collector_id: str):
    print(f"\n[DELETE] Collector {collector_id}")
    try:
        api("DELETE", f"/collectors/{collector_id}", access_token)
        print("  Deleted.")
    except requests.HTTPError as exc:
        print(f"  Warning – could not delete collector: {exc}")


def delete_survey(access_token: str, survey_id: str):
    print(f"\n[DELETE] Survey {survey_id}")
    try:
        api("DELETE", f"/surveys/{survey_id}", access_token)
        print("  Deleted.")
    except requests.HTTPError as exc:
        print(f"  Warning – could not delete survey: {exc}")


# ---------------------------------------------------------------------------
# Stream-fetch helpers  (mirrors tap stream definitions)
# ---------------------------------------------------------------------------

def _paginate(access_token: str, path: str, params: dict | None = None) -> list:
    """Collect all pages from a paginated SM endpoint and return a flat list."""
    params = dict(params or {})
    params.setdefault("per_page", 50)
    params.setdefault("page", 1)
    results = []
    while True:
        data = api("GET", path, access_token, params=params)
        items = data.get("data", [])
        results.extend(items)
        if not data.get("links", {}).get("next"):
            break
        params["page"] += 1
    return results


def fetch_stream_surveys(access_token: str, start_date: str | None = None) -> list:
    """stream: surveys – paginated list of all surveys."""
    params = {
        "sort_by": "date_modified",
        "sort_order": "ASC",
        "include": "response_count,date_created,date_modified,language,question_count",
    }
    if start_date:
        params["start_modified_at"] = start_date
    return _paginate(access_token, "/surveys", params)


def fetch_stream_survey_details(access_token: str, survey_ids: list[str]) -> list:
    """stream: survey_details – one detail record per survey."""
    records = []
    for sid in survey_ids:
        print(f"  [survey_details] fetching survey {sid}")
        detail = api("GET", f"/surveys/{sid}/details", access_token)
        records.append(detail)
    return records


def fetch_stream_responses(
    access_token: str,
    survey_ids: list[str],
    simple: bool = False,
    start_date: str | None = None,
) -> list:
    """stream: responses / simplified_responses – bulk responses per survey."""
    all_records = []
    for sid in survey_ids:
        label = "simplified_responses" if simple else "responses"
        print(f"  [{label}] fetching survey {sid}")
        params: dict = {
            "sort_by": "date_modified",
            "sort_order": "ASC",
        }
        if simple:
            params["simple"] = "true"
        if start_date:
            params["start_modified_at"] = start_date
        records = _paginate(access_token, f"/surveys/{sid}/responses/bulk", params)
        # tag each record with the parent survey_id for context
        for r in records:
            r.setdefault("survey_id", sid)
        all_records.extend(records)
    return all_records


def fetch_all_streams(
    access_token: str,
    survey_id: str | None = None,
    start_date: str | None = None,
) -> dict:
    """
    Fetch data for every tap stream and return a dict keyed by stream name.

    If *survey_id* is given only that survey is fetched; otherwise all surveys
    accessible by the token are discovered first.
    """
    print("\n========================================")
    print("Fetching data for all tap streams …")
    print("========================================")

    # ---- surveys ----
    print("\n[STREAM] surveys")
    if survey_id:
        # minimal survey record so downstream streams still work
        surveys = _paginate(
            access_token, "/surveys",
            {"include": "response_count,date_created,date_modified,language,question_count"}
        )
        surveys = [s for s in surveys if s["id"] == survey_id] or [{"id": survey_id}]
    else:
        surveys = fetch_stream_surveys(access_token, start_date)
    print(f"  → {len(surveys)} survey(s)")

    survey_ids = [s["id"] for s in surveys]

    # ---- survey_details ----
    print("\n[STREAM] survey_details")
    survey_details = fetch_stream_survey_details(access_token, survey_ids)
    print(f"  → {len(survey_details)} record(s)")

    # ---- responses ----
    print("\n[STREAM] responses")
    responses = fetch_stream_responses(access_token, survey_ids, simple=False, start_date=start_date)
    print(f"  → {len(responses)} record(s)")

    # ---- simplified_responses ----
    print("\n[STREAM] simplified_responses")
    simplified_responses = fetch_stream_responses(access_token, survey_ids, simple=True, start_date=start_date)
    print(f"  → {len(simplified_responses)} record(s)")

    return {
        "surveys": surveys,
        "survey_details": survey_details,
        "responses": responses,
        "simplified_responses": simplified_responses,
    }


def write_stream_data(stream_data: dict, output_path: str):
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as fh:
        json.dump(stream_data, fh, indent=2, default=str)
    print(f"\n[OUTPUT] Stream data written to: {path.resolve()}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def load_config(args) -> tuple[str, dict | None, str | None]:
    """
    Returns (access_token, config_dict_or_None, config_path_or_None).
    config_dict is only set when --config was supplied so we can write back to it.
    """
    if args.access_token:
        return args.access_token, None, None
    if args.config:
        with open(args.config) as fh:
            cfg = json.load(fh)
        token = cfg.get("access_token")
        if not token:
            sys.exit("access_token not found in config file.")
        return token, cfg, args.config
    sys.exit("Provide --access-token or --config.")


def resolve_survey_id(
    access_token: str,
    cli_survey_id: str | None,
    cfg: dict | None,
    cfg_path: str | None,
) -> str | None:
    """
    Determine the survey_id to use.

    Priority:
      1. --survey-id CLI flag  (trusted as-is)
      2. survey_id already in config.json  →  validated with a live API call;
         if 404 / inaccessible it is discarded and we fall through to step 3
      3. Most-recently-modified survey from the API  →  written back to config.json
    """
    def _write_back(sid: str):
        if cfg is not None and cfg_path:
            cfg["survey_id"] = sid
            with open(cfg_path, "w", encoding="utf-8") as fh:
                json.dump(cfg, fh, indent=4)
            print(f"  [CONFIG] survey_id {sid!r} written back to {cfg_path}")

    def _verify(sid: str) -> bool:
        """Return True if the survey is accessible under this token."""
        try:
            api("GET", f"/surveys/{sid}", access_token)
            return True
        except requests.HTTPError as exc:
            code = exc.response.status_code if exc.response is not None else 0
            if code in (403, 404):
                print(f"  [CONFIG] survey_id {sid!r} returned {code} – discarding.")
                return False
            raise

    if cli_survey_id:
        return cli_survey_id

    if cfg and cfg.get("survey_id"):
        candidate = cfg["survey_id"]
        print(f"\n[CONFIG] Validating survey_id from config: {candidate!r} …")
        if _verify(candidate):
            print(f"  OK – using survey_id {candidate!r}")
            return candidate
        # fall through to auto-fetch

    print("\n[CONFIG] Fetching most-recently-modified survey from API …")
    surveys = _paginate(
        access_token, "/surveys",
        {"sort_by": "date_modified", "sort_order": "DESC", "per_page": 1},
    )
    if not surveys:
        print("  No surveys found for this account.")
        return None

    survey_id = surveys[0]["id"]
    print(f"  Found survey: {survey_id!r}  ({surveys[0].get('title', '')})")
    _write_back(survey_id)
    return survey_id


def load_access_token(args) -> str:
    """Kept for backwards-compat with the CRUD helpers that call it directly."""
    token, _, _ = load_config(args)
    return token


def main():
    parser = argparse.ArgumentParser(description="SurveyMonkey CRUD test-data script")
    parser.add_argument("--access-token", help="SurveyMonkey OAuth2 access token")
    parser.add_argument("--config", help="Path to tap JSON config file")
    parser.add_argument("--cleanup", action="store_true",
                        help="Delete created resources when done")
    parser.add_argument("--show-raw", action="store_true",
                        help="Print raw JSON for every API response")
    parser.add_argument("--survey-id",
                        help="Override survey ID (default: read from config, or auto-fetched and "
                             "written back to config if absent)")
    parser.add_argument("--output", default="tmp/stream_data.json",
                        help="Output file path for stream data (default: tmp/stream_data.json)")
    parser.add_argument("--start-date",
                        help="start_date filter for surveys/responses (ISO-8601, e.g. 2024-01-01T00:00:00Z)")
    parser.add_argument("--fetch-only", action="store_true",
                        help="Skip CRUD step and only fetch stream data for existing surveys")
    parser.add_argument("--add-responses", action="store_true",
                        help="For use with --fetch-only + --survey-id: create a collector and "
                             "synthetic responses on the existing survey before fetching streams")
    args = parser.parse_args()

    access_token, cfg, cfg_path = load_config(args)
    # Resolve (and if needed fetch+persist) the survey_id before doing anything else
    resolved_survey_id = resolve_survey_id(access_token, args.survey_id, cfg, cfg_path)

    created: dict = {}

    try:
        if not args.fetch_only:
            # ---- CREATE ----
            survey = create_survey(access_token)
            created["survey_id"] = survey["id"]
            if args.show_raw:
                pretty("Survey (create)", survey)
            # Persist the new survey_id back to config.json immediately
            if cfg is not None and cfg_path:
                cfg["survey_id"] = survey["id"]
                with open(cfg_path, "w", encoding="utf-8") as fh:
                    json.dump(cfg, fh, indent=4)
                print(f"  [CONFIG] survey_id {survey['id']!r} written to {cfg_path}")

            page = create_page(access_token, survey["id"])
            if args.show_raw:
                pretty("Page (create)", page)

            # Pass page_id back into question dicts so we can use it later
            questions = create_questions(access_token, survey["id"], page["id"])
            # Fetch full question details (include choices with IDs)
            questions_full = []
            for q in questions:
                full = api("GET", f"/surveys/{survey['id']}/pages/{page['id']}/questions/{q['id']}",
                           access_token)
                full["page_id"] = page["id"]
                questions_full.append(full)
                if args.show_raw:
                    pretty(f"Question {q['id']} (full)", full)

            collector = create_collector(access_token, survey["id"])
            if collector:
                created["collector_id"] = collector["id"]
                if args.show_raw:
                    pretty("Collector (create)", collector)

                # Small pause so the collector is fully active before submitting
                time.sleep(1)
                response = create_response(access_token, collector["id"], questions_full)
                if args.show_raw:
                    pretty("Response (create)", response)
            else:
                print("  NOTE: Collector could not be created – responses stream will be empty.")
                print("  To populate responses, verify your email at app.surveymonkey.com,")
                print("  then submit responses manually or re-run with --add-responses.")

            # ---- READ ----
            survey_details = read_survey_details(access_token, survey["id"])
            if args.show_raw:
                pretty("Survey details (read)", survey_details)

            responses = read_responses(access_token, survey["id"])
            if args.show_raw:
                pretty("Responses (read)", responses)

            collectors = read_collectors(access_token, survey["id"])
            if args.show_raw:
                pretty("Collectors (read)", collectors)

            # ---- UPDATE ----
            update_survey_title(access_token, survey["id"])
            if collector:
                update_collector_name(access_token, collector["id"])
            else:
                print("\n[UPDATE] Collector name – SKIP (no collector created)")

            # ---- Summary ----
            print("\n========================================")
            print("Test data summary:")
            print(f"  Survey ID    : {created.get('survey_id')}")
            print(f"  Collector ID : {created.get('collector_id')}")
            print(f"  Collector URL: {collector.get('url', 'n/a') if collector else 'n/a'}")
            print("========================================")

            if not args.cleanup:
                print("\nResources left in place.  Re-run with --cleanup to delete them.")
                print("You can use these IDs in your tap config (survey_id) for integration tests.")

        # ---- ADD RESPONSES to an existing survey (--fetch-only + --add-responses) ----
        if args.fetch_only and args.add_responses:
            target_id = resolved_survey_id
            if not target_id:
                print("\n[ADD-RESPONSES] No survey_id available; skipping.")
            else:
                print(f"\n[ADD-RESPONSES] Seeding collector + responses for survey {target_id}")
                # Retrieve full question definitions (with answer choice IDs)
                details = api("GET", f"/surveys/{target_id}/details", access_token)
                questions_full = []
                for page in details.get("pages", []):
                    for q in page.get("questions", []):
                        q["page_id"] = page["id"]
                        questions_full.append(q)

                if not questions_full:
                    print("  No questions found on survey – cannot create responses.")
                else:
                    coll = create_collector(access_token, target_id)
                    if coll:
                        created["collector_id"] = coll["id"]
                        time.sleep(1)
                        create_response(access_token, coll["id"], questions_full)
                    else:
                        print("  Could not create collector; responses not seeded.")
                        print("  Verify your email at app.surveymonkey.com and re-try.")

        # ---- FETCH ALL STREAMS ----
        # Prefer the just-created survey; fall back to the resolved survey_id.
        scope_survey_id = created.get("survey_id") or resolved_survey_id
        stream_data = fetch_all_streams(
            access_token,
            survey_id=scope_survey_id,
            start_date=args.start_date,
        )
        write_stream_data(stream_data, args.output)

        # Optionally dump to stdout
        if args.show_raw:
            pretty("All stream data", stream_data)

    finally:
        if args.cleanup and created:
            print("\n[CLEANUP] Removing created resources…")
            if "collector_id" in created:
                delete_collector(access_token, created["collector_id"])
            if "survey_id" in created:
                delete_survey(access_token, created["survey_id"])
            print("[CLEANUP] Done.")


if __name__ == "__main__":
    main()
