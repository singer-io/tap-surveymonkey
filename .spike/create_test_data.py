"""
SurveyMonkey test-data script.

Creates a survey with questions, attempts to seed a response, then fetches all
tap streams (surveys, survey_details, responses, simplified_responses) into a
JSON file so you can inspect the exact payloads the tap will produce.

Usage:
    python .spike/create_test_data.py --config tmp/configs/config.json [options]

Options:
    --config       Path to tap config JSON (must contain access_token)
    --access-token OAuth2 token (alternative to --config)
    --output       Output file for stream data  [default: tmp/stream_data.json]
    --fetch-only   Skip survey creation; just fetch streams for the resolved survey
    --cleanup      Delete the survey (and collector) created by this run
    --show-raw     Print every raw API response to stdout
"""

import argparse
import json
import sys
import time
from pathlib import Path

import requests

BASE_URL = "https://api.surveymonkey.com/v3"


# ---------------------------------------------------------------------------
# Core API helpers
# ---------------------------------------------------------------------------

def _get_rate_limit_sleep(resp) -> int:
    """Return seconds to sleep after a 429, or 0 to fall back to exponential."""
    try:
        day_rem = int(resp.headers.get("X-Ratelimit-App-Global-Day-Remaining", -1))
        if day_rem == 0:
            return int(resp.headers.get("X-Ratelimit-App-Global-Day-Reset", 60)) + 2
        min_rem = int(resp.headers.get("X-Ratelimit-App-Global-Minute-Remaining", -1))
        if min_rem == 0:
            return int(resp.headers.get("X-Ratelimit-App-Global-Minute-Reset", 60)) + 2
    except (ValueError, TypeError):
        pass
    return 0


def api(method: str, path: str, token: str, **kwargs):
    url = f"{BASE_URL}/{path.lstrip('/')}"
    max_tries = 5
    for attempt in range(max_tries):
        resp = requests.request(
            method, url,
            headers={"Authorization": f"bearer {token}", "Content-Type": "application/json"},
            timeout=30,
            **kwargs,
        )
        if resp.status_code == 429:
            sleep = _get_rate_limit_sleep(resp) or min(2 ** (attempt + 1), 300)
            print(f"  429 rate-limited - sleeping {sleep}s (attempt {attempt + 1}/{max_tries})")
            time.sleep(sleep)
            continue
        if resp.status_code >= 500 and attempt < max_tries - 1:
            sleep = min(2 ** (attempt + 1), 60)
            print(f"  {resp.status_code} server error - retrying in {sleep}s (attempt {attempt + 1}/{max_tries})")
            time.sleep(sleep)
            continue
        if not resp.ok:
            print(f"  ERROR {resp.status_code}: {resp.text}")
            resp.raise_for_status()
        # 204 No Content or empty body (e.g. DELETE) → return None
        if resp.status_code == 204 or not resp.content:
            return None
        return resp.json()
    # exhausted all retries
    print(f"  ERROR {resp.status_code}: {resp.text}")
    resp.raise_for_status()


def paginate(token: str, path: str, params: dict | None = None) -> list:
    """Walk all pages of a SM list endpoint and return a flat list."""
    params = {**{"per_page": 50, "page": 1}, **(params or {})}
    out = []
    while True:
        data = api("GET", path, token, params=params)
        out.extend(data.get("data", []))
        if not data.get("links", {}).get("next"):
            break
        params["page"] += 1
    return out


# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------

def load_config(args) -> tuple[str, dict | None, str | None]:
    """Return (access_token, cfg_dict_or_None, cfg_path_or_None)."""
    if args.access_token:
        return args.access_token, None, None
    if args.config:
        with open(args.config) as fh:
            cfg = json.load(fh)
        token = cfg.get("access_token") or sys.exit("access_token missing in config.")
        return token, cfg, args.config
    sys.exit("Provide --config or --access-token.")


def save_config(cfg: dict, cfg_path: str):
    with open(cfg_path, "w", encoding="utf-8") as fh:
        json.dump(cfg, fh, indent=4)


def resolve_survey_id(token: str, cli_id: str | None, cfg: dict | None, cfg_path: str | None) -> str | None:
    """
    Return the survey_id to scope fetching to, writing it back to config if freshly discovered.
    Priority: --survey-id CLI > config.json value (validated) > most-recent from API.
    """
    if cli_id:
        return cli_id

    candidate = cfg.get("survey_id") if cfg else None
    if candidate:
        print(f"\n[CONFIG] Validating survey_id {candidate!r} ...")
        try:
            api("GET", f"/surveys/{candidate}", token)
            print(f"  OK - using {candidate!r}")
            return candidate
        except requests.HTTPError as exc:
            code = exc.response.status_code if exc.response is not None else 0
            if code in (403, 404):
                print(f"  {code} - stale survey_id, will auto-fetch.")
            else:
                raise

    print("\n[CONFIG] Auto-fetching most-recent survey ...")
    surveys = paginate(token, "/surveys", {"sort_by": "date_modified", "sort_order": "DESC", "per_page": 1})
    if not surveys:
        print("  No surveys found on this account.")
        return None
    sid = surveys[0]["id"]
    print(f"  Found: {sid!r}  ({surveys[0].get('title', '')})")
    if cfg is not None and cfg_path:
        cfg["survey_id"] = sid
        save_config(cfg, cfg_path)
        print(f"  Written to {cfg_path}")
    return sid


# ---------------------------------------------------------------------------
# Survey / question creation
# ---------------------------------------------------------------------------

QUESTION_DEFS = [
    {
        "headings": [{"heading": "How satisfied are you with our service?"}],
        "family": "single_choice", "subtype": "vertical", "position": 1,
        "answers": {"choices": [
            {"text": "Very satisfied"}, {"text": "Satisfied"}, {"text": "Neutral"},
            {"text": "Dissatisfied"}, {"text": "Very dissatisfied"},
        ]},
    },
    {
        "headings": [{"heading": "Any additional comments?"}],
        "family": "open_ended", "subtype": "essay", "position": 2,
    },
    {
        "headings": [{"heading": "Which features do you use? (select all that apply)"}],
        "family": "multiple_choice", "subtype": "vertical", "position": 3,
        "answers": {"choices": [
            {"text": "Surveys"}, {"text": "Reports"},
            {"text": "Integrations"}, {"text": "Team collaboration"},
        ]},
    },
]


def create_survey_with_questions(token: str) -> tuple[str, str, list]:
    """Create survey + page + questions. Returns (survey_id, page_id, full_question_dicts)."""
    survey = api("POST", "/surveys", token, json={"title": "TAP Test Survey", "language": "en", "nickname": "tap-test"})
    sid = survey["id"]
    print(f"  Survey  id={sid!r}")

    page = api("POST", f"/surveys/{sid}/pages", token, json={"title": "Page 1"})
    pid = page["id"]
    print(f"  Page    id={pid!r}")

    questions = []
    for q_def in QUESTION_DEFS:
        q = api("POST", f"/surveys/{sid}/pages/{pid}/questions", token, json=q_def)
        # Re-fetch to get choice IDs populated
        q_full = api("GET", f"/surveys/{sid}/pages/{pid}/questions/{q['id']}", token)
        q_full["page_id"] = pid
        questions.append(q_full)
        print(f"  Question id={q['id']!r}  family={q_def['family']!r}")

    return sid, pid, questions


# ---------------------------------------------------------------------------
# Collector / response seeding
# ---------------------------------------------------------------------------

def create_collector(token: str, survey_id: str) -> dict | None:
    """Try weblink then manual; return collector dict or None if both fail."""
    for ctype in ("weblink", "manual"):
        try:
            c = api("POST", f"/surveys/{survey_id}/collectors", token,
                    json={"type": ctype, "name": "TAP Test Collector"})
            print(f"  Collector id={c['id']!r}  type={ctype!r}")
            return c
        except requests.HTTPError as exc:
            code = exc.response.status_code if exc.response is not None else 0
            msg = ""
            try:
                msg = exc.response.json().get("error", {}).get("message", "")
            except Exception:
                pass
            print(f"  {code} ({ctype}): {msg}")
            if code in (400, 403, 500):
                continue
            raise
    print("  SKIP - verify email at app.surveymonkey.com to enable collector creation")
    return None


def seed_response(token: str, collector_id: str, questions: list) -> dict:
    """Build and POST a synthetic response for the given questions."""
    answers = []
    for q in questions:
        qid, family = q["id"], q.get("family")
        choices = q.get("answers", {}).get("choices", [])
        if family == "single_choice" and choices:
            answers.append({"question_id": qid, "answers": [{"choice_id": choices[0]["id"]}]})
        elif family == "open_ended":
            answers.append({"question_id": qid, "answers": [{"text": "Automated test response."}]})
        elif family == "multiple_choice" and choices:
            answers.append({"question_id": qid, "answers": [{"choice_id": c["id"]} for c in choices[:2]]})

    page_id = questions[0].get("page_id") if questions else None
    page = {"questions": answers}
    if page_id:
        page["id"] = page_id

    result = api("POST", f"/collectors/{collector_id}/responses", token, json={"pages": [page]})
    print(f"  Response id={result.get('id', '?')!r}")
    return result


# ---------------------------------------------------------------------------
# Stream fetchers  (mirror tap stream definitions)
# ---------------------------------------------------------------------------

def fetch_all_streams(token: str, survey_id: str | None, start_date: str | None = None) -> dict:
    """Fetch all four tap streams; return dict keyed by stream name."""
    print("\n" + "=" * 40 + "\nFetching tap streams ...\n" + "=" * 40)

    # surveys
    print("\n[STREAM] surveys")
    params = {
        "sort_by": "date_modified", "sort_order": "ASC",
        "include": "response_count,date_created,date_modified,language,question_count",
    }
    if start_date:
        params["start_modified_at"] = start_date
    all_surveys = paginate(token, "/surveys", params)
    surveys = [s for s in all_surveys if s["id"] == survey_id] if survey_id else all_surveys
    if not surveys and survey_id:
        surveys = [{"id": survey_id}]   # stub so details/responses still attempt fetch
    print(f"  -> {len(surveys)} record(s)")

    sids = [s["id"] for s in surveys]

    # survey_details
    print("\n[STREAM] survey_details")
    survey_details = []
    for sid in sids:
        print(f"  fetching {sid}")
        survey_details.append(api("GET", f"/surveys/{sid}/details", token))
    print(f"  -> {len(survey_details)} record(s)")

    # responses + simplified_responses share the same logic
    def _fetch_responses(simple: bool) -> list:
        label = "simplified_responses" if simple else "responses"
        print(f"\n[STREAM] {label}")
        rparams: dict = {"sort_by": "date_modified", "sort_order": "ASC"}
        if simple:
            rparams["simple"] = True
        if start_date:
            rparams["start_modified_at"] = start_date
        records = []
        for sid in sids:
            print(f"  fetching {sid}")
            batch = paginate(token, f"/surveys/{sid}/responses/bulk", rparams)
            for r in batch:
                r.setdefault("survey_id", sid)
            records.extend(batch)
        print(f"  -> {len(records)} record(s)")
        return records

    return {
        "surveys": surveys,
        "survey_details": survey_details,
        "responses": _fetch_responses(simple=False),
        "simplified_responses": _fetch_responses(simple=True),
    }


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="SurveyMonkey tap test-data helper")
    parser.add_argument("--config", help="Path to tap config JSON (access_token + optional survey_id)")
    parser.add_argument("--access-token", help="OAuth2 access token (alternative to --config)")
    parser.add_argument("--output", default="tmp/stream_data.json",
                        help="Output JSON file  [default: tmp/stream_data.json]")
    parser.add_argument("--start-date", help="ISO-8601 start date filter (e.g. 2024-01-01T00:00:00Z)")
    parser.add_argument("--survey-id", help="Override survey ID (auto-fetched + saved to config if omitted)")
    parser.add_argument("--fetch-only", action="store_true", help="Skip creation; only fetch streams")
    parser.add_argument("--cleanup", action="store_true", help="Delete created survey/collector when done")
    parser.add_argument("--show-raw", action="store_true", help="Print the aggregated stream data JSON to stdout after all fetches complete")
    args = parser.parse_args()

    token, cfg, cfg_path = load_config(args)
    survey_id = resolve_survey_id(token, args.survey_id, cfg, cfg_path)

    collector = None
    created_survey_id = None

    try:
        if not args.fetch_only:
            print("\n[CREATE] Building survey ...")
            sid, _, questions = create_survey_with_questions(token)
            created_survey_id = sid
            survey_id = sid

            # Persist new survey_id to config immediately
            if cfg is not None and cfg_path:
                cfg["survey_id"] = sid
                save_config(cfg, cfg_path)
                print(f"  [CONFIG] survey_id {sid!r} -> {cfg_path}")

            print("\n[CREATE] Seeding collector + response ...")
            collector = create_collector(token, sid)
            if collector:
                time.sleep(1)
                seed_response(token, collector["id"], questions)
            else:
                print("  Responses will be empty until email is verified at app.surveymonkey.com")

            api("PATCH", f"/surveys/{sid}", token, json={"title": "TAP Test Survey (updated)"})
            print(f"\n[UPDATE] Survey title -> 'TAP Test Survey (updated)'")
            print(f"\nCreated  survey_id={sid!r}  collector={'n/a' if not collector else collector['id']!r}")

        # Fetch all four tap streams
        stream_data = fetch_all_streams(token, survey_id, args.start_date)

        if args.show_raw:
            print(json.dumps(stream_data, indent=2, default=str))

        out = Path(args.output)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(stream_data, indent=2, default=str), encoding="utf-8")
        print(f"\n[OUTPUT] {out.resolve()}")

    finally:
        if args.cleanup:
            print("\n[CLEANUP]")
            for kind, rid, endpoint in [
                ("collector", collector["id"] if collector else None, "collectors"),
                ("survey", created_survey_id, "surveys"),
            ]:
                if rid:
                    try:
                        api("DELETE", f"/{endpoint}/{rid}", token)
                        print(f"  Deleted {kind} {rid}")
                    except requests.HTTPError as exc:
                        print(f"  Could not delete {kind}: {exc}")


if __name__ == "__main__":
    main()
