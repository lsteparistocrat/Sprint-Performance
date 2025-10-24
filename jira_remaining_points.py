#!/usr/bin/env python3
import os, sys, csv, time, math, base64, datetime as dt
import requests
from urllib.parse import urljoin

# -------- Config from env -------
JIRA_BASE_URL        = os.getenv("JIRA_BASE_URL")            # e.g. https://yourdomain.atlassian.net/
JIRA_EMAIL           = os.getenv("JIRA_EMAIL")               # your Atlassian email
JIRA_API_TOKEN       = os.getenv("JIRA_API_TOKEN")           # API token
BOARD_ID             = os.getenv("BOARD_ID")                 # optional if you pass sprint id directly
SPRINT_ID            = os.getenv("SPRINT_ID")                # run for one sprint (preferred)
SP_FIELD             = os.getenv("STORY_POINTS_FIELD")       # e.g. customfield_10016
TRACK_STATUS_NAMES   = os.getenv("TRACK_STATUS_NAMES", "To Do,In Progress,Code Review").split(",")
UAT_STATUS_NAME      = os.getenv("UAT_STATUS_NAME", "UAT")
TREAT_UAT_AS_DONE_PERMANENT = os.getenv("UAT_IS_PERMANENT", "true").lower() == "true"
OUT_DIR              = os.getenv("OUT_DIR", "data")

if not all([JIRA_BASE_URL, JIRA_EMAIL, JIRA_API_TOKEN, SP_FIELD]) or (not SPRINT_ID and not BOARD_ID):
    print("Missing env vars. Need JIRA_BASE_URL, JIRA_EMAIL, JIRA_API_TOKEN, STORY_POINTS_FIELD, and SPRINT_ID or BOARD_ID.", file=sys.stderr)
    sys.exit(2)

auth = (JIRA_EMAIL, JIRA_API_TOKEN)
session = requests.Session()
session.auth = auth
session.headers.update({"Accept":"application/json"})

def get(url, params=None):
    r = session.get(url, params=params, timeout=30)
    if r.status_code == 429:
        time.sleep(5)
        r = session.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def jira_url(path):
    return urljoin(JIRA_BASE_URL, path)

def get_sprint(sprint_id:str):
    return get(jira_url(f"/rest/agile/1.0/sprint/{sprint_id}"))

def list_sprint_issues(sprint_id:str):
    issues = []
    start_at = 0
    while True:
        data = get(jira_url(f"/rest/agile/1.0/sprint/{sprint_id}/issue"), params={"maxResults":50, "startAt":start_at})
        issues.extend(data.get("issues", []))
        if start_at + data.get("maxResults", 50) >= data.get("total", 0):
            break
        start_at += data.get("maxResults", 50)
    return [i["key"] for i in issues]

def get_issue_with_changelog(key:str):
    # paginate changelog if needed
    data = get(jira_url(f"/rest/api/3/issue/{key}"), params={"expand":"changelog"})
    changelog = data.get("changelog", {})
    histories = changelog.get("histories", [])
    total = changelog.get("total", len(histories))
    start_at = changelog.get("startAt", 0)
    max_results = changelog.get("maxResults", len(histories)) or 100
    while start_at + max_results < total:
        start_at += max_results
        page = get(jira_url(f"/rest/api/3/issue/{key}/changelog"), params={"startAt":start_at, "maxResults":100})
        histories.extend(page.get("values", []))
        total = page.get("total", total)
        max_results = page.get("maxResults", 100)
    return data, histories

def iso_to_date(iso):
    # Jira timestamps: 2025-10-24T11:23:00.000+0000
    return dt.datetime.fromisoformat(iso.replace("Z","+00:00").replace("+0000","+00:00")).date()

def daterange(start_date, end_date):
    cur = start_date
    while cur <= end_date:
        yield cur
        cur += dt.timedelta(days=1)

def sprint_date_bounds(sprint):
    # startDate / endDate present on active/closed sprints
    sd = sprint.get("startDate")
    ed = sprint.get("endDate") or sprint.get("completeDate")
    if not sd or not ed:
        raise RuntimeError("Sprint missing startDate/endDate; the board must be a Scrum board with timeboxed sprints.")
    return iso_to_date(sd), iso_to_date(ed)

def extract_status_changes(histories):
    """Return list of (when_date, from, to) for status field."""
    events = []
    for h in histories:
        when = h.get("created")
        for item in h.get("items", []):
            if item.get("field") == "status":
                events.append((iso_to_date(when), item.get("fromString"), item.get("toString")))
    events.sort(key=lambda x: x[0])
    return events

def first_date_reaching_status(histories, target:str):
    for d, _f, to in extract_status_changes(histories):
        if to == target:
            return d
    return None

def status_on_date(initial_status:str, histories, day:dt.date):
    current = initial_status
    for d, _f, to in extract_status_changes(histories):
        if d <= day:
            current = to
        else:
            break
    return current

def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    sprint = get_sprint(SPRINT_ID) if SPRINT_ID else None
    if not sprint and BOARD_ID:
        print("You provided BOARD_ID but not SPRINT_ID. Please specify SPRINT_ID to run for a specific sprint.", file=sys.stderr)
        sys.exit(2)

    sprint_start, sprint_end = sprint_date_bounds(sprint)
    issue_keys = list_sprint_issues(sprint["id"])
    print(f"Found {len(issue_keys)} issues in sprint {sprint['name']} ({sprint['id']}).")

    rows = []
    # Pre-build the daily timeline
    days = list(daterange(sprint_start, sprint_end))

    for key in issue_keys:
        issue, histories = get_issue_with_changelog(key)
        fields = issue.get("fields", {})
        sp = fields.get(SP_FIELD) or 0.0
        # Some teams put points on subtasks too; keep as float
        try:
            sp = float(sp)
        except:
            sp = 0.0

        initial_status = fields["status"]["name"]
        created_date = iso_to_date(fields["created"])
        hit_uat_on = first_date_reaching_status(histories, UAT_STATUS_NAME)

        for day in days:
            # If created after this day, it contributes nothing yet
            if day < created_date:
                continue

            # If once UAT is reached and we consider permanent done
            if hit_uat_on and day >= hit_uat_on and TREAT_UAT_AS_DONE_PERMANENT:
                remain = 0.0
            else:
                status_today = status_on_date(initial_status, histories, day)
                if hit_uat_on and day >= hit_uat_on and not TREAT_UAT_AS_DONE_PERMANENT:
                    # If not permanent, then only 0 while actually in UAT or beyond
                    if status_today == UAT_STATUS_NAME:
                        remain = 0.0
                    else:
                        remain = sp if status_today in TRACK_STATUS_NAMES else 0.0
                else:
                    remain = sp if status_today in TRACK_STATUS_NAMES else 0.0

            rows.append({
                "date": day.isoformat(),
                "issue": key,
                "status": status_on_date(initial_status, histories, day),
                "story_points": sp,
                "remaining_for_issue": remain
            })

        # polite pacing for API limits
        time.sleep(0.1)

    # Aggregate per day
    by_day = {}
    for r in rows:
        by_day.setdefault(r["date"], 0.0)
        by_day[r["date"]] += r["remaining_for_issue"]

    out_path = os.path.join(OUT_DIR, f"sprint_{sprint['id']}_remaining.csv")
    with open(out_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["date","remaining_story_points"])
        for d in sorted(by_day.keys()):
            w.writerow([d, f"{by_day[d]:.2f}"])

    # Optional detailed breakdown per issue per day
    details_path = os.path.join(OUT_DIR, f"sprint_{sprint['id']}_remaining_details.csv")
    with open(details_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["date","issue","status","story_points","remaining_for_issue"])
        w.writeheader()
        for r in sorted(rows, key=lambda x:(x["date"], x["issue"])):
            w.writerow(r)

    print(f"Wrote {out_path}")
    print(f"Wrote {details_path}")

if __name__ == "__main__":
    main()
