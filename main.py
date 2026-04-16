import functions_framework
import jwt
import time
import requests
from google.cloud import bigquery, secretmanager
from datetime import datetime, timedelta
import json

PROJECT_ID = "ai-statistics-493215"
DATASET_ID = "github_copilot"
APP_ID = "3379809"
INSTALLATION_ID = "123996873"
ORG = "optelgroup-copilot"
SECRET_NAME = f"projects/{PROJECT_ID}/secrets/github-copilot-private-key/versions/latest"
GH_API_VERSION = "2026-03-10"

def get_private_key():
    client = secretmanager.SecretManagerServiceClient()
    response = client.access_secret_version(request={"name": SECRET_NAME})
    return response.payload.data.decode("UTF-8")

def get_installation_token(private_key):
    now = int(time.time())
    payload = {"iat": now - 60, "exp": now + 600, "iss": APP_ID}
    encoded_jwt = jwt.encode(payload, private_key, algorithm="RS256")
    headers = {
        "Authorization": f"Bearer {encoded_jwt}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": GH_API_VERSION
    }
    url = f"https://api.github.com/app/installations/{INSTALLATION_ID}/access_tokens"
    response = requests.post(url, headers=headers)
    response.raise_for_status()
    return response.json()["token"]

def fetch_ndjson(token, endpoint, date):
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": GH_API_VERSION
    }
    url = f"https://api.github.com/orgs/{ORG}/copilot/metrics/reports/{endpoint}"
    r = requests.get(url, headers=headers, params={"day": date})
    if r.status_code in [404, 204]:
        return []
    r.raise_for_status()
    links = r.json().get("download_links", [])
    records = []
    for link in links:
        resp = requests.get(link)
        resp.raise_for_status()
        for line in resp.text.strip().split("\n"):
            if line:
                records.append(json.loads(line))
    return records

def insert_org_metrics(records, date):
    client = bigquery.Client(project=PROJECT_ID)
    rows = []
    for r in records:
        rows.append({
            "day": date,
            "organization_id": r.get("organization_id", ""),
            "daily_active_users": r.get("daily_active_users", 0),
            "daily_active_cli_users": r.get("daily_active_cli_users", 0),
            "daily_active_copilot_cloud_agent_users": r.get("daily_active_copilot_cloud_agent_users", 0),
            "weekly_active_users": r.get("weekly_active_users", 0),
            "monthly_active_users": r.get("monthly_active_users", 0),
            "monthly_active_chat_users": r.get("monthly_active_chat_users", 0),
            "monthly_active_agent_users": r.get("monthly_active_agent_users", 0),
            "user_initiated_interaction_count": r.get("user_initiated_interaction_count", 0),
            "code_generation_activity_count": r.get("code_generation_activity_count", 0),
            "code_acceptance_activity_count": r.get("code_acceptance_activity_count", 0)
        })
    if rows:
        errors = client.insert_rows_json(f"{PROJECT_ID}.{DATASET_ID}.daily_org_metrics", rows)
        if errors:
            print(f"daily_org_metrics errors: {errors}")
    print(f"  Org metrics: {len(rows)} rows")

def insert_user_metrics(records, date):
    client = bigquery.Client(project=PROJECT_ID)
    ide_rows = []
    model_rows = []
    for r in records:
        user_login = r.get("user_login", "")
        user_id = r.get("user_id", 0)
        org_id = r.get("organization_id", "")
        for ide in r.get("totals_by_ide", []):
            plugin_version = ""
            ide_version = ""
            lpv = ide.get("last_known_plugin_version")
            liv = ide.get("last_known_ide_version")
            if lpv:
                plugin_version = lpv.get("plugin_version", "")
            if liv:
                ide_version = liv.get("ide_version", "")
            ide_rows.append({
                "day": date,
                "user_login": user_login,
                "user_id": user_id,
                "organization_id": org_id,
                "ide": ide.get("ide", ""),
                "user_initiated_interaction_count": ide.get("user_initiated_interaction_count", 0),
                "code_generation_activity_count": ide.get("code_generation_activity_count", 0),
                "code_acceptance_activity_count": ide.get("code_acceptance_activity_count", 0),
                "loc_suggested_to_add_sum": ide.get("loc_suggested_to_add_sum", 0),
                "loc_suggested_to_delete_sum": ide.get("loc_suggested_to_delete_sum", 0),
                "loc_added_sum": ide.get("loc_added_sum", 0),
                "loc_deleted_sum": ide.get("loc_deleted_sum", 0),
                "plugin_version": plugin_version,
                "ide_version": ide_version
            })
        for lm in r.get("totals_by_language_model", []):
            model_rows.append({
                "day": date,
                "user_login": user_login,
                "user_id": user_id,
                "organization_id": org_id,
                "language": lm.get("language", ""),
                "model": lm.get("model", ""),
                "code_generation_activity_count": lm.get("code_generation_activity_count", 0),
                "code_acceptance_activity_count": lm.get("code_acceptance_activity_count", 0),
                "loc_suggested_to_add_sum": lm.get("loc_suggested_to_add_sum", 0),
                "loc_suggested_to_delete_sum": lm.get("loc_suggested_to_delete_sum", 0),
                "loc_added_sum": lm.get("loc_added_sum", 0),
                "loc_deleted_sum": lm.get("loc_deleted_sum", 0)
            })
    if ide_rows:
        errors = client.insert_rows_json(f"{PROJECT_ID}.{DATASET_ID}.user_daily_by_ide", ide_rows)
        if errors:
            print(f"user_daily_by_ide errors: {errors}")
    if model_rows:
        errors = client.insert_rows_json(f"{PROJECT_ID}.{DATASET_ID}.user_language_model", model_rows)
        if errors:
            print(f"user_language_model errors: {errors}")
    print(f"  User metrics: {len(ide_rows)} ide rows, {len(model_rows)} model rows")

@functions_framework.http
def main(request):
    yesterday = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")
    print(f"Fetching Copilot metrics for {yesterday}")
    private_key = get_private_key()
    token = get_installation_token(private_key)
    org_records = fetch_ndjson(token, "organization-1-day", yesterday)
    insert_org_metrics(org_records, yesterday)
    user_records = fetch_ndjson(token, "users-1-day", yesterday)
    insert_user_metrics(user_records, yesterday)
    return f"OK - {yesterday}", 200
