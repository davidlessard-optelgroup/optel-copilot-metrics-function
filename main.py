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
    payload = {
        "iat": now - 60,
        "exp": now + 600,
        "iss": APP_ID
    }
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

def get_copilot_metrics(token, date):
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": GH_API_VERSION
    }
    url = f"https://api.github.com/orgs/{ORG}/copilot/metrics/reports/organization-1-day"
    params = {"day": date}
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    download_links = response.json().get("download_links", [])
    print(f"Got {len(download_links)} download link(s) for {date}")
    all_records = []
    for link in download_links:
        r = requests.get(link)
        r.raise_for_status()
        for line in r.text.strip().split("\n"):
            if line:
                all_records.append(json.loads(line))
    return all_records

def insert_metrics(records, date):
    client = bigquery.Client(project=PROJECT_ID)
    daily_rows = []
    editor_rows = []
    chat_rows = []
    for record in records:
        daily_rows.append({
            "date": date,
            "total_active_users": record.get("total_active_users", 0),
            "total_engaged_users": record.get("total_engaged_users", 0)
        })
        completions = record.get("copilot_ide_code_completions") or {}
        for editor in completions.get("editors", []):
            editor_name = editor.get("name")
            for model in editor.get("models", []):
                model_name = model.get("name")
                is_custom = model.get("is_custom_model", False)
                for lang in model.get("languages", []):
                    editor_rows.append({
                        "date": date,
                        "editor": editor_name,
                        "model_name": model_name,
                        "is_custom_model": is_custom,
                        "language": lang.get("name"),
                        "total_engaged_users": lang.get("total_engaged_users", 0),
                        "total_code_suggestions": lang.get("total_code_suggestions", 0),
                        "total_code_acceptances": lang.get("total_code_acceptances", 0),
                        "total_code_lines_suggested": lang.get("total_code_lines_suggested", 0),
                        "total_code_lines_accepted": lang.get("total_code_lines_accepted", 0)
                    })
        chat = record.get("copilot_ide_chat") or {}
        for editor in chat.get("editors", []):
            editor_name = editor.get("name")
            for model in editor.get("models", []):
                chat_rows.append({
                    "date": date,
                    "editor": editor_name,
                    "model_name": model.get("name"),
                    "total_engaged_users": model.get("total_engaged_users", 0),
                    "total_chats": model.get("total_chats", 0),
                    "total_chat_insertion_events": model.get("total_chat_insertion_events", 0),
                    "total_chat_copy_events": model.get("total_chat_copy_events", 0)
                })
    if daily_rows:
        errors = client.insert_rows_json(f"{PROJECT_ID}.{DATASET_ID}.daily_metrics", daily_rows)
        if errors:
            print(f"daily_metrics errors: {errors}")
    if editor_rows:
        errors = client.insert_rows_json(f"{PROJECT_ID}.{DATASET_ID}.editor_model_language", editor_rows)
        if errors:
            print(f"editor_model_language errors: {errors}")
    if chat_rows:
        errors = client.insert_rows_json(f"{PROJECT_ID}.{DATASET_ID}.chat_metrics", chat_rows)
        if errors:
            print(f"chat_metrics errors: {errors}")
    print(f"Inserted: {len(daily_rows)} daily, {len(editor_rows)} editor/model/lang, {len(chat_rows)} chat rows")

@functions_framework.http
def main(request):
    yesterday = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")
    print(f"Fetching Copilot metrics for {yesterday}")
    private_key = get_private_key()
    token = get_installation_token(private_key)
    records = get_copilot_metrics(token, yesterday)
    insert_metrics(records, yesterday)
    return f"OK - {yesterday}", 200
