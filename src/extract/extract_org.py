import os
import requests
import time
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

ZENDESK_SUBDOMAIN = os.getenv("ZENDESK_SUBDOMAIN")
ZENDESK_EMAIL = os.getenv("ZENDESK_EMAIL")
ZENDESK_TOKEN = os.getenv("ZENDESK_TOKEN")

def get_unix_timestamp_of_yesterday():
    yesterday = datetime.utcnow().date() - timedelta(days=1)
    start_of_yesterday = datetime.combine(yesterday, datetime.min.time())
    return int(time.mktime(start_of_yesterday.timetuple()))

def extract_organizations_incremental():
    unix_yesterday = get_unix_timestamp_of_yesterday()
    url = f"https://{ZENDESK_SUBDOMAIN}.zendesk.com/api/v2/incremental/organizations.json?start_time={unix_yesterday}"
    auth = (f"{ZENDESK_EMAIL}/token", ZENDESK_TOKEN)
    headers = {"Content-Type": "application/json"}

    organizations = []
    while url:
        response = requests.get(url, headers=headers, auth=auth)
        if response.status_code != 200:
            raise Exception(f"Error al obtener organizaciones incrementales: {response.status_code} - {response.text}")
        
        data = response.json()
        organizations.extend(data.get("organizations", []))
        url = data.get("next_page") if data.get("end_of_stream") is False else None

    return organizations
