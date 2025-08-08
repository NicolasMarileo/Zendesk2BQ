import requests
import os
from dotenv import load_dotenv

load_dotenv()

ZENDESK_SUBDOMAIN = os.getenv("ZENDESK_SUBDOMAIN")
ZENDESK_EMAIL = os.getenv("ZENDESK_EMAIL")
ZENDESK_TOKEN = os.getenv("ZENDESK_TOKEN")

def extract_organization_memberships():
    url = f"https://{ZENDESK_SUBDOMAIN}.zendesk.com/api/v2/organization_memberships.json"
    auth = (f"{ZENDESK_EMAIL}/token", ZENDESK_TOKEN)
    all_data = []

    while url:
        response = requests.get(url, auth=auth)
        response.raise_for_status()
        data = response.json()
        all_data.extend(data.get("organization_memberships", []))
        url = data.get("next_page")

    return all_data
