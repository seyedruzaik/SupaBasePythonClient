from supabase import create_client, Client
import os
from dotenv import load_dotenv
import requests

# Load environment variables from the .env file
load_dotenv()

url = os.getenv('SUPABASE_URL')
key = os.getenv('SUPABASE_KEY')
supabase: Client = create_client(url, key)

# Fetch data from the account table
account_response = supabase.table("account").select("*, phone_book(*)").execute()
account_data = account_response.data

# Fetch data from the contact table
contact_response = supabase.table("contact").select("*, phone_book(*)").execute()
contact_data = contact_response.data

# Fetch data from the deal table
deal_response = supabase.table("deal").select("id, name, revenue, currency, entity_stage(name), deal_lead_source(name), score, close_date").execute()
deal_data = deal_response.data

# Fetch data from the leads table
leads_response = supabase.table("lead").select("*, phone_book(*), entity_stage(name), deal_lead_source(name)").execute()
leads_data = leads_response.data

# Ensure we have data from the account table
if account_data:
    for account_record in account_data:
        phone_data = account_record['phone_book']
        account_payload = {
            "domain": account_record['domain'],
            "phone_book": {
                "first_name": phone_data['first_name'],
                "email": phone_data['email'],
                "phone": phone_data['phone'],
                "website": phone_data.get('website', None),
                "street": phone_data.get('street', None),
                "city": phone_data.get('city', None),
                "state": phone_data.get('state', None),
                "country": phone_data['country'],
                "created_at": phone_data['created_at'],
                "description": phone_data['description'],
                "do_not_call": phone_data['do_not_call']
            },
            "description": phone_data['description'],
            "industry": account_record['industry'],
            "no_of_employees": account_record['no_of_employees']
        }

        create_accounts_url = "https://api.integration.app/connections/salesforce/actions/create-accounts/run"
        headers = {
            "Authorization": f"Bearer {os.getenv('INTEGRATION_APP_TOKEN')}",
            "Content-Type": "application/json"
        }

        account_response = requests.post(create_accounts_url, headers=headers, json=account_payload)
        print(account_response.text)
else:
    print("No records found in the account table")

# Ensure we have data from the contact table
if contact_data:
    for contact_record in contact_data:
        phone_data = contact_record['phone_book']
        contact_payload = {
            "fullName": f"{phone_data.get('first_name', '')} {phone_data.get('last_name', '')}".strip(),
            "firstName": phone_data.get('first_name', ''),
            "lastName": phone_data.get('last_name', ''),
            "primaryEmail": phone_data.get('email', ''),
            "emails": [
                {
                    "value": phone_data.get('email', '')
                }
            ],
            "primaryPhone": phone_data.get('phone', ''),
            "primaryAddress": {
                "full": f"{phone_data.get('street', '')}, {phone_data.get('city', '')}, {phone_data.get('state', '')}.",
                "street": phone_data.get('street', ''),
                "city": phone_data.get('city', ''),
                "state": phone_data.get('state', ''),
                "country": phone_data['country'],
            },
            "department": phone_data.get('department', ''),
            "companyName": phone_data.get('company', ''),
            "jobTitle": phone_data.get('title', ''),
        }

        create_contacts_url = "https://api.integration.app/connections/salesforce/actions/create-contact/run"
        headers = {
            "Authorization": f"Bearer {os.getenv('INTEGRATION_APP_TOKEN')}",
            "Content-Type": "application/json"
        }

        # contact_response = requests.post(create_contacts_url, headers=headers, json=contact_payload)
        # print(contact_response.text)
else:
    print("No records found in the contact table")

# Ensure we have data from the deal table
if deal_data:
    deal_payloads = []
    for record in deal_data:
        # Initialize the deal_payload dictionary for each record
        deal_payload = {
            "name": record.get("name", None),
            "amount": record.get("revenue", None),
            "currency": record.get("currency", "LKR"),  # Use default if None
            "source": record.get("deal_lead_source", {}).get("name") if record.get("deal_lead_source") else None,
            "stage": record.get("entity_stage", {}).get("name") if record.get("entity_stage") else None,
            "probability": str(record.get("score", None)),  # Converting to string
            "closeTime": record.get("close_date", None)
        }

        # Add the mapped deal_payload to the list
        deal_payloads.append(deal_payload)

    # Print the mapped deal_payloads
    # print(deal_payloads)

    create_deals_url = "https://api.integration.app/connections/salesforce/actions/create-deal/run"
    headers = {
        "Authorization": f"Bearer {os.getenv('INTEGRATION_APP_TOKEN')}",
        "Content-Type": "application/json"
    }

    # Send each deal payload to the integration app
    # for deal_payload in deal_payloads:
    #     deal_response = requests.post(create_deals_url, headers=headers, json=deal_payload)
    #     print(deal_response.text)
else:
    print("No records found in the deal table")

# Ensure we have data from the leads table
if leads_data:
    lead_payloads = []

    for record in leads_data:
        # Initialize the lead_payload dictionary for each record
        phone_book = record.get('phone_book', {}) or {}
        entity_stage = record.get('entity_stage', {}) or {}
        deal_lead_source = record.get('deal_lead_source', {}) or {}

        lead_payload = {
            "fullName": f"{phone_book.get('first_name', '')} {phone_book.get('last_name', '')}".strip() or None,
            "firstName": phone_book.get('first_name') or None,
            "lastName": phone_book.get('last_name') or None,
            "primaryEmail": phone_book.get('email') or None,
            "primaryPhone": phone_book.get('phone') or None,
            "primaryAddress": {
                "street": phone_book.get('street') or None,
                "city": phone_book.get('city') or None,
                "state": phone_book.get('state') or None,
                "country": phone_book.get('country') or None
            },
            "companyName": phone_book.get('company') or None,
            "source": deal_lead_source.get("name") if deal_lead_source else None,
            "jobTitle": phone_book.get('title') or None
        }

        # Add the mapped lead_payload to the list
        lead_payloads.append(lead_payload)

    # Print the mapped lead_payloads
    # print(lead_payloads)

    create_leads_url = "https://api.integration.app/connections/salesforce/actions/create-lead/run"
    headers = {
        "Authorization": f"Bearer {os.getenv('INTEGRATION_APP_TOKEN')}",
        "Content-Type": "application/json"
    }

    # Send each lead payload to the integration app
    # for lead_payload in lead_payloads:
    #     lead_response = requests.post(create_leads_url, headers=headers, json=lead_payload)
    #     print(lead_response.text)
else:
    print("No records found in the leads table")
