from supabase import create_client, Client
import os
from dotenv import load_dotenv
import requests

# Load environment variables from the .env file
load_dotenv()

url = os.getenv('SUPABASE_URL')
key = os.getenv('SUPABASE_KEY')
supabase: Client = create_client(url, key)

# Define the payload and headers for the API requests
payload = {}
headers = {
    "Authorization": f"Bearer {os.getenv('INTEGRATION_APP_TOKEN')}",
    "Content-Type": "application/json"
}

# Get all Salesforce accounts
get_accounts_url = "https://api.integration.app/connections/salesforce/actions/get-all-accounts/run"
response = requests.request("POST", get_accounts_url, headers=headers, json=payload)

# Get all Salesforce contacts
get_contacts_url = "https://api.integration.app/connections/salesforce/actions/get-contacts/run"
contacts_response = requests.request("POST", get_contacts_url, headers=headers, json={})

# Get all Salesforce deals
get_deals_url = "https://api.integration.app/connections/salesforce/actions/get-deals/run"
deals_response = requests.request("POST", get_deals_url, headers=headers, json={})

# Get all Salesforce leads
get_leads_url = "https://api.integration.app/connections/salesforce/actions/get-leads/run"
leads_response = requests.request("POST", get_leads_url, headers=headers, json={})

# Print responses for debugging
print(response.text)
print(contacts_response.text)
print(deals_response.text)
print(leads_response.text)

# Parse the JSON responses
account_json_data = response.json()
contacts_json_data = contacts_response.json()
deals_json_data = deals_response.json()
leads_json_data = leads_response.json()

# Extract the desired fields from accounts
extracted_account_data = []
for record in account_json_data["output"]["records"]:
    extracted_record = {
        "id": record["id"],
        "createdTime": record["createdTime"],
        "industry": record["fields"]["Industry"],
        "isDeleted": record["fields"]["IsDeleted"],
        "website": record["fields"]["Website"],
        "NumberOfEmployees": record["fields"]["NumberOfEmployees"],
        "updatedTime": record["updatedTime"]
    }
    extracted_account_data.append(extracted_record)

# Define the payload template for Supabase accounts
payload_for_supabase_accounts = {
    'created_at': '2024-05-29T09:24:16.023915+00:00',
    'is_deleted': False,
    'entity_priority_id': 109,
    'phone_book_id': 115,
    'industry': 'Technology',
    'annual_revenue': None,
    'no_of_employees': 500,
    'last_updated_at': '2024-05-29T09:24:16.023915+00:00',
    'entity_stage_id': 152,
    'created_by': '16e53cc9-912a-4bdd-b864-ad19caf290e4',
    'last_updated_by': '16e53cc9-912a-4bdd-b864-ad19caf290e4',
    'headquarters': 'San Francisco',
    'domain': 'acme.com',
    'group_id': 36,
    'owner_id': '16e53cc9-912a-4bdd-b864-ad19caf290e4'
}

# Populate the payload for each record in extracted_account_data
populated_account_payloads = []
for record in extracted_account_data:
    populated_payload = payload_for_supabase_accounts.copy()
    populated_payload.update({
        'created_at': record['createdTime'],
        'is_deleted': record['isDeleted'],
        'industry': record['industry'],
        'no_of_employees': record['NumberOfEmployees'],
        'last_updated_at': record['updatedTime'],
        'domain': record['website']
    })
    populated_account_payloads.append((populated_payload, record['id']))

# Insert the populated account payloads into the 'account' table and insert the id into 'entity_integration' table
for payload, salesforce_id in populated_account_payloads:
    response = supabase.table('account').insert(payload).execute()
    print(response)
    if response:
        integration_payload = {
            "salesforce_id": salesforce_id
        }
        integration_response = supabase.table("entity_integration").insert(integration_payload).execute()
        print(integration_response)

# Extract the desired fields from contacts
extracted_contacts_data = []
for record in contacts_json_data["output"]["records"]:
    extracted_record = {
        "id": record["id"],
        "createdTime": record["createdTime"],
        "updatedTime": record["updatedTime"]
    }
    extracted_contacts_data.append(extracted_record)

# Define the payload template for Supabase contacts
payload_for_supabase_contacts = {
    'created_at': '2024-05-29T10:00:42.923+00:00',
    'is_deleted': True,
    'account_id': None,
    'phone_book_id': 130,
    'created_by': 'e365e184-45fe-4982-b94a-6e2de7cd141b',
    'last_updated_at': '2024-05-29T10:00:43.792002+00:00',
    'last_updated_by': 'e365e184-45fe-4982-b94a-6e2de7cd141b',
    'entity_stage_id': 74,
    'entity_priority_id': 2,
    'group_id': 19,
}

# Populate the payload for each record in extracted_contacts_data
populated_contact_payloads = []
for record in extracted_contacts_data:
    populated_payload = payload_for_supabase_contacts.copy()
    populated_payload.update({
        'created_at': record['createdTime'],
        'last_updated_at': record['updatedTime'],
    })
    populated_contact_payloads.append((populated_payload, record['id']))

# Insert the populated contact payloads into the 'contact' table and insert the id into 'entity_integration' table
# for payload, salesforce_id in populated_contact_payloads:
#     response = supabase.table('contact').insert(payload).execute()
#     print(response)
#     if response:
#         integration_payload = {
#             "salesforce_id": salesforce_id
#         }
#         integration_response = supabase.table("entity_integration").insert(integration_payload).execute()
#         print(integration_response)

# Extract the desired fields from deals
extracted_deals_data = []
for record in deals_json_data["output"]["records"]:
    fields = record["fields"]
    extracted_record = {
        "id": record["id"],
        "name": fields["name"],
        "revenue": fields["amount"],
        "currency": None,  # Assuming currency is not provided, set to None
        "score": fields["probability"],
        "close_date": fields["closeTime"],
        "entity_stage_id": None,  # Assuming a default value for the stage id
        "source_id": None,  # Assuming a default value for the source id
        "created_at": fields["createdTime"],
        "last_updated_at": fields["updatedTime"],
        "created_by": "e365e184-45fe-4982-b94a-6e2de7cd141b",
        "last_updated_by": "e365e184-45fe-4982-b94a-6e2de7cd141b",
        "is_deleted": False,  # Assuming a default value
        "expected_revenue": None,  # Assuming a default value
        "last_interaction_date": None,  # Assuming a default value
        "next_interaction_date": None,  # Assuming a default value
        "expected_close_date": None,  # Assuming a default value
        "owner_id": "16e53cc9-912a-4bdd-b864-ad19caf290e4",
        "group_id": None,  # Assuming a default value
        "entity_priority_id": None,  # Assuming a default value
        "is_committed": False,  # Assuming a default value
        "score_history": None  # Assuming a default value
    }
    extracted_deals_data.append(extracted_record)

# Populate the payload for each record in extracted_deals_data
populated_deal_payloads = []
for record in extracted_deals_data:
    populated_payload = record.copy()
    populated_deal_payloads.append((populated_payload, record['id']))

# Insert the populated deal payloads into the 'deal' table and insert the id into 'entity_integration' table
# for payload, salesforce_id in populated_deal_payloads:
#     response = supabase.table('deal').insert(payload).execute()
#     print(response)
#     if response:
#         integration_payload = {
#             "salesforce_id": salesforce_id
#         }
#         integration_response = supabase.table("entity_integration").insert(integration_payload).execute()
#         print(integration_response)

# Extract the desired fields from leads
extracted_leads_data = []
for record in leads_json_data["output"]["records"]:
    fields = record["fields"]
    extracted_record = {
        "id": record["id"],
        "created_at": fields["createdTime"],
        "is_deleted": False,
        "phone_book_id": 165,  # Assuming a default value or separate logic to fetch
        "is_converted": False,
        "converted_date": None,
        "converted_account_id": None,
        "converted_contact_id": None,
        "created_by": "e365e184-45fe-4982-b94a-6e2de7cd141b",
        "last_updated_at": fields["updatedTime"],
        "last_updated_by": "e365e184-45fe-4982-b94a-6e2de7cd141b",
        "converted_deal_id": None,
        "entity_stage_id": None,  # Assuming a default value
        "entity_priority_id": None,  # Assuming a default value
        "owner_id": "16e53cc9-912a-4bdd-b864-ad19caf290e4",
        "group_id": None,  # Assuming a default value
        "score": None,  # Assuming a default value
        "source_id": None,  # Assuming a default value
    }
    extracted_leads_data.append(extracted_record)

# Populate the payload for each record in extracted_leads_data
populated_lead_payloads = []
for record in extracted_leads_data:
    populated_payload = record.copy()
    populated_lead_payloads.append((populated_payload, record['id']))

# Insert the populated lead payloads into the 'lead' table and insert the id into 'entity_integration' table
# for payload, salesforce_id in populated_lead_payloads:
#     response = supabase.table('lead').insert(payload).execute()
#     print(response)
#     if response:
#         integration_payload = {
#             "salesforce_id": salesforce_id
#         }
#         integration_response = supabase.table("entity_integration").insert(integration_payload).execute()
#         print(integration_response)
