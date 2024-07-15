from supabase import create_client, Client
import os
from dotenv import load_dotenv
import requests

# Load environment variables from the .env file
load_dotenv()

supabase_url = os.getenv('SUPABASE_URL')
supabase_key = os.getenv('SUPABASE_KEY')
supabase: Client = create_client(supabase_url, supabase_key)

# Define the payload and headers for the API requests
headers = {
    "Authorization": f"Bearer {os.getenv('INTEGRATION_APP_TOKEN')}",
    "Content-Type": "application/json"
}


def map_i(row: dict) -> dict:
    """
    Field mapping from supabase to salesforce
    """
    return {
        "fullName": f"{row['phone_book'].get('first_name', '')} {row['phone_book'].get('last_name', '')}".strip(),
        "firstName": row['phone_book'].get('first_name', ''),
        "lastName": row['phone_book'].get('last_name', ''),
        "primaryEmail": row['phone_book'].get('email', ''),
        "emails": [
            {
                "value": row['phone_book'].get('email', '')
            }
        ],
        "primaryPhone": row['phone_book'].get('phone', ''),
        "primaryAddress": {
            "full": f"{row['phone_book'].get('street', '')}, {row['phone_book'].get('city', '')}, {row['phone_book'].get('state', '')}.",
            "street": row['phone_book'].get('street', ''),
            "city": row['phone_book'].get('city', ''),
            "state": row['phone_book'].get('state', ''),
            "country": row['phone_book']['country'],
        },
        "department": row['phone_book'].get('department', ''),
        "companyName": row['phone_book'].get('company', ''),
        "jobTitle": row['phone_book'].get('title', ''),
    }


def map_o(row: dict, tenant_id, owner_id) -> dict:
    """Field mapping from salesforce to supabase"""
    fields = row['fields']

    # Get the group ID using the tenant ID
    group_response = supabase.table('entity_group').select('id').eq('tenant_id', tenant_id).limit(1).execute()
    group_id = group_response.data[0]['id'] if group_response.data else None

    # Get the stage ID using the group ID
    stage_response = supabase.table('entity_stage').select('id').eq('group_id', group_id).limit(1).execute()
    stage_id = stage_response.data[0]['id'] if stage_response.data else None

    # Get the priority ID using the group ID
    priority_response = supabase.table('entity_priority').select('id').eq('group_id', group_id).limit(1).execute()
    priority_id = priority_response.data[0]['id'] if priority_response.data else None

    return {

        '_group_id': group_id,
        '_entity_stage_id': stage_id,
        '_entity_priority_id': priority_id,
        '_first_name': fields['firstName'],
        '_last_name': fields['lastName'],
        '_company_name': '',
        '_owner': owner_id,
        '_title': fields['jobTitle'],
        '_email': fields['primaryEmail'],
        '_phone': fields['primaryPhone'],
        '_location': fields['primaryAddress']['country'],
        '_comments': '',

    }


def from_salesforce(owner_id: str, tenant_id):
    """
    Import salesforce contacts to Salesforce
    """
    integration_url = "https://api.integration.app/connections/salesforce/actions/get-contacts/run"
    contacts = requests.post(integration_url, headers=headers).json()
    for contact in contacts["output"]["records"]:
        payload = map_o(contact, tenant_id, owner_id)
        print(contact["id"])
        print("payload: ", payload)
        # response = supabase.rpc('brain_create_contacts', payload)
        # TODO: Add/Update row to entity_integration table
        # if row:
        #     # Add/Update row to entity_integration table
        #     integration_payload = {
        #         "salesforce_id": contact["id"],
        #     }
        #     supabase.table("entity_integration").insert(integration_payload).execute()


def to_salesforce(created_by: str):
    """
    Export supabase Contact to Salesforce
    """
    integration_url = "https://api.integration.app/connections/salesforce/actions/create-contact/run"
    contacts = supabase.table("contact").select("*, phone_book(*)").eq("created_by", created_by).execute().data
    for contact in contacts:
        payload = map_i(contact)
        response = requests.post(integration_url, headers=headers, json=payload)
        if response.status_code == 200:
            print(f"Successfully exported contact {payload['fullName']}")
            # TODO: Add/Update row to entity_integration table
        if response.status_code != 200:
            print(response.json())
            continue


if __name__ == "__main__":
    # to_salesforce("6ec1e664-d14c-43ec-9276-3c360df337a1")
    from_salesforce("16e53cc9-912a-4bdd-b864-ad19caf290e4", 43)
