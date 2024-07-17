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
        "domain": row['domain'],
        "phone_book": {
            "first_name": row['phone_book']['first_name'],
            "email": row['phone_book']['email'],
            "phone": row['phone_book']['phone'],
            "website": row['phone_book']['website'],
            "street": row['phone_book']['street'],
            "city": row['phone_book']['city'],
            "state": row['phone_book']['state'],
            "country": row['phone_book']['country'],
            "created_at": row['phone_book']['created_at'],
            "description": row['phone_book']['description'],
            "do_not_call": row['phone_book']['do_not_call']
        },
        "description": row['phone_book']['description'],
        "industry": row['industry'],
        "no_of_employees": row['no_of_employees'],
        # "owner_id": "005GA00000ACuZGYA1"
    }


def map_o(row: dict, tenant_id, owner_id) -> dict:
    """Field mapping from salesforce to supabase"""
    # TODO
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
        "phone_book": {
            "email": None,  # No email field in API response
            "phone": fields["Phone"],
            "website": fields["Website"],
            "street": fields["BillingStreet"],
            "city": fields["BillingCity"],
            "state": fields["BillingState"],
            "country": fields["BillingCountry"],
            "department": None,  # No department field in API response
            "description": fields["Description"],
            "created_by": owner_id,
            "created_at": row["createdTime"],
            "last_updated_at": row["updatedTime"],
            "last_updated_by": owner_id,
            "first_name": row["name"],
            "last_name": None,  # No last name field in API response
            "do_not_call": None,  # No do_not_call field in API response
            "title": None,  # No title field in API response
            "company": fields["Industry"],
            "location": fields["BillingCity"]
        },
        # , "account": {
        #     '_group_id': group_id,
        #     '_entity_stage_id': stage_id,
        #     '_entity_priority_id': priority_id,
        #     '_company_name': row['name'],
        #     '_website': fields['Website'],
        #     '_industry': fields['Industry'],
        #     '_no_of_employees': fields['NumberOfEmployees'],
        #     '_description': fields['Description'],
        #     '_headquarters': ''
        # }
        "account": {
            'group_id': group_id,
            'entity_stage_id': stage_id,
            'entity_priority_id': priority_id,
            'domain': fields['Website'],
            'industry': fields['Industry'],
            'no_of_employees': fields['NumberOfEmployees'],
            'headquarters': '',
            'owner_id': owner_id,
            "created_by": owner_id,
            "created_at": row["createdTime"],
            "last_updated_at": row["updatedTime"],
            "last_updated_by": owner_id
        }
    }


def to_salesforce(owner_id: str):
    """
    Export supabase accounts to Salesforce
    """
    integration_url = "https://api.integration.app/connections/salesforce/actions/create-accounts/run"
    accounts = supabase.table("account").select("*, phone_book(*)").eq("owner_id", owner_id).execute().data
    for account in accounts:
        payload = map_i(account)
        response = requests.post(integration_url, headers=headers, json=payload)
        if response.status_code == 200:
            print(f"Successfully exported account {account['phone_book']['first_name']}")
            # TODO: Add/Update row to entity_integration table
        if response.status_code != 200:
            print(response.json())
            continue


def from_salesforce(owner_id: str, tenant_id):
    """
    Import salesforce accounts to Salesforce
    """
    integration_url = "https://api.integration.app/connections/salesforce/actions/get-all-accounts/run"
    accounts = requests.post(integration_url, headers=headers).json()
    for account in accounts["output"]["records"]:
        payload = map_o(account, tenant_id, owner_id)
        print("phone payload: ", payload['phone_book'])
        print("accounts payload: ", payload['account'])
        phone_book_response = supabase.table("phone_book").insert(payload['phone_book']).execute()
        phone_book_id = phone_book_response.data[0]['id']
        account_response = supabase.table("account").insert({**payload['account'], "phone_book_id": phone_book_id}).execute()
        # response = supabase.rpc('brain_create_account', payload).execute()
        # TODO: Add/Update row to entity_integration table
        # if row:
        #     # Add/Update row to entity_integration table
        #     integration_payload = {
        #         "salesforce_id": account["id"],
        #     }
        #     supabase.table("entity_integration").insert(integration_payload).execute()


if __name__ == "__main__":
    # to_salesforce("16e53cc9-912a-4bdd-b864-ad19caf290e4")
    from_salesforce("16e53cc9-912a-4bdd-b864-ad19caf290e4", 43)
