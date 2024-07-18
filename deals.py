from datetime import datetime

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
    # Parse the string to a datetime object
    datetime_obj = datetime.fromisoformat(row.get("close_date", None))

    # Extract the date part
    date_part = datetime_obj.date()

    return {
        "name": row.get("name", None),
        "amount": row.get("revenue", None),
        "currency": row.get("currency", None),  # Use default if None
        "source": row.get("deal_lead_source", {}).get("name") if row.get("deal_lead_source") else None,
        "stage": row.get("entity_stage", {}).get("name") if row.get("entity_stage") else None,
        "probability": str(row.get("score", None)),  # Converting to string
        "closeTime": date_part.isoformat()
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

    return {

        "deal": {
            'group_id': group_id,
            'entity_stage_id': stage_id,
            'name': row['name'],
            'expected_revenue': fields['amount'],
            'expected_close_date': None,
            'close_date': fields['closeTime'],
            "created_by": owner_id,
            "score": fields['probability'],
            "created_at": row["createdTime"],
            "last_updated_at": row["updatedTime"],
            "last_updated_by": owner_id,
            'owner_id': owner_id
        },
        "source": {
            "name": fields['source'] or "",
            "created_at": row["createdTime"]
        }
    }

    # return {
    #
    #     '_group_id': group_id,
    #     '_entity_stage_id': stage_id,
    #     '_deal_name': row['name'],
    #     '_expected_revenue': fields['amount'],
    #     '_expected_close_date': '',
    #     '_close_date': fields['closeTime'],
    #
    # }


def to_salesforce(owner_id: str):
    """
    Export supabase Deals to Salesforce
    """
    integration_url = "https://api.integration.app/connections/salesforce/actions/create-deal/run"
    contacts = supabase.table("deal").select("*, entity_stage(*), deal_lead_source(*)").eq("owner_id",
                                                                                           owner_id).execute().data
    for contact in contacts:
        payload = map_i(contact)
        response = requests.post(integration_url, headers=headers, json=payload)
        if response.status_code == 200:
            print(f"Successfully exported deal {payload['name']}")
        #     # TODO: Add/Update row to entity_integration table
        if response.status_code != 200:
            print(response.json())
            continue


def from_salesforce(owner_id: str, tenant_id):
    """
    Import salesforce Deals to Salesforce
    """
    integration_url = "https://api.integration.app/connections/salesforce/actions/get-deals/run"
    deals = requests.post(integration_url, headers=headers).json()
    for deal in deals["output"]["records"]:
        payload = map_o(deal, tenant_id, owner_id)
        print("source payload: ", payload['source'])
        print("deal payload: ", payload['deal'])
        source_response = supabase.table("deal_lead_source").insert(payload['source']).execute()
        source_id = source_response.data[0]['id']
        print("source id", source_id)
        deal_response = supabase.table("deal").insert(
            {**payload['deal'], "source_id": source_id}).execute()
        # TODO: Add/Update row to entity_integration table
        # if row:
        #     # Add/Update row to entity_integration table
        #     integration_payload = {
        #         "salesforce_id": contact["id"],
        #     }
        #     supabase.table("entity_integration").insert(integration_payload).execute()


if __name__ == "__main__":
    # to_salesforce("16e53cc9-912a-4bdd-b864-ad19caf290e4")
    from_salesforce("16e53cc9-912a-4bdd-b864-ad19caf290e4", 43)
