import os

import requests
from dotenv import load_dotenv
from supabase import create_client, Client

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
        "fullName": f"{row['phone_book'].get('first_name', '')} {row['phone_book'].get('last_name', '')}".strip() or None,
        "firstName": row['phone_book'].get('first_name') or None,
        "lastName": row['phone_book'].get('last_name') or None,
        "primaryEmail": row['phone_book'].get('email') or None,
        "primaryPhone": row['phone_book'].get('phone') or None,
        "primaryAddress": {
            "street": row['phone_book'].get('street') or None,
            "city": row['phone_book'].get('city') or None,
            "state": row['phone_book'].get('state') or None,
            "country": row['phone_book'].get('country') or None
        },
        "companyName": row['phone_book'].get('company') or None,
        "source": row.get("name") or None,
        "jobTitle": row['phone_book'].get('title') or None
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

        "lead": {
            "created_at": fields["createdTime"],
            "created_by": owner_id,
            "last_updated_at": fields["updatedTime"],
            "last_updated_by": owner_id,
            "converted_deal_id": None,
            "entity_stage_id": stage_id,
            "entity_priority_id": priority_id,
            "owner_id": owner_id,
            "group_id": group_id,
            "score": None,
        },
        "phone_book": {
            "email": fields["primaryEmail"],  # No email field in API response
            "phone": fields["primaryPhone"],
            "website": None,
            "street": fields["primaryAddress"]["street"],
            "city": fields["primaryAddress"]["city"],
            "state": fields["primaryAddress"]["state"],
            "country": fields["primaryAddress"]["country"],
            "department": None,  # No department field in API response
            "description": None,
            "created_by": owner_id,
            "created_at": row["createdTime"],
            "last_updated_at": row["updatedTime"],
            "last_updated_by": owner_id,
            "first_name": fields["firstName"],
            "last_name": fields["lastName"],
            "do_not_call": None,
            "title": fields["jobTitle"],
            "company": fields["companyName"],
            "location": fields["primaryAddress"]["city"]
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
    #     '_first_name': fields['firstName'],
    #     '_last_name': fields['lastName'],
    #     '_company_name': fields['companyName'],
    #     '_owner': owner_id,
    #     '_title': fields['jobTitle'],
    #     '_email': fields['primaryEmail'],
    #     '_phone': fields['primaryPhone'],
    #     '_location': fields['primaryAddress']['country'],
    #     '_comments': '',
    #
    # }


def from_salesforce(owner_id: str, tenant_id):
    """
    Import salesforce Leads to Salesforce
    """
    integration_url = "https://api.integration.app/connections/salesforce/actions/get-leads/run"
    leads = requests.post(integration_url, headers=headers).json()
    for lead in leads["output"]["records"]:
        payload = map_o(lead, tenant_id, owner_id)
        print("phone payload: ", payload['phone_book'])
        print("source payload: ", payload['source'])
        print("lead payload: ", payload['lead'])
        phone_book_response = supabase.table("phone_book").insert(payload['phone_book']).execute()
        phone_book_id = phone_book_response.data[0]['id']
        source_response = supabase.table("deal_lead_source").insert(payload['source']).execute()
        source_id = source_response.data[0]['id']
        print("Source id: ", source_id)
        lead_response = supabase.table("lead").insert(
            {**payload['lead'], "phone_book_id": phone_book_id, "source_id": source_id}).execute()
        # TODO: Add/Update row to entity_integration table
        # if row:
        #     # Add/Update row to entity_integration table
        #     integration_payload = {
        #         "salesforce_id": contact["id"],
        #     }
        #     supabase.table("entity_integration").insert(integration_payload).execute()


def to_salesforce(owner_id: str):
    """
    Export supabase Leads to Salesforce
    """
    integration_url = "https://api.integration.app/connections/salesforce/actions/create-lead/run"
    leads = supabase.table("lead").select("*, phone_book(*), deal_lead_source(*)").eq("owner_id",
                                                                                      owner_id).execute().data
    for lead in leads:
        payload = map_i(lead)
        # print(payload)
        response = requests.post(integration_url, headers=headers, json=payload)
        if response.status_code == 200:
            print(f"Successfully exported deal {payload['fullName']}")
        #     # TODO: Add/Update row to entity_integration table
        if response.status_code != 200:
            print(response.json())
            continue


if __name__ == "__main__":
    # to_salesforce("16e53cc9-912a-4bdd-b864-ad19caf290e4")
    from_salesforce("16e53cc9-912a-4bdd-b864-ad19caf290e4", 43)
