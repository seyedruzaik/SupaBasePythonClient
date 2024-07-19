import requests

from sync import sb
from sync.enums import EntityType


class Accounts:
    def __init__(self, access_token: str, salesforce_id: str):
        session = requests.session()
        session.headers.update({'Authorization': f'Bearer {access_token}'})
        self.session = session
        self.salesforce_id = salesforce_id

    @staticmethod
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
            # "owner_id": self.salesforce_id
        }

    @staticmethod
    def map_o(row: dict, tenant_id) -> dict:
        """Field mapping from salesforce to supabase"""

        fields = row['fields']

        # Get the group ID using the tenant ID
        group_response = sb.table('entity_group').select('id').eq('tenant_id', tenant_id).limit(1).execute()
        group_id = group_response.data[0]['id'] if group_response.data else None

        # Get the stage ID using the group ID
        stage_response = sb.table('entity_stage').select('id').eq('group_id', group_id).limit(1).execute()
        stage_id = stage_response.data[0]['id'] if stage_response.data else None

        # Get the priority ID using the group ID
        priority_response = sb.table('entity_priority').select('id').eq('group_id', group_id).limit(1).execute()
        priority_id = priority_response.data[0]['id'] if priority_response.data else None

        return {
            '_group_id': group_id,
            '_entity_stage_id': stage_id,
            '_entity_priority_id': priority_id,
            '_company_name': row['name'],
            '_website': fields['Website'],
            '_industry': fields['Industry'],
            '_no_of_employees': fields['NumberOfEmployees'],
            '_description': fields['Description'],
            '_headquarters': ''
        }

    @staticmethod
    def track_record(id_: str, salesforce_id):
        is_in_db = sb.table("entity_integration").select("*").eq("entity_based_id", id_).execute().data
        if not is_in_db:
            data = {
                "entity_based_id": id_,
                "salesforce_id": salesforce_id,
                "entity_type_id": EntityType.ACCOUNT.value
            }
            sb.table("entity_integration").insert(data).execute()
        else:
            (sb.table("entity_integration").update({"salesforce_id": salesforce_id}).eq("entity_based_id", id_)
             .execute())

    def to_salesforce(self, owner_id: str):
        """
        Export supabase accounts to Salesforce
        """
        integration_url = "https://api.integration.app/connections/salesforce/actions/create-accounts/run"
        accounts = sb.table("account").select("*, phone_book(*)").eq("owner_id", owner_id).execute().data
        for account in accounts:
            payload = self.map_i(account)
            response = self.session.post(integration_url, json=payload)
            if response.status_code == 200:
                id_ = response.json()["output"]["id"]
                self.track_record(account["id"], id_)
            else:
                res_json = response.json()
                duplicate_data = res_json.get("data", {}).get("response", {}).get("data", [])
                if response.status_code == 400 and duplicate_data:
                    salesforce_id = duplicate_data[0]["duplicateResult"]["matchResults"][0][
                        "matchRecords"][0]["record"]["Id"]
                    self.track_record(account["id"], salesforce_id)

    def from_salesforce(self, owner_id: str, tenant_id):
        """
        Import salesforce accounts to Salesforce
        """
        integration_url = "https://api.integration.app/connections/salesforce/actions/get-all-accounts/run"
        accounts = self.session.post(integration_url).json()
        for account in accounts["output"]["records"]:
            payload = self.map_o(account, tenant_id)
            id_ = sb.rpc('brain_create_account', payload).execute().data
            # TODO: Add/Update row to entity_integration table
            sb.table("entity_integration").insert(
                {"entity_based_id": account["id"], "salesforce_id": id_, "entity_type_id": 2}).execute()
