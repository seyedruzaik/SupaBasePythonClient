import requests

from sync import sb
from sync.enums import EntityType


class Accounts:
    def __init__(self, access_token: str, salesforce_id: str):
        session = requests.session()
        session.headers.update({'Authorization': f'Bearer {access_token}'})
        self.session = session
        self.salesforce_id = salesforce_id

    def delete_from_supabase(self, record_id: str):
        """
        Delete an account from Supabase, the entity_integration table, and Salesforce.

        :param record_id: The ID of the account to delete
        """
        # Retrieve the salesforce_id from the entity_integration table
        integration_response = sb.table('entity_integration').select('salesforce_id').eq('entity_based_id',
                                                                                         record_id).execute()
        if not integration_response.data:
            print(
                f"Failed to retrieve salesforce_id for account {record_id} from entity_integration: "
                f"{integration_response.json()}")
            return

        salesforce_id = integration_response.data[0]['salesforce_id']

        # Attempt to delete from Salesforce using the retrieved salesforce_id
        self.delete_from_salesforce(salesforce_id)

    def delete_from_salesforce(self, salesforce_id: str):
        """
        Delete an account from Salesforce and Supabase.

        :param salesforce_id: The Salesforce ID of the account to delete
        """
        url = "https://api.integration.app/connections/salesforce/actions/delete-records/run"
        payload = {
            "id": salesforce_id
        }
        salesforce_response = self.session.post(url, json=payload)
        if salesforce_response.status_code == 200:
            print(f"Successfully deleted account {salesforce_id} from Salesforce")
        else:
            print(f"Failed to delete account {salesforce_id} from Salesforce: {salesforce_response.json()}")
            return

        # Retrieve the entity_based_id from the entity_integration table
        integration_response = sb.table('entity_integration').select('entity_based_id').eq('salesforce_id',
                                                                                           salesforce_id).execute()
        if not integration_response.data:
            print(
                f"Failed to retrieve entity_based_id for salesforce_id {salesforce_id} from entity_integration: "
                f"{integration_response.json()}")
            return

        entity_based_id = integration_response.data[0]['entity_based_id']

        # Delete from Supabase account table
        supabase_response = sb.table('account').delete().eq('id', entity_based_id).execute()
        if supabase_response.data:
            print(f"Successfully deleted account {entity_based_id} from Supabase")
        else:
            print(f"Failed to delete account {entity_based_id} from Supabase: {supabase_response.json()}")

        # Delete from entity_integration table
        integration_response = sb.table('entity_integration').delete().eq('entity_based_id', entity_based_id).execute()
        if integration_response.data:
            print(f"Successfully deleted entity integration for account {entity_based_id} from Supabase")
        else:
            print(
                f"Failed to delete entity integration for account {entity_based_id} from Supabase: "
                f"{integration_response.json()}")

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
    def map_o(row: dict, tenant_id, owner_id) -> dict:
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

    @staticmethod
    def check_salesforce_id(salesforce_id: str) -> bool:
        """
        Check if the given salesforce_id exists in the entity_integration table.

        :param salesforce_id: The Salesforce ID to check
        :return: True if the Salesforce ID exists, False otherwise
        """
        response = sb.table('entity_integration').select('id').eq('salesforce_id',
                                                                  salesforce_id).execute()

        # Return True if any rows are returned, otherwise False
        return bool(response.data)

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
                # duplicate_data = res_json.get("data", {}).get("response", {}).get("data", [])
                # if response.status_code == 400 and duplicate_data:
                #     salesforce_id = duplicate_data[0]["duplicateResult"]["matchResults"][0][
                #         "matchRecords"][0]["record"]["Id"]
                #     self.track_record(account["id"], salesforce_id)
                res_json = response.json()
                print(res_json)

    def from_salesforce(self, owner_id: str, tenant_id):
        """
        Import salesforce accounts to Salesforce
        """
        integration_url = "https://api.integration.app/connections/salesforce/actions/get-all-accounts/run"
        accounts = self.session.post(integration_url).json()
        for account in accounts["output"]["records"]:
            account_id = account['id']

            # Check if the salesforce_id exists before proceeding
            if self.check_salesforce_id(account_id):
                print(f"Salesforce ID {account_id} already exists in the entity_integration table.")
                # Update the existing record
                existing_record_response = sb.table('entity_integration').select('entity_based_id').eq('salesforce_id',
                                                                                                       account_id).execute()
                entity_based_id = existing_record_response.data[0]['entity_based_id']

                # Get the phone_book_id from the account table
                account_response = sb.table('account').select('phone_book_id').eq('id', entity_based_id).execute()
                phone_book_id = account_response.data[0]['phone_book_id']
                print("Phone book ID: ", phone_book_id, " Account ID: ", entity_based_id)

                payload = self.map_o(account, tenant_id, owner_id)
                print("phone payload: ", payload['phone_book'])
                print("accounts payload: ", payload['account'])
                print()
                # Update the phone_book and account tables using the retrieved phone_book_id
                sb.table("phone_book").update(payload['phone_book']).eq('id', phone_book_id).execute()
                sb.table("account").update({**payload['account'], "phone_book_id": phone_book_id}).eq('id',
                                                                                                      entity_based_id).execute()
                print(f"Successfully updated Salesforce ID {account_id} in the phone_book and account tables.")
                print()
            else:
                payload = self.map_o(account, tenant_id, owner_id)
                print("phone payload: ", payload['phone_book'])
                print("accounts payload: ", payload['account'])
                phone_book_response = sb.table("phone_book").insert(payload['phone_book']).execute()
                phone_book_id = phone_book_response.data[0]['id']
                account_response = sb.table("account").insert(
                    {**payload['account'], "phone_book_id": phone_book_id}).execute()
                id_ = account_response.data[0]['id']
                print("id ", id_, "salesforce id: ", account['id'])
                print()
                # Add/Update row to entity_integration table
                sb.table("entity_integration").insert(
                    {"entity_based_id": id_, "salesforce_id": account["id"], "entity_type_id": 2}).execute()
