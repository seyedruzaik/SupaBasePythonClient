import requests

from sync import sb
from sync.enums import EntityType


class HubspotAccounts:
    def __init__(self, access_token: str, hubspot_id: str):
        session = requests.session()
        session.headers.update({'Authorization': f'Bearer {access_token}'})
        self.session = session
        self.hubspot_id = hubspot_id

    def delete_orphaned_hubspot_ids(self):
        """
        Delete records from Supabase that have a hubspot_id in entity_integration but not in hubspot.
        """
        # Retrieve all hubspot_ids from the entity_integration table
        integration_response = sb.table('entity_integration').select('hubspot_id').eq('entity_type_id',
                                                                                      2).execute()
        if not integration_response.data:
            print(f"Failed to retrieve hubspot_ids from entity_integration: {integration_response.json()}")
            return

        supabase_hubspot_ids = {record['hubspot_id'] for record in integration_response.data}

        # Retrieve all accounts from hubspot
        integration_url = "https://api.integration.app/connections/hubspot/actions/get-all-accounts/run"
        accounts_response = self.session.post(integration_url).json()
        hubspot_accounts = accounts_response.get("output", {}).get("records", [])
        hubspot_account_ids = {account['id'] for account in hubspot_accounts}

        # Find hubspot_ids that are in Supabase but not in hubspot
        orphaned_hubspot_ids = supabase_hubspot_ids - hubspot_account_ids

        # Delete orphaned records from Supabase
        for hubspot_id in orphaned_hubspot_ids:
            print(f"Deleting orphaned hubspot_id: {hubspot_id}")
            # Retrieve the entity_based_id from the entity_integration table
            integration_response = sb.table('entity_integration').select('entity_based_id').eq('hubspot_id',
                                                                                               hubspot_id).execute()
            if not integration_response.data:
                print(f"Failed to retrieve entity_based_id for hubspot_id {hubspot_id} from entity_integration: "
                      f"{integration_response.json()}")
                continue

            entity_based_id = integration_response.data[0]['entity_based_id']

            # Delete from Supabase account table
            supabase_response = sb.table('account').delete().eq('id', entity_based_id).execute()
            if supabase_response.data:
                print(f"Successfully deleted account {entity_based_id} from Supabase")
            else:
                print(f"Failed to delete account {entity_based_id} from Supabase: {supabase_response.json()}")

            # Delete from entity_integration table
            integration_response = sb.table('entity_integration').delete().eq('entity_based_id',
                                                                              entity_based_id).execute()
            if integration_response.data:
                print(f"Successfully deleted entity integration for account {entity_based_id} from Supabase")
            else:
                print(f"Failed to delete entity integration for account {entity_based_id} from Supabase: "
                      f"{integration_response.json()}")

    def delete_missing_in_supabase(self):
        """
        Delete hubspot IDs that exist in hubspot but are missing in Supabase.
        """
        # Retrieve all hubspot_ids from the entity_integration table
        integration_response = (sb.table('entity_integration').select('hubspot_id').
                                eq('entity_type_id', 2).execute())
        if not integration_response.data:
            print(f"Failed to retrieve hubspot_ids from entity_integration: {integration_response.json()}")
            return

        supabase_hubspot_ids = {record['hubspot_id'] for record in integration_response.data}

        # Retrieve all accounts from hubspot
        integration_url = "https://api.integration.app/connections/hubspot/actions/get-all-accounts/run"
        accounts_response = self.session.post(integration_url).json()
        hubspot_accounts = accounts_response.get("output", {}).get("records", [])
        hubspot_account_ids = {account['id'] for account in hubspot_accounts}

        # Find hubspot_ids that are in hubspot but not in Supabase
        missing_in_supabase_ids = hubspot_account_ids - supabase_hubspot_ids
        print(hubspot_accounts)

        # Delete records from hubspot that are not in Supabase
        for hubspot_id in missing_in_supabase_ids:
            print(f"Deleting hubspot_id from hubspot: {hubspot_id}")
            self.delete_from_hubspot(hubspot_id)

    def delete_from_hubspot(self, hubspot_id: str):
        """
        Delete an account from hubspot.

        :param hubspot_id: The hubspot ID of the account to delete
        """
        url = "https://api.integration.app/connections/hubspot/actions/delete-records/run"
        payload = {
            "id": hubspot_id
        }
        hubspot_response = self.session.post(url, json=payload)
        if hubspot_response.status_code == 200:
            print(f"Successfully deleted account {hubspot_id} from hubspot")
        else:
            print(f"Failed to delete account {hubspot_id} from hubspot: {hubspot_response.json()}")

    @staticmethod
    def map_i(row: dict) -> dict:
        """
        Field mapping from supabase to hubspot
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
            "annual_revenue": row['annual_revenue']
            # "owner_id": self.hubspot_id
        }

    @staticmethod
    def map_o(row: dict, tenant_id, owner_id) -> dict:
        """Field mapping from HubSpot to Supabase"""

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
                "phone": fields.get("phone"),
                "website": fields.get("website"),
                "street": fields.get("address"),
                "city": fields.get("city"),
                "state": fields.get("state"),
                "country": fields.get("country"),
                "department": None,  # No department field in API response
                "description": fields.get("description"),
                "created_by": owner_id,
                "created_at": row.get("createdTime"),
                "last_updated_at": row.get("updatedTime"),
                "last_updated_by": owner_id,
                "first_name": fields.get("name"),
                "last_name": None,  # No last name field in API response
                "do_not_call": None,  # No do_not_call field in API response
                "title": None,  # No title field in API response
                "company": fields.get("industry"),
                "location": fields.get("city")
            },
            "account": {
                'group_id': group_id,
                'entity_stage_id': stage_id,
                'entity_priority_id': priority_id,
                'domain': fields.get('domain'),
                'industry': fields.get('industry'),
                'no_of_employees': fields.get('numberofemployees'),
                'headquarters': '',
                'annual_revenue': fields.get('annualrevenue'),
                'owner_id': owner_id,
                "created_by": owner_id,
                "created_at": row.get("createdTime"),
                "last_updated_at": row.get("updatedTime"),
                "last_updated_by": owner_id
            }
        }

    @staticmethod
    def check_hubspot_id(hubspot_id: str) -> bool:
        """
        Check if the given hubspot_id exists in the entity_integration table.

        :param hubspot_id: The hubspot ID to check
        :return: True if the hubspot ID exists, False otherwise
        """
        response = sb.table('entity_integration').select('id').eq('hubspot_id',
                                                                  hubspot_id).execute()

        # Return True if any rows are returned, otherwise False
        return bool(response.data)

    @staticmethod
    def track_record(id_: str, hubspot_id):
        is_in_db = sb.table("entity_integration").select("*").eq("entity_based_id", id_).execute().data
        if not is_in_db:
            data = {
                "entity_based_id": id_,
                "hubspot_id": hubspot_id,
                "entity_type_id": EntityType.ACCOUNT.value
            }
            sb.table("entity_integration").insert(data).execute()
        else:
            (sb.table("entity_integration").update({"hubspot_id": hubspot_id}).eq("entity_based_id", id_)
             .execute())

    @staticmethod
    def extract_hubspot_id(json_response):
        """
        Extract hubspot ID from the JSON response
        """
        try:
            match_records = json_response['data']['response']['data'][0]['duplicateResult']['matchResults'][0][
                'matchRecords']
            if match_records:
                return match_records[0]['record']['Id']
        except (KeyError, IndexError) as e:
            print(f"Error extracting hubspot ID: {e}")
        return None

    def to_hubspot(self, owner_id: str):
        """
        Export supabase accounts to hubspot
        """
        integration_url = "https://api.integration.app/connections/hubspot/actions/create-accounts/run"
        accounts = sb.table("account").select("*, phone_book(*)").eq("owner_id", owner_id).execute().data
        for account in accounts:
            payload = self.map_i(account)
            entity_table_id = sb.table("entity_integration").select("hubspot_id").eq('entity_based_id',
                                                                                     account["id"]).execute().data
            if entity_table_id:
                integration_update_url = ("https://api.integration.app/connections/hubspot/actions/update"
                                          "-accounts/run")
                hubspot_id = entity_table_id[0]['hubspot_id']
                payload["id"] = hubspot_id
                updated_response = self.session.post(integration_update_url, json=payload)
                if updated_response.status_code == 200:
                    print(f"Successfully updated account {account['phone_book']['first_name']}")
                    print()
                else:
                    print(updated_response.json())
                continue
            else:
                payload = self.map_i(account)
                response = self.session.post(integration_url, json=payload)
                if response.status_code == 200:
                    id_ = response.json()["output"]["id"]
                    self.track_record(account["id"], id_)
                    print(f"Successfully exported account {account['phone_book']['first_name']}")
                    print()
                else:
                    res_json = response.json()
                    # Extracting the status and error code
                    status = res_json['data']['response']['status']
                    error_code = res_json['data']['response']['data']['message']
                    print(f"Status: {status}", f"Error Code: {error_code}")

    def from_hubspot(self, owner_id: str, tenant_id):
        """
        Import hubspot accounts to Supabase
        """
        integration_url = "https://api.integration.app/connections/hubspot/actions/get-all-accounts/run"
        accounts = self.session.post(integration_url).json()
        for account in accounts["output"]["records"]:
            account_id = account['id']

            # Check if the hubspot_id exists before proceeding
            if self.check_hubspot_id(account_id):
                print(f"hubspot ID {account_id} already exists in the entity_integration table.")
                # Update the existing record
                existing_record_response = (sb.table('entity_integration').select('entity_based_id')
                                            .eq('hubspot_id', account_id).execute())

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
                (sb.table("account").update({**payload['account'], "phone_book_id": phone_book_id})
                 .eq('id', entity_based_id).execute())

                print(f"Successfully updated hubspot ID {account_id} in the phone_book and account tables.")
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
                print("id ", id_, "hubspot id: ", account['id'])
                # Add/Update row to entity_integration table
                sb.table("entity_integration").insert(
                    {"entity_based_id": id_, "hubspot_id": account["id"], "entity_type_id": 2}).execute()
                print(f"Successfully created {id_} in the phone_book and account tables.")
                print()
