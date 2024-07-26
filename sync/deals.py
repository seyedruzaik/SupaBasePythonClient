import requests
from datetime import datetime
from sync import sb
from sync.enums import EntityType


class Deals:
    def __init__(self, access_token: str, salesforce_id: str):
        session = requests.session()
        session.headers.update({'Authorization': f'Bearer {access_token}'})
        self.session = session
        self.salesforce_id = salesforce_id

    def delete_orphaned_salesforce_ids(self):
        """
        Delete records from Supabase that have a salesforce_id in entity_integration but not in Salesforce.
        """
        # Retrieve all salesforce_ids from the entity_integration table
        integration_response = (sb.table('entity_integration').select('salesforce_id').
                                eq('entity_type_id', 3).execute())
        if not integration_response.data:
            print(f"Failed to retrieve salesforce_ids from entity_integration: {integration_response.json()}")
            return

        supabase_salesforce_ids = {record['salesforce_id'] for record in integration_response.data}

        # Retrieve all deals from Salesforce
        integration_url = "https://api.integration.app/connections/salesforce/actions/get-deals/run"
        deals_response = self.session.post(integration_url).json()
        salesforce_deals = deals_response.get("output", {}).get("records", [])
        salesforce_deal_ids = {deal['id'] for deal in salesforce_deals}

        # Find salesforce_ids that are in Supabase but not in Salesforce
        orphaned_salesforce_ids = supabase_salesforce_ids - salesforce_deal_ids

        # Delete orphaned records from Supabase
        for salesforce_id in orphaned_salesforce_ids:
            print(f"Deleting orphaned salesforce_id: {salesforce_id}")
            # Retrieve the entity_based_id from the entity_integration table
            integration_response = (sb.table('entity_integration').select('entity_based_id').
                                    eq('salesforce_id', salesforce_id).execute())
            if not integration_response.data:
                print(f"Failed to retrieve entity_based_id for salesforce_id {salesforce_id} from entity_integration: "
                      f"{integration_response.json()}")
                continue

            entity_based_id = integration_response.data[0]['entity_based_id']

            # Delete from Supabase deal table
            supabase_response = sb.table('deal').delete().eq('id', entity_based_id).execute()
            if supabase_response.data:
                print(f"Successfully deleted deal {entity_based_id} from Supabase")
            else:
                print(f"Failed to delete deal {entity_based_id} from Supabase: {supabase_response.json()}")

            # Delete from entity_integration table
            integration_response = sb.table('entity_integration').delete().eq('entity_based_id',
                                                                              entity_based_id).execute()
            if integration_response.data:
                print(f"Successfully deleted entity integration for deal {entity_based_id} from Supabase")
            else:
                print(f"Failed to delete entity integration for deal {entity_based_id} from Supabase: "
                      f"{integration_response.json()}")

    def delete_missing_in_supabase(self):
        """
        Delete Salesforce IDs that exist in Salesforce but are missing in Supabase.
        """
        # Retrieve all salesforce_ids from the entity_integration table
        integration_response = (sb.table('entity_integration').select('salesforce_id').
                                eq('entity_type_id', 3).execute())
        if not integration_response.data:
            print(f"Failed to retrieve salesforce_ids from entity_integration: {integration_response.json()}")
            return

        supabase_salesforce_ids = {record['salesforce_id'] for record in integration_response.data}

        # Retrieve all deals from Salesforce
        integration_url = "https://api.integration.app/connections/salesforce/actions/get-deals/run"
        deals_response = self.session.post(integration_url).json()
        salesforce_deals = deals_response.get("output", {}).get("records", [])
        salesforce_deal_ids = {deal['id'] for deal in salesforce_deals}

        # Find salesforce_ids that are in Salesforce but not in Supabase
        missing_in_supabase_ids = salesforce_deal_ids - supabase_salesforce_ids

        # Delete records from Salesforce that are not in Supabase
        for salesforce_id in missing_in_supabase_ids:
            print(f"Deleting salesforce_id from Salesforce: {salesforce_id}")
            self.delete_from_salesforce(salesforce_id)

    def delete_from_salesforce(self, salesforce_id: str):
        """
        Delete a deal from Salesforce.

        :param salesforce_id: The Salesforce ID of the deal to delete
        """
        url = "https://api.integration.app/connections/salesforce/actions/delete-deals/run"
        payload = {
            "id": salesforce_id
        }
        salesforce_response = self.session.post(url, json=payload)
        if salesforce_response.status_code == 200:
            print(f"Successfully deleted deal {salesforce_id} from Salesforce")
        else:
            print(f"Failed to delete deal {salesforce_id} from Salesforce: {salesforce_response.json()}")

    @staticmethod
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
            "currency": row.get("currency", None),
            "source": row.get("deal_lead_source", {}).get("name") if row.get("deal_lead_source") else None,
            "stage": row.get("entity_stage", {}).get("name") if row.get("entity_stage") else None,
            "probability": str(row.get("score", None)),  # Converting to string
            "closeTime": date_part.isoformat()
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

        return {
            "deal": {
                'group_id': group_id,
                'entity_stage_id': stage_id,
                'name': row['name'],
                'revenue': fields['amount'],
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

    @staticmethod
    def check_salesforce_id(salesforce_id: str) -> bool:
        """
        Check if the given salesforce_id exists in the entity_integration table.

        :param salesforce_id: The Salesforce ID to check
        :return: True if the Salesforce ID exists, False otherwise
        """
        response = sb.table('entity_integration').select('id').eq('salesforce_id', salesforce_id).execute()

        # Return True if any rows are returned, otherwise False
        return bool(response.data)

    @staticmethod
    def track_record(id_: str, salesforce_id):
        is_in_db = sb.table("entity_integration").select("*").eq("entity_based_id", id_).execute().data
        if not is_in_db:
            data = {
                "entity_based_id": id_,
                "salesforce_id": salesforce_id,
                "entity_type_id": EntityType.DEAL.value
            }
            sb.table("entity_integration").insert(data).execute()
        else:
            sb.table("entity_integration").update({"salesforce_id": salesforce_id}).eq("entity_based_id", id_).execute()

    def update_salesforce_deal(self, owner_id: str):
        """
        Update supabase deals to Salesforce
        """
        integration_url = "https://api.integration.app/connections/salesforce/actions/update-deal/run"
        deals = sb.table("deal").select("*, entity_stage(*), deal_lead_source(*)").eq("owner_id",
                                                                                      owner_id).execute().data

        for deal in deals:
            deal_id = deal['id']
            salesforce_ids = (sb.table('entity_integration').select('salesforce_id')
                              .eq('entity_based_id', deal_id).eq('entity_type_id',
                                                                 3).execute())

            if salesforce_ids.data and salesforce_ids.data[0]['salesforce_id']:
                salesforce_id = salesforce_ids.data[0]['salesforce_id']

                payload = self.map_i(deal)
                payload["id"] = salesforce_id  # Add the salesforce_id to the payload

                response = self.session.post(integration_url, json=payload)
                response_data = response.json()

                if response.status_code == 200:
                    print(f"Deal {deal_id} updated successfully in Salesforce.")
                    print()
                else:
                    res_json = response_data
                    # Extracting the status and error code
                    status = res_json['data']['response']['status']
                    error_code = res_json['data']['response']['data'][0]['errorCode']

                    # Printing the status and error code
                    print(f"Status: {status}", f"Error Code: {error_code}")
                    print()
            else:
                print(f"No Salesforce ID found for deal {deal_id}!")
                print()

    def to_salesforce_deals(self, owner_id: str):
        """
        Export supabase deals to Salesforce
        """
        integration_url = "https://api.integration.app/connections/salesforce/actions/create-deal/run"
        deals = sb.table("deal").select("*, entity_stage(*), deal_lead_source(*)").eq("owner_id",
                                                                                      owner_id).execute().data
        for deal in deals:
            payload = self.map_i(deal)
            response = self.session.post(integration_url, json=payload)
            if response.status_code == 200:
                print(f"Successfully exported deal {payload['name']}")
                id_ = response.json()["output"]["id"]
                self.track_record(deal["id"], id_)
                print()
            else:
                res_json = response.json()
                # Extracting the status and error code
                status = res_json['data']['response']['status']
                error_code = res_json['data']['response']['data'][0]['errorCode']

                # Printing the status and error code
                print(f"Status: {status}", f"Error Code: {error_code}")
                print()
                continue

    def from_salesforce_deals(self, owner_id: str, tenant_id):
        """
        Import salesforce deals to Salesforce
        """
        integration_url = "https://api.integration.app/connections/salesforce/actions/get-deals/run"
        deals = self.session.post(integration_url).json()
        for deal in deals["output"]["records"]:
            salesforce_id = deal['id']

            # Check if the salesforce_id exists before proceeding
            if self.check_salesforce_id(salesforce_id):
                print(f"Salesforce ID {salesforce_id} already exists in the entity_integration table.")
                # Update the existing record
                existing_record_response = (sb.table('entity_integration').
                                            select('entity_based_id').eq('salesforce_id', salesforce_id).execute())
                entity_based_id = existing_record_response.data[0]['entity_based_id']

                # Get the source_id from the deal table
                deal_response = sb.table('deal').select('source_id').eq('id', entity_based_id).execute()
                source_id = deal_response.data[0]['source_id']

                payload = self.map_o(deal, tenant_id, owner_id)
                print("source payload: ", payload['source'])
                print("deal payload: ", payload['deal'])

                # Update the deal_lead_source and deal tables using the retrieved source_id
                sb.table("deal_lead_source").update(payload['source']).eq('id', source_id).execute()
                sb.table("deal").update({**payload['deal'], "source_id": source_id}).eq('id', entity_based_id).execute()

                print(f"Successfully updated Salesforce ID {salesforce_id} in the deal_lead_source and deal tables.")
                print()
            else:
                payload = self.map_o(deal, tenant_id, owner_id)
                print("source payload: ", payload['source'])
                print("deal payload: ", payload['deal'])
                source_response = sb.table("deal_lead_source").insert(payload['source']).execute()
                source_id = source_response.data[0]['id']
                deal_response = sb.table("deal").insert(
                    {**payload['deal'], "source_id": source_id}).execute()
                id_ = deal_response.data[0]['id']
                print("id ", id_, "salesforce id: ", deal['id'])
                # Add/Update row to entity_integration table
                sb.table("entity_integration").insert(
                    {"entity_based_id": id_, "salesforce_id": deal["id"],
                     "entity_type_id": 3}).execute()

                print(
                    f"Successfully inserted Salesforce ID {salesforce_id} into the deal_lead_source, deal, and "
                    f"entity_integration tables.")
                print()
