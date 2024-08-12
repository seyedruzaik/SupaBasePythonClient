import requests
from datetime import datetime
from sync import sb
from sync.enums import EntityType


class HubspotDeals:
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
        integration_response = (sb.table('entity_integration').select('hubspot_id').
                                eq('entity_type_id', 3).execute())
        if not integration_response.data:
            print(f"Failed to retrieve hubspot_ids from entity_integration: {integration_response.json()}")
            return

        supabase_hubspot_ids = {record['hubspot_id'] for record in integration_response.data}

        # Retrieve all deals from hubspot
        integration_url = "https://api.integration.app/connections/hubspot/actions/get-deals/run"
        deals_response = self.session.post(integration_url).json()
        hubspot_deals = deals_response.get("output", {}).get("records", [])
        hubspot_deal_ids = {deal['id'] for deal in hubspot_deals}

        # Find hubspot_ids that are in Supabase but not in hubspot
        orphaned_hubspot_ids = supabase_hubspot_ids - hubspot_deal_ids
        # Delete orphaned records from Supabase
        for hubspot_id in orphaned_hubspot_ids:
            if hubspot_id is not None:
                print(f"Deleting orphaned hubspot_id: {hubspot_id}")
                # Retrieve the entity_based_id from the entity_integration table
                integration_response = (sb.table('entity_integration').select('entity_based_id').
                                        eq('hubspot_id', hubspot_id).execute())
                if not integration_response.data:
                    print(f"Failed to retrieve entity_based_id for hubspot_id {hubspot_id} from entity_integration: "
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
            else:
                continue

    def delete_missing_in_supabase(self):
        """
        Delete hubspot IDs that exist in hubspot but are missing in Supabase.
        """
        # Retrieve all hubspot_ids from the entity_integration table
        integration_response = (sb.table('entity_integration').select('hubspot_id').
                                eq('entity_type_id', 3).execute())
        if not integration_response.data:
            print(f"Failed to retrieve hubspot_ids from entity_integration: {integration_response.json()}")
            return

        supabase_hubspot_ids = {record['hubspot_id'] for record in integration_response.data}

        # Retrieve all deals from hubspot
        integration_url = "https://api.integration.app/connections/hubspot/actions/get-deals/run"
        deals_response = self.session.post(integration_url).json()
        hubspot_deals = deals_response.get("output", {}).get("records", [])
        hubspot_deal_ids = {deal['id'] for deal in hubspot_deals}

        # Find hubspot_ids that are in hubspot but not in Supabase
        missing_in_supabase_ids = hubspot_deal_ids - supabase_hubspot_ids

        # Delete records from hubspot that are not in Supabase
        for hubspot_id in missing_in_supabase_ids:
            print(f"Deleting hubspot_id from hubspot: {hubspot_id}")
            self.delete_from_hubspot(hubspot_id)

    def delete_from_hubspot(self, hubspot_id: str):
        """
        Delete a deal from hubspot.

        :param hubspot_id: The hubspot ID of the deal to delete
        """
        url = "https://api.integration.app/connections/hubspot/actions/delete-deals/run"
        payload = {
            "id": hubspot_id
        }
        hubspot_response = self.session.post(url, json=payload)
        if hubspot_response.status_code == 200:
            print(f"Successfully deleted deal {hubspot_id} from hubspot")
        else:
            print(f"Failed to delete deal {hubspot_id} from hubspot: {hubspot_response.json()}")

    @staticmethod
    def map_i(row: dict) -> dict:
        """
        Field mapping from Supabase to HubSpot
        """
        # Parse the close_date to a datetime object, handling None or empty strings
        close_date = row.get("close_date", '')

        # Check if close_date is None or an empty string
        if not close_date:
            date_part = None  # No date to extract if it's empty
        else:
            try:
                # Parse the date only if it's a valid string
                datetime_obj = datetime.fromisoformat(close_date)
                date_part = datetime_obj.date()
            except ValueError as e:
                print("Error parsing close_date:", e)
                date_part = None  # Handle the error by setting date_part to None

        # Convert probability to a float
        probability = row.get("score", None)
        if probability is not None:
            probability = float(probability)
            if probability > 1:
                probability /= 100
        else:
            probability = 0.0  # Default value if score is None

        # Prepare the mapped dictionary
        return {
            "name": row.get("name", None),
            "amount": row.get("revenue", None),
            "currency": row.get("currency", None),
            "source": row.get("deal_lead_source", {}).get("name") if row.get("deal_lead_source") else None,
            "stage": row.get("entity_stage", {}).get("name") if row.get("entity_stage") else None,
            "probability": probability,  # Keeping as float for calculations
            "closeTime": date_part.isoformat() if date_part else None  # Handle None for date_part
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

        return {
            "deal": {
                'group_id': group_id,
                'entity_stage_id': stage_id,
                'name': row['name'],
                'revenue': fields['amount'],
                'expected_close_date': fields['closedate'],
                'close_date': fields['closedate'],
                "created_by": owner_id,
                "score": fields['hs_deal_stage_probability'],
                "created_at": row["createdTime"],
                "last_updated_at": row["updatedTime"],
                "last_updated_by": owner_id,
                'owner_id': owner_id
            },
            "source": {
                "name": fields['hs_analytics_source'] or "",
                "created_at": row["createdTime"]
            }
        }

    @staticmethod
    def check_hubspot_id(hubspot_id: str) -> bool:
        """
        Check if the given hubspot_id exists in the entity_integration table.

        :param hubspot_id: The hubspot ID to check
        :return: True if the hubspot ID exists, False otherwise
        """
        response = sb.table('entity_integration').select('id').eq('hubspot_id', hubspot_id).execute()

        # Return True if any rows are returned, otherwise False
        return bool(response.data)

    @staticmethod
    def track_record(id_: str, hubspot_id):
        is_in_db = sb.table("entity_integration").select("*").eq("entity_based_id", id_).execute().data
        if not is_in_db:
            data = {
                "entity_based_id": id_,
                "hubspot_id": hubspot_id,
                "entity_type_id": EntityType.DEAL.value
            }
            sb.table("entity_integration").insert(data).execute()
        else:
            sb.table("entity_integration").update({"hubspot_id": hubspot_id}).eq("entity_based_id", id_).execute()

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

    def to_hubspot_deals(self, owner_id: str):
        """
        Export supabase deals to hubspot
        """
        integration_url = "https://api.integration.app/connections/hubspot/actions/create-deal/run"
        integration_update_url = "https://api.integration.app/connections/hubspot/actions/update-deal/run"

        # Fetching the entity_based_id and hubspot_id from entity_integration table
        entity_integration_data = sb.table("entity_integration").select("entity_based_id, hubspot_id").eq(
            "entity_type_id", 3).execute().data
        entity_based_ids = [item['entity_based_id'] for item in entity_integration_data]
        hubspot_ids = {item['entity_based_id']: item['hubspot_id'] for item in entity_integration_data}

        # Fetching all deals with the specified owner_id
        all_deals = sb.table("deal").select("*, entity_stage(*), deal_lead_source(*)").eq("owner_id",
                                                                                          owner_id).execute().data

        # Filtering deals where the id is not in entity_based_ids
        deals = [deal for deal in all_deals if deal["id"] not in entity_based_ids]

        # Printing the deals where the id is in entity_based_ids
        excluded_deals = [deal for deal in all_deals if deal["id"] in entity_based_ids]
        # print("Deals excluded from export:")
        # for excluded_deal in excluded_deals:
        #     print(f"Excluded Deal ID: {excluded_deal['id']}, Name: {excluded_deal['name']}")

        # Exporting deals
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
                print(res_json)
                # Extracting the status and error code
                status = res_json['data']['response']['status']
                error_code = res_json['data']['response']['data'][0]['errorCode']
                print(f"Failed to export deal {payload['name']}: Status {status}, Error Code {error_code}")

        # Updating excluded deals
        for excluded_deal in excluded_deals:
            payload = self.map_i(excluded_deal)
            payload["id"] = hubspot_ids.get(excluded_deal["id"], None)
            response = self.session.post(integration_update_url, json=payload)
            if response.status_code == 200:
                print(f"Successfully updated excluded deal {payload['name']}")
                print()
            else:
                res_json = response.json()
                # Extracting the status and error code
                # status = res_json['data']['response']['status']
                # error_code = res_json['data']['response']['data'][0]['errorCode']
                # print(f"Failed to update excluded deal {payload['name']}: Status {status}, Error Code {error_code}")
                print(res_json)

    def from_hubspot_deals(self, owner_id: str, tenant_id):
        """
        Import hubspot deals to hubspot
        """
        integration_url = "https://api.integration.app/connections/hubspot/actions/get-deals/run"
        deals = self.session.post(integration_url).json()
        for deal in deals["output"]["records"]:
            hubspot_id = deal['id']

            # Check if the hubspot_id exists before proceeding
            if self.check_hubspot_id(hubspot_id):
                print(f"hubspot ID {hubspot_id} already exists in the entity_integration table.")
                # Update the existing record
                existing_record_response = (sb.table('entity_integration').
                                            select('entity_based_id').eq('hubspot_id', hubspot_id).execute())
                entity_based_id = existing_record_response.data[0]['entity_based_id']

                # Get the source_id from the deal table
                deal_response = sb.table('deal').select('source_id').eq('id', entity_based_id).execute()
                source_id = deal_response.data[0]['source_id']

                payload = self.map_o(deal, tenant_id, owner_id)
                print("source payload: ", payload['source'])
                print("deal payload: ", payload['deal'])

                # Update the deal_lead_source and deal tables using the retrieved source_id
                sb.table("deal_lead_source").update(payload['source']).eq('id', source_id).execute()
                (sb.table("deal").update({**payload['deal'], "source_id": source_id})
                 .eq('id', entity_based_id).execute())

                print(f"Successfully updated hubspot ID {hubspot_id} in the deal_lead_source and deal tables.")
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
                print("id ", id_, "hubspot id: ", deal['id'])
                # Add/Update row to entity_integration table
                sb.table("entity_integration").insert(
                    {"entity_based_id": id_, "hubspot_id": deal["id"],
                     "entity_type_id": 3}).execute()

                print(
                    f"Successfully inserted hubspot ID {hubspot_id} into the deal_lead_source, deal, and "
                    f"entity_integration tables.")
                print()
