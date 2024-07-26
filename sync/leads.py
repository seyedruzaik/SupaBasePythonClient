import requests
from sync import sb
from sync.enums import EntityType


class Leads:
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
        integration_response = sb.table('entity_integration').select('salesforce_id').eq('entity_type_id',
                                                                                         0).execute()
        if not integration_response.data:
            print(f"Failed to retrieve salesforce_ids from entity_integration: {integration_response.json()}")
            return

        supabase_salesforce_ids = {record['salesforce_id'] for record in integration_response.data}

        # Retrieve all leads from Salesforce
        integration_url = "https://api.integration.app/connections/salesforce/actions/get-leads/run"
        leads_response = self.session.post(integration_url).json()
        salesforce_leads = leads_response.get("output", {}).get("records", [])
        salesforce_lead_ids = {lead['id'] for lead in salesforce_leads}

        # Find salesforce_ids that are in Supabase but not in Salesforce
        orphaned_salesforce_ids = supabase_salesforce_ids - salesforce_lead_ids

        # Delete orphaned records from Supabase
        for salesforce_id in orphaned_salesforce_ids:
            print(f"Deleting orphaned salesforce_id: {salesforce_id}")
            # Retrieve the entity_based_id from the entity_integration table
            integration_response = sb.table('entity_integration').select('entity_based_id').eq('salesforce_id',
                                                                                               salesforce_id).execute()
            if not integration_response.data:
                print(f"Failed to retrieve entity_based_id for salesforce_id {salesforce_id} from entity_integration: "
                      f"{integration_response.json()}")
                continue

            entity_based_id = integration_response.data[0]['entity_based_id']

            # Delete from Supabase lead table
            supabase_response = sb.table('lead').delete().eq('id', entity_based_id).execute()
            if supabase_response.data:
                print(f"Successfully deleted lead {entity_based_id} from Supabase")
            else:
                print(f"Failed to delete lead {entity_based_id} from Supabase: {supabase_response.json()}")

            # Delete from entity_integration table
            integration_response = sb.table('entity_integration').delete().eq('entity_based_id',
                                                                              entity_based_id).execute()
            if integration_response.data:
                print(f"Successfully deleted entity integration for lead {entity_based_id} from Supabase")
            else:
                print(f"Failed to delete entity integration for lead {entity_based_id} from Supabase: "
                      f"{integration_response.json()}")

    def delete_missing_in_supabase(self):
        """
        Delete Salesforce IDs that exist in Salesforce but are missing in Supabase.
        """
        # Retrieve all salesforce_ids from the entity_integration table
        integration_response = sb.table('entity_integration').select('salesforce_id').eq('entity_type_id',
                                                                                         0).execute()
        if not integration_response.data:
            print(f"Failed to retrieve salesforce_ids from entity_integration: {integration_response.json()}")
            return

        supabase_salesforce_ids = {record['salesforce_id'] for record in integration_response.data}

        # Retrieve all leads from Salesforce
        integration_url = "https://api.integration.app/connections/salesforce/actions/get-leads/run"
        leads_response = self.session.post(integration_url).json()
        salesforce_leads = leads_response.get("output", {}).get("records", [])
        salesforce_lead_ids = {lead['id'] for lead in salesforce_leads}

        print(salesforce_lead_ids)
        print(supabase_salesforce_ids)
        # Find salesforce_ids that are in Salesforce but not in Supabase
        missing_in_supabase_ids = salesforce_lead_ids - supabase_salesforce_ids

        # Delete records from Salesforce that are not in Supabase
        for salesforce_id in missing_in_supabase_ids:
            print(f"Deleting salesforce_id from Salesforce: {salesforce_id}")
            self.delete_from_salesforce(salesforce_id)

    def delete_from_salesforce(self, salesforce_id: str):
        """
        Delete a lead from Salesforce.

        :param salesforce_id: The Salesforce ID of the lead to delete
        """
        url = "https://api.integration.app/connections/salesforce/actions/delete-leads/run"
        payload = {
            "id": salesforce_id
        }
        salesforce_response = self.session.post(url, json=payload)
        if salesforce_response.status_code == 200:
            print(f"Successfully deleted lead {salesforce_id} from Salesforce")
        else:
            print(f"Failed to delete lead {salesforce_id} from Salesforce: {salesforce_response.json()}")

    @staticmethod
    def map_i(row: dict) -> dict:
        """
        Field mapping from supabase to salesforce
        """
        # Accessing the deal_lead_source information
        deal_lead_source = row.get('deal_lead_source')

        # Ensure deal_lead_source is not None before accessing its attributes
        if deal_lead_source is not None:
            deal_lead_source_name = deal_lead_source.get('name', '')
        else:
            deal_lead_source_name = ''

        return {
            "fullName": f"{row['phone_book'].get('first_name', '')} {row['phone_book'].get('last_name', '')}"
                        .strip() or None,
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
            "source": deal_lead_source_name,
            "jobTitle": row['phone_book'].get('title') or None
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
                "entity_type_id": EntityType.LEAD.value
            }
            sb.table("entity_integration").insert(data).execute()
        else:
            sb.table("entity_integration").update({"salesforce_id": salesforce_id}).eq("entity_based_id", id_).execute()

    def update_salesforce_lead(self, owner_id: str):
        """
        Update supabase leads to Salesforce
        """
        integration_url = "https://api.integration.app/connections/salesforce/actions/update-lead/run"
        leads = sb.table("lead").select("*, phone_book(*), deal_lead_source(*)").eq("owner_id", owner_id).execute().data

        for lead in leads:
            lead_id = lead['id']
            salesforce_ids = (sb.table('entity_integration').select('salesforce_id')
                              .eq('entity_based_id', lead_id).eq('entity_type_id',
                                                                 0).execute())

            if salesforce_ids.data and salesforce_ids.data[0]['salesforce_id']:
                salesforce_id = salesforce_ids.data[0]['salesforce_id']

                payload = self.map_i(lead)
                payload["id"] = salesforce_id

                response = self.session.post(integration_url, json=payload)
                response_data = response.json()

                if response.status_code == 200:
                    print(f"Lead {lead_id} updated successfully in Salesforce.")
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
                print(f"No Salesforce ID found for lead {lead_id}!")
                print()

    def to_salesforce_leads(self, owner_id: str):
        """
        Export supabase leads to Salesforce
        """
        integration_url = "https://api.integration.app/connections/salesforce/actions/create-lead/run"
        leads = sb.table("lead").select("*, phone_book(*), deal_lead_source(*)").eq("owner_id", owner_id).execute().data
        for lead in leads:
            payload = self.map_i(lead)
            print(payload)
            response = self.session.post(integration_url, json=payload)
            if response.status_code == 200:
                print(f"Successfully exported lead {payload['fullName']}")
                id_ = response.json()["output"]["id"]
                self.track_record(lead["id"], id_)
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

    def from_salesforce_leads(self, owner_id: str, tenant_id):
        """
        Import salesforce leads to Salesforce
        """
        integration_url = "https://api.integration.app/connections/salesforce/actions/get-leads/run"
        leads = self.session.post(integration_url).json()
        for lead in leads["output"]["records"]:
            salesforce_id = lead['id']

            # Check if the salesforce_id exists before proceeding
            if self.check_salesforce_id(salesforce_id):
                print(f"Salesforce ID {salesforce_id} already exists in the entity_integration table.")
                # Update the existing record
                existing_record_response = (sb.table('entity_integration').select('entity_based_id').
                                            eq('salesforce_id', salesforce_id).execute())

                entity_based_id = existing_record_response.data[0]['entity_based_id']

                # Get the phone_book_id and source_id from the lead table
                lead_response = sb.table('lead').select('phone_book_id', 'source_id').eq('id',
                                                                                         entity_based_id).execute()
                phone_book_id = lead_response.data[0]['phone_book_id']
                source_id = lead_response.data[0]['source_id']

                payload = self.map_o(lead, tenant_id, owner_id)
                print("phone payload: ", payload['phone_book'])
                print("source payload: ", payload['source'])
                print("lead payload: ", payload['lead'])

                # Update the phone_book and deal_lead_source tables using the retrieved phone_book_id and source_id
                sb.table("phone_book").update(payload['phone_book']).eq('id', phone_book_id).execute()
                sb.table("deal_lead_source").update(payload['source']).eq('id', source_id).execute()
                sb.table("lead").update({**payload['lead'], "phone_book_id": phone_book_id, "source_id": source_id}).eq(
                    'id', entity_based_id).execute()

                print(
                    f"Successfully updated Salesforce ID {salesforce_id} in the phone_book, deal_lead_source, and "
                    f"lead tables.")
                print()
            else:
                payload = self.map_o(lead, tenant_id, owner_id)
                print("phone payload: ", payload['phone_book'])
                print("source payload: ", payload['source'])
                print("lead payload: ", payload['lead'])
                phone_book_response = sb.table("phone_book").insert(payload['phone_book']).execute()
                phone_book_id = phone_book_response.data[0]['id']
                source_response = sb.table("deal_lead_source").insert(payload['source']).execute()
                source_id = source_response.data[0]['id']
                lead_response = sb.table("lead").insert(
                    {**payload['lead'], "phone_book_id": phone_book_id, "source_id": source_id}).execute()
                id_ = lead_response.data[0]['id']
                print("id ", id_, "salesforce id: ", lead['id'])
                # Add/Update row to entity_integration table
                sb.table("entity_integration").insert(
                    {"entity_based_id": id_, "salesforce_id": lead["id"],
                     "entity_type_id": 0}).execute()

                print(
                    f"Successfully inserted Salesforce ID {salesforce_id} into the phone_book, deal_lead_source, "
                    f"lead, and entity_integration tables.")
                print()
