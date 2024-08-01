import sys

import requests

from sync import sb
from sync.enums import EntityType


class HubspotContacts:
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
                                                                                      1).execute()
        if not integration_response.data:
            print(f"Failed to retrieve hubspot_ids from entity_integration: {integration_response.json()}")
            return

        supabase_hubspot_ids = {record['hubspot_id'] for record in integration_response.data}

        # Retrieve all contacts from hubspot
        integration_url = "https://api.integration.app/connections/hubspot/actions/get-contacts/run"
        contacts_response = self.session.post(integration_url).json()
        hubspot_contacts = contacts_response.get("output", {}).get("records", [])
        hubspot_contact_ids = {contact['id'] for contact in hubspot_contacts}

        # Find hubspot_ids that are in Supabase but not in hubspot
        orphaned_hubspot_ids = supabase_hubspot_ids - hubspot_contact_ids

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

            # Delete from Supabase contact table
            supabase_response = sb.table('contact').delete().eq('id', entity_based_id).execute()
            if supabase_response.data:
                print(f"Successfully deleted contact {entity_based_id} from Supabase")
            else:
                print(f"Failed to delete contact {entity_based_id} from Supabase: {supabase_response.json()}")

            # Delete from entity_integration table
            integration_response = sb.table('entity_integration').delete().eq('entity_based_id',
                                                                              entity_based_id).execute()
            if integration_response.data:
                print(f"Successfully deleted entity integration for contact {entity_based_id} from Supabase")
            else:
                print(f"Failed to delete entity integration for contact {entity_based_id} from Supabase: "
                      f"{integration_response.json()}")

    def delete_missing_in_supabase(self):
        """
        Delete hubspot IDs that exist in hubspot but are missing in Supabase.
        """
        # Retrieve all hubspot_ids from the entity_integration table
        integration_response = sb.table('entity_integration').select('hubspot_id').eq('entity_type_id',
                                                                                      1).execute()
        if not integration_response.data:
            print(f"Failed to retrieve hubspot_ids from entity_integration: {integration_response.json()}")
            return

        supabase_hubspot_ids = {record['hubspot_id'] for record in integration_response.data}

        # Retrieve all contacts from hubspot
        integration_url = "https://api.integration.app/connections/hubspot/actions/get-contacts/run"
        contacts_response = self.session.post(integration_url).json()
        hubspot_contacts = contacts_response.get("output", {}).get("records", [])
        hubspot_contact_ids = {contact['id'] for contact in hubspot_contacts}

        # Find hubspot_ids that are in hubspot but not in Supabase
        missing_in_supabase_ids = hubspot_contact_ids - supabase_hubspot_ids

        # Delete records from hubspot that are not in Supabase
        for hubspot_id in missing_in_supabase_ids:
            print(f"Deleting hubspot_id from hubspot: {hubspot_id}")
            self.delete_from_hubspot(hubspot_id)

    def delete_from_hubspot(self, hubspot_id: str):
        """
        Delete a contact from hubspot.

        :param hubspot_id: The hubspot ID of the contact to delete
        """
        url = "https://api.integration.app/connections/hubspot/actions/delete-contacts/run"
        payload = {
            "id": hubspot_id
        }
        hubspot_response = self.session.post(url, json=payload)
        if hubspot_response.status_code == 200:
            print(f"Successfully deleted contact {hubspot_id} from hubspot")
        else:
            print(f"Failed to delete contact {hubspot_id} from hubspot: {hubspot_response.json()}")

    @staticmethod
    def map_i(row: dict) -> dict:
        """
        Field mapping from supabase to hubspot
        """
        # Get the hubspot account ID using the supabase account ID
        account_id = row['account_id']
        account_response = (sb.table('entity_integration').select('hubspot_id')
                            .eq('entity_based_id', account_id)
                            .eq('entity_type_id', 2).limit(1).execute())
        hubspot_account_id = account_response.data[0]['hubspot_id'] if account_response.data else ""

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
                "full": f"{row['phone_book'].get('street', '')}, {row['phone_book'].get('city', '')}, "
                        f"{row['phone_book'].get('state', '')}.",
                "street": row['phone_book'].get('street', ''),
                "city": row['phone_book'].get('city', ''),
                "state": row['phone_book'].get('state', ''),
                "country": row['phone_book']['country'],
            },
            "department": row['phone_book'].get('department', ''),
            "companyName": row['phone_book'].get('company', ''),
            "jobTitle": row['phone_book'].get('title', ''),
            "companyId": hubspot_account_id
        }

    @staticmethod
    def map_o(row: dict, tenant_id, owner_id) -> dict:
        """Field mapping from hubspot to supabase"""
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
                "email": fields["primaryEmail"],
                "phone": fields["primaryPhone"],
                "website": None,
                "street": fields["primaryAddress"]["street"],
                "city": fields["primaryAddress"]["city"],
                "state": fields["primaryAddress"]["state"],
                "country": fields["primaryAddress"]["country"],
                "department": None,
                "description": None,
                "created_by": owner_id,
                "created_at": row["createdTime"],
                "last_updated_at": row["updatedTime"],
                "last_updated_by": owner_id,
                "first_name": fields["firstName"],
                "last_name": fields["lastName"],
                "do_not_call": None,
                "title": fields["jobTitle"],
                "company": None,
                "location": fields["primaryAddress"]["city"]
            },
            "contact": {
                'created_at': row["createdTime"],
                'created_by': owner_id,
                'last_updated_at': row["updatedTime"],
                'last_updated_by': owner_id,
                'entity_stage_id': stage_id,
                'entity_priority_id': priority_id,
                'group_id': group_id,
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
                "entity_type_id": EntityType.CONTACT.value
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

    def to_hubspot_contacts(self, user_id: str):
        """
        Export supabase contacts to hubspot
        """
        integration_url = "https://api.integration.app/connections/hubspot/actions/create-contact/run"
        contacts = sb.table("contact").select("*, phone_book(*)").eq("created_by", user_id).execute().data
        for contact in contacts:
            payload = self.map_i(contact)
            entity_table_id = sb.table("entity_integration").select("hubspot_id").eq('entity_based_id',
                                                                                     contact["id"]).execute().data
            if entity_table_id:
                integration_update_url = "https://api.integration.app/connections/hubspot/actions/update-contacts/run"
                hubspot_id = entity_table_id[0]['hubspot_id']
                payload["id"] = hubspot_id
                # Exclude companyId if it is an empty string or None
                if payload.get("companyId") in ("", None):
                    del payload["companyId"]
                updated_response = self.session.post(integration_update_url, json=payload)
                if updated_response.status_code == 200:
                    print(f"Successfully updated contact {payload['fullName']}")
                    print()
                else:
                    print(updated_response.json())
                continue
            else:
                payload = self.map_i(contact)
                response = self.session.post(integration_url, json=payload)
                # Extract the log entry with status 201
                log_entry_201 = None
                for log in response.json()['logs']:
                    if 'response' in log and log['response']['status'] == 201:
                        log_entry_201 = log
                        break

                # Check the status codes and handle accordingly
                if response.status_code == 200 or (log_entry_201 and log_entry_201['response']['status'] == 201):
                    id_ = ""
                    if log_entry_201 and log_entry_201['response']['status'] == 201:
                        response_data = log_entry_201['response']['data']
                        response_id = response_data.get('id')
                        id_ = response_id
                    else:
                        id_ = response.json()["output"]["id"]
                    self.track_record(contact["id"], id_)
                    print(f"Successfully exported account {contact['phone_book']['first_name']}")
                    print()
                else:
                    res_json = response.json()
                    print(res_json)
                    print()

    def from_hubspot_contacts(self, owner_id: str, tenant_id):
        """
        Import hubspot contacts to hubspot
        """
        integration_url = "https://api.integration.app/connections/hubspot/actions/get-contacts/run"
        contacts = self.session.post(integration_url).json()
        for contact in contacts["output"]["records"]:
            hubspot_id = contact['id']
            company_name = contact['fields']['companyName']
            phone_id = sb.table('phone_book').select('id').eq('first_name', company_name).limit(1).execute().data

            # Check if the hubspot_id exists before proceeding
            if self.check_hubspot_id(hubspot_id):
                print(f"hubspot ID {hubspot_id} already exists in the entity_integration table.")
                # Update the existing record
                existing_record_response = (sb.table('entity_integration').select('entity_based_id')
                                            .eq('hubspot_id', hubspot_id).execute())
                entity_based_id = existing_record_response.data[0]['entity_based_id']

                # Get the phone_book_id from the contact table
                contact_response = sb.table('contact').select('phone_book_id').eq('id',
                                                                                  entity_based_id).execute()
                phone_book_id = contact_response.data[0]['phone_book_id']

                # Query the contact table to get the account_id
                contact_id_response = sb.table('contact').select('account_id').eq('id', entity_based_id).execute()
                acc_id = contact_id_response.data[0]['account_id']

                payload = self.map_o(contact, tenant_id, owner_id)
                print("phone payload: ", payload['phone_book'])
                print("contact payload: ", payload['contact'])

                # Update the phone_book and contact tables using the retrieved phone_book_id
                sb.table("phone_book").update(payload['phone_book']).eq('id', phone_book_id).execute()
                (sb.table("contact").update(
                    {**payload['contact'], "phone_book_id": phone_book_id, "account_id": acc_id})
                 .eq('id', entity_based_id).execute())

                print(f"Successfully updated hubspot ID {hubspot_id} in the phone_book and contact tables.")
                print()
            else:
                payload = self.map_o(contact, tenant_id, owner_id)
                if phone_id:
                    company_data = (sb.table('account').select('id').eq('phone_book_id', phone_id[0]['id'])
                                    .limit(1).execute().data)
                    if company_data:
                        company_id = company_data[0]['id']
                        print("company id: ", company_id)
                        account_id = company_id
                        print(f"Entity Based ID: {account_id}")
                        phone_book_response = sb.table("phone_book").insert(payload['phone_book']).execute()
                        phone_book_id = phone_book_response.data[0]['id']
                        contact_response = sb.table("contact").insert(
                            {**payload['contact'], "phone_book_id": phone_book_id, "account_id": account_id}).execute()
                        id_ = contact_response.data[0]['id']
                        print("id ", id_, "hubspot id: ", contact['id'])
                        # Add/Update row to entity_integration table
                        sb.table("entity_integration").insert(
                            {"entity_based_id": id_, "hubspot_id": contact["id"], "entity_type_id":
                                1}).execute()

                        print(
                            f"Successfully inserted hubspot ID {hubspot_id} into the phone_book, contact, and "
                            f"entity_integration tables.")
                        print()
                    else:
                        print("No account records found.")
                        print()
                else:
                    print("Company name could not be found under this contact")
                    print()
