import requests
from sync import sb
from sync.enums import EntityType


class Contacts:
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
                                                                                         1).execute()
        if not integration_response.data:
            print(f"Failed to retrieve salesforce_ids from entity_integration: {integration_response.json()}")
            return

        supabase_salesforce_ids = {record['salesforce_id'] for record in integration_response.data}

        # Retrieve all contacts from Salesforce
        integration_url = "https://api.integration.app/connections/salesforce/actions/get-contacts/run"
        contacts_response = self.session.post(integration_url).json()
        salesforce_contacts = contacts_response.get("output", {}).get("records", [])
        salesforce_contact_ids = {contact['id'] for contact in salesforce_contacts}

        # Find salesforce_ids that are in Supabase but not in Salesforce
        orphaned_salesforce_ids = supabase_salesforce_ids - salesforce_contact_ids

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
        Delete Salesforce IDs that exist in Salesforce but are missing in Supabase.
        """
        # Retrieve all salesforce_ids from the entity_integration table
        integration_response = sb.table('entity_integration').select('salesforce_id').eq('entity_type_id',
                                                                                         1).execute()
        if not integration_response.data:
            print(f"Failed to retrieve salesforce_ids from entity_integration: {integration_response.json()}")
            return

        supabase_salesforce_ids = {record['salesforce_id'] for record in integration_response.data}

        # Retrieve all contacts from Salesforce
        integration_url = "https://api.integration.app/connections/salesforce/actions/get-contacts/run"
        contacts_response = self.session.post(integration_url).json()
        salesforce_contacts = contacts_response.get("output", {}).get("records", [])
        salesforce_contact_ids = {contact['id'] for contact in salesforce_contacts}

        # Find salesforce_ids that are in Salesforce but not in Supabase
        missing_in_supabase_ids = salesforce_contact_ids - supabase_salesforce_ids

        # Delete records from Salesforce that are not in Supabase
        for salesforce_id in missing_in_supabase_ids:
            print(f"Deleting salesforce_id from Salesforce: {salesforce_id}")
            self.delete_from_salesforce(salesforce_id)

    def delete_from_salesforce(self, salesforce_id: str):
        """
        Delete a contact from Salesforce.

        :param salesforce_id: The Salesforce ID of the contact to delete
        """
        url = "https://api.integration.app/connections/salesforce/actions/delete-contacts/run"
        payload = {
            "id": salesforce_id
        }
        salesforce_response = self.session.post(url, json=payload)
        if salesforce_response.status_code == 200:
            print(f"Successfully deleted contact {salesforce_id} from Salesforce")
        else:
            print(f"Failed to delete contact {salesforce_id} from Salesforce: {salesforce_response.json()}")

    @staticmethod
    def map_i(row: dict) -> dict:
        """
        Field mapping from supabase to salesforce
        """
        # Get the salesforce account ID using the supabase account ID
        account_id = row['account_id']
        account_response = (sb.table('entity_integration').select('salesforce_id')
                            .eq('entity_based_id', account_id)
                            .eq('entity_type_id', 1).limit(1).execute())
        salesforce_account_id = account_response.data[0]['salesforce_id'] if account_response.data else None

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
                "full": f"{row['phone_book'].get('street', '')}, {row['phone_book'].get('city', '')}, {row['phone_book'].get('state', '')}.",
                "street": row['phone_book'].get('street', ''),
                "city": row['phone_book'].get('city', ''),
                "state": row['phone_book'].get('state', ''),
                "country": row['phone_book']['country'],
            },
            "department": row['phone_book'].get('department', ''),
            "companyName": row['phone_book'].get('company', ''),
            "jobTitle": row['phone_book'].get('title', ''),
            "companyId": salesforce_account_id
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
                "entity_type_id": EntityType.CONTACT.value
            }
            sb.table("entity_integration").insert(data).execute()
        else:
            sb.table("entity_integration").update({"salesforce_id": salesforce_id}).eq("entity_based_id", id_).execute()

    def update_salesforce_contact(self, user_id: str):
        """
        Update supabase contacts to Salesforce
        """
        integration_url = "https://api.integration.app/connections/salesforce/actions/update-contacts/run"
        contacts = sb.table("contact").select("*, phone_book(*)").eq("created_by", user_id).execute().data

        for contact in contacts:
            contact_id = contact['id']
            salesforce_ids = (sb.table('entity_integration').select('salesforce_id')
                              .eq('entity_based_id', contact_id).eq('entity_type_id',
                                                                    1).execute())

            if salesforce_ids.data and salesforce_ids.data[0]['salesforce_id']:
                salesforce_id = salesforce_ids.data[0]['salesforce_id']

                payload = self.map_i(contact)
                payload["id"] = salesforce_id  # Add the salesforce_id to the payload

                response = self.session.post(integration_url, json=payload)
                response_data = response.json()

                if response.status_code == 200:
                    print(f"Contact {contact_id} updated successfully in Salesforce.")
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
                print(f"No Salesforce ID found for contact {contact_id}!")
                print()

    def to_salesforce_contacts(self, user_id: str):
        """
        Export supabase contacts to Salesforce
        """
        integration_url = "https://api.integration.app/connections/salesforce/actions/create-contact/run"
        contacts = sb.table("contact").select("*, phone_book(*)").eq("created_by", user_id).execute().data
        for contact in contacts:
            payload = self.map_i(contact)
            response = self.session.post(integration_url, json=payload)
            if response.status_code == 200:
                print(f"Successfully exported contact {payload['fullName']}")
                id_ = response.json()["output"]["id"]
                self.track_record(contact["id"], id_)
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

    def from_salesforce_contacts(self, owner_id: str, tenant_id):
        """
        Import salesforce contacts to Salesforce
        """
        integration_url = "https://api.integration.app/connections/salesforce/actions/get-contacts/run"
        contacts = self.session.post(integration_url).json()
        for contact in contacts["output"]["records"]:
            salesforce_id = contact['id']
            company_id = contact['fields']['companyId']

            account_data = sb.table('entity_integration').select('entity_based_id') \
                .eq('salesforce_id', company_id) \
                .eq('entity_type_id', 2) \
                .limit(1).execute()

            # Check if the salesforce_id exists before proceeding
            if self.check_salesforce_id(salesforce_id):
                print(f"Salesforce ID {salesforce_id} already exists in the entity_integration table.")
                # Update the existing record
                existing_record_response = sb.table('entity_integration').select('entity_based_id').eq('salesforce_id',
                                                                                                       salesforce_id).execute()
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
                sb.table("contact").update(
                    {**payload['contact'], "phone_book_id": phone_book_id, "account_id": acc_id}).eq('id',
                                                                                                     entity_based_id).execute()

                print(f"Successfully updated Salesforce ID {salesforce_id} in the phone_book and contact tables.")
                print()
            else:
                entity_based_ids = account_data.data
                payload = self.map_o(contact, tenant_id, owner_id)
                if entity_based_ids:
                    account_id = entity_based_ids[0]['entity_based_id']
                    print(f"Entity Based ID: {account_id}")
                    phone_book_response = sb.table("phone_book").insert(payload['phone_book']).execute()
                    phone_book_id = phone_book_response.data[0]['id']
                    contact_response = sb.table("contact").insert(
                        {**payload['contact'], "phone_book_id": phone_book_id, "account_id": account_id}).execute()
                    id_ = contact_response.data[0]['id']
                    print("id ", id_, "salesforce id: ", contact['id'])
                    # Add/Update row to entity_integration table
                    sb.table("entity_integration").insert(
                        {"entity_based_id": id_, "salesforce_id": contact["id"], "entity_type_id":
                            1}).execute()

                    print(
                        f"Successfully inserted Salesforce ID {salesforce_id} into the phone_book, contact, and "
                        f"entity_integration tables.")
                else:
                    print("No records found.")
