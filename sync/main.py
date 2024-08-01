from sync import sb
from sync.accounts import Accounts
from sync.contacts import Contacts
from sync.deals import Deals
from sync.hubspot_accounts import HubspotAccounts
from sync.hubspot_contacts import HubspotContacts
from sync.hubspot_deals import HubspotDeals
from sync.leads import Leads


class Sync:

    @staticmethod
    def hubspot_conns():
        connections = (sb.table("integration_connection")
                       .select("connection_id,connection_details,tenant_id")
                       .eq("connection_key", "hubspot")
                       .execute().data)
        for connection in connections:
            connection["users"] = sb.table("user_role").select("user_id").eq("tenant_id",
                                                                             connection["tenant_id"]).execute().data

        return connections

    @staticmethod
    def salesforce_conns():
        connections = (sb.table("integration_connection")
                       .select("connection_id,connection_details,tenant_id")
                       .eq("connection_key", "salesforce")
                       .execute().data)
        for connection in connections:
            connection["users"] = sb.table("user_role").select("user_id").eq("tenant_id",
                                                                             connection["tenant_id"]).execute().data

        return connections

    def sync_salesforce(self):
        for connection in self.salesforce_conns():
            accounts = Accounts(connection["connection_details"]["access_token"], "")
            contacts = Contacts(connection["connection_details"]["access_token"], "")
            deals = Deals(connection["connection_details"]["access_token"], "")
            leads = Leads(connection["connection_details"]["access_token"], "")
            for user in connection["users"]:
                print(user["user_id"])

                """
                    Delete records from Salesforce or Hubspot
                """

                # leads.delete_missing_in_supabase()
                # contacts.delete_missing_in_supabase()
                # accounts.delete_missing_in_supabase()
                # deals.delete_missing_in_supabase()

                """
                    Delete records from Supabase
                """
                # leads.delete_orphaned_salesforce_ids()
                # contacts.delete_orphaned_salesforce_ids()
                # accounts.delete_orphaned_salesforce_ids()
                # deals.delete_orphaned_salesforce_ids()

                """
                    Export Supabase records to Salesforce or Hubspot
                """

                # leads.to_salesforce_leads(user["user_id"])
                # contacts.to_salesforce_contacts(user["user_id"])
                # accounts.to_salesforce(user["user_id"])
                # deals.to_salesforce_deals(user["user_id"])

                """
                    Export Salesforce or Hubspot records to Supabase
                """

                # leads.from_salesforce_leads(user["user_id"], 7)
                # deals.from_salesforce_deals(user["user_id"], 7)
                # contacts.from_salesforce_contacts(user["user_id"], 7)
                # accounts.from_salesforce(user["user_id"], 7)

    def sync_hubspot(self):
        for connection in self.hubspot_conns():
            hubspot_accounts = HubspotAccounts(connection["connection_details"]["access_token"], "")
            hubspot_contacts = HubspotContacts(connection["connection_details"]["access_token"], "")
            hubspot_deals = HubspotDeals(connection["connection_details"]["access_token"], "")
            for user in connection["users"]:
                print(user["user_id"])

                """
                    Delete records from Hubspot
                """

                # hubspot_accounts.delete_missing_in_supabase()
                # hubspot_contacts.delete_missing_in_supabase()
                # hubspot_deals.delete_missing_in_supabase()

                """
                    Delete records from Supabase
                """

                # hubspot_accounts.delete_orphaned_hubspot_ids()
                # hubspot_contacts.delete_orphaned_hubspot_ids()
                # hubspot_deals.delete_orphaned_hubspot_ids()

                """
                    Export Supabase records to Hubspot
                """

                # hubspot_accounts.to_hubspot(user["user_id"])
                # hubspot_contacts.to_hubspot_contacts(user["user_id"])
                # hubspot_deals.to_hubspot_deals(user["user_id"])

                """
                    Export Hubspot records to Supabase
                """

                # hubspot_accounts.from_hubspot(user["user_id"], 7)
                # hubspot_contacts.from_hubspot_contacts(user["user_id"], 7)
                # hubspot_deals.from_hubspot_deals(user["user_id"], 7)


if __name__ == "__main__":
    sync = Sync()
    sync.sync_salesforce()
    sync.sync_hubspot()
