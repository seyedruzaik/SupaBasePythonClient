from sync import sb
from sync.accounts import Accounts
from sync.contacts import Contacts
from sync.deals import Deals
from sync.leads import Leads


class Sync:

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
                    Delete records from Salesforce
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
                    Export Supabase records to Salesforce
                """

                # leads.to_salesforce_leads(user["user_id"])
                # contacts.to_salesforce_contacts(user["user_id"])
                # accounts.to_salesforce(user["user_id"])
                # deals.to_salesforce_deals(user["user_id"])

                """
                    Export Salesforce accounts to Salesforce
                """

                # leads.from_salesforce_leads(user["user_id"], 7)
                # deals.from_salesforce_deals(user["user_id"], 7)
                # contacts.from_salesforce_contacts(user["user_id"], 7)
                # accounts.from_salesforce(user["user_id"], 7)


if __name__ == "__main__":
    sync = Sync()
    sync.sync_salesforce()
