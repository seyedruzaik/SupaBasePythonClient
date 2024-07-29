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
                    Delete records from both ways
                """

                # leads.delete_missing_in_supabase()
                # leads.delete_orphaned_salesforce_ids()
                # deals.delete_from_salesforce("006dL000002lBDNQA2")
                # deals.delete_from_supabase("de38aa2a-cde3-44f0-acd4-be4e1e4431a3")
                # leads.delete_from_salesforce("00QdL000005xh7MUAQ")
                # leads.delete_from_supabase("3bdb7e4f-6e10-4aa6-b59f-308a023ceeaa")
                # contacts.delete_from_salesforce("003dL000003QCYpQAO")
                # contacts.delete_from_supabase("4909efbc-827e-41da-8941-2b5c2f677140")
                # accounts.delete_from_salesforce("001dL00000CbSs5QAF")
                # accounts.delete_from_supabase("0324aadd-0220-4837-ad2d-8e273d7990f1")

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
