from sync import sb
from sync.accounts import Accounts


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
            for user in connection["users"]:
                accounts.to_salesforce(user["user_id"])
                # accounts.from_salesforce()

if __name__ == "__main__":
    sync = Sync()
    sync.sync_salesforce()
