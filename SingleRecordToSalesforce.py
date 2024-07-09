from supabase import create_client, Client
import os
from dotenv import load_dotenv
import requests

# Load environment variables from the .env file
load_dotenv()

url = os.getenv('SUPABASE_URL')
key = os.getenv('SUPABASE_KEY')
supabase: Client = create_client(url, key)

response = supabase.table("account").select("*").limit(1).execute()
data = response.data

# Print the fetched accounts data
if data:
    record = data[0]  # Get the first (and only) record
    print("DATA: ", record)
else:
    print("No records found")

response = supabase.table("phone_book").select("*").limit(1).execute()
phone_data = response.data

# Print the fetched phone book data
if phone_data:
    record = phone_data[0]  # Get the first (and only) record
    print("Phone_DATA:",record)
else:
    print("No records found")

response = supabase.table("user").select("*").limit(1).execute()
user_data = response.data

# Print the fetched user data
if user_data:
    record = user_data[0]  # Get the first (and only) record
    print(record)
else:
    print("No records found")

payload = {
    "domain": data[0]['domain'],
    "phone_book": {
        "first_name": phone_data[0]['first_name'],
        "email": phone_data[0]['email'],
        "phone": phone_data[0]['phone'],
        "website": phone_data[0]['website'],
        "street": phone_data[0]['street'],
        "city": phone_data[0]['city'],
        "state": phone_data[0]['state'],
        "country": phone_data[0]['country'],
        "created_at": phone_data[0]['created_at'],
        "description": phone_data[0]['description'],
        "do_not_call": phone_data[0]['do_not_call']
    },
    "description": phone_data[0]['description'],
    "industry": data[0]['industry'],
    "no_of_employees": data[0]['no_of_employees'],
    "owner_id": "005GA00000ACuZGYA1"
}

create_contacts_url = "https://api.integration.app/connections/salesforce/actions/create-accounts/run"

headers = {
    "Authorization": f"Bearer {os.getenv('INTEGRATION_APP_TOKEN')}",
    "Content-Type": "application/json"
}

response = requests.request("POST", create_contacts_url, headers=headers, json=payload)
print(response.text)
