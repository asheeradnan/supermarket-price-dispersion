import requests
import json

url = "https://admin.metro-online.pk/api/read/Store"
headers = {
    'Origin': 'https://www.metro-online.pk',
    'Referer': 'https://www.metro-online.pk/'
}
r = requests.get(url, headers=headers)
data = r.json().get('data', [])
for s in data:
    print(f"{s.get('store_name')} -> storeId: {s.get('id')}")
