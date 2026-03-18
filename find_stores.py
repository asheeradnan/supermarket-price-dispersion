import requests

r = requests.get('https://admin.metro-online.pk/api/read/Stores', headers={'Origin': 'https://www.metro-online.pk'})
data = r.json().get('data', [])
for d in data:
    print(d.get('storeCode'), "->", d.get('storeName'), "-> ID:", d.get('id'))
