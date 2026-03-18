import requests
import json

url = "https://admin.metro-online.pk/api/read/Products?&type=Products_nd_associated_Brands&order=product_scoring__DESC&filter=||tier1Id&filterValue=||8619&filter=||tier2Id&filterValue=||8619&filter=||tier3Id&filterValue=||8619&filter=||tier4Id&filterValue=||8619&&offset=0&limit=1&filter=active&filterValue=true&filter=storeId&filterValue=12&filter=!url&filterValue=!null&filter=Op.available_stock&filterValue=Op.gt__0&"

headers = {
    'Origin': 'https://www.metro-online.pk',
    'Referer': 'https://www.metro-online.pk/'
}

r = requests.get(url, headers=headers)
print(json.dumps(r.json(), indent=2))
