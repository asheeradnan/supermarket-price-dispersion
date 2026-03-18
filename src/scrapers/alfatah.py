import pandas as pd
import os
from src.scrapers.base_api import BaseAPIScraper

class AlFatahScraper(BaseAPIScraper):
    def __init__(self):
        # Al-Fatah is powered by Shopify, which makes scraping incredibly efficient!
        super().__init__(base_url="https://www.alfatah.pk/", store_name="AlFatah")
        
        # Shopify limits to 250 products per request
        self.limit = 250

        # Al-Fatah operates as a unified national online store
        # You've specified targeting Faisalabad.
        self.cities = ["Faisalabad"]

    def fetch_products(self, page=1):
        """
        Pulls a page of products directly from Shopify's open JSON API.
        """
        endpoint = "products.json"
        params = {
            'limit': self.limit,
            'page': page
        }
        
        try:
            return self.fetch_api(endpoint=endpoint, params=params)
        except Exception as e:
            self.logger.error(f"Failed to fetch Al-Fatah Page {page}: {e}")
            return None

    def parse_products(self, raw_json):
        """
        Parses Shopify's product and variant tree.
        """
        parsed_data = []
        if not raw_json or 'products' not in raw_json:
            return parsed_data
            
        products = raw_json.get('products', [])
        
        for p in products:
            # Shopify allows multiple sizes/flavors per product (Variants)
            # We want to treat each variant as a distinct row 
            for variant in p.get('variants', []):
                
                # Use variant title as size unless it's strictly 'Default Title'
                size_or_weight = variant.get('title', "")
                if size_or_weight.lower() == "default title":
                    size_or_weight = ""

                parsed_data.append({
                    "store": self.store_name,
                    "city": "National", # Al-Fatah acts as a unified platform online
                    "product_id": variant.get("id"),
                    "product_name": p.get("title"),
                    "brand": p.get("vendor", "Unknown"),
                    "price": variant.get("price"),
                    "original_price": variant.get("compare_at_price") or variant.get("price"),
                    "size_or_weight": size_or_weight,
                    "category": p.get("product_type"),
                    "in_stock": variant.get("available", True)
                })
                
        return parsed_data

    def run(self):
        self.logger.info("Starting Al-Fatah Scraper...")
        all_products = []
        
        page = 1
        from tqdm import tqdm
        
        # Shopify doesn't tell us how many pages ahead of time in this endpoint, 
        # so we will use an infinite loop until it returns empty.
        with tqdm(desc="Fetching Al-Fatah Pages") as pbar:
            while True:
                raw_data = self.fetch_products(page)
                
                if not raw_data or not raw_data.get('products'):
                    self.logger.info(f"Reached end of catalog at page {page-1}")
                    break
                    
                products = self.parse_products(raw_data)
                if not products:
                    break
                    
                all_products.extend(products)
                pbar.update(1)
                
                if len(raw_data['products']) < self.limit:
                    self.logger.info(f"Reached end of catalog at page {page}")
                    break
                    
                page += 1
                
        if all_products:
            # Tag the pulled inventory with the chosen city
            final_list = []
            for city in self.cities:
                for item in all_products:
                    item_copy = item.copy()
                    item_copy['city'] = city
                    final_list.append(item_copy)

            df = pd.DataFrame(final_list)
            output_path = os.path.join("data", "raw", f"alfatah_raw_faisalabad.csv")
            df.to_csv(output_path, index=False)
            self.logger.info(f"Successfully saved {len(df)} Al-Fatah products to {output_path}")
        else:
            self.logger.warning("No products scraped for Al-Fatah.")

if __name__ == "__main__":
    scraper = AlFatahScraper()
    scraper.run()
