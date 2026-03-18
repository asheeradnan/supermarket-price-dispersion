import pandas as pd
import time
import os
from src.scrapers.base_api import BaseAPIScraper

class MetroScraper(BaseAPIScraper):
    def __init__(self):
        # Base URL for Metro Pakistan's backend API
        super().__init__(base_url="https://admin.metro-online.pk/api/read/", store_name="Metro")
        
        # Adding Metro-specific headers
        self.default_headers.update({
            'Origin': 'https://www.metro-online.pk',
            'Referer': 'https://www.metro-online.pk/'
        })

        # Store IDs across Pakistan (12 = Karachi, 10 = Lahore, 11 = Islamabad)
        self.store_locations = {
            "Karachi": "12",
            "Lahore": "10",
            "Islamabad": "11"
        }

    def fetch_category_ids(self, store_id):
        """
        Dynamically fetches all available category IDs for a given store.
        """
        endpoint = "Categories"
        params = [
            ('filter', 'storeId'), ('filterValue', store_id),
            ('filter', 'enable'), ('filterValue', 'true'),
            ('limit', 5000) # Ensure we get all of them
        ]
        try:
            response_json = self.fetch_api(endpoint=endpoint, params=params)
            if response_json and 'data' in response_json:
                category_ids = []
                for cat in response_json['data']:
                    if 'id' in cat:
                        category_ids.append(cat['id'])
                # Deduplicate and return
                return list(set(category_ids))
        except Exception as e:
            self.logger.error(f"Failed to fetch categories for store {store_id}: {e}")
        return []

    def fetch_products(self, city_name, store_id, category_id, page=1):
        """
        Fetches a specific page of products from a category.
        """
        endpoint = "Products"
        limit = 100
        offset = (page - 1) * limit
        
        # Metro requires identical filter/filterValue keys, best done with a list of tuples in requests
        params = [
            ('type', 'Products_nd_associated_Brands'),
            ('order', 'product_scoring__DESC'),
            ('filter', '||tier1Id'), ('filterValue', f'||{category_id}'),
            ('filter', '||tier2Id'), ('filterValue', f'||{category_id}'),
            ('filter', '||tier3Id'), ('filterValue', f'||{category_id}'),
            ('filter', '||tier4Id'), ('filterValue', f'||{category_id}'),
            ('offset', offset),
            ('limit', limit),
            ('filter', 'active'), ('filterValue', 'true'),
            ('filter', 'storeId'), ('filterValue', store_id),
            ('filter', '!url'), ('filterValue', '!null'),
            ('filter', 'Op.available_stock'), ('filterValue', 'Op.gt__0')
        ]
        
        try:
            response_json = self.fetch_api(endpoint=endpoint, params=params)
            return response_json
        except Exception as e:
            self.logger.error(f"Failed to fetch {city_name} - {category_id} - Page {page}: {e}")
            return None

    def parse_products(self, raw_json, city_name):
        """
        Extracts the relevant fields from the JSON payload into a flat dictionary.
        """
        parsed_data = []
        if not raw_json or 'data' not in raw_json:
            return parsed_data
            
        items = raw_json.get('data', [])
        
        for item in items:
            parsed_data.append({
                "store": self.store_name,
                "city": city_name,
                "product_id": item.get("id"),
                "product_name": item.get("product_name"),
                "brand": item.get("brand_name", "Unknown"),
                "price": item.get("sell_price"),
                "original_price": item.get("price"),
                "size_or_weight": str(item.get("weight", "")) + " " + str(item.get("unit_type", "")),
                "category": item.get("tier2Name", item.get("teir1Name", "")),
                "in_stock": item.get("available_stock", 0) > 0
            })
            
        return parsed_data

    def run(self):
        """
        Main execution loop to scrape multiple cities and all categories.
        """
        self.logger.info("Starting Metro Scraper...")
        
        for city_name, store_id in self.store_locations.items():
            self.logger.info(f"--- Scraping City: {city_name} ---")
            
            # Dynamically fetch all category IDs instead of hardcoding
            target_categories = self.fetch_category_ids(store_id)
            self.logger.info(f"Discovered {len(target_categories)} active categories for {city_name}.")
            
            all_products = []
            
            # Using tqdm to show a progress bar in the terminal
            from tqdm import tqdm
            for category_id in tqdm(target_categories, desc=f"Categories ({city_name})"):
                page = 1
                while True:
                    self.logger.debug(f"Fetching Category {category_id} Page {page}...")
                    raw_data = self.fetch_products(city_name, store_id, category_id, page)
                    
                    if not raw_data or 'data' not in raw_data or len(raw_data['data']) == 0:
                        break
                        
                    products = self.parse_products(raw_data, city_name)
                    all_products.extend(products)
                    
                    # If we got less than the limit, it's the last page
                    if len(products) < 100:
                        break
                        
                    page += 1
                    
            # Save to Raw Layer progressively by City
            if all_products:
                df = pd.DataFrame(all_products)
                output_path = os.path.join("data", "raw", f"metro_raw_{city_name.lower()}.csv")
                df.to_csv(output_path, index=False)
                self.logger.info(f"Successfully saved {len(df)} Metro products for {city_name} to {output_path}")
            else:
                self.logger.warning(f"No products scraped for {city_name}.")

if __name__ == "__main__":
    scraper = MetroScraper()
    scraper.run()
