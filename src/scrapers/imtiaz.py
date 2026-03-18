import pandas as pd
import os
from src.scrapers.base_api import BaseAPIScraper

class ImtiazScraper(BaseAPIScraper):
    def __init__(self):
        # Base URL for Imtiaz's API
        super().__init__(base_url="https://shop.imtiaz.com.pk/api/", store_name="Imtiaz")
        
        # Adding Imtiaz-specific headers. They require these custom ones!
        self.default_headers.update({
            'app-name': 'imtiazsuperstore',
            'rest-id': '55126', # Global REST ID
            'Origin': 'https://shop.imtiaz.com.pk',
            'Referer': 'https://shop.imtiaz.com.pk/'
        })

        # Imtiaz's Blink online shopping portal (restId 55126) currently only lists active 
        # menus for Karachi. They do not appear to offer online delivery inventories 
        # for Lahore or Islamabad through this specific API at this time.
        # As requested, we will use one single major branch for Karachi.
        self.store_locations = {
            "Karachi": "54943"    # Karachi (Clifton) Branch
        }

    def fetch_sub_categories(self, branch_id):
        """
        Fetches the master menu and extracts all the deepest level sub-sections
        so we can query products from them correctly.
        """
        endpoint = "menu"
        params = {
            'restId': '55126',
            'rest_brId': branch_id
        }
        
        sub_categories = []
        try:
            response = self.fetch_api(endpoint=endpoint, params=params)
            data = response.get('data', [])
            
            for main_menu in data:
                # E.g. "Beverages"
                for section in main_menu.get('all_section', []):
                    # E.g. "Carbonated Soft Drinks"
                    parent_name = section.get('name', '')
                    for sub in section.get('all_sub_section', []):
                        # E.g. "Malt Drinks"
                        sub_categories.append({
                            'id': sub.get('id'),
                            'name': sub.get('name'),
                            'parent_category': parent_name
                        })
            
            # Deduplicate just in case
            unique_subs = {v['id']: v for v in sub_categories}.values()
            return list(unique_subs)
            
        except Exception as e:
            self.logger.error(f"Failed to fetch categories for Imtiaz branch {branch_id}: {e}")
            return []

    def fetch_products(self, branch_id, sub_section_id, page=1):
        """
        Fetches a specific page of products from a sub_section.
        """
        endpoint = "items-by-subsection"
        params = {
            'restId': '55126',
            'rest_brId': branch_id,
            'sub_section_id': sub_section_id,
            'delivery_type': '0',
            'page_no': page,
            'limit': 100
        }
        
        try:
            return self.fetch_api(endpoint=endpoint, params=params)
        except Exception as e:
            self.logger.error(f"Failed to fetch Imtiaz subsection {sub_section_id} Page {page}: {e}")
            return None

    def parse_products(self, raw_json, city_name, category_name):
        """
        Flattens the Imtiaz JSON into our standard dictionary format.
        """
        parsed_data = []
        if not raw_json or 'data' not in raw_json:
            return parsed_data
            
        items = raw_json.get('data', [])
        
        for item in items:
            raw_desc = item.get("desc", "") or ""
            parsed_data.append({
                "store": self.store_name,
                "city": city_name,
                "product_id": item.get("id"),
                "product_name": item.get("name"),
                "brand": item.get("brand_name", "Unknown"),
                "price": item.get("discount_price") if float(item.get("discount_price", 0)) > 0 else item.get("price"),
                "original_price": item.get("price"),
                "size_or_weight": item.get("tp_uom", ""),  # Imtiaz unit of measure (e.g., 'Piece', 'Kg', 'Pack')
                "category": category_name,
                "in_stock": item.get("availability", 0) == 1
            })
            
        return parsed_data

    def run(self):
        self.logger.info("Starting Imtiaz Scraper...")
        
        for city_name, branch_id in self.store_locations.items():
            self.logger.info(f"--- Scraping City: {city_name} (Branch {branch_id}) ---")
            
            sub_sections = self.fetch_sub_categories(branch_id)
            self.logger.info(f"Discovered {len(sub_sections)} active sub-sections categories for {city_name}.")
            
            if not sub_sections:
                continue
                
            all_products = []
            
            from tqdm import tqdm
            for sub in tqdm(sub_sections, desc=f"Sections ({city_name})"):
                sub_id = sub['id']
                cat_name = sub['parent_category']
                
                page = 1
                last_page_ids = set()
                
                while True:
                    raw_data = self.fetch_products(branch_id, sub_id, page)
                    
                    if not raw_data or 'data' not in raw_data or len(raw_data['data']) == 0:
                        break
                        
                    # Imtiaz API bug: Returns Page 1 continuously if page out of bounds.
                    # We detect this by checking if the IDs are exactly the same as the last page.
                    current_page_ids = {item.get('id') for item in raw_data['data']}
                    if current_page_ids.issubset(last_page_ids) and len(current_page_ids) > 0:
                        break
                    
                    products = self.parse_products(raw_data, city_name, cat_name)
                    all_products.extend(products)
                    
                    if len(products) < 100: # Limit was 100, if less means end of list
                        break
                        
                    last_page_ids = current_page_ids
                    page += 1
            
            if all_products:
                df = pd.DataFrame(all_products)
                output_path = os.path.join("data", "raw", f"imtiaz_raw_{city_name.lower()}.csv")
                df.to_csv(output_path, index=False)
                self.logger.info(f"Successfully saved {len(df)} Imtiaz products for {city_name} to {output_path}")
            else:
                self.logger.warning(f"No products scraped for {city_name}.")

if __name__ == "__main__":
    scraper = ImtiazScraper()
    scraper.run()
