import pandas as pd
import json
import re
from tqdm import tqdm
import math

from src.scrapers.base_api import BaseAPIScraper

class NaheedScraper(BaseAPIScraper):
    def __init__(self):
        super().__init__(base_url="https://www.naheed.pk/", store_name="Naheed")
        
        # Algolia headers and configs will be fetched dynamically
        self.app_id = None
        self.api_key = None
        self.index_name = "npk_live_default_products"
        
        self.total_fetched = 0
        self.all_products = []
        
    def setup_algolia(self):
        """Scrape the homepage HTML to extract the dynamic Algolia credentials."""
        self.logger.info("Initializing Algolia credentials from naheed.pk...")
        try:
            r = self.session.get(self.base_url, timeout=15)
            r.raise_for_status()
            
            app_id_match = re.search(r'"applicationId"\s*:\s*"([^"]+)"', r.text)
            api_key_match = re.search(r'"apiKey"\s*:\s*"([^"]+)"', r.text)
            
            if app_id_match and api_key_match:
                self.app_id = app_id_match.group(1)
                self.api_key = api_key_match.group(1)
                self.logger.info(f"Successfully configured Algolia! App ID: {self.app_id}")
            else:
                self.logger.error("Could not find Algolia credentials in Naheed's homepage.")
                
        except Exception as e:
            self.logger.error(f"Failed to setup Algolia: {e}")
            
    def fetch_algolia(self, payload):
        """Make a raw request to Algolia."""
        algolia_url = f"https://{self.app_id.lower()}-dsn.algolia.net/1/indexes/{self.index_name}/query"
        headers = {
            "X-Algolia-API-Key": self.api_key,
            "X-Algolia-Application-Id": self.app_id,
            "Content-Type": "application/json"
        }
        
        response = self.session.post(algolia_url, headers=headers, json=payload, timeout=15)
        response.raise_for_status()
        return response.json()
        
    def get_facets(self, facet_level, facet_filter=""):
        """Get all sub-categories for a specific parent category."""
        payload = {
            "query": "",
            "facets": [facet_level],
            "maxValuesPerFacet": 1000,
            "hitsPerPage": 0
        }
        if facet_filter:
            payload["facetFilters"] = [[facet_filter]]
            
        data = self.fetch_algolia(payload)
        facets_dict = data.get('facets', {}).get(facet_level, {})
        return facets_dict

    def fetch_products_for_facet(self, facet_filter, limit_pages=10):
        """Fetch all pages for a specific category facet up to the 1000 item limit (10 pages of 100)."""
        products = []
        for page in range(limit_pages):
            payload = {
                "query": "",
                "hitsPerPage": 100,
                "page": page,
                "facetFilters": [[facet_filter]]
            }
            data = self.fetch_algolia(payload)
            
            hits = data.get('hits', [])
            if not hits:
                break
                
            for p in hits:
                # Resolve price
                price_data = p.get('price', {}).get('PKR', {})
                price = price_data.get('default', 0) if isinstance(price_data, dict) else 0
                
                products.append({
                    "store": self.store_name,
                    "city": "Karachi", # Naheed is primarily KHI / Nationwide Delivery
                    "product_id": p.get('sku') or p.get('objectID'),
                    "product_name": p.get('name'),
                    "brand": p.get('manufacturer', 'Unknown'),
                    "price": price,
                    "original_price": price, # Algolia structure sometimes doesn't expose list price cleanly
                    "size_or_weight": "", # Extractible from name later if needed
                    "category": facet_filter,
                    "in_stock": p.get('in_stock', 1)
                })
                
            # If we reached the final page according to Algolia
            if page >= data.get('nbPages', 0) - 1:
                break
                
        return products

    def scrape_recursively(self, level, parent_filter=""):
        """
        Recursively break down categories if they exceed Algolia's 1000 records 
        limit per query window.
        """
        if level > 3:  
            # Max depth reached, just scrape what we can (first 1000 items)
            if parent_filter:
                items = self.fetch_products_for_facet(parent_filter)
                self.all_products.extend(items)
                self.pbar.update(len(items))
            return

        facet_name = f"categories.level{level}"
        sub_facets = self.get_facets(facet_name, parent_filter)
        
        # If there are no further subcategories, pull the parent directly
        if not sub_facets and parent_filter:
            items = self.fetch_products_for_facet(parent_filter)
            self.all_products.extend(items)
            self.pbar.update(len(items))
            return
            
        for child_category, count in sub_facets.items():
            child_filter = f"{facet_name}:{child_category}"
            
            if count > 1000 and level < 3:
                # Too many items, drill down deeper
                self.logger.info(f"Category '{child_category}' has {count} items. Drilling down to level {level+1}...")
                self.scrape_recursively(level + 1, child_filter)
            else:
                # Within limits or reached max depth, pull it directly
                items = self.fetch_products_for_facet(child_filter, limit_pages=min(10, math.ceil(count/100)))
                self.all_products.extend(items)
                self.pbar.update(len(items))

    def run(self):
        self.logger.info(f"Starting {self.store_name} scraper...")
        self.setup_algolia()
        
        if not self.app_id:
            return None
            
        # Get total global count for the progress bar
        payload = {"query": "", "hitsPerPage": 0}
        initial_data = self.fetch_algolia(payload)
        total_hits = initial_data.get('nbHits', 30000)
        
        self.pbar = tqdm(total=total_hits, desc="Scraping Naheed Products")
        
        # Start at level 0 (Root categories)
        self.scrape_recursively(level=0)
        
        self.pbar.close()
        
        # Deduplicate using product_id
        df = pd.DataFrame(self.all_products)
        df = df.drop_duplicates(subset=['product_id'])
        
        self.logger.info(f"Successfully scraped {len(df)} unique Naheed products!")
        
        # Save Raw Data
        import os
        os.makedirs(os.path.join("data", "raw"), exist_ok=True)
        output_file = os.path.join("data", "raw", "naheed_raw.csv")
        df.to_csv(output_file, index=False)
        self.logger.info(f"Data saved to {output_file}")
        
        return df

if __name__ == "__main__":
    scraper = NaheedScraper()
    scraper.run()
