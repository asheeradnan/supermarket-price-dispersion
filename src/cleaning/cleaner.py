import pandas as pd
import glob
import os
import re

def clean_price(price_val):
    if pd.isna(price_val):
        return None
    # If price is already numeric, return it
    if isinstance(price_val, (int, float)):
        return float(price_val)
    
    # Clean up string formats like "Rs. 1,000", "PKR 50.00", etc.
    price_str = str(price_val).replace(',', '')
    numeric_matches = re.findall(r'\d+\.?\d*', price_str)
    if numeric_matches:
        return float(numeric_matches[0])
    return None

def normalize_city(city_val):
    if pd.isna(city_val):
        return "Unknown"
    city_str = str(city_val).lower().strip()
    
    # Group branch-specific names into their metropolitan hubs
    if "karachi" in city_str or "khi" in city_str:
        return "Karachi"
    if "lahore" in city_str or "lhr" in city_str:
        return "Lahore"
    if "islamabad" in city_str or "isb" in city_str or "rawalpindi" in city_str or "pindi" in city_str:
        return "Islamabad/Rawalpindi"
    if "faisalabad" in city_str or "fsd" in city_str:
        return "Faisalabad"
        
    return city_str.title()

def clean_raw_data():
    print("Starting Phase 3: Data Cleaning & Normalization")
    
    raw_files = glob.glob(os.path.join("data", "raw", "*.csv"))
    if not raw_files:
        print("No raw data found!")
        return

    all_dfs = []
    for file in raw_files:
        try:
            df = pd.read_csv(file)
            print(f"Reading {os.path.basename(file)}... ({len(df)} rows)")
            all_dfs.append(df)
        except Exception as e:
            print(f"Error reading {file}: {e}")
            
    if not all_dfs:
        return
        
    # 1. Merge into Master DataFrame
    master_df = pd.concat(all_dfs, ignore_index=True)
    print(f"\nInitial Master Data Shape: {master_df.shape}")
    
    # 2. Standardize Column Names
    # Expecting: store, city, product_id, product_name, brand, price, original_price, size_or_weight, category, in_stock
    master_df.columns = [c.lower().strip() for c in master_df.columns]
    master_df = master_df.rename(columns={'title': 'product_name'}) # Failsafe for Al-Fatah
    
    # 3. Clean Text & Handle Missing
    print("Normalizing Product Names...")
    master_df['product_name'] = master_df['product_name'].astype(str).str.strip().str.lower()
    
    print("Normalizing Brands...")
    master_df['brand'] = master_df['brand'].fillna("Unknown").astype(str).str.strip().str.upper()
    
    print("Cleaning Prices...")
    # Fill missing prices with original_price if available, then apply regex cleaner
    master_df['price'] = master_df['price'].fillna(master_df['original_price']).apply(clean_price)
    master_df['original_price'] = master_df['original_price'].apply(clean_price)
    
    # Remove rows with absolutely no valid price
    master_df = master_df.dropna(subset=['price'])
    
    print("Standardizing Locations...")
    master_df['city'] = master_df['city'].apply(normalize_city)
    
    print("Enforcing Types...")
    master_df['product_id'] = master_df['product_id'].astype(str)
    master_df['in_stock'] = master_df['in_stock'].astype(bool)
    
    # 4. Final Deduplication (In case of overlapping pagination errors)
    # Deduplicate on the specific combination of Store + internal ID
    master_df = master_df.drop_duplicates(subset=['store', 'product_id'], keep='last')
    
    print(f"\nFinal Cleaned Master Data Shape: {master_df.shape}")
    
    # 5. Export to Processed Directory
    os.makedirs(os.path.join("data", "processed"), exist_ok=True)
    output_path = os.path.join("data", "processed", "master_cleaned_products.csv")
    master_df.to_csv(output_path, index=False)
    
    print(f"Cleaned dataset saved successfully to: {output_path}")
    print("\nSample Preview:")
    print(master_df[['store', 'city', 'product_name', 'brand', 'price', 'in_stock']].head())

if __name__ == "__main__":
    clean_raw_data()
