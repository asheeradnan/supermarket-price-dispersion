import pandas as pd
import os
from rapidfuzz import process, fuzz
from tqdm import tqdm
import re

def standardize_product_text(text):
    """Deep text standardization for optimal fuzzy matching"""
    if pd.isna(text):
        return ""
    text = str(text).lower().strip()
    
    # Standardize weights and volumes
    text = re.sub(r'\b(kg|kilo|kilogram)\b', 'kg', text)
    text = re.sub(r'\b(g|gm|grams|gram)\b', 'g', text)
    text = re.sub(r'\b(ml|mililiter)\b', 'ml', text)
    text = re.sub(r'\b(ltr|liter|litre|l|litres)\b', 'l', text)

    # Remove special characters except decimal points
    text = re.sub(r'[^\w\s\.]', ' ', text)
    # Remove excess whitespace
    text = re.sub(r'\s+', ' ', text).strip()
    return text

def extract_measurement(text):
    """Extract standard volume/weight to prevent comparing 500ml to 4l."""
    if not text:
        return None
    # Looking for patterns like 500ml, 4l, 2.5kg, 100g
    match = re.search(r'(\d+(?:\.\d+)?)\s*(ml|l|kg|g)', text)
    if match:
        number = float(match.group(1))
        unit = match.group(2)
        # Normalize slightly to make safe comparisons (e.g. 1000ml = 1l, 1000g = 1kg)
        if unit == 'ml' and number >= 1000:
            number = number / 1000.0
            unit = 'l'
        elif unit == 'g' and number >= 1000:
            number = number / 1000.0
            unit = 'kg'
        return f"{number}{unit}"
    return None

def is_multipack(text):
    """Detect if a product is a pack/carton to avoid mapping to single items.
       Matches: x24, x5, pack of 6, carton, 12pack, etc.
    """
    if not text:
        return False
    # Check for formats like "x24", "x 5", "pack of 6", "12 pack", "carton", "box"
    return bool(re.search(r'\b(x\s?\d+|pack of \d+|\d+\s?pack|carton|box|tray)\b', str(text).lower()))
    

def run_entity_resolution(threshold=85):
    print("Starting Phase 4: Cross-Store Entity Resolution (Fuzzy Matching)")

    input_path = os.path.join("data", "processed", "master_cleaned_products.csv")
    if not os.path.exists(input_path):
        print(f"Error: {input_path} not found.")
        return

    df = pd.read_csv(input_path)

    print("Preparing text for fuzzy matching...")
    df['match_key'] = df['product_name'].apply(standardize_product_text)
    df['measurement'] = df['match_key'].apply(extract_measurement)

    # Note the city on the store name so the user knows (e.g., Metro (Islamabad))
    df['store_with_city'] = df.apply(lambda row: f"{row['store']} ({row['city']})" if pd.notna(row['city']) and row['city'] != "" else row['store'], axis=1)

    stores = df['store'].unique()

    if len(stores) < 2:
        print("Need at least 2 distinct stores for entity resolution.")
        return

    anchor_store = "Naheed" if "Naheed" in stores else stores[0]
    print(f"Using '{anchor_store}' as the Reference Anchor Store.")

    anchor_df = df[df['store'] == anchor_store].copy()
    other_df = df[df['store'] != anchor_store].copy()

    # Create a lookup mapping for quick access
    anchor_names = anchor_df['match_key'].tolist()
    anchor_measurements = anchor_df['measurement'].tolist()
    
    results = []

    # Loop over the other stores and match against our anchor store
    print(f"Matching {len(other_df)} products against {len(anchor_df)} anchor products...")
    # Iterate with a progress bar
    for idx, row in tqdm(other_df.iterrows(), total=len(other_df), desc="Fuzzy Matching"):
        search_query = row['match_key']
        search_meas = row['measurement']
        orig_search_name = row['product_name']
        if not search_query or len(search_query) < 4:
            continue

        # If it's a multipack, let's just gently skip comparing it against single items to avoid 1kg vs 1kg x5 bugs 
        is_search_multipack = is_multipack(orig_search_name)

        # If we have a measurement, we can heavily penalize or skip those that don't match
        # Let's get the top 3 matches and filter
        matches = process.extract(
            search_query,
            anchor_names,
            scorer=fuzz.token_sort_ratio,
            limit=3,
            score_cutoff=threshold
        )

        best_valid_match = None
        for best_match_str, score, match_idx in matches:
            anchor_meas = anchor_measurements[match_idx]
            anchor_orig_name = anchor_df.iloc[match_idx]['product_name']
            is_anchor_multipack = is_multipack(anchor_orig_name)
            
            # Strict Rule 1: Measurements must match (e.g. 500ml != 4L)
            if search_meas and anchor_meas:
                if search_meas != anchor_meas:
                    continue
            
            # Strict Rule 2: Don't compare a single item to a multipack (e.g., 200ml vs 200ml x24)
            if is_search_multipack != is_anchor_multipack:
                continue
            
            best_valid_match = (best_match_str, score, match_idx)
            break # We found the best one that doesn't conflict

        if best_valid_match:
            best_match_str, score, match_idx = best_valid_match
            anchor_row = anchor_df.iloc[match_idx]

            results.append({
                'matched_product': anchor_row['product_name'], # use the real original name for clarity
                'anchor_store': anchor_row['store_with_city'],
                'anchor_price': anchor_row['price'],
                'comparison_store': row['store_with_city'],
                'comparison_product': row['product_name'],
                'comparison_price': row['price'],
                'price_diff_pkr': row['price'] - anchor_row['price'],
                'confidence_score_pct': round(score, 2)
            })

    final_matches_df = pd.DataFrame(results)

    if len(final_matches_df) > 0:
        # Sort by exact matches first to see the best results at the top
        final_matches_df = final_matches_df.sort_values(by='confidence_score_pct', ascending=False)
        os.makedirs(os.path.join("data", "matched"), exist_ok=True)
        output_path = os.path.join("data", "matched", "cross_store_price_dispersion.csv")
        final_matches_df.to_csv(output_path, index=False)

        print(f"\nSuccessfully found {len(final_matches_df)} valid cross-store product matches!")
        print(f"Results saved to {output_path}")
    else:
        print("No matches met the threshold criteria.")


if __name__ == "__main__":
    run_entity_resolution(threshold=86)