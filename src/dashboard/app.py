import streamlit as st
import pandas as pd
import plotly.express as px
import os

# Page Configuration
st.set_page_config(
    page_title="Supermarket Price Dashboard",
    page_icon="🛒",
    layout="wide"
)

# --- Data Loading ---
@st.cache_data
def load_data():
    file_path = "data/matched/cross_store_price_dispersion.csv"
    if not os.path.exists(file_path):
        return None
    df = pd.read_csv(file_path)
    return df

df = load_data()

# --- Title and Header ---
st.title("🛒 Supermarket Price Dispersion Dashboard")
st.markdown("Analyze price differences uniquely matched products across major supermarkets.")

if df is None:
    st.error("⚠️ Data file not found! Please run the Phase 4 Matching pipeline first.")
    st.stop()

# --- Preprocessing & Metrics ---
total_matches = len(df)
stores_compared = df['comparison_store'].nunique()
avg_price_diff = df['price_diff_pkr'].mean()

# Three columns for metrics
col1, col2, col3 = st.columns(3)
col1.metric("Total Matched Products", f"{total_matches:,}")
col2.metric("Stores Compared (vs Naheed)", stores_compared)
col3.metric("Average Price Difference", f"Rs. {avg_price_diff:.2f}")

st.divider()

# --- Tabs for different views ---
tab1, tab2, tab3 = st.tabs(["📊 Market Overview", "🔍 Product Search", "🗄️ Raw Data"])

with tab1:
    st.header("Overall Store Comparison")
    
    # Left column: Bar Chart, Right column: Box Plot
    chart_col1, chart_col2 = st.columns(2)
    
    with chart_col1:
        st.subheader("Average Price Difference vs Naheed")
        store_diff = df.groupby('comparison_store')['price_diff_pkr'].mean().reset_index()
        fig_bar = px.bar(
            store_diff, 
            x='comparison_store', 
            y='price_diff_pkr',
            color='comparison_store',
            labels={'comparison_store': 'Store', 'price_diff_pkr': 'Difference (PKR)'},
            text_auto='.2f'
        )
        st.plotly_chart(fig_bar, use_container_width=True)

    with chart_col2:
        st.subheader("Price Difference Distribution")
        fig_box = px.box(
            df, 
            x='comparison_store', 
            y='price_diff_pkr', 
            color='comparison_store',
            labels={'comparison_store': 'Store', 'price_diff_pkr': 'Difference (PKR)'}
        )
        st.plotly_chart(fig_box, use_container_width=True)

    st.subheader("Top 10 Largest Price Dispersions (Same Product)")
    biggest_diffs = df.sort_values(by='price_diff_pkr', ascending=False).head(10)[
        ['matched_product', 'comparison_store', 'anchor_price', 'comparison_price', 'price_diff_pkr']
    ]
    st.dataframe(biggest_diffs, use_container_width=True)

with tab2:
    st.header("Search & Compare Specific Deals")
    
    search_term = st.text_input("Search for a product (e.g., 'oil', 'shampoo'):")
    
    if search_term:
        filtered_df = df[df['matched_product'].str.contains(search_term.lower(), na=False)]
        
        st.write(f"Found **{len(filtered_df)}** matches for '{search_term}'.")
        # Display the result table, sorted by price diff
        st.dataframe(
            filtered_df[['matched_product', 'comparison_store', 'anchor_price', 'comparison_price', 'price_diff_pkr']]
            .sort_values(by='price_diff_pkr'),
            use_container_width=True
        )
    else:
        st.info("Enter a product name above to see specific cross-store differences.")

with tab3:
    st.header("Raw Matched Data")
    st.dataframe(df, use_container_width=True)
