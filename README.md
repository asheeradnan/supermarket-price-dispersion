# 🛒 RetailRadar | Supermarket Price Intelligence

**RetailRadar** is a comprehensive Data Engineering and Data Science pipeline built to analyze retail pricing across major supermarket chains in Pakistan. This project programmatically scrapes, cleans, merges, and visualizes retail data to answer one simple question: *Who actually offers the best prices?*

---

## 📊 Overview

This project was built to solve the complex problem of **Entity Resolution** in retail. Supermarkets rarely use the same naming conventions for their products. For example:
- Store A: `"Nestle Fruita Vitals Red Grape 1000ml"`
- Store B: `"Fruita Vitals Grape Lrg"`

This pipeline processes **61,000+ localized raw products**, rigorously standardizes unit measurements using regex, performs semantic fuzzy matching to link identical products across competitors natively, and synthesizes the findings into an interactive dashboard.

###  Supported Supermarket Chains Scraped:
1. **Metro** (Islamabad, Karachi, Lahore)
2. **Naheed** (Nationwide)
3. **Imtiaz** (Karachi)
4. **Al-Fatah** (Faisalabad)

---

## 🛠️ Pipeline Architecture
The project is divided into 5 distinct phases:

### Phase 1: Data Acquisition (`src/scrapers/`)
Python-based scraping bots that interact directly with the internal GraphQL and REST APIs of the targeted supermarkets to extract raw product catalogs, bypassing standard HTML parsing overhead.

### Phase 2: Data Cleaning & Normalization
Standardizes complex strings, forces lowercase formatting, extracts implied brands, removes duplicates, and standardizes all base metrics to PKR.

### Phase 3: Entity Resolution (`src/matching/`)
The core Data Science engine. Uses `thefuzz` library to calculate Levenshtein Distance ratios between product names. **Strict Volume Enforcement** was implemented utilizing regex (extracting `ml`, `L`, `x24`) to ensure a 5-Litre bottle of oil is never mapped against a 500ml pouch.

### Phase 4: Exploratory Data Analysis (`notebooks/`)
A Jupyter notebook (`eda_analysis.ipynb`) executing Pandas and Plotly to deduce high-level statistical facts, such as the *Leader Dominance Index (LDI)* across datasets.

### Phase 5: RetailRadar Dashboard (`src/dashboard/`)
A multi-page modern `Streamlit` application utilizing `plotly.express` and custom CSS allowing users to dynamically interact with the finalized dataset, query specific products, and visualize price dispersion globally.

---

## 🚀 How to Run Locally

### 1. Requirements
Ensure you have Python 3.10+ installed. This project uses `pip` for package management.

### 2. Installation
Clone the repository and install the dependencies:
```bash
git clone https://github.com/YOUR_USERNAME/supermarket_pipeline.git
cd supermarket_pipeline

# Create a virtual environment (Optional but Recommended)
python -m venv .venv
source .venv/Scripts/activate  # Windows
# source .venv/bin/activate    # Mac/Linux

# Install Requirements
pip install -r requirements.txt
```

### 3. Running the Dashboard
To boot up the interactive **RetailRadar** Streamlit interface:
```bash
streamlit run src/dashboard/app.py
```

### 4. Running the Scrapers (Optional)
If you wish to fetch the absolute latest data:
```bash
python -m src.scrapers.metro
python -m src.scrapers.naheed
python -m src.scrapers.imtiaz
python -m src.scrapers.alfatah
```

### 5. Running the Matcher
To recompute cross-store product matching after pulling new data:
```bash
python src/matching/entity_resolution.py
```

---

## 📈 Key Metrics & Features Discovered
- **Strict Size Parity**: Discovered that pure fuzzy text matching fails aggressively in retail environments without rigid regex-based volume constraints.
- **Provider Dirty Data**: Highlighted "Invisible Carton" anomalies where stores (e.g. Metro) omit packaging constraints from their internal APIs, causing vast arbitrary price spikes.

*Built as part of a University Data Science practical assignment.*
