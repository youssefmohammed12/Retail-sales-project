"""
Retail Sales Data Warehouse Builder
====================================
A comprehensive data cleaning, enrichment, expansion, and Star Schema modeling script.
This script performs the following steps:
1. Loads and cleans the base retail sales dataset.
2. Builds deterministic dimension mappings for products, customers, locations, and employees.
3. Expands the dataset to 50,000 rows using intelligent sampling and synthetic data generation.
4. Enriches the dataset with business logic for financial calculations and operational attributes.
5. Models the data into a Star Schema with surrogate keys and referential integrity.
6. Exports the dimension and fact tables to CSV files for downstream use.
"""

import pandas as pd
import numpy as np
from datetime import date as dtdate
import random
import os
import warnings

warnings.filterwarnings('ignore')

# =============================================================================
# CONFIGURATION
# =============================================================================
SEED = 42
TARGET_ROWS = 50000
INPUT_FILE = '/mnt/agents/upload/cleaned_retail_sales (1).csv'
OUTPUT_DIR = '/mnt/agents/output/retail_star_schema'

# Set all seeds for perfect reproducibility
random.seed(SEED)
np.random.seed(SEED)

# =============================================================================
# STEP 1: LOAD & CLEAN BASE DATA
# =============================================================================
print("=" * 60)
print("STEP 1: LOADING & CLEANING BASE DATA")
print("=" * 60)

df_base = pd.read_csv(INPUT_FILE)
print(f"Base data loaded: {df_base.shape[0]:,} rows x {df_base.shape[1]} columns")

# Standardize column names to snake_case
df = df_base.copy()
df.columns = (
    df.columns
    .str.replace(' ', '_')
    .str.replace('-', '_')
    .str.lower()
)

# Enforce strict data types
df['transaction_date'] = pd.to_datetime(df['transaction_date'], errors='coerce')
df['price_per_unit'] = pd.to_numeric(df['price_per_unit'], errors='coerce').astype('float64')
df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce').astype('int64')
df['total_spent'] = pd.to_numeric(df['total_spent'], errors='coerce').astype('float64')
df['discount_applied'] = df['discount_applied'].astype(bool)

# Clean whitespace in all string/categorical columns
for col in df.select_dtypes(include=['object', 'category']).columns:
    df[col] = df[col].astype(str).str.strip()

# Normalize category names to a strict standard set
STANDARD_CATEGORIES = [
    'Beverages', 'Butchers', 'Computers & Electric Accessories',
    'Electric Household Essentials', 'Food', 'Furniture', 'Milk Products', 'Patisserie'
]

def normalize_category(val):
    val_clean = val.strip().title()
    mapping = {
        'Electric Household Essentials': 'Electric Household Essentials',
        'Computers And Electric Accessories': 'Computers & Electric Accessories',
        'Milk Products': 'Milk Products',
        'Patisserie': 'Patisserie',
        'Butchers': 'Butchers',
        'Beverages': 'Beverages',
        'Food': 'Food',
        'Furniture': 'Furniture'
    }
    return mapping.get(val_clean, val_clean)

df['category'] = df['category'].apply(normalize_category)

# Standardize location values
location_map = {'Online': 'Online', 'In-Store': 'In-Store', 'In-store': 'In-Store'}
df['location'] = df['location'].replace(location_map)

# Validate financial math
calc_total = (df['price_per_unit'] * df['quantity']).round(2)
obs_total = df['total_spent'].round(2)
assert (calc_total != obs_total).sum() == 0, "Financial inconsistency detected!"

# Intelligent outlier flagging
df['order_size_segment'] = pd.cut(
    df['total_spent'],
    bins=[0, 50, 100, 200, float('inf')],
    labels=['Small', 'Medium', 'Large', 'Bulk'],
    include_lowest=True
)

print(f"Cleaning complete. Shape: {df.shape}")

# =============================================================================
# STEP 2: BUILD DETERMINISTIC DIMENSION MAPPINGS (using Faker)
# =============================================================================
print("\n" + "=" * 60)
print("STEP 2: BUILDING DIMENSION MAPPINGS")
print("=" * 60)

try:
    from faker import Faker
except ImportError:
    raise ImportError("Faker is required. Install with: pip install faker")

fake = Faker('en_US')
fake.seed_instance(SEED)

# ---------------------------------------------------------------------------
# 2.1 Product Dimension Mapping (200 items)
# ---------------------------------------------------------------------------
unique_items = df[['item', 'category']].drop_duplicates().sort_values('item').reset_index(drop=True)

SUBCATEGORY_MAP = {
    'Beverages': ['Soft Drinks', 'Juices', 'Coffee & Tea', 'Bottled Water'],
    'Butchers': ['Fresh Meat', 'Poultry', 'Processed Meat', 'Seafood'],
    'Computers & Electric Accessories': ['Laptops', 'Accessories', 'Components', 'Peripherals'],
    'Electric Household Essentials': ['Kitchen Appliances', 'Cleaning', 'Climate Control', 'Lighting'],
    'Food': ['Fresh Produce', 'Pantry Staples', 'Frozen Food', 'Snacks'],
    'Furniture': ['Living Room', 'Bedroom', 'Office', 'Outdoor'],
    'Milk Products': ['Fresh Milk', 'Cheese', 'Yogurt', 'Butter & Cream'],
    'Patisserie': ['Bread', 'Cakes', 'Pastries', 'Desserts']
}

PRODUCT_TEMPLATES = {
    'Beverages': ['Sparkling Cola', 'Orange Juice', 'Green Tea Blend', 'Mineral Water',
                   'Energy Drink', 'Iced Coffee', 'Apple Cider', 'Lemonade'],
    'Butchers': ['Ground Beef', 'Chicken Breast', 'Pork Chops', 'Lamb Shank',
                 'Turkey Mince', 'Beef Steak', 'Sausages', 'Ham Slices'],
    'Computers & Electric Accessories': ['Wireless Mouse', 'USB-C Hub', 'Mechanical Keyboard',
                                          'HDMI Cable', 'Webcam HD', 'SSD Drive', 'RAM Module', 'Power Bank'],
    'Electric Household Essentials': ['Air Fryer', 'Robot Vacuum', 'Space Heater',
                                       'LED Desk Lamp', 'Blender Pro', 'Steam Iron', 'Humidifier', 'Coffee Maker'],
    'Food': ['Organic Carrots', 'Basmati Rice', 'Frozen Pizza', 'Mixed Nuts',
             'Olive Oil', 'Pasta Penne', 'Cereal Crunch', 'Honey Jar'],
    'Furniture': ['Sectional Sofa', 'Queen Bed Frame', 'Ergonomic Chair', 'Patio Set',
                   'Bookshelf Oak', 'Coffee Table', 'Nightstand', 'Dining Bench'],
    'Milk Products': ['Whole Milk', 'Cheddar Block', 'Greek Yogurt', 'Whipping Cream',
                      'Skim Milk', 'Mozzarella', 'Probiotic Yogurt', 'Salted Butter'],
    'Patisserie': ['Sourdough Loaf', 'Chocolate Cake', 'Croissant Butter', 'Macaron Set',
                    'Baguette', 'Cheesecake', 'Danish Pastry', 'Fruit Tart']
}

BRANDS = ['FreshFarm', 'TechGear', 'HomeComfort', 'NatureBest',
          'DailySelect', 'PrimeChoice', 'SmartLiving', 'TasteMaster']
SUPPLIERS = ['Global Foods Ltd', 'ElectroSource Inc', 'ComfortLiving Co', 'FarmDirect',
             'MetroSupply Chain', 'PremiumGoods', 'UrbanWare', 'HomeStyle Imports']

COST_RATIO_BY_CAT = {
    'Beverages': 0.55, 'Butchers': 0.60, 'Computers & Electric Accessories': 0.65,
    'Electric Household Essentials': 0.62, 'Food': 0.58, 'Furniture': 0.55,
    'Milk Products': 0.65, 'Patisserie': 0.60
}

product_records = []
for idx, row in unique_items.iterrows():
    cat = row['category']
    item_code = row['item']
    subcats = SUBCATEGORY_MAP.get(cat, ['General'])
    templates = PRODUCT_TEMPLATES.get(cat, ['Generic Product'])
    
    subcategory = subcats[idx % len(subcats)]
    base_name = templates[idx % len(templates)]
    
    if cat in ['Food', 'Butchers', 'Beverages', 'Milk Products', 'Patisserie']:
        weights = ['100g', '250g', '500g', '1kg', '330ml', '1L']
        variant = f"{base_name} ({weights[idx % len(weights)]})"
    elif cat == 'Furniture':
        variant = f"{base_name} ({['Small', 'Medium', 'Large', 'XL'][idx % 4]})"
    else:
        variant = f"{base_name} (Model {['A', 'B', 'C', 'D'][idx % 4]})"
    
    brand = BRANDS[idx % len(BRANDS)]
    supplier = SUPPLIERS[idx % len(SUPPLIERS)]
    unit_cost_ratio = COST_RATIO_BY_CAT.get(cat, 0.60) + random.uniform(-0.05, 0.05)
    
    product_records.append({
        'item_code': item_code,
        'product_name': f"{brand} {variant}",
        'category': cat,
        'subcategory': subcategory,
        'brand': brand,
        'supplier': supplier,
        'unit_cost_ratio': round(unit_cost_ratio, 3)
    })

dim_product_map = pd.DataFrame(product_records)
print(f"Product mapping: {len(dim_product_map)} items")

# ---------------------------------------------------------------------------
# 2.2 Geography / Location Mapping
# ---------------------------------------------------------------------------
GEO_DATA = [
    ('USA', 'Northeast', 'New York', 'New York City', 0.08875),
    ('USA', 'Northeast', 'New York', 'Buffalo', 0.08),
    ('USA', 'Northeast', 'Massachusetts', 'Boston', 0.0625),
    ('USA', 'Northeast', 'Pennsylvania', 'Philadelphia', 0.08),
    ('USA', 'Southeast', 'Florida', 'Miami', 0.07),
    ('USA', 'Southeast', 'Florida', 'Orlando', 0.065),
    ('USA', 'Southeast', 'Georgia', 'Atlanta', 0.07),
    ('USA', 'Southeast', 'North Carolina', 'Charlotte', 0.0475),
    ('USA', 'Midwest', 'Illinois', 'Chicago', 0.1025),
    ('USA', 'Midwest', 'Michigan', 'Detroit', 0.06),
    ('USA', 'Midwest', 'Ohio', 'Columbus', 0.0575),
    ('USA', 'Southwest', 'Texas', 'Houston', 0.0625),
    ('USA', 'Southwest', 'Texas', 'Dallas', 0.0625),
    ('USA', 'Southwest', 'Arizona', 'Phoenix', 0.056),
    ('USA', 'West', 'California', 'Los Angeles', 0.0950),
    ('USA', 'West', 'California', 'San Francisco', 0.085),
    ('USA', 'West', 'California', 'San Diego', 0.0775),
    ('USA', 'West', 'Washington', 'Seattle', 0.065),
    ('USA', 'Northwest', 'Oregon', 'Portland', 0.0),
    ('USA', 'Northwest', 'Colorado', 'Denver', 0.029),
    ('Canada', 'Central', 'Ontario', 'Toronto', 0.13),
    ('Canada', 'West', 'British Columbia', 'Vancouver', 0.12),
    ('UK', 'England', 'Greater London', 'London', 0.20),
    ('UK', 'England', 'West Midlands', 'Birmingham', 0.20),
    ('UK', 'Scotland', 'Glasgow', 'Glasgow', 0.20),
]

geo_df = pd.DataFrame(GEO_DATA, columns=['country', 'region', 'state', 'city', 'tax_rate'])
geo_df['location_key'] = range(1, len(geo_df) + 1)
print(f"Geography mapping: {len(geo_df)} locations")

# ---------------------------------------------------------------------------
# 2.3 Customer Dimension Mapping (500 customers, deterministic)
# ---------------------------------------------------------------------------
original_customers = sorted(df['customer_id'].unique())
new_customers = [f"CUST_{i:02d}" for i in range(26, 501)]
all_customers = original_customers + new_customers

segments = np.random.choice(
    ['Consumer', 'Small Business', 'Enterprise'],
    size=len(all_customers), p=[0.60, 0.25, 0.15]
)

loyalty_probs = {
    'Consumer': [0.40, 0.35, 0.20, 0.05],
    'Small Business': [0.20, 0.35, 0.30, 0.15],
    'Enterprise': [0.05, 0.20, 0.40, 0.35]
}
loyalty_tiers = [np.random.choice(['Bronze', 'Silver', 'Gold', 'Platinum'], p=loyalty_probs[s]) for s in segments]

customer_records = []
for idx, cust_id in enumerate(all_customers):
    fake.seed_instance(idx + 100)
    name = fake.name()
    email = fake.email()
    phone = fake.phone_number()
    
    if cust_id in original_customers:
        reg_date = fake.date_between(start_date=dtdate(2020, 1, 1), end_date=dtdate(2021, 6, 30))
    else:
        reg_date = fake.date_between(start_date=dtdate(2021, 7, 1), end_date=dtdate(2024, 1, 1))
    
    if segments[idx] == 'Enterprise':
        pref_channel = np.random.choice(['Online', 'In-Store', 'Both'], p=[0.3, 0.2, 0.5])
    elif segments[idx] == 'Small Business':
        pref_channel = np.random.choice(['Online', 'In-Store', 'Both'], p=[0.4, 0.3, 0.3])
    else:
        pref_channel = np.random.choice(['Online', 'In-Store', 'Both'], p=[0.5, 0.3, 0.2])
    
    customer_records.append({
        'customer_id': cust_id,
        'customer_name': name,
        'email': email,
        'phone': phone,
        'customer_segment': segments[idx],
        'loyalty_tier': loyalty_tiers[idx],
        'registration_date': reg_date,
        'preferred_channel': pref_channel
    })

dim_customer_map = pd.DataFrame(customer_records)
print(f"Customer mapping: {len(dim_customer_map)} customers")

# ---------------------------------------------------------------------------
# 2.4 Employee / Sales Rep Mapping
# ---------------------------------------------------------------------------
REGIONS = ['Northeast', 'Southeast', 'Midwest', 'Southwest', 'West', 'Northwest', 'Central', 'England', 'Scotland']
employee_records = []
for i in range(1, 11):
    fake.seed_instance(i + 500)
    emp_id = f"EMP_{i:03d}"
    emp_name = fake.name()
    region = REGIONS[i % len(REGIONS)]
    hire_date = fake.date_between(start_date=dtdate(2019, 1, 1), end_date=dtdate(2023, 12, 31))
    job_title = np.random.choice(['Sales Associate', 'Senior Sales Rep', 'Account Manager'], p=[0.5, 0.35, 0.15])
    employee_records.append({
        'employee_id': emp_id, 'employee_name': emp_name, 'region': region,
        'hire_date': hire_date, 'job_title': job_title
    })

dim_employee_map = pd.DataFrame(employee_records)
print(f"Employee mapping: {len(dim_employee_map)} employees")

# =============================================================================
# STEP 3: EXPAND DATASET TO 50,000 ROWS
# =============================================================================
print("\n" + "=" * 60)
print("STEP 3: DATASET EXPANSION")
print("=" * 60)

# Build date dimension
start_date = df['transaction_date'].min()
end_date = df['transaction_date'].max()
date_range = pd.date_range(start=start_date, end=end_date, freq='D')
dim_date = pd.DataFrame({'date_key': range(1, len(date_range) + 1), 'date': date_range})
dim_date['year'] = dim_date['date'].dt.year
dim_date['quarter'] = dim_date['date'].dt.quarter
dim_date['month'] = dim_date['date'].dt.month
dim_date['month_name'] = dim_date['date'].dt.month_name()
dim_date['week'] = dim_date['date'].dt.isocalendar().week.astype(int)
dim_date['day'] = dim_date['date'].dt.day
dim_date['day_of_week'] = dim_date['date'].dt.day_name()
dim_date['is_weekend'] = dim_date['date'].dt.weekday >= 5
dim_date['is_holiday'] = dim_date['date'].isin([
    '2022-01-01', '2022-12-25', '2023-01-01', '2023-12-25', '2024-01-01', '2024-12-25', '2025-01-01'
])
dim_date['fiscal_quarter'] = dim_date['quarter']
dim_date['fiscal_period'] = dim_date['year'].astype(str) + '-Q' + dim_date['quarter'].astype(str)

# Seasonal weights
seasonality_weights = pd.Series(1.0, index=dim_date.index)
seasonality_weights[dim_date['month'].isin([11, 12])] = 1.3
seasonality_weights[dim_date['month'] == 2] = 0.8
seasonality_weights[dim_date['month'].isin([6, 7, 8])] = 1.1

sample_dates = dim_date.sample(n=TARGET_ROWS, replace=True, weights=seasonality_weights, random_state=SEED)['date'].reset_index(drop=True)

# Customer pool: original 25 get ~400 each, new 475 get ~84 each
customer_pool = []
for cust in original_customers:
    count = max(int(np.random.normal(400, 30)), 300)
    customer_pool.extend([cust] * count)
for cust in new_customers:
    count = max(int(np.random.normal(84, 20)), 20)
    customer_pool.extend([cust] * count)

random.shuffle(customer_pool)
customer_pool = customer_pool[:TARGET_ROWS]
while len(customer_pool) < TARGET_ROWS:
    customer_pool.extend(random.choices(all_customers, k=TARGET_ROWS - len(customer_pool)))
customer_pool = customer_pool[:TARGET_ROWS]

# Sampling distributions from base data
item_weights = df['item'].value_counts(normalize=True).to_dict()
items_list = list(item_weights.keys())
items_probs = list(item_weights.values())

payment_weights = df['payment_method'].value_counts(normalize=True).to_dict()
payment_list = list(payment_weights.keys())
payment_probs = list(payment_weights.values())

location_weights = df['location'].value_counts(normalize=True).to_dict()
location_list = list(location_weights.keys())
location_probs = list(location_weights.values())

quantity_dist = df['quantity'].value_counts(normalize=True).sort_index()
quantity_list = quantity_dist.index.tolist()
quantity_probs = quantity_dist.values.tolist()

discount_rate = df['discount_applied'].mean()

# Build expanded rows
expanded_rows = []
for i in range(TARGET_ROWS):
    item = np.random.choice(items_list, p=items_probs)
    cat = dim_product_map[dim_product_map['item_code'] == item]['category'].values[0]
    cat_prices = df[df['category'] == cat]['price_per_unit'].values
    price = round(np.random.choice(cat_prices), 2)
    qty = int(np.random.choice(quantity_list, p=quantity_probs))
    gross_total = round(price * qty, 2)
    
    expanded_rows.append({
        'transaction_id': f"TXN_{i+1:08d}",
        'customer_id': customer_pool[i],
        'category': cat,
        'item': item,
        'price_per_unit': price,
        'quantity': qty,
        'total_spent': gross_total,
        'payment_method': np.random.choice(payment_list, p=payment_probs),
        'location': np.random.choice(location_list, p=location_probs),
        'transaction_date': sample_dates[i],
        'discount_applied': np.random.random() < discount_rate
    })

df_expanded = pd.DataFrame(expanded_rows)
print(f"Expanded dataset: {df_expanded.shape}")

# =============================================================================
# STEP 4: BUSINESS LOGIC ENRICHMENT
# =============================================================================
print("\n" + "=" * 60)
print("STEP 4: BUSINESS LOGIC ENRICHMENT")
print("=" * 60)

df_enriched = df_expanded.copy()

# Geography assignment
in_store_locs = geo_df[geo_df['location_key'] <= 15].copy()
online_locs = geo_df.copy()

def assign_geography(location_type):
    if location_type == 'In-Store':
        row = in_store_locs.sample(1, random_state=random.randint(1, 100000)).iloc[0]
    else:
        row = online_locs.sample(1, random_state=random.randint(1, 100000)).iloc[0]
    return pd.Series({
        'country': row['country'], 'region': row['region'],
        'state': row['state'], 'city': row['city'], 'tax_rate': row['tax_rate']
    })

geo_assigned = df_enriched['location'].apply(assign_geography)
df_enriched = pd.concat([df_enriched, geo_assigned], axis=1)

# Merge customer dimension
customer_cols = ['customer_id', 'customer_name', 'email', 'phone', 'customer_segment',
                 'loyalty_tier', 'registration_date', 'preferred_channel']
df_enriched = df_enriched.merge(dim_customer_map[customer_cols], on='customer_id', how='left')

# Merge product dimension (selective columns to avoid category conflict)
product_cols = ['item_code', 'product_name', 'subcategory', 'brand', 'supplier', 'unit_cost_ratio']
df_enriched = df_enriched.merge(dim_product_map[product_cols], left_on='item', right_on='item_code', how='left')
df_enriched = df_enriched.drop(columns=['item_code'])

# Financial calculations
DISCOUNT_RATES_BY_CAT = {
    'Beverages': [0.10, 0.15], 'Butchers': [0.15, 0.20],
    'Computers & Electric Accessories': [0.10, 0.20, 0.25],
    'Electric Household Essentials': [0.10, 0.15, 0.20],
    'Food': [0.05, 0.10, 0.15], 'Furniture': [0.15, 0.20, 0.25],
    'Milk Products': [0.10, 0.15], 'Patisserie': [0.10, 0.15, 0.20]
}

def get_discount_rate(row):
    if not row['discount_applied']:
        return 0.0
    rates = DISCOUNT_RATES_BY_CAT.get(row['category'], [0.10])
    return random.choice(rates)

df_enriched['discount_percent'] = df_enriched.apply(get_discount_rate, axis=1)
df_enriched['discount_amount'] = (df_enriched['total_spent'] * df_enriched['discount_percent']).round(2)
df_enriched['net_revenue'] = (df_enriched['total_spent'] - df_enriched['discount_amount']).round(2)
df_enriched['tax_amount'] = (df_enriched['net_revenue'] * df_enriched['tax_rate']).round(2)
df_enriched['total_revenue'] = (df_enriched['net_revenue'] + df_enriched['tax_amount']).round(2)

df_enriched['unit_cost'] = (df_enriched['price_per_unit'] * df_enriched['unit_cost_ratio']).round(2)
df_enriched['cost_of_goods_sold'] = (df_enriched['unit_cost'] * df_enriched['quantity']).round(2)
df_enriched['gross_profit'] = (df_enriched['net_revenue'] - df_enriched['cost_of_goods_sold']).round(2)
df_enriched['profit_margin'] = (df_enriched['gross_profit'] / df_enriched['net_revenue'].replace(0, np.nan)).round(4)

df_enriched['order_size_segment'] = pd.cut(
    df_enriched['total_spent'],
    bins=[0, 50, 100, 200, float('inf')],
    labels=['Small', 'Medium', 'Large', 'Bulk'],
    include_lowest=True
)

# Operations enrichment
def assign_employee(row):
    matching = dim_employee_map[dim_employee_map['region'] == row['region']]
    if len(matching) > 0:
        chosen = matching.sample(1, random_state=hash(row['transaction_id']) % (2**31)).iloc[0]
    else:
        chosen = dim_employee_map.sample(1, random_state=hash(row['transaction_id']) % (2**31)).iloc[0]
    return pd.Series({'employee_id': chosen['employee_id'], 'employee_name': chosen['employee_name']})

emp_data = df_enriched.apply(assign_employee, axis=1)
df_enriched = pd.concat([df_enriched, emp_data], axis=1)

# Order status logic
today = pd.Timestamp('2025-01-18')
df_enriched['days_ago'] = (today - df_enriched['transaction_date']).dt.days

def get_order_status(days_ago):
    if days_ago < 2:
        return np.random.choice(['Processing', 'Shipped'], p=[0.6, 0.4])
    elif days_ago < 7:
        return np.random.choice(['Shipped', 'Completed'], p=[0.3, 0.7])
    return np.random.choice(['Completed', 'Returned'], p=[0.95, 0.05])

df_enriched['order_status'] = df_enriched['days_ago'].apply(get_order_status)

def get_return_status(order_status):
    if order_status == 'Returned':
        return np.random.choice(['Full Return', 'Partial Return'], p=[0.7, 0.3])
    return 'No Return'

df_enriched['return_status'] = df_enriched['order_status'].apply(get_return_status)

def get_shipping_type(row):
    if row['location'] == 'In-Store':
        return 'Store Pickup'
    if row['order_size_segment'] == 'Bulk':
        return np.random.choice(['Express Freight', 'Standard Freight'], p=[0.3, 0.7])
    return np.random.choice(['Standard', 'Express', 'Next-Day'], p=[0.6, 0.3, 0.1])

df_enriched['shipping_type'] = df_enriched.apply(get_shipping_type, axis=1)

def get_fulfillment_days(row):
    if row['shipping_type'] == 'Store Pickup':
        return 0
    elif row['shipping_type'] == 'Next-Day':
        return 1
    elif row['shipping_type'] == 'Express':
        return random.randint(2, 4)
    elif row['shipping_type'] == 'Express Freight':
        return random.randint(3, 6)
    return random.randint(3, 8)

df_enriched['days_to_fulfill'] = df_enriched.apply(get_fulfillment_days, axis=1)
df_enriched = df_enriched.drop(columns=['days_ago'])

print(f"Enrichment complete: {df_enriched.shape[0]:,} rows x {df_enriched.shape[1]} columns")

# =============================================================================
# STEP 5: STAR SCHEMA MODELING
# =============================================================================
print("\n" + "=" * 60)
print("STEP 5: STAR SCHEMA MODELING")
print("=" * 60)

# Finalize dimensions with surrogate keys
# DIM_DATE
dim_date_final = dim_date.copy()

# DIM_CUSTOMER
dim_customer_final = dim_customer_map.copy()
dim_customer_final['customer_key'] = range(1, len(dim_customer_final) + 1)
cust_final_cols = ['customer_key', 'customer_id', 'customer_name', 'email', 'phone',
                    'customer_segment', 'loyalty_tier', 'registration_date', 'preferred_channel']
dim_customer_final = dim_customer_final[cust_final_cols]

# DIM_PRODUCT
dim_product_final = dim_product_map.copy()
dim_product_final['product_key'] = range(1, len(dim_product_final) + 1)
dim_product_final['typical_unit_cost'] = (
    dim_product_final['unit_cost_ratio'] *
    df.groupby('item')['price_per_unit'].mean().reindex(dim_product_final['item_code']).values
).round(2)
prod_final_cols = ['product_key', 'item_code', 'product_name', 'category', 'subcategory',
                    'brand', 'supplier', 'unit_cost_ratio', 'typical_unit_cost']
dim_product_final = dim_product_final[prod_final_cols]

# DIM_LOCATION
dim_location_final = geo_df.copy()
dim_location_final['location_type'] = np.where(dim_location_final['location_key'] <= 15, 'In-Store', 'Online')
loc_final_cols = ['location_key', 'location_type', 'country', 'region', 'state', 'city', 'tax_rate']
dim_location_final = dim_location_final[loc_final_cols]

# DIM_EMPLOYEE
dim_employee_final = dim_employee_map.copy()
dim_employee_final['employee_key'] = range(1, len(dim_employee_final) + 1)
emp_final_cols = ['employee_key', 'employee_id', 'employee_name', 'region', 'hire_date', 'job_title']
dim_employee_final = dim_employee_final[emp_final_cols]

# Build FactSales with surrogate key joins
fact_sales = df_enriched.copy()

# Date key
fact_sales = fact_sales.merge(dim_date_final[['date_key', 'date']],
                               left_on='transaction_date', right_on='date', how='left')

# Customer key
fact_sales = fact_sales.merge(dim_customer_final[['customer_key', 'customer_id']],
                               on='customer_id', how='left')

# Product key
fact_sales = fact_sales.merge(dim_product_final[['product_key', 'item_code']],
                               left_on='item', right_on='item_code', how='left')

# Location key (match by city + state)
geo_lookup = dim_location_final.copy()
geo_lookup['city_state'] = geo_lookup['city'] + '|' + geo_lookup['state']
fact_sales['city_state'] = fact_sales['city'] + '|' + fact_sales['state']
fact_sales = fact_sales.merge(geo_lookup[['location_key', 'city_state']],
                               on='city_state', how='left')

# Employee key
fact_sales = fact_sales.merge(dim_employee_final[['employee_key', 'employee_id']],
                               on='employee_id', how='left')

# Select final fact columns
fact_cols = [
    'date_key', 'customer_key', 'product_key', 'location_key', 'employee_key',
    'transaction_id', 'quantity', 'price_per_unit', 'unit_cost',
    'discount_percent', 'discount_amount', 'net_revenue', 'tax_amount',
    'total_revenue', 'cost_of_goods_sold', 'gross_profit', 'profit_margin',
    'payment_method', 'order_status', 'return_status', 'shipping_type', 'days_to_fulfill'
]

fact_sales_final = fact_sales[fact_cols].copy()
fact_sales_final['sale_key'] = range(1, len(fact_sales_final) + 1)
fact_sales_final = fact_sales_final[['sale_key'] + fact_cols]

# Referential integrity validation
assert fact_sales_final['date_key'].isin(dim_date_final['date_key']).all()
assert fact_sales_final['customer_key'].isin(dim_customer_final['customer_key']).all()
assert fact_sales_final['product_key'].isin(dim_product_final['product_key']).all()
assert fact_sales_final['location_key'].isin(dim_location_final['location_key']).all()
assert fact_sales_final['employee_key'].isin(dim_employee_final['employee_key']).all()

print(f"Star Schema built successfully:")
print(f"  DimDate:      {len(dim_date_final):,} rows")
print(f"  DimCustomer:  {len(dim_customer_final):,} rows")
print(f"  DimProduct:   {len(dim_product_final):,} rows")
print(f"  DimLocation:  {len(dim_location_final):,} rows")
print(f"  DimEmployee:  {len(dim_employee_final):,} rows")
print(f"  FactSales:    {len(fact_sales_final):,} rows")
print(f"  Referential Integrity: VALIDATED")

# =============================================================================
# STEP 6: EXPORT TO CSV
# =============================================================================
print("\n" + "=" * 60)
print("STEP 6: EXPORTING CSV FILES")
print("=" * 60)

os.makedirs(OUTPUT_DIR, exist_ok=True)

dim_date_final.to_csv(f'{OUTPUT_DIR}/dim_date.csv', index=False)
dim_customer_final.to_csv(f'{OUTPUT_DIR}/dim_customer.csv', index=False)
dim_product_final.to_csv(f'{OUTPUT_DIR}/dim_product.csv', index=False)
dim_location_final.to_csv(f'{OUTPUT_DIR}/dim_location.csv', index=False)
dim_employee_final.to_csv(f'{OUTPUT_DIR}/dim_employee.csv', index=False)
fact_sales_final.to_csv(f'{OUTPUT_DIR}/fact_sales.csv', index=False)
df_enriched.to_csv(f'{OUTPUT_DIR}/enriched_flat_dataset.csv', index=False)

print("Export complete:")
for f in sorted(os.listdir(OUTPUT_DIR)):
    fpath = os.path.join(OUTPUT_DIR, f)
    size_mb = os.path.getsize(fpath) / 1024**2
    print(f"  {f:35s} {size_mb:>8.2f} MB")

print("\n" + "=" * 60)
print("ALL STEPS COMPLETED SUCCESSFULLY")
print("=" * 60)
