## Retail Sales Analytics & Star Schema Implementation

📌 Project Overview
This project demonstrates an end-to-end data engineering and analytics workflow. I transformed a raw dataset of 50,000 synthetic retail transactions into a structured Star Schema to optimize data retrieval and business intelligence reporting.

The goal was to move from a flat, messy dataset to a relational model that tracks sales performance across customers, products, locations, and employees between 2022 and 2025.

🛠️ Tools Used
Python (Pandas): For data cleaning, handling missing values, and normalizing the flat file into relational tables.

Power BI: For building the Star Schema model, writing DAX measures, and creating the interactive dashboard visuals.

Git/GitHub: For version control and documentation.

🏗️ Data Architecture (Star Schema)
The dataset is organized into a central fact table and five supporting dimension tables to ensure efficient querying and clear reporting:

fact_sales.csv: The core table containing quantitative measures, including sales amounts, quantities, and keys connecting to all dimensions.

dim_customer.csv: Contains descriptive attributes for customer segmentation and demographics.

dim_product.csv: Includes details regarding product categories, pricing, and brand information.

dim_date.csv: A specialized calendar table allowing for time-series analysis (trends by month, quarter, and year).

dim_location.csv: Stores geographical data to enable regional sales performance tracking.

dim_employee.csv: Provides information on staff members to analyze sales performance by representative.

## Overview

This document describes the Star Schema data warehouse model built from the enriched retail sales dataset.

- **Dataset**: 50,000 synthetic retail transactions
- **Date Range**: 2022-01-01 to 2025-01-18
- **Dimensions**: 5 dimension tables + 1 fact table
- **Referential Integrity**: 100% validated across all foreign keys

---

## Table: FactSales (Fact Table)

| Column             | Data Type              | Description                                            |
| ------------------ | ---------------------- | ------------------------------------------------------ |
| sale_key           | INT (PK)               | Surrogate primary key, auto-incremented                |
| date_key           | INT (FK → DimDate)     | Date of transaction                                    |
| customer_key       | INT (FK → DimCustomer) | Customer who made the purchase                         |
| product_key        | INT (FK → DimProduct)  | Product purchased                                      |
| location_key       | INT (FK → DimLocation) | Location of purchase                                   |
| employee_key       | INT (FK → DimEmployee) | Sales rep associated with order                        |
| transaction_id     | VARCHAR                | Original business transaction identifier               |
| quantity           | INT                    | Units purchased                                        |
| price_per_unit     | DECIMAL(10,2)          | Selling price per unit                                 |
| unit_cost          | DECIMAL(10,2)          | Cost per unit from supplier                            |
| discount_percent   | DECIMAL(5,4)           | Discount rate applied (0.00 - 0.25)                    |
| discount_amount    | DECIMAL(10,2)          | Total discount in currency                             |
| net_revenue        | DECIMAL(10,2)          | Revenue after discount                                 |
| tax_amount         | DECIMAL(10,2)          | Sales tax based on location                            |
| total_revenue      | DECIMAL(10,2)          | Net revenue + tax (total collected)                    |
| cost_of_goods_sold | DECIMAL(10,2)          | unit_cost × quantity                                   |
| gross_profit       | DECIMAL(10,2)          | net_revenue - cost_of_goods_sold                       |
| profit_margin      | DECIMAL(6,4)           | gross_profit / net_revenue                             |
| payment_method     | VARCHAR                | Cash / Credit Card / Digital Wallet                    |
| order_status       | VARCHAR                | Processing / Shipped / Completed / Returned            |
| return_status      | VARCHAR                | No Return / Partial Return / Full Return               |
| shipping_type      | VARCHAR                | Standard / Express / Next-Day / Freight / Store Pickup |
| days_to_fulfill    | INT                    | Days from order to fulfillment                         |

---

## Table: DimDate (Dimension)

| Column         | Data Type | Description                |
| -------------- | --------- | -------------------------- |
| date_key       | INT (PK)  | Surrogate primary key      |
| date           | DATE      | Calendar date              |
| year           | INT       | Calendar year (2022-2025)  |
| quarter        | INT       | Calendar quarter (1-4)     |
| month          | INT       | Month number (1-12)        |
| month_name     | VARCHAR   | January, February, etc.    |
| week           | INT       | ISO week number            |
| day            | INT       | Day of month               |
| day_of_week    | VARCHAR   | Monday, Tuesday, etc.      |
| is_weekend     | BOOLEAN   | True if Saturday or Sunday |
| is_holiday     | BOOLEAN   | True for major holidays    |
| fiscal_quarter | INT       | Same as calendar quarter   |
| fiscal_period  | VARCHAR   | e.g., "2024-Q2"            |

---

## Table: DimCustomer (Dimension)

| Column            | Data Type | Description                                       |
| ----------------- | --------- | ------------------------------------------------- |
| customer_key      | INT (PK)  | Surrogate primary key                             |
| customer_id       | VARCHAR   | Original business customer ID (CUST_01..CUST_500) |
| customer_name     | VARCHAR   | Full name (Faker-generated, deterministic)        |
| email             | VARCHAR   | Contact email                                     |
| phone             | VARCHAR   | Contact phone                                     |
| customer_segment  | VARCHAR   | Consumer / Small Business / Enterprise            |
| loyalty_tier      | VARCHAR   | Bronze / Silver / Gold / Platinum                 |
| registration_date | DATE      | Account creation date                             |
| preferred_channel | VARCHAR   | Online / In-Store / Both                          |

---

## Table: DimProduct (Dimension)

| Column            | Data Type     | Description                                |
| ----------------- | ------------- | ------------------------------------------ |
| product_key       | INT (PK)      | Surrogate primary key                      |
| item_code         | VARCHAR       | Original item code (Item_XX_CAT)           |
| product_name      | VARCHAR       | Realistic branded product name             |
| category          | VARCHAR       | 8 product categories                       |
| subcategory       | VARCHAR       | Granular subcategory (4 per category)      |
| brand             | VARCHAR       | One of 8 fictional brands                  |
| supplier          | VARCHAR       | One of 8 fictional suppliers               |
| unit_cost_ratio   | DECIMAL(4,3)  | Cost as ratio of selling price (0.45-0.75) |
| typical_unit_cost | DECIMAL(10,2) | Calculated typical cost per unit           |

**Categories & Subcategories:**

- Beverages: Soft Drinks, Juices, Coffee & Tea, Bottled Water
- Butchers: Fresh Meat, Poultry, Processed Meat, Seafood
- Computers & Electric Accessories: Laptops, Accessories, Components, Peripherals
- Electric Household Essentials: Kitchen Appliances, Cleaning, Climate Control, Lighting
- Food: Fresh Produce, Pantry Staples, Frozen Food, Snacks
- Furniture: Living Room, Bedroom, Office, Outdoor
- Milk Products: Fresh Milk, Cheese, Yogurt, Butter & Cream
- Patisserie: Bread, Cakes, Pastries, Desserts

---

## Table: DimLocation (Dimension)

| Column        | Data Type    | Description                     |
| ------------- | ------------ | ------------------------------- |
| location_key  | INT (PK)     | Surrogate primary key           |
| location_type | VARCHAR      | Online vs In-Store              |
| country       | VARCHAR      | USA / Canada / UK               |
| region        | VARCHAR      | Geographic region               |
| state         | VARCHAR      | State or province               |
| city          | VARCHAR      | City name                       |
| tax_rate      | DECIMAL(5,4) | Local sales tax rate (0% - 20%) |

---

## Table: DimEmployee (Dimension)

| Column        | Data Type | Description                                          |
| ------------- | --------- | ---------------------------------------------------- |
| employee_key  | INT (PK)  | Surrogate primary key                                |
| employee_id   | VARCHAR   | Business employee ID (EMP_001..EMP_010)              |
| employee_name | VARCHAR   | Full name (Faker-generated)                          |
| region        | VARCHAR   | Assigned sales territory                             |
| hire_date     | DATE      | Employment start date                                |
| job_title     | VARCHAR   | Sales Associate / Senior Sales Rep / Account Manager |

---

## Star Schema Diagram (Text)

```
                    +------------------+
                    |    DimDate       |
                    |    (date_key)    |
                    +--------+---------+
                             |
                             | 1
                             |
         +-------------------+-------------------+
         |                   |                   |
         | 1                 | 1                 | 1
+--------v--------+  +--------v---------+  +------v--------+
|  DimCustomer    |  |   DimProduct     |  |  DimLocation  |
| (customer_key)  |  |  (product_key)   |  | (location_key)|
+--------+--------+  +--------+---------+  +------+--------+
         |                   |                   |
         |                   |                   |
         |                   |                   |
         +---------+---------+---------+---------+
                   |                   |
                   | 1                 | 1
                   |                   |
            +------v-------------------v------+
            |          FactSales              |
            |   (sale_key, all FKs, metrics) |
            +---------------------------------+
                             |
                             | 1
                             |
                    +--------v---------+
                    |   DimEmployee    |
                    |  (employee_key)  |
                    +------------------+
```

## Power BI / Data Warehouse Load Order

1. **Load Dimensions first** (in any order):
   - DimDate
   - DimCustomer
   - DimProduct
   - DimLocation
   - DimEmployee

2. **Load Fact table last**:
   - FactSales (foreign keys will resolve to loaded dimensions)

## Files Delivered

| File                      | Rows   | Purpose                         |
| ------------------------- | ------ | ------------------------------- |
| dim_date.csv              | 1,114  | Date dimension                  |
| dim_customer.csv          | 500    | Customer master data            |
| dim_product.csv           | 200    | Product catalog                 |
| dim_location.csv          | 25     | Geography & tax rates           |
| dim_employee.csv          | 10     | Sales team roster               |
| fact_sales.csv            | 50,000 | Transaction facts & metrics     |
| enriched_flat_dataset.csv | 50,000 | Complete flat table (reference) |
| data_quality_report.txt   | N/A    | Validation summary              |
| data_dictionary.md        | N/A    | This documentation              |
