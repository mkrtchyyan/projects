# ☕ Enterprise Data Warehouse - Coffee Shop Sales Analytics

## 📖 Project Overview
An end-to-end Data Warehouse solution built during EPAM Systems training program for a multinational coffee shop chain. Processes both online and offline sales data to enable comprehensive business intelligence and analytics.

**Business Value:** Enables analysis of sales performance, customer behavior, product profitability, and regional performance across 100+ stores.

## 🏗️ Architecture
```
Source Files (CSV) → Staging Layer (SA) → 3NF Normalized Layer (BL_3NF) → Dimensional Model (BL_DM)
```

## 📁 Project Structure
```
DWH_project/
├── BL_3NF/tables/                    # 3NF Normalized Layer
│   ├── ce_cities.sql
│   ├── ce_countries.sql
│   ├── ce_customers_scd.sql          # SCD Type 2 Implementation
│   └── ...
├── BL_CL/                            # ETL Control Layer
│   ├── functions/
│   │   └── fnc_get_countries.sql
│   ├── procedures/                   # Complete ETL Pipeline
│   │   ├── load_ce_cities.sql
│   │   ├── load_ce_customers_scd.sql
│   │   ├── master_procedure.sql      # Main ETL Orchestrator
│   │   └── ...
│   └── tables/
│       ├── etl_log.sql              # ETL Monitoring
│       ├── load_tracker.sql         # Incremental Load Tracking
│       └── t_map_customers.sql      # Data Mapping Tables
├── BL_DM/tables/                     # Dimensional Model
│   ├── dim_customers_scd.sql
│   ├── dim_products.sql
│   ├── fct_sales_dd.sql             # Partitioned Fact Table
│   └── ...
├── SA_OFFLINE_SALES/                 # Staging Area - Offline
│   ├── ext_offline_coffee_shop_transactions.sql
│   └── src_offline_coffee_shop_transactions.sql
├── SA_ONLINE_SALES/                  # Staging Area - Online  
│   ├── ext_online_coffee_shop_transactions.sql
│   └── src_online_coffee_shop_transactions.sql
├── Coffee_shops_dwh_project_Manan_Mkrtchyan.pptx
├── full_script.sql                   # Complete script (backup)
├── initial_dataset.7z
├── prepared_data_to_show_incremental_load.7z
└── README.md
```

## 🛠️ Technologies Used
- **DBMS:** PostgreSQL
- **ETL/ELT:** PL/pgSQL
- **Data Modeling:** 3NF + Dimensional Modeling (Inmon + Kimball)
- **Architecture:** Layered DWH (Staging → 3NF → Dimensional)

## 🚀 Key Features
- **Hybrid Architecture:** Combines Inmon (3NF) and Kimball (dimensional) approaches
- **Incremental Loading:** Efficient ETL with change data capture using `load_tracker`
- **SCD Type 2:** Full historization for customer data in `ce_customers_scd`
- **Table Partitioning:** Monthly partitioning in `fct_sales_dd` for performance
- **Data Quality:** Comprehensive validation with `etl_log` monitoring
- **Modular Design:** Separated by schema layers for maintainability

## 📊 Data Model Highlights

### 3NF Layer (15+ Tables)
- `CE_CUSTOMERS_SCD` - Type 2 Slowly Changing Dimensions
- `CE_PRODUCTS` - Product hierarchy (Group → Category → Type)
- `CE_SALES` - Transactional facts with incremental loading

### Dimensional Model
- **Fact Tables:** `FCT_SALES_DD` (date-partitioned)
- **Dimension Tables:** `DIM_CUSTOMERS_SCD`, `DIM_PRODUCTS`, `DIM_STORES`

## 🏃‍♂️ Installation & Setup

### Prerequisites
- PostgreSQL 
- Sample data files (`initial_dataset.7z`)
- Data files for incremental load (`prepared_data_to_show_incremental_load.7z`)

### Execution Order
1. **Staging Layer:** Execute scripts in `SA_ONLINE_SALES/` and `SA_OFFLINE_SALES/`
2. **3NF Layer:** Create tables in `BL_3NF/tables/`
3. **ETL Setup:** Create control tables in `BL_CL/tables/`
4. **Procedures:** Deploy ETL procedures in `BL_CL/procedures/`
5. **Dimensional Layer:** Create `BL_DM/tables/`

### Run Complete ETL
```sql
-- Execute the master ETL procedure
CALL bl_cl.master_procedure();

-- Check ETL logs
SELECT * FROM bl_cl.etl_log ORDER BY log_time DESC;
```

## 📈 Business Insights Enabled
- Sales performance by product, store, region
- Customer loyalty program effectiveness  
- Promotion and discount impact analysis
- Staff performance and productivity
- Regional sales trends and comparisons

## 🎯 Training Context
This project was developed as part of **EPAM Systems' Data Engineering training program**, demonstrating enterprise-level data warehouse development skills and best practices.

## 👨‍💻 Author
**Manan Mkrtchyan**  
[LinkedIn](https://www.linkedin.com/in/manan-mkrtchyan/) | [GitHub](https://github.com/mkrtchyyan/)
