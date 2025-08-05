# Customer-Loyalty-Tier-Rebuilder
An automated ETL pipeline that processes customer transaction data to rebuild loyalty tier classifications for e-commerce marketing campaigns.

## Overview
This project automates the customer loyalty tier assignment process by:

- Extracting transaction data from MySQL database
- Processing large datasets using PySpark in Databricks
- Applying business rules to calculate tier assignments
- Outputting analysis-ready data for marketing and BI teams

**Business Impact:** Eliminates 8-10 hours of monthly manual work while ensuring 100% accurate tier calculations.

```
MySQL Database → Python ETL → PySpark Processing → Parquet Output
     ↓              ↓              ↓                ↓
Transaction    Secure Data     Business Logic    BI-Ready
   Data        Extraction      Application       Dataset
```