# Customer-Loyalty-Tier-Rebuilder
[![Python](https://img.shields.io/badge/python-v3.8+-blue.svg)](https://www.python.org/downloads/)
[![PySpark](https://img.shields.io/badge/PySpark-v3.0+-orange.svg)](https://spark.apache.org/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

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


## Tech Stack
 - **Python 3.8+** - Core orchestration and database connectivity
 - **PySpark 3.0+** - Large-scale data processing
 - **MySQL Connector** - Database connectivity with connection pooling
 - **Databricks** - Cloud processing platform
 - **Parquet** - Optimized data storage format

## Prerequisites
- Python 3.8 or higher
- Access to Databricks workspace
- MySQL database with transaction data
- Required Python packages (see `requirements.txt`)

## Quick Start
### 1. Clone the Repository
```bash
git clone https://github.com/lemirser/Customer-Loyalty-Tier-Rebuilder.git
cd customer-loyalty-tier-rebuilder
```

### 2. Install Dependencies
```bash
pip install -r requirements.txt
```

### 3. Configure Environment
Create a `.env` file in the project root:
```env
# Database Configuration
DB_HOST=127.0.0.1
DB_NAME='database_name'
DB_USER='username'
DB_PASSWORD='password'
DB_PORT=3306
```
- Refer to the `.env.example`.

### 4. Run the Pipeline
```bash
python etl_main.py
```

## Loyalty Tier Business Rules

| Tier | Total Spent | Transaction Count |
|------|-------------|-------------------|
| **Platinum** | ≥ ₱100,000 | ≥ 20 |
| **Gold** | ≥ ₱50,000 | ≥ 10 |
| **Silver** | ≥ ₱20,000 | ≥ 5 |
| **Bronze** | < ₱20,000 | < 5 |

## Project Structure

```
customer-loyalty-tier-rebuilder/
├── helpers/
│   ├── __init__.py
│   ├── db_connection.py            # MySQL connection handler
│   ├── db_operations.py            # MySQL query operations
│   ├── etl_utils.py                # Data to CSV transformation
│   ├── file_uploader.py            # File uploader operation to Databricks
│   └── logger_config.py            # Logging setup
├── extract/
│   ├── raw_transaction.csv         # Raw data extracted from MySQL database
│   ├── archive/                    # Backup data
│   ├── upload/                     # Data for uploading to Databrics
│   └── sample_data/                # MySQL DML file
│       └── transaction_records.sql # DML query
├── notebooks/
│   └── loyalty_tier.ipynb          # PySpark notebook from Databricks
├── etl_main.py                     # Main orchestration script
├── requirements.txt                # Python dependencies
├── .env.example                    # Environment variables template
├── pipeline.log_example            # Auto-generated log file
└── README.md                       # This file
```

## Configuration
### Database Schema
The pipeline expects a `transactions` table with the following structure:
```sql
CREATE TABLE transactions (
  transaction_id INT PRIMARY KEY,
  customer_id INT,
  transaction_date DATE,
  amount DECIMAL(10,2)
);
```

### Databricks Output Locations
- **Raw Data:** `/Volumes/pyspark_tut/loyalty_program/loyalty_pipeline/uploaded_file_"%Y%m%d_%H%M%S".csv`
- **Processed Data:** `/Volumes/pyspark_tut/loyalty_program/loyalty_pipelinecleaned/loyalty_tier.parquet`

## Error Handling

The pipeline includes comprehensive error handling for:
- Database connection failures with automatic retry logic
- Data quality validation and anomaly detection
- File system errors during read/write operations
- Schema validation and type checking

Check logs in `pipeline.log` for detailed error information.

## Logging

The application uses structured logging with different levels:
- **INFO:** Pipeline progress and completion status
- **ERROR:** Critical failures requiring attention

## Security Considerations

- Database credentials stored in environment variables
- Connection pooling prevents database overload
- Secure file permissions on output data

## Roadmap

### Phase 2: Real-time Processing
- [ ] Implement streaming data processing
- [ ] Add real-time tier update notifications
- [ ] Integration with marketing automation tools

### Phase 3: Advanced Analytics
- [ ] Predictive tier movement analysis
- [ ] Customer lifetime value calculations
- [ ] Automated tier recommendation engine


**Last Updated:** August 2025