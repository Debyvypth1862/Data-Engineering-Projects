# Retail Sales ETL Pipeline

## Overview
This project implements an end-to-end data pipeline for processing retail sales data using modern data stack technologies. The pipeline extracts raw sales data, transforms it into a dimensional model, and loads it into a PostgreSQL database for analysis.

## Architecture
![Architecture](https://raw.githubusercontent.com/username/retail-sales-etl/main/docs/architecture.png)

### Technologies Used
- **Apache Airflow**: Orchestrates the ETL workflow
- **dbt (data build tool)**: Handles data transformation and modeling
- **PostgreSQL**: Serves as the data warehouse
- **Docker**: Containerizes the entire solution for easy deployment

## Project Structure
```
retail-sales-etl/
├── airflow/
│   ├── dags/
│   │   └── retail_etl_dag.py    # Main Airflow DAG definition
│   ├── Dockerfile               # Airflow container configuration
│   └── requirements.txt         # Python dependencies
├── dbt/
│   └── retail_dbt_project/
│       ├── models/
│       │   ├── staging/        # Initial data staging
│       │   │   └── stg_sales.sql
│       │   └── marts/          # Dimensional modeling
│       │       ├── dim_customers.sql
│       │       ├── dim_products.sql
│       │       └── fact_sales.sql
│       └── dbt_project.yml     # dbt configuration
├── data/
│   ├── generate_data.py       # Script to generate sample data
│   └── raw_sales.csv          # Raw sales data
├── docker-compose.yml         # Docker services configuration
└── README.md
```

## Data Model
The project implements a star schema with the following tables:
- **fact_sales**: Main fact table containing sales transactions
- **dim_customers**: Customer dimension table
- **dim_products**: Product dimension table
- **stg_sales**: Staging table for raw data

## Data Generation
The project includes a data generation script (`data/generate_data.py`) that can create synthetic retail sales data for testing and development purposes. To generate new sample data:

1. Ensure you have Python with pandas and numpy installed
2. Run the data generation script:
   ```bash
   python data/generate_data.py
   ```
   
This will create a new `raw_sales.csv` file with:
- 150,000 sample sales records
- 1,000 unique customers
- 500 unique products
- Dates ranging from 2023 to 2024
- Realistic quantity distributions

## Prerequisites
- Docker and Docker Compose
- At least 4GB of RAM available for Docker
- Git (optional)

## Setup Instructions

1. **Clone the Repository**
   ```bash
   git clone <repository-url>
   cd retail-sales-etl
   ```

2. **Start the Services**
   ```bash
   docker-compose up -d
   ```

3. **Access the Services**
   - Airflow UI: http://localhost:8080
     - Username: airflow
     - Password: airflow
   - PostgreSQL:
     - Host: localhost
     - Port: 5432
     - Database: retail
     - Username: airflow
     - Password: airflow

4. **Run the Pipeline**
   1. Open Airflow UI at http://localhost:8080
   2. Navigate to DAGs
   3. Find and enable the 'retail_sales_etl' DAG
   4. Trigger the DAG manually for the first run

## Pipeline Steps
1. **Data Extraction**
   - Raw sales data is loaded from CSV into PostgreSQL

2. **Data Staging**
   - Raw data is staged using dbt's staging model
   - Basic data type conversions and cleaning are applied

3. **Data Transformation**
   - Dimensional models are created:
     - Customer dimension is extracted
     - Product dimension is created
     - Sales fact table is generated

## Monitoring and Maintenance

### Airflow DAG Status
- Monitor DAG runs through the Airflow UI
- Check task logs for detailed execution information
- Set up email alerts for DAG failures (optional)

### Data Quality
- dbt tests ensure data consistency
- Monitor transformation steps through dbt logs

## Troubleshooting

### Common Issues
1. **Docker Container Fails to Start**
   - Check Docker logs: `docker-compose logs`
   - Ensure ports 8080 and 5432 are available
   - Verify sufficient system resources

2. **DAG Failure**
   - Check Airflow task logs in the UI
   - Verify PostgreSQL connection
   - Ensure raw data file exists and is readable

3. **dbt Transformation Errors**
   - Check dbt logs in Airflow task output
   - Verify SQL syntax in model files
   - Ensure proper schema permissions

## Development

### Adding New Transformations
1. Create new SQL files in appropriate dbt model directories
2. Update dbt_project.yml if needed
3. Test locally using dbt commands
4. Commit changes and restart services

### Modifying the Pipeline
1. Edit retail_etl_dag.py to modify the Airflow DAG
2. Update docker-compose.yml for infrastructure changes
3. Rebuild containers: `docker-compose up -d --build`

## Contributing
1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Submit a pull request

## License
This project is licensed under the MIT License - see the LICENSE file for details.

## Contact
For support or queries, please contact [Your Contact Information]