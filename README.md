# 340B OPAIS Data Pipeline

## Overview
This project implements a modern ELT (Extract, Load, Transform) pipeline for processing the 340B OPAIS Covered Entity Daily Export data. Rather than following a traditional ETL approach, this implementation leverages dbt (data build tool) to perform transformations in the data warehouse, providing greater visibility, testability, and maintainability.

## Architecture Decision
While the project prompt suggested an ETL approach, I deliberately chose ELT for several key advantages:

1. **Visibility**: By loading raw data first, we maintain a complete audit trail and can easily debug data issues
2. **Collaboration**: Using dbt's SQL-first approach makes the transformation logic accessible to data analysts and scientists
3. **Testing**: dbt's built-in testing framework enables test-driven development throughout the transformation process
4. **Documentation**: Automated documentation generation through yml files keeps technical documentation in sync with the code
5. **Version Control**: All transformation logic lives in version-controlled SQL files, enabling proper code review and change management

## Implementation Steps

### 1. Data Exploration
- Created Python scripts (`explore_json.py`, `json_size_checker.py`) to analyze the large JSON structure
- Generated a comprehensive markdown document (`340b_core_structure.md`) documenting the data structure
- Used this analysis to inform the database schema design and transformation strategy

### 2. Infrastructure Setup
- Implemented a Docker-based development environment (`docker-compose.yml`, `Dockerfile`)
- Configured PostgreSQL database with separate schemas for raw and transformed data
- Set up dbt project structure for transformations

### 3. Data Loading
- Developed a streaming JSON loader (`loader.py`) that:
  - Efficiently processes large JSON files
  - Handles incremental loads
  - Includes error handling and load auditing
  - Stores raw data in the `raw_340b` schema

### 4. Data Transformation
Implemented a multi-layer dbt transformation pipeline:

#### Staging Layer
- Created atomic models that extract and type individual entities:
  - `stg_contract_pharmacies`
  - `stg_covered_entities`
  - `stg_covered_entity_addresses`
  - `stg_covered_entity_contacts`
  - `stg_covered_entity_medicaid`
  - `stg_covered_entity_npi`

#### Intermediate Layer
- Built joining and reshaping models:
  - `int_340b_entities_with_addresses`
  - `int_340b_entities_with_contacts`
  - `int_340b_entities_with_npi`

#### Warehouse Layer
- Dimensional models:
  - `dim_contacts`
  - `dim_covered_entities`
  - `dim_location`
- Fact tables:
  - `fact_pharmacy_relationships`
  - `fact_provider_participation`

#### Reporting Layer
- Example report models:
  - `participation_overview_340b_report`
  - `state_340b_participation_analysis`

### 5. Quality Control
- Implemented comprehensive testing through yml files:
  - Uniqueness constraints
  - Not-null checks
  - Relationship validation
  - Custom data quality rules
- Created reusable macros (e.g., `get_region.sql`) for consistent logic

## Meeting Evaluation Criteria

### Technical Skills
- Demonstrated SQL expertise through dbt models
- Python implementation for data loading and exploration
- Advanced data modeling concepts in dimensional model design
- Modern data warehousing practices with ELT approach

### Problem-Solving
- Identified and addressed JSON parsing challenges
- Implemented streaming processing for large data files
- Created flexible schema design accommodating various entity relationships
- Developed reusable macros for common transformations

### Communication
- Comprehensive documentation in yml files
- Clear model naming conventions and structure
- Detailed README explaining architectural decisions
- Example reports demonstrating business value

### Code Quality
- Modular design with clear separation of concerns
- Consistent naming conventions
- Version controlled transformations
- Test-driven development approach
- DRY principles through macro usage

## Business Value Alignment
The pipeline enables several key business capabilities:

1. **Provider Network Analysis**: Understanding covered entity distributions and relationships
2. **Compliance Monitoring**: Tracking certification and participation status
3. **Geographic Coverage**: Analysis of service areas and gaps
4. **Relationship Management**: Monitoring pharmacy partnerships and provider relationships

## Future Enhancements
1. Implement dbt snapshots for historical tracking
2. Add more comprehensive data quality tests
3. Develop additional business-specific metrics
4. Create materialized views for common report patterns
5. Implement CI/CD pipeline for automated testing and deployment

## Getting Started

## Project Structure
```
├── data/
│   ├── raw/                          # Raw data files
│   │   └── OPA_CE_DAILY_PUBLIC.JSON  # Daily OPAIS export
│   └── processed/                    # Processed/intermediate data files
├── dbt/
│   ├── analyses/                     # Ad-hoc analytical SQL queries
│   ├── dbt_packages/                 # External dbt packages
│   ├── logs/                         # dbt execution logs
│   ├── macros/                       # Reusable SQL macros
│   │   ├── generate_schema_name.sql  # Custom schema naming
│   │   └── get_region.sql           # Region classification logic
│   ├── models/                       # Data transformation models
│   │   ├── example/                 # Example models
│   │   ├── intermediate/            # Intermediate tables
│   │   ├── marts/                   # Business-facing dimensional models
│   │   │   ├── dim_location.sql
│   │   │   ├── dim_provider.sql
│   │   │   └── fact_provider_participation.sql
│   │   └── reports/                 # Report-specific models
│   ├── seeds/                       # Static CSV reference data
│   └── tests/                       # Custom data tests
├── sql/                             # Database setup scripts
│   └── init.sql                     # Initial schema creation
├── src/                             # Source code
│   ├── etl/                         # ETL processing scripts
│   │   ├── __pycache__/
│   │   ├── loader.py               # JSON data loader
│   │   └── transformer.py          # Data transformation utilities
│   └── utils/                      # Shared utility functions
├── tests/                          # Python test files
│   └── test_loader.py
├── docker-compose.yml              # Docker services config
├── Dockerfile                      # Container build instructions
├── explore_json.py                 # JSON exploration script
├── json_size_checker.py           # File size validation
├── poetry.lock                    # Locked dependencies
├── pyproject.toml                 # Project configuration
└── README.md                      # Project documentation
```

### Prerequisites
- Docker and Docker Compose installed
- Python 3.11 or higher
- Poetry (Python package manager)
- dbt installed locally (optional, can be run through Docker)
- The 340B OPAIS Covered Entity Daily Export JSON file https://340bopais.hrsa.gov/reports

### Initial Setup

1. **Download the Data**
```bash
# Create the data directory structure
mkdir -p data/raw

# Download the JSON file from OPAIS and place it in data/raw/
# Filename should be: OPA_CE_DAILY_PUBLIC.JSON
```

2. **Environment Setup**
```bash
# Install Python dependencies using Poetry
poetry install

# Build and start the PostgreSQL container
docker-compose up -d db

# Wait for the database to be ready (healthy)
# You can check the status with:
docker-compose ps
```

3. **Load Raw Data**
```bash
# Run the loader script to process the JSON file
docker-compose run loader

# Alternatively, if running locally:
poetry run python src/etl/loader.py
```

4. **Initialize dbt**
```bash
# Move into the dbt project directory
cd dbt

# Install dbt dependencies
dbt deps

# Test the connection
dbt debug
```

5. **Run dbt Models**
```bash
# Run all models
dbt run

# Or run specific model groups:

# Run just the staging models
dbt run --models staging

# Run the marts models
dbt run --models marts

# Run the reports
dbt run --models reports
```

6. **Run Tests**
```bash
# Run all tests
dbt test

# Test specific models
dbt test --models staging
dbt test --models marts
```

7. **Generate Documentation**
```bash
# Generate dbt docs
dbt docs generate

# Serve the documentation locally
dbt docs serve
```

### Monitoring and Maintenance

#### View Database Status
```bash
# Connect to the database
psql postgresql://postgres:postgres@localhost:5432/opais_340b

# List all schemas
\dn

# List tables in raw_340b schema
\dt raw_340b.*

# List marts tables
\dt marts.*
```

#### Common Issues and Solutions

1. **Database Connection Issues**
```bash
# Check if database container is running
docker-compose ps

# View database logs
docker-compose logs db

# Restart the database
docker-compose restart db
```

2. **Data Loading Issues**
```bash
# Check loader logs
docker-compose logs loader

# Reset the database (warning: destroys all data)
docker-compose down -v
docker-compose up -d
```

3. **dbt Issues**
```bash
# Clean and recompile models
dbt clean
dbt compile

# Debug specific models
dbt run --models your_model_name --debug
```

### Development Workflow

When making changes to the models:

1. Create a new branch
2. Make changes to models
3. Run tests: `dbt test`
4. Generate documentation: `dbt docs generate`
5. Review changes in dbt docs: `dbt docs serve`
6. Commit and create pull request
