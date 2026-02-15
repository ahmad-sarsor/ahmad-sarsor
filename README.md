# dbt Analytics Data Warehouse

Welcome to the **dbt Analytics Data Warehouse** repository! 🚀  
This project demonstrates a production-grade data warehouse built with dbt Core and BigQuery, featuring 107+ models for tech ecosystem analytics. Designed as a portfolio project, it highlights industry best practices in modern data engineering and analytics engineering.

---

## 🏗️ Data Architecture

The data architecture for this project follows the **Three-Layer dbt Architecture** with Google Cloud Build orchestration:

![Data Architecture](docs/data_architecture.png)

1. **Staging Layer**: Raw data connection from source systems. Light transformations including column renaming, type casting, and basic filtering. Data is stored as Views.
2. **Intermediate Layer**: Business logic transformations including data cleansing, joining entities, business rules, calculated fields, and data enrichment. Data Model is Normalized.
3. **Marts Layer**: Business-ready analytics with star schema design. Includes data integration, aggregations, KPI calculations, and final metrics for reporting.

---

## 📖 Project Overview

This project involves:

1. **Data Architecture**: Designing a Modern Data Warehouse using dbt's three-layer architecture (Staging → Intermediate → Marts).
2. **ELT Pipelines**: Extracting data from multiple sources and transforming with dbt Core on BigQuery.
3. **Data Modeling**: Developing fact and dimension tables using dimensional modeling best practices.
4. **Data Quality**: Implementing comprehensive testing, documentation, and real-time monitoring.
5. **CI/CD**: Automated deployment with Google Cloud Build, Cloud Run, and Cloud Scheduler.
6. **Analytics & Reporting**: Powering Power BI dashboards and organizational reports.

🎯 This repository is an excellent resource for professionals looking to showcase expertise in:
- Analytics Engineering
- dbt Development
- Data Engineering  
- Data Modeling  
- BigQuery & GCP
- CI/CD for Data

---

## 🛠️ Important Links & Tools

Everything you need to understand and replicate this project:

- **[dbt Core](https://docs.getdbt.com/):** Data transformation framework
- **[Google BigQuery](https://cloud.google.com/bigquery):** Cloud data warehouse
- **[Google Cloud Build](https://cloud.google.com/build):** CI/CD orchestration
- **[Power BI](https://powerbi.microsoft.com/):** Business intelligence and reporting
- **[Draw.io](https://www.drawio.com/):** Design data architecture and diagrams
- **[Git Repository](https://github.com/):** Version control and collaboration

---

## 🚀 Project Requirements

### Building the Data Warehouse (Data Engineering)

#### Objective
Develop a modern data warehouse using dbt Core and BigQuery to consolidate tech ecosystem data, enabling analytical reporting and informed decision-making.

#### Specifications
- **Data Sources**: Import data from multiple source systems (MySQL, Google Analytics 4, APIs) via BigQuery Federation.
- **Data Quality**: Cleanse and resolve data quality issues using dbt tests and custom validations.
- **Integration**: Combine all sources into a single, user-friendly data model designed for analytical queries.
- **Automation**: Daily scheduled refreshes with Cloud Scheduler and Cloud Run.
- **Documentation**: Provide clear documentation of the data model using dbt docs.

---

### BI: Analytics & Reporting (Data Analysis)

#### Objective
Develop analytics models to deliver detailed insights into:
- **Funding Trends**: Investment rounds, amounts, and sectors
- **Company Performance**: Growth metrics and sector analysis
- **Investor Analysis**: Active investors and participation patterns
- **Economic Indicators**: GDP contribution and employment trends

These insights empower stakeholders with key business metrics for the Israeli Tech Ecosystem reports.

---

## 📂 Repository Structure

```
dbt-analytics-dwh/
│
├── models/                              # dbt models organized by layer
│   ├── staging/                         # Raw data transformations (30+ models)
│   │   ├── mysql/                       # Models from MySQL source
│   │   ├── google_sheets/               # Models from Google Sheets
│   │   ├── ga4/                         # Models from Google Analytics 4
│   │   └── staging.yml                  # Schema definitions
│   │
│   ├── intermediate/                    # Business logic layer (40+ models)
│   │   ├── entities/                    # Entity transformations
│   │   ├── metrics/                     # Calculated metrics
│   │   └── intermediate.yml             # Schema definitions
│   │
│   └── marts/                           # Business-ready models (35+ models)
│       ├── core/                        # Core dimensions and facts
│       ├── finance/                     # Finance-specific models
│       ├── marketing/                   # Marketing models
│       └── marts.yml                    # Schema definitions
│
├── tests/                               # Custom data tests
│   ├── generic/                         # Reusable test definitions
│   └── singular/                        # One-off test queries
│
├── macros/                              # Reusable Jinja macros
│
├── seeds/                               # Static reference data (CSV)
│
├── snapshots/                           # Slowly changing dimensions
│
├── docs/                                # Project documentation
│   ├── data_architecture.png            # Architecture diagram
│   ├── erd_diagram.png                  # Entity Relationship Diagram
│   └── data_catalog.md                  # Field descriptions and metadata
│
├── scripts/                             # Deployment and utility scripts
│   ├── deploy/                          # Cloud Run deployment
│   └── alerts/                          # Slack notification scripts
│
├── dbt_project.yml                      # dbt project configuration
├── packages.yml                         # dbt package dependencies
├── profiles.yml.example                 # Example connection profile
├── Dockerfile                           # Container configuration
├── README.md                            # Project documentation
├── LICENSE                              # License information
└── .gitignore                           # Git ignore rules
```

---

## 📊 Data Flow Summary

| Layer | Object Type | Materialization | Models | Prefix |
|-------|-------------|-----------------|--------|--------|
| **Staging** | Views | View | 30+ | `stg_` |
| **Intermediate** | Tables | Table / Incremental | 40+ | `int_` |
| **Marts** | Tables | Table | 35+ | `dim_` / `fct_` |

---

## 🔧 Tech Stack

| Category | Technology |
|----------|------------|
| **Transformation** | dbt Core |
| **Data Warehouse** | Google BigQuery |
| **Orchestration** | Google Cloud Build, Cloud Run, Cloud Scheduler |
| **BI & Reporting** | Power BI |
| **Version Control** | Git, GitHub |
| **Data Sources** | MySQL (CloudSQL), Google Analytics 4, APIs |

---

## 🚀 Getting Started

### Prerequisites
- Python 3.9+
- dbt Core 1.7+
- Google Cloud SDK
- BigQuery access

### Installation

```bash
# Clone the repository
git clone https://github.com/ahmad-sarsor/dbt-analytics-dwh.git
cd dbt-analytics-dwh

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Configure dbt profile
cp profiles.yml.example ~/.dbt/profiles.yml

# Verify installation
dbt debug
```

### Running the Project

```bash
# Install dbt packages
dbt deps

# Run all models
dbt run

# Run tests
dbt test

# Generate documentation
dbt docs generate
dbt docs serve
```

---

## 🛡️ License

This project is licensed under the [MIT License](LICENSE). You are free to use, modify, and share this project with proper attribution.

---

## 🌟 About Me

Hi there! I'm **Ahmad Sarsor**, a BI & Data Engineer with expertise in building end-to-end cloud-based data infrastructure and analytics solutions. Specialized in dbt Core, BigQuery, and Google Cloud Platform.

[![LinkedIn](https://img.shields.io/badge/LinkedIn-0077B5?style=for-the-badge&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/ahmad-sarsor/)
[![GitHub](https://img.shields.io/badge/GitHub-100000?style=for-the-badge&logo=github&logoColor=white)](https://github.com/ahmad-sarsor)
[![Email](https://img.shields.io/badge/Email-D14836?style=for-the-badge&logo=gmail&logoColor=white)](mailto:ahmad.kefah11sar@gmail.com)
