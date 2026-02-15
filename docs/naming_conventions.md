# **Naming Conventions**

This document outlines the naming conventions used for schemas, tables, views, columns, and other objects in the dbt Analytics Data Warehouse.

## **Table of Contents**

1. [General Principles](#general-principles)
2. [Table Naming Conventions](#table-naming-conventions)
   - [Staging Rules](#staging-rules)
   - [Intermediate Rules](#intermediate-rules)
   - [Marts Rules](#marts-rules)
3. [Column Naming Conventions](#column-naming-conventions)
   - [Primary Keys](#primary-keys)
   - [Foreign Keys](#foreign-keys)
   - [Technical Columns](#technical-columns)
4. [Source Naming Conventions](#source-naming-conventions)
5. [Test Naming Conventions](#test-naming-conventions)
6. [Macro Naming Conventions](#macro-naming-conventions)

---

## **General Principles**

- **Naming Style**: Use `snake_case`, with lowercase letters and underscores (`_`) to separate words.
- **Language**: Use English for all names.
- **Avoid Reserved Words**: Do not use SQL or BigQuery reserved words as object names.
- **Be Descriptive**: Names should clearly indicate the content or purpose of the object.
- **Consistency**: Apply naming rules consistently across all layers and objects.

---

## **Table Naming Conventions**

### **Staging Rules**

- All staging models must start with the prefix `stg_`, followed by the source system name and entity.
- **`stg_<sourcesystem>__<entity>`**
  - `stg_`: Prefix indicating staging layer.
  - `<sourcesystem>`: Name of the source system (e.g., `mysql`, `sheets`, `ga4`).
  - `__`: Double underscore separates source system from entity.
  - `<entity>`: Table name from the source system.
  - Examples:
    - `stg_mysql__companies` → Companies table from MySQL source.
    - `stg_sheets__campaigns` → Campaigns data from Google Sheets.
    - `stg_ga4__events` → Events data from Google Analytics 4.

#### **Staging Layer Guidelines**

| Rule | Description |
|------|-------------|
| Materialization | `view` (default) |
| Transformations | Column renaming, type casting, basic filtering only |
| One-to-One | Each source table has exactly one staging model |
| No Joins | Staging models should not join with other tables |

---

### **Intermediate Rules**

- All intermediate models must start with the prefix `int_`, followed by a descriptive entity name and optional transformation description.
- **`int_<entity>__<transformation>`**
  - `int_`: Prefix indicating intermediate layer.
  - `<entity>`: Descriptive name of the business entity.
  - `__`: Double underscore separates entity from transformation (optional).
  - `<transformation>`: Describes what transformation is applied (optional).
  - Examples:
    - `int_companies__enriched` → Companies with enriched data.
    - `int_funding_rounds__aggregated` → Aggregated funding rounds.
    - `int_investors__deduplicated` → Deduplicated investors list.

#### **Intermediate Layer Guidelines**

| Rule | Description |
|------|-------------|
| Materialization | `table` or `incremental` |
| Transformations | Data cleansing, joining, business rules, calculations |
| Purpose | Prepare data for marts, apply business logic |
| Joins Allowed | Can join multiple staging models |

---

### **Marts Rules**

- All mart models must use meaningful, business-aligned names with a category prefix.
- **`<category>_<entity>`**
  - `<category>`: Describes the role of the table (`dim`, `fct`, `rpt`, `agg`).
  - `<entity>`: Descriptive name aligned with the business domain.
  - Examples:
    - `dim_companies` → Dimension table for company data.
    - `dim_investors` → Dimension table for investor data.
    - `fct_funding_rounds` → Fact table containing funding transactions.
    - `rpt_quarterly_summary` → Report table for quarterly metrics.

#### **Glossary of Category Patterns**

| Pattern | Meaning | Example(s) |
|---------|---------|------------|
| `dim_` | Dimension table | `dim_companies`, `dim_investors`, `dim_sectors` |
| `fct_` | Fact table | `fct_funding_rounds`, `fct_acquisitions` |
| `rpt_` | Report/Summary table | `rpt_annual_report`, `rpt_quarterly_metrics` |
| `agg_` | Aggregated table | `agg_monthly_funding`, `agg_sector_summary` |
| `bridge_` | Bridge table (M:M) | `bridge_company_investor` |

#### **Marts Layer Guidelines**

| Rule | Description |
|------|-------------|
| Materialization | `table` |
| Data Model | Star Schema, Flat Tables, Aggregated Tables |
| Purpose | Business-ready data for reporting and analytics |
| Consumers | Power BI, SQL Queries, Reports |

---

## **Column Naming Conventions**

### **Primary Keys**

- All primary keys must use the suffix `_id` for natural keys or `_key` for surrogate keys.
- **`<entity>_id`** or **`<entity>_key`**
  - `<entity>`: Name of the table or entity.
  - `_id`: Suffix for natural/business keys.
  - `_key`: Suffix for surrogate keys.
  - Examples:
    - `company_id` → Natural key for companies.
    - `investor_key` → Surrogate key in dimension table.

### **Foreign Keys**

- Foreign keys must reference the primary key name of the related table.
- **`<related_entity>_id`** or **`<related_entity>_key`**
  - Examples:
    - `company_id` in `fct_funding_rounds` → References `dim_companies`.
    - `sector_id` in `dim_companies` → References `dim_sectors`.

### **Technical Columns**

- All technical/metadata columns must start with an underscore prefix `_`.
- **`_<column_name>`**
  - `_`: Prefix for system-generated metadata.
  - `<column_name>`: Descriptive name indicating the column's purpose.
  - Examples:
    - `_loaded_at` → Timestamp when the record was loaded.
    - `_updated_at` → Timestamp of last update.
    - `_source_system` → Name of the source system.
    - `_is_deleted` → Soft delete flag.

### **Date and Time Columns**

| Pattern | Usage | Example |
|---------|-------|---------|
| `<event>_date` | Date only | `funding_date`, `founded_date` |
| `<event>_at` | Timestamp | `created_at`, `updated_at` |
| `<period>_year` | Year | `fiscal_year`, `report_year` |
| `<period>_quarter` | Quarter | `fiscal_quarter` |
| `<period>_month` | Month | `report_month` |

### **Boolean Columns**

- Boolean columns must start with `is_` or `has_`.
- Examples:
  - `is_active` → Indicates if record is active.
  - `is_public` → Indicates if company is public.
  - `has_funding` → Indicates if company has received funding.

### **Amount and Metric Columns**

| Pattern | Usage | Example |
|---------|-------|---------|
| `<metric>_amount` | Monetary values | `funding_amount`, `revenue_amount` |
| `<metric>_usd` | USD values | `funding_amount_usd` |
| `<metric>_count` | Counts | `employee_count`, `round_count` |
| `<metric>_pct` | Percentages | `growth_pct`, `share_pct` |

---

## **Source Naming Conventions**

- Sources are defined in `_sources.yml` files within each staging folder.
- **`src_<sourcesystem>`**
  - Examples:
    - `src_mysql` → MySQL database source.
    - `src_sheets` → Google Sheets source.
    - `src_ga4` → Google Analytics 4 source.

---

## **Test Naming Conventions**

### **Generic Tests**

- Defined in YAML schema files using dbt's built-in tests.
- Examples: `unique`, `not_null`, `relationships`, `accepted_values`

### **Singular Tests**

- Custom SQL tests in the `tests/` folder.
- **`assert_<entity>_<condition>`**
  - Examples:
    - `assert_funding_amount_positive` → Validates funding amounts are positive.
    - `assert_company_has_sector` → Validates all companies have a sector.

---

## **Macro Naming Conventions**

- All macros must use descriptive names indicating their purpose.
- **`<action>_<description>`**
  - Examples:
    - `generate_schema_name` → Generates schema names dynamically.
    - `get_custom_alias` → Returns custom table alias.
    - `cents_to_dollars` → Converts cents to dollars.
    - `clean_string` → Cleans and standardizes strings.

---

## **File Naming Conventions**

| File Type | Pattern | Example |
|-----------|---------|---------|
| Models | `<prefix>_<entity>.sql` | `stg_mysql__companies.sql` |
| Schema | `_<layer>__schema.yml` | `_staging__schema.yml` |
| Sources | `_<layer>__sources.yml` | `_staging__sources.yml` |
| Tests | `assert_<description>.sql` | `assert_funding_positive.sql` |
| Macros | `<name>.sql` | `generate_schema_name.sql` |
| Seeds | `<entity>.csv` | `sector_mapping.csv` |

---

## **Schema (Dataset) Naming Conventions**

- BigQuery datasets follow the pattern based on environment and layer.
- **`<project>_<layer>`** or **`<project>_<layer>_<env>`**
  - Examples:
    - `analytics_staging` → Staging layer in production.
    - `analytics_intermediate` → Intermediate layer in production.
    - `analytics_marts` → Marts layer in production.
    - `analytics_staging_dev` → Staging layer in development.
