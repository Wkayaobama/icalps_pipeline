# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is an IC'ALPS data pipeline project focused on building a reproducible data processing pipeline using Python, xlwings, and DuckDB. The project extracts data from SQL Server databases and processes it through Excel workbooks using xlwings Lite technology.

## Key Technologies

- **Python**: Primary programming language for all data processing
- **xlwings**: For Excel integration and custom scripts
- **DuckDB**: For data transformation and analytics
- **SQL Server**: Source database with ADODB connections
- **Excel/xlwings Lite**: Frontend interface with custom scripts

## Data Architecture

### Pipeline Flow
1. **Bronze Layer**: Raw SQL extracts from SQL Server (prefix all raw extracts with "Bronze")
2. **Data Transformation**: Processing through DuckDB for joins, aggregations, and business logic
3. **Excel Integration**: Final output through xlwings custom scripts

### Core Principles
- **Upstream Processing**: All data joins, aggregations, and transformations must be done as upstream as possible (during SQL extraction and DuckDB processing)
- **Downstream Refinement**: Business logic implementation and final refinements in Excel layer
- **Single Language**: Use Python exclusively; convert any snippets from other languages (PowerQuery, R, etc.)

## Database Connection Strategy

### SQL Server Connection Requirements
- Use ADODB connections with proper connection strings
- Include database name and server name as variables
- Always close connections when finished
- Create new sheets for each report run
- Set recordset objects with active connection and SQL statements

### Connection Pattern
```python
# Store server and database names as variables
SERVER_NAME = "your_server"
DATABASE_NAME = "your_database"

# Use subroutines for database connections with parameters:
# - database name
# - SQL statement
```

## xlwings Implementation

### Custom Scripts Structure
- Use `@script` decorator for Excel-callable functions
- Follow pattern: `def function_name(book: xw.Book):`
- Load each sheet into DuckDB for data transformation
- Use proper error handling and connection management

### Data Processing Pattern
```python
import xlwings as xw
from xlwings import script
import duckdb
import pandas as pd

@script
def process_data(book: xw.Book):
    # Read data from Excel
    data_sheet = book.sheets[1]
    df = read_data(data_sheet)

    # Process with DuckDB
    with duckdb.connect() as conn:
        conn.register("df", df)
        result = conn.query("SELECT ...").df()

    # Write results back to Excel
```

## Business Logic Requirements

### Deal Pipeline Management
- **Hardware Pipeline**: 01-Identification → 02-Qualifiée → 03-Evaluation technique → 04-Construction propositions → 05-Négociations
- **Software Pipeline**: Similar stages with different nomenclature
- **Double Granularity**: Final opportunity stages (No-go, Abandonnée, Perdue, Gagnée) for quality analysis

### Data Model
- **Companies**: Primary entity with one-to-many relationships
- **Contacts**: Associated with companies (0,1 to 0,n cardinality)
- **Deals**: Linked to companies and contacts with forecast/amount tracking
- **Communications**: Many-to-many relationships with all entities

### Computed Columns Required
- Map IC_alps Forecast into Amount
- Set Weighted Forecast as Amount × IC_alps Certainty
- Set Net amount (forecast - cost)
- Net Weighted Amount

## Development Guidelines

### File Organization
- Prefix all raw extracts with "Bronze"
- Use descriptive module names for database connections
- Maintain clear separation between data extraction and business logic

### Code Standards
- Use Python exclusively for all data processing
- Implement proper error handling for database connections
- Document all data transformations and business rules
- Ensure reproducible pipeline execution

### Data Processing Rules
- Perform data transformations as upstream as possible
- Use DuckDB for complex joins and aggregations
- Minimize processing in Excel layer
- Maintain data lineage and transformation documentation

## Common Development Tasks

Since this is primarily a documentation and strategy repository without traditional build tools, development focuses on:

1. **Data Pipeline Development**: Creating Python scripts for data extraction and transformation
2. **xlwings Script Creation**: Building Excel-integrated functions using xlwings Lite
3. **Database Schema Management**: Maintaining SQL Server connection patterns and data models
4. **Business Logic Implementation**: Implementing deal pipeline rules and calculations

## Main Feature Implementation Todo List

### Phase 1: Foundation Infrastructure
- [ ] **Database Connection Framework**
  - [ ] Create `src/database/connection.py` with DatabaseConnector class
  - [ ] Implement `src/config/database_config.py` for configuration management
  - [ ] Add connection string templates and error handling
  - [ ] Write unit tests for database connectivity
- [ ] **Bronze Layer Data Extraction**
  - [ ] Implement `src/extractors/bronze_extractor.py` base class
  - [ ] Create company data extractor with "Bronze_" prefix
  - [ ] Create deal/opportunity data extractor
  - [ ] Create contact/person data extractor
  - [ ] Create communication logs extractor
  - [ ] Add automated sheet creation for each report run
- [ ] **DuckDB Processing Engine**
  - [ ] Implement `src/processors/duckdb_engine.py` core processor
  - [ ] Create SQL transformation scripts directory
  - [ ] Add business logic processing modules
  - [ ] Implement proper connection and memory management

### Phase 2: Core Data Model Implementation
- [ ] **Entity Relationship Implementation**
  - [ ] Implement Company entity processing (1 to many relationships)
  - [ ] Implement Contact entity processing (0-1 to 0-n cardinality)
  - [ ] Implement Deal entity processing with pipeline logic
  - [ ] Implement Communication entity with many-to-many relationships
  - [ ] Create DuckDB views for entity relationships
- [ ] **Business Logic Engine**
  - [ ] Implement Hardware pipeline stage mapping (5 stages)
  - [ ] Implement Software pipeline stage mapping (5 stages)
  - [ ] Add double granularity final stages (No-go, Abandonnée, Perdue, Gagnée)
  - [ ] Create computed column calculations:
    - [ ] IC_alps Forecast → Amount mapping
    - [ ] Weighted Forecast = Amount × IC_alps Certainty
    - [ ] Net Amount = Forecast - Cost
    - [ ] Net Weighted Amount calculation
  - [ ] Add business rule validation and testing

### Phase 3: xlwings Integration Layer
- [ ] **xlwings Custom Scripts Framework**
  - [ ] Create `src/xlwings_scripts/data_loader.py` for Bronze data loading
  - [ ] Create `src/xlwings_scripts/data_processor.py` for DuckDB processing
  - [ ] Create `src/xlwings_scripts/dashboard_generator.py` for dashboards
  - [ ] Create `src/xlwings_scripts/report_generator.py` for reports
  - [ ] Add Excel template files for standardized layouts
- [ ] **Interactive Dashboard Components**
  - [ ] Implement CellPrinter class for formatted Excel output
  - [ ] Create DataProcessing class for dashboard data preparation
  - [ ] Add KPI calculation functions (total deals, win rate, pipeline value)
  - [ ] Implement interactive filtering capabilities
  - [ ] Add matplotlib chart generation and Excel integration
  - [ ] Create drill-down functionality by pipeline type

### Phase 4: Advanced Features & Optimization
- [ ] **Advanced Analytics Engine**
  - [ ] Implement forecast analysis with weighted pipeline values
  - [ ] Add risk assessment (High/Medium/Low based on certainty)
  - [ ] Create trend analysis for historical pipeline progression
  - [ ] Add conversion analytics for stage conversion rates
  - [ ] Implement deal outcome prediction models
- [ ] **Performance Optimization**
  - [ ] Add lazy loading for large datasets
  - [ ] Optimize SQL queries for better performance
  - [ ] Implement caching strategy for Bronze and processed data
  - [ ] Add memory management for large Excel workbooks
  - [ ] Create performance monitoring and benchmarking

### Phase 5: Production Deployment & Monitoring
- [ ] **Error Handling & Logging**
  - [ ] Implement centralized error handling system
  - [ ] Add structured logging with multiple levels
  - [ ] Create data validation and quality checks
  - [ ] Add user-friendly error messages and recovery guidance
- [ ] **Testing & Quality Assurance**
  - [ ] Create unit tests for all business logic components
  - [ ] Add integration tests for database connectivity
  - [ ] Implement end-to-end pipeline testing
  - [ ] Add performance tests for large datasets
  - [ ] Achieve >90% code coverage target
- [ ] **Documentation & Training**
  - [ ] Create user manuals for Excel interface
  - [ ] Write API documentation for all modules
  - [ ] Add troubleshooting guides
  - [ ] Create training materials and videos

### Critical Success Milestones
- [ ] **Milestone 1**: Successful data extraction from SQL Server with Bronze layer
- [ ] **Milestone 2**: Accurate pipeline mapping and computed column calculations
- [ ] **Milestone 3**: Interactive Excel interface with working dashboards
- [ ] **Milestone 4**: Performance targets met (<30 seconds full refresh)
- [ ] **Milestone 5**: Full system operational with monitoring and error handling

## Important Notes

- Always test database connections before implementing full data pipelines
- Ensure proper handling of Excel workbook states when using xlwings
- Maintain version control for all data transformation logic
- Document any changes to business rules or pipeline architecture