#!/usr/bin/env python3
"""
GitHub Issues Generator for IC'ALPS Pipeline Project
Generates comprehensive GitHub issues from the CLAUDE.md development plan.
"""

import json
import os
from datetime import datetime, timedelta
from typing import List, Dict, Any

class GitHubIssueGenerator:
    def __init__(self):
        self.base_date = datetime(2024, 1, 15)  # Project start date
        self.issues = []

    def generate_issue(self, title: str, body: str, labels: List[str],
                      phase: str, criticality: str = "Medium",
                      estimated_days: int = 5) -> Dict[str, Any]:
        """Generate a structured GitHub issue"""
        start_date = self.base_date + timedelta(days=len(self.issues) * 2)
        end_date = start_date + timedelta(days=estimated_days)

        issue = {
            "title": title,
            "body": body,
            "labels": labels + [f"phase-{phase.lower().replace(' ', '-')}"],
            "assignees": ["Wkayaobama"],
            "milestone": phase,
            "custom_fields": {
                "criticality": criticality,
                "start_date": start_date.strftime("%Y-%m-%d"),
                "end_date": end_date.strftime("%Y-%m-%d"),
                "estimated_hours": estimated_days * 8
            }
        }
        return issue

    def generate_phase_1_issues(self):
        """Phase 1: Foundation Infrastructure"""
        phase = "Phase 1: Foundation Infrastructure"

        # Database Connection Framework
        self.issues.append(self.generate_issue(
            title="[INFRA] Create Database Connection Framework",
            body="""## What should this workflow do?
Create a robust database connection framework with ADODB connections, connection pooling, retry logic, and comprehensive error handling.

## How does it start (trigger)?
- Application startup initialization
- On-demand connection requests from data extractors
- Configuration file changes

## Step-by-step process
1. Create `src/database/connection.py` with DatabaseConnector class
2. Implement `src/config/database_config.py` for configuration management
3. Add connection string templates and error handling
4. Implement connection pooling and retry logic with exponential backoff
5. Add comprehensive logging and monitoring
6. Write unit tests for database connectivity (>90% coverage)
7. Create usage documentation and examples

## What services does it connect to?
- SQL Server databases (primary CRM data source)
- Configuration management system
- Python logging framework
- Error monitoring system (future)

## What should be the output?
- DatabaseConnector class with full CRUD operations
- Configuration templates for different environments
- Comprehensive unit test suite
- Usage documentation and code examples
- Performance benchmarks

## What could go wrong?
- Connection timeout issues → implement proper timeout handling and retry logic
- Memory leaks from unclosed connections → connection pooling with proper cleanup
- Security vulnerabilities → secure credential management and SQL injection prevention
- Performance bottlenecks → connection pooling and query optimization
- Configuration errors → validation and environment-specific templates""",
            labels=["infrastructure", "database", "critical"],
            phase=phase,
            criticality="Critical",
            estimated_days=7
        ))

        # Bronze Layer Data Extraction
        bronze_extractors = [
            ("Company Data", "companies", "Company entity with one-to-many relationships"),
            ("Deal/Opportunity Data", "deals", "Deal pipeline with stage tracking and forecasting"),
            ("Contact/Person Data", "contacts", "Contact entities with 0-1 to 0-n cardinality"),
            ("Communication Logs", "communications", "Communication logs with many-to-many relationships")
        ]

        for name, prefix, description in bronze_extractors:
            self.issues.append(self.generate_issue(
                title=f"[INFRA] Create Bronze Layer {name} Extractor",
                body=f"""## What should this workflow do?
Extract {name.lower()} from SQL Server and create Bronze layer datasets with proper "Bronze_" prefixes.

## How does it start (trigger)?
- Manual execution via Python script
- Scheduled extraction jobs
- Excel button clicks via xlwings
- API requests from downstream processes

## Step-by-step process
1. Connect to SQL Server using DatabaseConnector
2. Execute optimized SQL queries for {name.lower()}
3. Apply data validation and quality checks
4. Create Bronze layer CSV with "Bronze_{prefix}" prefix
5. Generate metadata and lineage information
6. Handle errors and create audit logs
7. Update extraction status and notifications

## What services does it connect to?
- SQL Server CRM database
- File system (Bronze layer storage)
- Logging and monitoring system
- Data quality validation engine

## What should be the output?
- Bronze_{prefix}.csv with clean, validated data
- Extraction metadata and statistics
- Data quality reports
- Error logs and notifications
- Performance metrics

## What could go wrong?
- Database connection failures → retry logic and failover
- Data quality issues → validation rules and cleansing
- Large dataset memory issues → chunked processing
- File system permissions → proper error handling
- SQL query performance → query optimization and indexing

**Entity Description:** {description}""",
                labels=["infrastructure", "data-extraction", "bronze-layer"],
                phase=phase,
                criticality="High" if "company" in name.lower() else "Medium",
                estimated_days=5
            ))

        # DuckDB Processing Engine
        self.issues.append(self.generate_issue(
            title="[INFRA] Implement DuckDB Processing Engine",
            body="""## What should this workflow do?
Create a robust DuckDB processing engine for data transformations, business logic application, and analytics processing.

## How does it start (trigger)?
- Post Bronze layer extraction completion
- Manual processing requests via Python/Excel
- Scheduled transformation jobs
- Data change events

## Step-by-step process
1. Implement `src/processors/duckdb_engine.py` core processor
2. Create SQL transformation scripts directory structure
3. Add business logic processing modules
4. Implement memory management and connection pooling
5. Create data validation and quality assurance checks
6. Add performance monitoring and optimization
7. Build comprehensive test suite

## What services does it connect to?
- DuckDB in-memory/persistent databases
- Bronze layer CSV files
- Business logic validation engine
- Memory monitoring systems
- Performance analytics

## What should be the output?
- Processed and transformed datasets
- Business logic validation reports
- Performance metrics and benchmarks
- Data lineage and transformation logs
- Quality assurance reports

## What could go wrong?
- Memory exhaustion with large datasets → chunked processing and optimization
- Complex business logic conflicts → validation and audit procedures
- SQL transformation errors → comprehensive error handling and rollback
- Performance degradation → query optimization and indexing
- Data corruption during processing → backup and recovery procedures""",
            labels=["infrastructure", "duckdb", "data-processing"],
            phase=phase,
            criticality="Critical",
            estimated_days=8
        ))

    def generate_phase_2_issues(self):
        """Phase 2: Core Data Model Implementation"""
        phase = "Phase 2: Core Data Model Implementation"

        # Entity Relationship Implementation
        entities = [
            ("Company", "1 to many relationships", "Primary entity with hierarchical structure"),
            ("Contact", "0-1 to 0-n cardinality", "Person entities linked to companies"),
            ("Deal", "Pipeline logic implementation", "Opportunities with stage tracking"),
            ("Communication", "Many-to-many relationships", "Interaction logs across all entities")
        ]

        for entity, cardinality, description in entities:
            self.issues.append(self.generate_issue(
                title=f"[FEATURE] Implement {entity} Entity Processing",
                body=f"""## What should this workflow do?
Implement {entity.lower()} entity processing with proper relationships ({cardinality}) and business logic validation.

## How does it start (trigger)?
- Bronze layer data availability
- Entity relationship updates
- Business rule changes
- Data validation requests

## Step-by-step process
1. Design {entity} entity schema and relationships
2. Implement data validation and business rules
3. Create DuckDB views for entity relationships
4. Add computed columns and derived fields
5. Implement entity linking and relationship management
6. Create data quality validation procedures
7. Build comprehensive test coverage

## What services does it connect to?
- DuckDB processing engine
- Business rules validation system
- Entity relationship mapper
- Data quality assurance system

## What should be the output?
- {entity} entity with proper relationships
- Validated business logic implementation
- Entity relationship documentation
- Data quality reports
- Performance benchmarks

## What could go wrong?
- Relationship integrity issues → comprehensive validation and constraints
- Business rule conflicts → rule prioritization and conflict resolution
- Performance issues with complex joins → query optimization
- Data inconsistency → validation procedures and cleanup processes
- Circular dependencies → proper entity modeling and validation

**Entity Details:** {description}
**Cardinality:** {cardinality}""",
                labels=["feature", "data-model", "entity-processing"],
                phase=phase,
                criticality="High",
                estimated_days=6
            ))

        # Business Logic Engine
        self.issues.append(self.generate_issue(
            title="[FEATURE] Implement Business Logic Engine",
            body="""## What should this workflow do?
Implement comprehensive business logic engine with pipeline stage mapping, computed columns, and deal lifecycle management.

## How does it start (trigger)?
- Entity data processing completion
- Business rule updates
- Pipeline stage changes
- Computed column recalculation requests

## Step-by-step process
1. Implement Hardware pipeline stage mapping (5 stages)
2. Implement Software pipeline stage mapping (5 stages)
3. Add double granularity final stages (No-go, Abandonnée, Perdue, Gagnée)
4. Create computed column calculations:
   - IC_alps Forecast → Amount mapping
   - Weighted Forecast = Amount × IC_alps Certainty
   - Net Amount = Forecast - Cost
   - Net Weighted Amount calculation
5. Add business rule validation and testing
6. Implement audit trail and change tracking
7. Create performance optimization procedures

## What services does it connect to?
- DuckDB processing engine
- Entity relationship system
- Computed column calculator
- Audit and logging system

## What should be the output?
- Complete pipeline stage mapping system
- All computed columns with proper calculations
- Business rule validation engine
- Audit trail and change tracking
- Performance metrics and optimization

## What could go wrong?
- Business rule conflicts → rule prioritization system
- Computed column calculation errors → validation and testing procedures
- Pipeline stage mapping inconsistencies → comprehensive validation
- Performance issues with complex calculations → optimization and caching
- Audit trail corruption → backup and recovery procedures""",
            labels=["feature", "business-logic", "pipeline-mapping"],
            phase=phase,
            criticality="Critical",
            estimated_days=10
        ))

    def generate_phase_3_issues(self):
        """Phase 3: xlwings Integration Layer"""
        phase = "Phase 3: xlwings Integration Layer"

        xlwings_scripts = [
            ("Data Loader", "data_loader.py", "Bronze data loading with Excel integration"),
            ("Data Processor", "data_processor.py", "DuckDB processing with Excel controls"),
            ("Dashboard Generator", "dashboard_generator.py", "Interactive dashboards and KPIs"),
            ("Report Generator", "report_generator.py", "Automated report generation")
        ]

        for script_name, filename, description in xlwings_scripts:
            self.issues.append(self.generate_issue(
                title=f"[FEATURE] Create xlwings {script_name}",
                body=f"""## What should this workflow do?
Create xlwings custom script for {description} with Excel integration and user-friendly interface.

## How does it start (trigger)?
- Excel button clicks from custom ribbon
- Manual script execution from Python
- Scheduled execution (future enhancement)
- Workflow automation triggers

## Step-by-step process
1. Create `src/xlwings_scripts/{filename}` with @script decorator
2. Implement Excel data reading and validation
3. Add DuckDB integration for data processing
4. Create formatted Excel output with CellPrinter class
5. Add error handling and user notifications
6. Implement progress tracking and status updates
7. Create comprehensive testing and validation

## What services does it connect to?
- Excel workbooks via xlwings
- DuckDB processing engine
- Data validation system
- User notification system
- File system for templates and outputs

## What should be the output?
- Fully functional xlwings script with Excel integration
- Formatted Excel outputs with professional styling
- Error handling and user-friendly notifications
- Progress tracking and status indicators
- Comprehensive documentation and examples

## What could go wrong?
- Excel file locks → proper file handling and user guidance
- xlwings API errors → comprehensive error handling and fallbacks
- Data formatting issues → validation and template standardization
- Performance issues with large datasets → optimization and chunking
- User interface confusion → intuitive design and clear instructions

**Script Purpose:** {description}""",
                labels=["feature", "xlwings", "excel-integration"],
                phase=phase,
                criticality="High",
                estimated_days=7
            ))

        # Interactive Dashboard Components
        self.issues.append(self.generate_issue(
            title="[FEATURE] Implement Interactive Dashboard Components",
            body="""## What should this workflow do?
Create interactive dashboard components with KPI calculations, filtering capabilities, and drill-down functionality.

## How does it start (trigger)?
- Dashboard refresh requests from Excel
- Data update notifications
- Scheduled dashboard generation
- User interaction events

## Step-by-step process
1. Implement CellPrinter class for formatted Excel output
2. Create DataProcessing class for dashboard data preparation
3. Add KPI calculation functions (total deals, win rate, pipeline value)
4. Implement interactive filtering capabilities by various dimensions
5. Add matplotlib chart generation and Excel integration
6. Create drill-down functionality by pipeline type and stage
7. Implement caching for performance optimization

## What services does it connect to?
- Excel via xlwings for interactive interface
- DuckDB for data aggregation and filtering
- matplotlib for chart generation
- Business logic engine for KPI calculations

## What should be the output?
- Interactive Excel dashboards with professional formatting
- Dynamic KPI calculations with real-time updates
- Interactive charts and visualizations
- Filtering and drill-down capabilities
- Performance-optimized data loading

## What could go wrong?
- Performance issues with complex dashboards → caching and optimization
- Chart rendering problems → matplotlib compatibility and error handling
- Interactive filter conflicts → proper state management
- Excel UI responsiveness → asynchronous processing and progress indicators
- Data consistency issues → validation and refresh procedures""",
            labels=["feature", "dashboard", "kpi", "visualization"],
            phase=phase,
            criticality="High",
            estimated_days=8
        ))

    def generate_phase_4_issues(self):
        """Phase 4: Advanced Features & Optimization"""
        phase = "Phase 4: Advanced Features & Optimization"

        # Advanced Analytics Engine
        analytics_features = [
            ("Forecast Analysis", "Weighted pipeline values and forecasting models"),
            ("Risk Assessment", "High/Medium/Low risk categorization based on certainty"),
            ("Trend Analysis", "Historical pipeline progression and patterns"),
            ("Conversion Analytics", "Stage conversion rates and success metrics"),
            ("Deal Outcome Prediction", "Predictive models for deal success probability")
        ]

        for feature_name, description in analytics_features:
            self.issues.append(self.generate_issue(
                title=f"[FEATURE] Implement {feature_name}",
                body=f"""## What should this workflow do?
Implement {feature_name.lower()} capabilities with {description.lower()}.

## How does it start (trigger)?
- Analytics dashboard requests
- Scheduled analytical processing
- Management reporting requirements
- Performance review cycles

## Step-by-step process
1. Design analytical models and algorithms
2. Implement data aggregation and statistical processing
3. Create visualization components for insights
4. Add trend detection and pattern recognition
5. Implement predictive modeling capabilities
6. Create automated insight generation
7. Add comprehensive testing and validation

## What services does it connect to?
- DuckDB for analytical processing
- Historical data repositories
- Statistical analysis libraries
- Visualization and reporting systems

## What should be the output?
- Advanced analytical insights and recommendations
- Interactive visualizations and reports
- Automated trend detection and alerts
- Predictive models with accuracy metrics
- Performance benchmarks and optimization

## What could go wrong?
- Model accuracy issues → continuous validation and improvement
- Performance degradation with large datasets → optimization and sampling
- Complex statistical calculations → validation and testing procedures
- Data quality affecting predictions → robust preprocessing and validation
- User interpretation of complex analytics → clear documentation and training

**Feature Focus:** {description}""",
                labels=["feature", "analytics", "advanced"],
                phase=phase,
                criticality="Medium",
                estimated_days=6
            ))

        # Performance Optimization
        self.issues.append(self.generate_issue(
            title="[INFRA] Performance Optimization Implementation",
            body="""## What should this workflow do?
Implement comprehensive performance optimization across all system components with monitoring and benchmarking.

## How does it start (trigger)?
- Performance degradation detection
- Capacity planning requirements
- User experience improvement needs
- System scaling requirements

## Step-by-step process
1. Add lazy loading for large datasets
2. Optimize SQL queries for better performance
3. Implement caching strategy for Bronze and processed data
4. Add memory management for large Excel workbooks
5. Create performance monitoring and benchmarking
6. Implement automated performance testing
7. Add capacity planning and scaling guidance

## What services does it connect to?
- All system components for performance monitoring
- Memory management systems
- Caching infrastructure
- Performance testing frameworks

## What should be the output?
- Optimized system performance across all components
- Comprehensive performance monitoring dashboard
- Automated performance testing suite
- Capacity planning documentation
- Performance benchmarks and SLAs

## What could go wrong?
- Over-optimization causing complexity → balance performance with maintainability
- Memory leaks in optimization code → comprehensive testing and monitoring
- Caching inconsistency issues → proper cache invalidation strategies
- Performance regression → continuous monitoring and alerting
- Resource contention under load → proper resource management and throttling""",
            labels=["infrastructure", "performance", "optimization"],
            phase=phase,
            criticality="High",
            estimated_days=8
        ))

    def generate_phase_5_issues(self):
        """Phase 5: Production Deployment & Monitoring"""
        phase = "Phase 5: Production Deployment & Monitoring"

        production_components = [
            ("Error Handling & Logging", "Centralized error handling with structured logging"),
            ("Testing & Quality Assurance", "Comprehensive testing suite with >90% coverage"),
            ("Documentation & Training", "User manuals, API docs, and training materials")
        ]

        for component, description in production_components:
            self.issues.append(self.generate_issue(
                title=f"[INFRA] Implement {component}",
                body=f"""## What should this workflow do?
Implement {component.lower()} with {description.lower()}.

## How does it start (trigger)?
- Production readiness requirements
- Quality assurance milestones
- User onboarding needs
- Compliance and audit requirements

## Step-by-step process
1. Design comprehensive {component.lower()} framework
2. Implement all required components and procedures
3. Create automation and monitoring capabilities
4. Add comprehensive validation and testing
5. Create documentation and training materials
6. Implement continuous improvement processes
7. Add performance metrics and success criteria

## What services does it connect to?
- All system components for monitoring and quality assurance
- External monitoring and alerting systems
- Documentation and training platforms
- Compliance and audit systems

## What should be the output?
- Production-ready {component.lower()} system
- Comprehensive documentation and procedures
- Automated monitoring and alerting
- Training materials and user guides
- Quality metrics and success indicators

## What could go wrong?
- Incomplete coverage → comprehensive auditing and gap analysis
- Complex procedures → clear documentation and automation
- Training effectiveness → user feedback and continuous improvement
- Documentation becoming outdated → automated updates and maintenance
- Quality degradation over time → continuous monitoring and improvement

**Focus Area:** {description}""",
                labels=["infrastructure", "production", "quality"],
                phase=phase,
                criticality="Critical" if "Error" in component else "High",
                estimated_days=7
            ))

    def generate_milestones(self):
        """Generate project milestones"""
        milestones = [
            {
                "title": "Phase 1: Foundation Infrastructure",
                "description": "Complete database connections, Bronze layer extraction, and DuckDB processing engine",
                "due_date": "2024-02-15",
                "state": "open"
            },
            {
                "title": "Phase 2: Core Data Model Implementation",
                "description": "Entity relationships, business logic engine, and pipeline mapping",
                "due_date": "2024-03-15",
                "state": "open"
            },
            {
                "title": "Phase 3: xlwings Integration Layer",
                "description": "Excel integration, custom scripts, and interactive dashboards",
                "due_date": "2024-04-15",
                "state": "open"
            },
            {
                "title": "Phase 4: Advanced Features & Optimization",
                "description": "Analytics engine, performance optimization, and advanced features",
                "due_date": "2024-05-15",
                "state": "open"
            },
            {
                "title": "Phase 5: Production Deployment & Monitoring",
                "description": "Error handling, testing, documentation, and production readiness",
                "due_date": "2024-06-15",
                "state": "open"
            }
        ]
        return milestones

    def generate_all_issues(self):
        """Generate all issues for the project"""
        print("Generating GitHub issues for IC'ALPS Pipeline Project...")

        self.generate_phase_1_issues()
        self.generate_phase_2_issues()
        self.generate_phase_3_issues()
        self.generate_phase_4_issues()
        self.generate_phase_5_issues()

        return {
            "issues": self.issues,
            "milestones": self.generate_milestones(),
            "project_metadata": {
                "total_issues": len(self.issues),
                "estimated_total_days": sum(issue["custom_fields"]["estimated_hours"] for issue in self.issues) / 8,
                "project_start": self.base_date.strftime("%Y-%m-%d"),
                "estimated_completion": (self.base_date + timedelta(days=180)).strftime("%Y-%m-%d")
            }
        }

    def export_to_json(self, filename="github_issues_export.json"):
        """Export all issues to JSON file"""
        project_data = self.generate_all_issues()

        with open(filename, 'w') as f:
            json.dump(project_data, f, indent=2)

        print(f"Generated {len(project_data['issues'])} issues and {len(project_data['milestones'])} milestones")
        print(f"Exported to {filename}")
        return project_data

if __name__ == "__main__":
    generator = GitHubIssueGenerator()
    project_data = generator.export_to_json()

    print("\\n=== PROJECT SUMMARY ===")
    print(f"Total Issues: {project_data['project_metadata']['total_issues']}")
    print(f"Estimated Days: {project_data['project_metadata']['estimated_total_days']:.0f}")
    print(f"Start Date: {project_data['project_metadata']['project_start']}")
    print(f"Target Completion: {project_data['project_metadata']['estimated_completion']}")

    # Print phase breakdown
    phase_counts = {}
    for issue in project_data['issues']:
        for label in issue['labels']:
            if label.startswith('phase-'):
                phase = label.replace('phase-', '').replace('-', ' ').title()
                phase_counts[phase] = phase_counts.get(phase, 0) + 1

    print("\\n=== PHASE BREAKDOWN ===")
    for phase, count in phase_counts.items():
        print(f"{phase}: {count} issues")