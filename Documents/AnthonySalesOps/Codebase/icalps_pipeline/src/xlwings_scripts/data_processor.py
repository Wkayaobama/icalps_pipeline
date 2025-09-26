"""
XLWings Data Processor Scripts for IC'ALPS Pipeline
Handles DuckDB processing and business logic application in Excel
"""

import xlwings as xw
from xlwings import script
import pandas as pd
import logging
from typing import Dict, Optional
from processors.duckdb_engine import duckdb_processor
from extractors.bronze_extractor import bronze_extractor
from business_logic.business_rules import business_rules_engine

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Excel styling constants
GRAY_1 = "#CCCCCC"
GRAY_2 = "#657072"
GRAY_3 = "#4A606C"
BLUE_1 = "#1E4E5C"
GREEN_1 = "#28A745"
RED_1 = "#DC3545"
ORANGE_1 = "#FD7E14"
PURPLE_1 = "#6F42C1"

class ProcessingStatusReporter:
    """Helper class for reporting processing status in Excel"""

    def __init__(self, sheet: xw.Sheet):
        self.sheet = sheet
        self.current_row = 3

    def add_step(self, step_name: str, status: str = "Processing...", color: str = ORANGE_1):
        """Add processing step to status report"""
        self.sheet.range(f"A{self.current_row}").value = step_name
        self.sheet.range(f"B{self.current_row}").value = status
        self.sheet.range(f"B{self.current_row}").font.color = color
        self.current_row += 1

    def update_step(self, step_name: str, status: str, color: str = GREEN_1):
        """Update status of existing step"""
        # Find the row with the step name
        for row in range(3, self.current_row):
            if self.sheet.range(f"A{row}").value == step_name:
                self.sheet.range(f"B{row}").value = status
                self.sheet.range(f"B{row}").font.color = color
                break

@script(include=["Bronze_Data", "Processed_Data"])
def process_bronze_to_duckdb(book: xw.Book):
    """Process Bronze data through DuckDB transformations"""
    try:
        # Get or create processing status sheet
        try:
            status_sheet = book.sheets["Processing_Status"]
        except:
            status_sheet = book.sheets.add("Processing_Status")

        # Clear and setup status sheet
        status_sheet.clear()
        status_sheet.range("A1").value = "IC'ALPS Data Processing Status"
        status_sheet.range("A1").font.size = 16
        status_sheet.range("A1").font.bold = True
        status_sheet.range("A1").font.color = BLUE_1

        status_sheet.range("A2").value = f"Started: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}"

        reporter = ProcessingStatusReporter(status_sheet)

        # Step 1: Extract Bronze data
        reporter.add_step("1. Extracting Bronze Data")
        bronze_data = bronze_extractor.extract_all_bronze_data()
        if not bronze_data:
            reporter.update_step("1. Extracting Bronze Data", "❌ Failed", RED_1)
            return
        reporter.update_step("1. Extracting Bronze Data", f"✅ Complete ({len(bronze_data)} datasets)", GREEN_1)

        # Step 2: Initialize DuckDB
        reporter.add_step("2. Initializing DuckDB")
        with duckdb_processor as processor:
            if not processor.connection:
                reporter.update_step("2. Initializing DuckDB", "❌ Failed", RED_1)
                return
            reporter.update_step("2. Initializing DuckDB", "✅ Connected", GREEN_1)

            # Step 3: Register Bronze tables
            reporter.add_step("3. Registering Bronze Tables")
            if not processor.register_bronze_tables(bronze_data):
                reporter.update_step("3. Registering Bronze Tables", "❌ Failed", RED_1)
                return
            reporter.update_step("3. Registering Bronze Tables", "✅ Complete", GREEN_1)

            # Step 4: Create processed views
            reporter.add_step("4. Creating Processed Views")
            if not processor.create_all_views():
                reporter.update_step("4. Creating Processed Views", "⚠ Partial", ORANGE_1)
            else:
                reporter.update_step("4. Creating Processed Views", "✅ Complete", GREEN_1)

            # Step 5: Export processed data
            reporter.add_step("5. Exporting Processed Data")
            processed_data = {}

            view_names = [
                ('Processed_Companies', 'companies'),
                ('Processed_Opportunities', 'opportunities'),
                ('Processed_Persons', 'persons'),
                ('Processed_Communications', 'communications')
            ]

            for view_name, entity_type in view_names:
                try:
                    df = processor.execute_query(f"SELECT * FROM {view_name}")
                    if df is not None:
                        processed_data[entity_type] = df
                except Exception as e:
                    logger.warning(f"Could not export {view_name}: {str(e)}")

            if processed_data:
                reporter.update_step("5. Exporting Processed Data", f"✅ Complete ({len(processed_data)} datasets)", GREEN_1)
            else:
                reporter.update_step("5. Exporting Processed Data", "❌ Failed", RED_1)
                return

        # Step 6: Load to Excel
        reporter.add_step("6. Loading to Excel")
        load_processed_data_to_excel(book, processed_data)
        reporter.update_step("6. Loading to Excel", "✅ Complete", GREEN_1)

        # Final status
        reporter.add_step("Processing Complete", f"✅ Success at {pd.Timestamp.now().strftime('%H:%M:%S')}", GREEN_1)

        logger.info("DuckDB processing completed successfully")

    except Exception as e:
        logger.error(f"Error in DuckDB processing: {str(e)}")
        if 'reporter' in locals():
            reporter.add_step("ERROR", f"❌ {str(e)}", RED_1)

@script(include=["Processed_Data"])
def apply_business_rules(book: xw.Book):
    """Apply business rules to processed data"""
    try:
        # Get or create business rules status sheet
        try:
            status_sheet = book.sheets["Business_Rules_Status"]
        except:
            status_sheet = book.sheets.add("Business_Rules_Status")

        # Clear and setup status sheet
        status_sheet.clear()
        status_sheet.range("A1").value = "IC'ALPS Business Rules Application"
        status_sheet.range("A1").font.size = 16
        status_sheet.range("A1").font.bold = True
        status_sheet.range("A1").font.color = PURPLE_1

        reporter = ProcessingStatusReporter(status_sheet)

        # Step 1: Load processed data
        reporter.add_step("1. Loading Processed Data")

        # Read from Excel sheets
        processed_data = {}
        sheet_mappings = {
            'Processed_Opportunities': 'opportunities',
            'Processed_Companies': 'companies',
            'Processed_Persons': 'persons'
        }

        for sheet_name, entity_type in sheet_mappings.items():
            try:
                if sheet_name in [s.name for s in book.sheets]:
                    sheet = book.sheets[sheet_name]
                    # Read data starting from row 2 (assuming headers in row 1)
                    data_range = sheet.used_range
                    if data_range and data_range.shape[0] > 1:
                        df = pd.DataFrame(data_range.value[1:], columns=data_range.value[0])
                        processed_data[entity_type] = df
            except Exception as e:
                logger.warning(f"Could not read {sheet_name}: {str(e)}")

        if not processed_data:
            reporter.update_step("1. Loading Processed Data", "❌ No data found", RED_1)
            return

        reporter.update_step("1. Loading Processed Data", f"✅ Loaded {len(processed_data)} datasets", GREEN_1)

        # Step 2: Apply business rules to opportunities
        reporter.add_step("2. Applying Opportunity Rules")
        if 'opportunities' in processed_data:
            opportunities_df = business_rules_engine.apply_business_rules_to_opportunities(
                processed_data['opportunities']
            )
            processed_data['opportunities'] = opportunities_df
            reporter.update_step("2. Applying Opportunity Rules", "✅ Complete", GREEN_1)
        else:
            reporter.update_step("2. Applying Opportunity Rules", "⚠ No opportunities data", ORANGE_1)

        # Step 3: Apply business rules to companies
        reporter.add_step("3. Applying Company Rules")
        if 'companies' in processed_data:
            companies_df = business_rules_engine.apply_business_rules_to_companies(
                processed_data['companies']
            )
            processed_data['companies'] = companies_df
            reporter.update_step("3. Applying Company Rules", "✅ Complete", GREEN_1)
        else:
            reporter.update_step("3. Applying Company Rules", "⚠ No companies data", ORANGE_1)

        # Step 4: Apply business rules to persons
        reporter.add_step("4. Applying Person Rules")
        if 'persons' in processed_data:
            persons_df = business_rules_engine.apply_business_rules_to_persons(
                processed_data['persons']
            )
            processed_data['persons'] = persons_df
            reporter.update_step("4. Applying Person Rules", "✅ Complete", GREEN_1)
        else:
            reporter.update_step("4. Applying Person Rules", "⚠ No persons data", ORANGE_1)

        # Step 5: Generate business rules report
        reporter.add_step("5. Generating Report")
        rules_report = business_rules_engine.generate_business_rules_report(processed_data)

        # Create report sheet
        create_business_rules_report_sheet(book, rules_report)
        reporter.update_step("5. Generating Report", "✅ Complete", GREEN_1)

        # Step 6: Update Excel with enhanced data
        reporter.add_step("6. Updating Excel Data")
        load_enhanced_data_to_excel(book, processed_data)
        reporter.update_step("6. Updating Excel Data", "✅ Complete", GREEN_1)

        # Final status
        reporter.add_step("Business Rules Complete", f"✅ Success at {pd.Timestamp.now().strftime('%H:%M:%S')}", GREEN_1)

        logger.info("Business rules application completed successfully")

    except Exception as e:
        logger.error(f"Error applying business rules: {str(e)}")
        if 'reporter' in locals():
            reporter.add_step("ERROR", f"❌ {str(e)}", RED_1)

@script()
def full_pipeline_process(book: xw.Book):
    """Run the complete pipeline from Bronze to Business Rules"""
    try:
        logger.info("Starting full pipeline processing...")

        # Create main status sheet
        try:
            main_sheet = book.sheets["Pipeline_Status"]
        except:
            main_sheet = book.sheets.add("Pipeline_Status")

        main_sheet.clear()
        main_sheet.range("A1").value = "IC'ALPS Full Pipeline Processing"
        main_sheet.range("A1").font.size = 18
        main_sheet.range("A1").font.bold = True
        main_sheet.range("A1").font.color = BLUE_1

        main_sheet.range("A2").value = f"Started: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}"

        # Step 1: Process Bronze to DuckDB
        main_sheet.range("A4").value = "Phase 1: DuckDB Processing"
        main_sheet.range("A4").font.bold = True
        main_sheet.range("A4").font.color = BLUE_1

        process_bronze_to_duckdb(book)

        # Step 2: Apply Business Rules
        main_sheet.range("A6").value = "Phase 2: Business Rules Application"
        main_sheet.range("A6").font.bold = True
        main_sheet.range("A6").font.color = PURPLE_1

        apply_business_rules(book)

        # Completion
        main_sheet.range("A8").value = f"Pipeline Complete: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}"
        main_sheet.range("A8").font.bold = True
        main_sheet.range("A8").font.color = GREEN_1

        logger.info("Full pipeline processing completed")

    except Exception as e:
        logger.error(f"Error in full pipeline: {str(e)}")
        if 'main_sheet' in locals():
            main_sheet.range("A10").value = f"Error: {str(e)}"
            main_sheet.range("A10").font.color = RED_1

def load_processed_data_to_excel(book: xw.Book, processed_data: Dict[str, pd.DataFrame]):
    """Load processed data to Excel sheets"""
    for entity_type, df in processed_data.items():
        try:
            sheet_name = f"Processed_{entity_type.title()}"

            # Get or create sheet
            try:
                sheet = book.sheets[sheet_name]
            except:
                sheet = book.sheets.add(sheet_name)

            # Clear and load data
            sheet.clear()
            sheet.range("A1").value = df

            # Format headers
            if len(df.columns) > 0:
                header_range = sheet.range(f"A1:{chr(64 + len(df.columns))}1")
                header_range.font.bold = True
                header_range.color = GRAY_1

            logger.info(f"Loaded {len(df)} records to {sheet_name}")

        except Exception as e:
            logger.error(f"Error loading {entity_type} to Excel: {str(e)}")

def load_enhanced_data_to_excel(book: xw.Book, enhanced_data: Dict[str, pd.DataFrame]):
    """Load enhanced data with business rules to Excel sheets"""
    for entity_type, df in enhanced_data.items():
        try:
            sheet_name = f"Enhanced_{entity_type.title()}"

            # Get or create sheet
            try:
                sheet = book.sheets[sheet_name]
            except:
                sheet = book.sheets.add(sheet_name)

            # Clear and load data
            sheet.clear()
            sheet.range("A1").value = df

            # Format headers
            if len(df.columns) > 0:
                header_range = sheet.range(f"A1:{chr(64 + len(df.columns))}1")
                header_range.font.bold = True
                header_range.color = BLUE_1

            logger.info(f"Loaded enhanced {len(df)} records to {sheet_name}")

        except Exception as e:
            logger.error(f"Error loading enhanced {entity_type} to Excel: {str(e)}")

def create_business_rules_report_sheet(book: xw.Book, report: Dict):
    """Create business rules report sheet"""
    try:
        # Get or create report sheet
        try:
            sheet = book.sheets["Business_Rules_Report"]
        except:
            sheet = book.sheets.add("Business_Rules_Report")

        sheet.clear()

        # Title
        sheet.range("A1").value = "IC'ALPS Business Rules Report"
        sheet.range("A1").font.size = 16
        sheet.range("A1").font.bold = True
        sheet.range("A1").font.color = PURPLE_1

        row = 3

        # Summary section
        sheet.range(f"A{row}").value = "Summary"
        sheet.range(f"A{row}").font.bold = True
        row += 1

        for entity, summary in report.get('summary', {}).items():
            sheet.range(f"A{row}").value = f"{entity.title()}:"
            sheet.range(f"B{row}").value = f"{summary['total_records']} records, {summary['columns_count']} columns"
            row += 1

        row += 1

        # Pipeline statistics
        if 'pipeline_stats' in report:
            stats = report['pipeline_stats']
            sheet.range(f"A{row}").value = "Pipeline Statistics"
            sheet.range(f"A{row}").font.bold = True
            row += 1

            sheet.range(f"A{row}").value = f"Total Opportunities: {stats.get('total_opportunities', 0)}"
            row += 1

            sheet.range(f"A{row}").value = f"Low Confidence Mappings: {stats.get('low_confidence_count', 0)}"
            row += 1

        row += 1

        # Recommendations
        if 'recommendations' in report and report['recommendations']:
            sheet.range(f"A{row}").value = "Recommendations"
            sheet.range(f"A{row}").font.bold = True
            row += 1

            for recommendation in report['recommendations']:
                sheet.range(f"A{row}").value = f"• {recommendation}"
                row += 1

        logger.info("Business rules report sheet created")

    except Exception as e:
        logger.error(f"Error creating business rules report: {str(e)}")