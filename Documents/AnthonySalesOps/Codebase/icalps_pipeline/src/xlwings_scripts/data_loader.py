"""
XLWings Data Loader Scripts for IC'ALPS Pipeline
Handles loading CSV data into Excel and Bronze layer processing
"""

import xlwings as xw
from xlwings import script
import pandas as pd
import logging
from typing import Dict, Optional
from extractors.bronze_extractor import bronze_extractor
from database.csv_connector import csv_connector

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

class CellPrinter:
    """Helper class for formatted cell printing in Excel"""

    def __init__(self, sheet: xw.Sheet, font_size: int = 12):
        self.sheet = sheet
        self.font_size = font_size

    def print_header(self, cell: str, text: str, color: str = BLUE_1):
        """Print formatted header"""
        range_obj = self.sheet.range(cell)
        range_obj.value = text
        range_obj.font.size = self.font_size + 2
        range_obj.font.bold = True
        range_obj.font.color = color

    def print_value(self, cell: str, value, color: str = GRAY_3):
        """Print formatted value"""
        range_obj = self.sheet.range(cell)
        range_obj.value = value
        range_obj.font.size = self.font_size
        range_obj.font.color = color

    def print_kpi(self, cell: str, value, color: str = GREEN_1):
        """Print KPI value with emphasis"""
        range_obj = self.sheet.range(cell)
        range_obj.value = value
        range_obj.font.size = self.font_size + 4
        range_obj.font.bold = True
        range_obj.font.color = color

@script(include=["Bronze_Data"])
def load_bronze_companies(book: xw.Book):
    """Load Bronze companies data into Excel"""
    try:
        logger.info("Loading Bronze companies data...")

        # Get or create Bronze_Data sheet
        try:
            sheet = book.sheets["Bronze_Companies"]
        except:
            sheet = book.sheets.add("Bronze_Companies")

        # Clear existing data
        sheet.clear()

        # Load data
        df = bronze_extractor.extract_bronze_companies()
        if df is None or len(df) == 0:
            sheet.range("A1").value = "No companies data available"
            return

        # Write headers and data
        sheet.range("A1").value = "IC'ALPS Bronze Companies Data"
        sheet.range("A1").font.size = 16
        sheet.range("A1").font.bold = True
        sheet.range("A1").font.color = BLUE_1

        # Write timestamp
        sheet.range("A2").value = f"Extracted: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}"
        sheet.range("A2").font.color = GRAY_2

        # Write data starting from row 4
        sheet.range("A4").value = df

        # Format headers
        header_range = sheet.range(f"A4:{chr(64 + len(df.columns))}4")
        header_range.font.bold = True
        header_range.color = GRAY_1

        logger.info(f"Loaded {len(df)} companies to Excel")

    except Exception as e:
        logger.error(f"Error loading Bronze companies: {str(e)}")
        if 'sheet' in locals():
            sheet.range("A1").value = f"Error: {str(e)}"

@script(include=["Bronze_Data"])
def load_bronze_opportunities(book: xw.Book):
    """Load Bronze opportunities data into Excel"""
    try:
        logger.info("Loading Bronze opportunities data...")

        # Get or create sheet
        try:
            sheet = book.sheets["Bronze_Opportunities"]
        except:
            sheet = book.sheets.add("Bronze_Opportunities")

        # Clear existing data
        sheet.clear()

        # Load data
        df = bronze_extractor.extract_bronze_opportunities()
        if df is None or len(df) == 0:
            sheet.range("A1").value = "No opportunities data available"
            return

        # Write headers and data
        sheet.range("A1").value = "IC'ALPS Bronze Opportunities Data"
        sheet.range("A1").font.size = 16
        sheet.range("A1").font.bold = True
        sheet.range("A1").font.color = BLUE_1

        # Write summary
        cp = CellPrinter(sheet, 12)
        cp.print_value("A2", f"Total Opportunities: {len(df)}")
        cp.print_value("A3", f"Extracted: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")

        # Write data starting from row 5
        sheet.range("A5").value = df

        # Format headers
        if len(df.columns) > 0:
            header_range = sheet.range(f"A5:{chr(64 + len(df.columns))}5")
            header_range.font.bold = True
            header_range.color = GRAY_1

        logger.info(f"Loaded {len(df)} opportunities to Excel")

    except Exception as e:
        logger.error(f"Error loading Bronze opportunities: {str(e)}")
        if 'sheet' in locals():
            sheet.range("A1").value = f"Error: {str(e)}"

@script(include=["Bronze_Data"])
def load_bronze_persons(book: xw.Book):
    """Load Bronze persons data into Excel"""
    try:
        logger.info("Loading Bronze persons data...")

        # Get or create sheet
        try:
            sheet = book.sheets["Bronze_Persons"]
        except:
            sheet = book.sheets.add("Bronze_Persons")

        # Clear existing data
        sheet.clear()

        # Load data
        df = bronze_extractor.extract_bronze_persons()
        if df is None or len(df) == 0:
            sheet.range("A1").value = "No persons data available"
            return

        # Write headers and data
        cp = CellPrinter(sheet, 12)
        cp.print_header("A1", "IC'ALPS Bronze Persons Data")
        cp.print_value("A2", f"Total Persons: {len(df)}")
        cp.print_value("A3", f"Extracted: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")

        # Write data starting from row 5
        sheet.range("A5").value = df

        # Format headers
        if len(df.columns) > 0:
            header_range = sheet.range(f"A5:{chr(64 + len(df.columns))}5")
            header_range.font.bold = True
            header_range.color = GRAY_1

        logger.info(f"Loaded {len(df)} persons to Excel")

    except Exception as e:
        logger.error(f"Error loading Bronze persons: {str(e)}")
        if 'sheet' in locals():
            sheet.range("A1").value = f"Error: {str(e)}"

@script(include=["Bronze_Data"])
def load_all_bronze_data(book: xw.Book):
    """Load all Bronze layer data into Excel"""
    try:
        logger.info("Loading all Bronze data...")

        # Create summary sheet
        try:
            summary_sheet = book.sheets["Bronze_Summary"]
        except:
            summary_sheet = book.sheets.add("Bronze_Summary")

        # Clear summary sheet
        summary_sheet.clear()

        # Load all Bronze data
        bronze_data = bronze_extractor.extract_all_bronze_data()

        # Create summary
        cp = CellPrinter(summary_sheet, 14)
        cp.print_header("A1", "IC'ALPS Bronze Layer Summary", BLUE_1)
        cp.print_value("A2", f"Extraction Date: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")

        # Summary statistics
        row = 4
        cp.print_header(f"A{row}", "Dataset", GRAY_3)
        cp.print_header(f"B{row}", "Records", GRAY_3)
        cp.print_header(f"C{row}", "Columns", GRAY_3)
        cp.print_header(f"D{row}", "Status", GRAY_3)

        for entity_type, df in bronze_data.items():
            row += 1
            cp.print_value(f"A{row}", entity_type.title())
            cp.print_value(f"B{row}", len(df))
            cp.print_value(f"C{row}", len(df.columns))
            cp.print_value(f"D{row}", "✓ Loaded", GREEN_1)

        # Load individual datasets
        if 'companies' in bronze_data:
            load_bronze_companies(book)
        if 'opportunities' in bronze_data:
            load_bronze_opportunities(book)
        if 'persons' in bronze_data:
            load_bronze_persons(book)

        logger.info(f"Successfully loaded {len(bronze_data)} Bronze datasets")

    except Exception as e:
        logger.error(f"Error loading all Bronze data: {str(e)}")
        if 'summary_sheet' in locals():
            summary_sheet.range("A1").value = f"Error: {str(e)}"

@script()
def validate_csv_files(book: xw.Book):
    """Validate CSV files and display status"""
    try:
        # Get or create validation sheet
        try:
            sheet = book.sheets["File_Validation"]
        except:
            sheet = book.sheets.add("File_Validation")

        # Clear existing data
        sheet.clear()

        # Validate files
        validation_results = csv_connector.config.validate_csv_files()

        # Create header
        cp = CellPrinter(sheet, 12)
        cp.print_header("A1", "CSV File Validation Status", BLUE_1)
        cp.print_value("A2", f"Validation Date: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")

        # Headers
        row = 4
        cp.print_header(f"A{row}", "File", GRAY_3)
        cp.print_header(f"B{row}", "Status", GRAY_3)
        cp.print_header(f"C{row}", "Path", GRAY_3)

        # File status
        for file_key, exists in validation_results.items():
            row += 1
            cp.print_value(f"A{row}", file_key)
            if exists:
                cp.print_value(f"B{row}", "✓ Found", GREEN_1)
            else:
                cp.print_value(f"B{row}", "✗ Missing", RED_1)

            file_path = csv_connector.config.csv_files.get(file_key, "Unknown")
            cp.print_value(f"C{row}", file_path, GRAY_2)

        # Summary
        total_files = len(validation_results)
        found_files = sum(validation_results.values())

        row += 2
        cp.print_header(f"A{row}", "Summary:")
        cp.print_value(f"B{row}", f"{found_files}/{total_files} files found")

        if found_files == total_files:
            cp.print_value(f"C{row}", "✓ All files ready", GREEN_1)
        else:
            cp.print_value(f"C{row}", "⚠ Missing files", ORANGE_1)

        logger.info(f"File validation completed: {found_files}/{total_files} files found")

    except Exception as e:
        logger.error(f"Error validating CSV files: {str(e)}")
        if 'sheet' in locals():
            sheet.range("A1").value = f"Error: {str(e)}"

@script()
def show_data_summary(book: xw.Book):
    """Show data summary statistics"""
    try:
        # Get or create summary sheet
        try:
            sheet = book.sheets["Data_Summary"]
        except:
            sheet = book.sheets.add("Data_Summary")

        # Clear existing data
        sheet.clear()

        # Get data summary
        summary = csv_connector.get_data_summary()

        # Create header
        cp = CellPrinter(sheet, 12)
        cp.print_header("A1", "IC'ALPS Data Summary", BLUE_1)
        cp.print_value("A2", f"Generated: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")

        # Headers
        row = 4
        cp.print_header(f"A{row}", "Dataset", GRAY_3)
        cp.print_header(f"B{row}", "Rows", GRAY_3)
        cp.print_header(f"C{row}", "Columns", GRAY_3)
        cp.print_header(f"D{row}", "Null Values", GRAY_3)

        # Data summary
        total_rows = 0
        for dataset, stats in summary.items():
            row += 1
            cp.print_value(f"A{row}", dataset.title())
            cp.print_kpi(f"B{row}", stats['rows'])
            cp.print_value(f"C{row}", stats['columns'])
            cp.print_value(f"D{row}", stats['null_values'])
            total_rows += stats['rows']

        # Overall summary
        row += 2
        cp.print_header(f"A{row}", "Total Records:")
        cp.print_kpi(f"B{row}", total_rows, BLUE_1)

        logger.info(f"Data summary displayed: {len(summary)} datasets, {total_rows} total records")

    except Exception as e:
        logger.error(f"Error showing data summary: {str(e)}")
        if 'sheet' in locals():
            sheet.range("A1").value = f"Error: {str(e)}"