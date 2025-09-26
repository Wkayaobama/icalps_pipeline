"""
Main IC'ALPS Pipeline Runner
Orchestrates the complete data pipeline from CSV to processed Excel output
"""

import logging
import sys
import pandas as pd
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from extractors.bronze_extractor import bronze_extractor
from processors.duckdb_engine import duckdb_processor
from business_logic.business_rules import business_rules_engine
from config.database_config import config

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def test_csv_validation():
    """Test CSV file validation"""
    logger.info("Testing CSV file validation...")

    validation_results = config.validate_csv_files()

    print("\n" + "="*50)
    print("CSV FILE VALIDATION RESULTS")
    print("="*50)

    for file_key, exists in validation_results.items():
        status = "[FOUND]" if exists else "[MISSING]"
        file_path = config.csv_files.get(file_key, "Unknown")
        print(f"{file_key:20} {status:10} {file_path}")

    total_files = len(validation_results)
    found_files = sum(validation_results.values())

    print(f"\nSummary: {found_files}/{total_files} files found")

    if found_files == total_files:
        print("[OK] All CSV files are available")
        return True
    else:
        print("[ERROR] Some CSV files are missing")
        return False

def test_bronze_extraction():
    """Test Bronze layer data extraction"""
    logger.info("Testing Bronze layer extraction...")

    bronze_data = bronze_extractor.extract_all_bronze_data()

    print("\n" + "="*50)
    print("BRONZE LAYER EXTRACTION RESULTS")
    print("="*50)

    if not bronze_data:
        print("[ERROR] No Bronze data extracted")
        return None

    for entity_type, df in bronze_data.items():
        print(f"{entity_type:20} {len(df):8} records  {len(df.columns):3} columns")

    total_records = sum(len(df) for df in bronze_data.values())
    print(f"\nTotal Bronze records: {total_records:,}")

    return bronze_data

def test_duckdb_processing(bronze_data):
    """Test DuckDB processing"""
    logger.info("Testing DuckDB processing...")

    print("\n" + "="*50)
    print("DUCKDB PROCESSING RESULTS")
    print("="*50)

    try:
        with duckdb_processor as processor:
            # Register Bronze tables
            if not processor.register_bronze_tables(bronze_data):
                print("[ERROR] Failed to register Bronze tables")
                return None

            print("[OK] Bronze tables registered successfully")

            # Create processed views
            if not processor.create_all_views():
                print("[WARNING] Some views failed to create")
            else:
                print("[OK] All processed views created")

            # Export processed data
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
                        print(f"[OK] {entity_type:20} {len(df):8} processed records")
                    else:
                        print(f"[ERROR] {entity_type:20} Failed to process")
                except Exception as e:
                    print(f"[ERROR] {entity_type:20} Error: {str(e)}")

            return processed_data

    except Exception as e:
        print(f"[ERROR] DuckDB processing failed: {str(e)}")
        return None

def test_business_rules(processed_data):
    """Test business rules application"""
    logger.info("Testing business rules application...")

    print("\n" + "="*50)
    print("BUSINESS RULES APPLICATION RESULTS")
    print("="*50)

    if not processed_data:
        print("[ERROR] No processed data available")
        return None

    enhanced_data = {}

    # Apply business rules to opportunities
    if 'opportunities' in processed_data:
        try:
            opportunities_df = business_rules_engine.apply_business_rules_to_opportunities(
                processed_data['opportunities']
            )
            enhanced_data['opportunities'] = opportunities_df
            print(f"[OK] Opportunities    {len(opportunities_df):8} enhanced records")

            # Show pipeline mapping results
            if 'hubspot_pipeline' in opportunities_df.columns:
                pipeline_counts = opportunities_df['hubspot_pipeline'].value_counts()
                print(f"  Pipeline distribution:")
                for pipeline, count in pipeline_counts.items():
                    print(f"    {pipeline}: {count} deals")

        except Exception as e:
            print(f"[ERROR] Opportunities processing failed: {str(e)}")

    # Apply business rules to companies
    if 'companies' in processed_data:
        try:
            companies_df = business_rules_engine.apply_business_rules_to_companies(
                processed_data['companies']
            )
            enhanced_data['companies'] = companies_df
            print(f"[OK] Companies       {len(companies_df):8} enhanced records")
        except Exception as e:
            print(f"[ERROR] Companies processing failed: {str(e)}")

    # Apply business rules to persons
    if 'persons' in processed_data:
        try:
            persons_df = business_rules_engine.apply_business_rules_to_persons(
                processed_data['persons']
            )
            enhanced_data['persons'] = persons_df
            print(f"[OK] Persons         {len(persons_df):8} enhanced records")
        except Exception as e:
            print(f"[ERROR] Persons processing failed: {str(e)}")

    return enhanced_data

def test_business_rules_report(enhanced_data):
    """Test business rules report generation"""
    logger.info("Testing business rules report...")

    print("\n" + "="*50)
    print("BUSINESS RULES REPORT")
    print("="*50)

    if not enhanced_data:
        print("[ERROR] No enhanced data available")
        return

    try:
        report = business_rules_engine.generate_business_rules_report(enhanced_data)

        # Summary
        print("Summary:")
        for entity, summary in report.get('summary', {}).items():
            print(f"  {entity}: {summary['total_records']} records, {summary['columns_count']} columns")

        # Pipeline statistics
        if 'pipeline_stats' in report:
            stats = report['pipeline_stats']
            print(f"\nPipeline Statistics:")
            print(f"  Total Opportunities: {stats.get('total_opportunities', 0)}")
            print(f"  Low Confidence Mappings: {stats.get('low_confidence_count', 0)}")

            if 'pipeline_distribution' in stats:
                print(f"  Pipeline Distribution:")
                for pipeline, count in stats['pipeline_distribution'].items():
                    print(f"    {pipeline}: {count}")

        # Recommendations
        if 'recommendations' in report and report['recommendations']:
            print(f"\nRecommendations:")
            for recommendation in report['recommendations']:
                print(f"  • {recommendation}")

        print("✓ Business rules report generated successfully")

    except Exception as e:
        print(f"✗ Business rules report failed: {str(e)}")

def export_results_to_csv(enhanced_data):
    """Export enhanced data to CSV files"""
    logger.info("Exporting results to CSV...")

    print("\n" + "="*50)
    print("CSV EXPORT RESULTS")
    print("="*50)

    if not enhanced_data:
        print("✗ No enhanced data to export")
        return

    output_path = config.output_path
    output_path.mkdir(exist_ok=True)

    for entity_type, df in enhanced_data.items():
        try:
            csv_file = output_path / f"enhanced_{entity_type}.csv"
            df.to_csv(csv_file, index=False)
            print(f"✓ {entity_type:20} exported to {csv_file}")
        except Exception as e:
            print(f"✗ {entity_type:20} export failed: {str(e)}")

def run_full_pipeline_test():
    """Run complete pipeline test"""
    print("="*70)
    print("IC'ALPS DATA PIPELINE - FULL TEST")
    print("="*70)

    start_time = pd.Timestamp.now()

    # Step 1: Validate CSV files
    if not test_csv_validation():
        print("\n[ERROR] Pipeline test failed: Missing CSV files")
        return False

    # Step 2: Extract Bronze data
    bronze_data = test_bronze_extraction()
    if not bronze_data:
        print("\n[ERROR] Pipeline test failed: Bronze extraction failed")
        return False

    # Step 3: Process through DuckDB
    processed_data = test_duckdb_processing(bronze_data)
    if not processed_data:
        print("\n[ERROR] Pipeline test failed: DuckDB processing failed")
        return False

    # Step 4: Apply business rules
    enhanced_data = test_business_rules(processed_data)
    if not enhanced_data:
        print("\n[ERROR] Pipeline test failed: Business rules application failed")
        return False

    # Step 5: Generate report
    test_business_rules_report(enhanced_data)

    # Step 6: Export results
    export_results_to_csv(enhanced_data)

    # Final summary
    end_time = pd.Timestamp.now()
    duration = end_time - start_time

    print("\n" + "="*70)
    print("PIPELINE TEST COMPLETED SUCCESSFULLY")
    print("="*70)
    print(f"Duration: {duration}")
    print(f"Enhanced datasets: {len(enhanced_data)}")

    total_enhanced_records = sum(len(df) for df in enhanced_data.values())
    print(f"Total enhanced records: {total_enhanced_records:,}")

    print("\n[SUCCESS] The IC'ALPS pipeline is ready for Excel integration!")
    print("\nNext steps:")
    print("1. Open Excel and enable xlwings add-in")
    print("2. Run xlwings scripts to load and process data")
    print("3. Generate interactive dashboards")

    return True

if __name__ == "__main__":
    try:
        success = run_full_pipeline_test()
        if success:
            logger.info("Pipeline test completed successfully")
            sys.exit(0)
        else:
            logger.error("Pipeline test failed")
            sys.exit(1)

    except Exception as e:
        logger.error(f"Pipeline test crashed: {str(e)}")
        print(f"\n[ERROR] Pipeline test crashed: {str(e)}")
        sys.exit(1)