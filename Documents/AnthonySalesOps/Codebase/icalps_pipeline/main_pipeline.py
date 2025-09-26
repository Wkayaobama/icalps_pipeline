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
from extractors.bronze_extractor_amended import bronze_extractor_amended
from processors.duckdb_engine import duckdb_processor
from processors.business_transformation_processor import business_transformation_processor
from processors.associations_processor import associations_processor
from processors.site_aggregation_processor import site_aggregation_processor
from processors.hubspot_transformation_processor import hubspot_transformation_processor
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

def apply_business_transformation(processed_data):
    """Apply business transformation rules to deals (critical intermediary step)"""
    logger.info("Applying business transformation rules...")
    
    print("\n" + "="*50)
    print("BUSINESS TRANSFORMATION (deals_transformed_tohubspot)")
    print("="*50)
    
    if not processed_data:
        print("[ERROR] No processed data available for transformation")
        return None
    
    companies_df = processed_data.get('companies')
    contacts_df = processed_data.get('persons') 
    opportunities_df = processed_data.get('opportunities')
    
    if not all([companies_df is not None, contacts_df is not None, opportunities_df is not None]):
        print("[ERROR] Missing core data for business transformation")
        return None
    
    try:
        # Apply business transformation to deals
        transformed_deals_df = business_transformation_processor.transform_deals_to_hubspot_format(
            opportunities_df, companies_df, contacts_df
        )
        
        if len(transformed_deals_df) == 0:
            print("[ERROR] Business transformation produced no results")
            return None
        
        # Generate transformation report
        report = business_transformation_processor.generate_transformation_report(transformed_deals_df)
        
        print(f"[SUCCESS] Business Transformation -> {len(transformed_deals_df):8} deals transformed")
        print(f"          Studies Pipeline: {report['transformation_summary']['studies_count']}")
        print(f"          Sales Pipeline: {report['transformation_summary']['opportunities_count']}")
        print(f"          Total Deal Value: ${report['transformation_summary']['total_deal_value']:,.0f}")
        print(f"          Average Certainty: {report['transformation_summary']['average_certainty']:.1f}%")
        
        return transformed_deals_df
        
    except Exception as e:
        print(f"[ERROR] Business transformation failed: {str(e)}")
        return None

def create_communication_associations(processed_data, transformed_deals_df):
    """Create communication associations with transformed deals (intermediary step)"""
    logger.info("Creating communication associations...")
    
    print("\n" + "="*50)
    print("COMMUNICATION ASSOCIATIONS (intermediary step)")
    print("="*50)
    
    communications_df = processed_data.get('communications')
    companies_df = processed_data.get('companies')
    contacts_df = processed_data.get('persons')
    
    if not all([communications_df is not None, companies_df is not None, contacts_df is not None]):
        print("[ERROR] Missing data for communication associations")
        return None
    
    try:
        # Create communication associations using the transformed deals
        comm_associations_df = business_transformation_processor.create_communication_associations(
            communications_df, transformed_deals_df, companies_df, contacts_df
        )
        
        if len(comm_associations_df) == 0:
            print("[ERROR] Communication associations produced no results")
            return None
            
        # Calculate association statistics
        total_comms = len(comm_associations_df)
        deal_associations = len(comm_associations_df[comm_associations_df['deal_association_status'] == 'SUCCESS'])
        company_associations = len(comm_associations_df[comm_associations_df['company_association_status'] == 'SUCCESS'])
        contact_associations = len(comm_associations_df[comm_associations_df['contact_association_status'] == 'SUCCESS'])
        
        print(f"[SUCCESS] Communication Associations -> {total_comms:8} communications processed")
        print(f"          Deal Associations: {deal_associations}")
        print(f"          Company Associations: {company_associations}")
        print(f"          Contact Associations: {contact_associations}")
        
        return comm_associations_df
        
    except Exception as e:
        print(f"[ERROR] Communication associations failed: {str(e)}")
        return None

def process_social_networks(processed_data):
    """Process social networks associations"""
    logger.info("Processing social networks associations...")
    
    print("\n" + "="*50)
    print("SOCIAL NETWORKS ASSOCIATIONS")
    print("="*50)
    
    social_networks_df = processed_data.get('social_networks')
    companies_df = processed_data.get('companies')
    contacts_df = processed_data.get('persons')
    
    if not all([social_networks_df is not None, companies_df is not None, contacts_df is not None]):
        print("[ERROR] Missing data for social networks associations")
        return None
    
    try:
        # Process social networks associations
        social_associations_df = associations_processor.process_social_networks_with_associations(
            social_networks_df, companies_df, contacts_df
        )
        
        if len(social_associations_df) == 0:
            print("[WARNING] Social networks associations produced no results")
            return pd.DataFrame()
        
        # Calculate statistics
        total_social = len(social_associations_df)
        company_social = len(social_associations_df[social_associations_df['entity_type'] == 'Company'])
        contact_social = len(social_associations_df[social_associations_df['entity_type'] == 'Person'])
        
        print(f"[SUCCESS] Social Networks -> {total_social:8} associations processed")
        print(f"          Company Social Links: {company_social}")
        print(f"          Contact Social Links: {contact_social}")
        
        return social_associations_df
        
    except Exception as e:
        print(f"[ERROR] Social networks processing failed: {str(e)}")
        return None

def apply_site_aggregation(processed_data):
    """Apply site aggregation with child-to-parent company logic (after business transformation)"""
    logger.info("Applying site aggregation...")
    
    print("\n" + "="*50)
    print("SITE AGGREGATION (child-to-parent logic)")
    print("="*50)
    
    companies_df = processed_data.get('companies')
    contacts_df = processed_data.get('persons')
    
    if not all([companies_df is not None, contacts_df is not None]):
        print("[ERROR] Missing companies or contacts data for site aggregation")
        return None
    
    try:
        # Apply site aggregation logic
        site_aggregation_df = site_aggregation_processor.process_site_aggregation(
            companies_df, contacts_df
        )
        
        if len(site_aggregation_df) == 0:
            print("[WARNING] Site aggregation produced no multi-site groups")
            return pd.DataFrame()
        
        # Generate aggregation report
        report = site_aggregation_processor.create_site_aggregation_report(site_aggregation_df)
        
        print(f"[SUCCESS] Site Aggregation -> {len(site_aggregation_df):8} records created")
        print(f"          Site Records: {report['summary']['site_records']}")
        print(f"          Standalone Records: {report['summary']['standalone_records']}")
        print(f"          Parent Records: {report['summary']['parent_aggregator_records']}")
        print(f"          Total Contacts: {report['summary']['total_contacts']}")
        
        return site_aggregation_df
        
    except Exception as e:
        print(f"[ERROR] Site aggregation failed: {str(e)}")
        return None

def export_enhanced_pipeline_results(processed_data, transformed_deals_df, comm_associations_df, social_associations_df, site_aggregation_df):
    """Export all enhanced pipeline results with success suffix"""
    logger.info("Exporting enhanced pipeline results...")
    
    print("\n" + "="*50)
    print("ENHANCED PIPELINE EXPORT (success suffix)")
    print("="*50)
    
    output_path = config.output_path
    output_path.mkdir(exist_ok=True)
    
    exports_completed = 0
    
    try:
        # Export 1: Transformed deals (core business transformation)
        if transformed_deals_df is not None and len(transformed_deals_df) > 0:
            deals_file = output_path / "deals_transformed_tohubspot_success.csv"
            transformed_deals_df.to_csv(deals_file, index=False)
            print(f"[SUCCESS] Transformed Deals -> deals_transformed_tohubspot_success.csv")
            print(f"          {len(transformed_deals_df):8} deals with business rules applied")
            exports_completed += 1
        
        # Export 2: Communication associations
        if comm_associations_df is not None and len(comm_associations_df) > 0:
            comm_file = output_path / "communications_associations_success.csv"
            comm_associations_df.to_csv(comm_file, index=False)
            print(f"[SUCCESS] Communication Assoc -> communications_associations_success.csv")
            print(f"          {len(comm_associations_df):8} communications with associations")
            exports_completed += 1
        
        # Export 3: Social networks associations
        if social_associations_df is not None and len(social_associations_df) > 0:
            social_file = output_path / "social_networks_associations_success.csv"
            social_associations_df.to_csv(social_file, index=False)
            print(f"[SUCCESS] Social Networks    -> social_networks_associations_success.csv")
            print(f"          {len(social_associations_df):8} social links with associations")
            exports_completed += 1
        
        # Export 4: Site aggregation
        if site_aggregation_df is not None and len(site_aggregation_df) > 0:
            site_file = output_path / "companies_site_aggregation_success.csv"
            site_aggregation_df.to_csv(site_file, index=False)
            print(f"[SUCCESS] Site Aggregation   -> companies_site_aggregation_success.csv")
            print(f"          {len(site_aggregation_df):8} aggregation records")
            exports_completed += 1
        
        # Export 5: Enhanced companies (with site grouping logic applied)
        if processed_data.get('companies') is not None:
            companies_enhanced = processed_data['companies'].copy()
            # Add site grouping information if available
            if site_aggregation_df is not None and len(site_aggregation_df) > 0:
                companies_enhanced = companies_enhanced.merge(
                    site_aggregation_df[['company_id', 'parent_company_id', 'has_multiple_sites', 'site_order']],
                    left_on='Comp_CompanyId', right_on='company_id', how='left'
                )
            
            companies_file = output_path / "companies_enhanced_success.csv"
            companies_enhanced.to_csv(companies_file, index=False)
            print(f"[SUCCESS] Enhanced Companies -> companies_enhanced_success.csv")
            print(f"          {len(companies_enhanced):8} companies with site logic")
            exports_completed += 1
        
        # Export 6: Enhanced contacts
        if processed_data.get('persons') is not None:
            contacts_file = output_path / "contacts_enhanced_success.csv"
            processed_data['persons'].to_csv(contacts_file, index=False)
            print(f"[SUCCESS] Enhanced Contacts  -> contacts_enhanced_success.csv")
            print(f"          {len(processed_data['persons']):8} contacts")
            exports_completed += 1
        
        print(f"\n[SUCCESS] {exports_completed} enhanced pipeline files exported")
        return exports_completed > 0
        
    except Exception as e:
        print(f"[ERROR] Enhanced pipeline export failed: {str(e)}")
        return False

def run_enhanced_pipeline():
    """
    Run the enhanced pipeline with proper business transformation sequence:
    1. CSV → Bronze → DuckDB
    2. Business Transformation (deals → HubSpot format)  
    3. Communication Associations (with transformed deals)
    4. Site Aggregation (child-to-parent logic)
    5. Export with success suffix
    """
    print("="*70)
    print("IC'ALPS ENHANCED PIPELINE (Business Rules → Associations → Site Aggregation)")
    print("="*70)
    
    start_time = pd.Timestamp.now()
    
    try:
        # Step 1: Validate CSV files
        logger.info("Step 1: Validating CSV files...")
        if not test_csv_validation():
            print("\n[ERROR] Pipeline failed: Missing CSV files")
            return False
        
        # Step 2: Extract Bronze data
        logger.info("Step 2: Extracting Bronze layer data...")
        bronze_data = test_bronze_extraction()
        if not bronze_data:
            print("\n[ERROR] Pipeline failed: Bronze extraction failed")
            return False
        
        # Step 3: Process through DuckDB
        logger.info("Step 3: Processing through DuckDB...")
        processed_data = test_duckdb_processing(bronze_data)
        if not processed_data:
            print("\n[ERROR] Pipeline failed: DuckDB processing failed")
            return False
        
        # Step 4: Apply Business Transformation (CRITICAL INTERMEDIARY STEP)
        logger.info("Step 4: Applying business transformation...")
        transformed_deals_df = apply_business_transformation(processed_data)
        if transformed_deals_df is None:
            print("\n[ERROR] Pipeline failed: Business transformation failed")
            return False
        
        # Step 5: Create Communication Associations (with transformed deals)
        logger.info("Step 5: Creating communication associations...")
        comm_associations_df = create_communication_associations(processed_data, transformed_deals_df)
        if comm_associations_df is None:
            print("\n[WARNING] Communication associations failed - continuing...")
            comm_associations_df = pd.DataFrame()
        
        # Step 6: Process Social Networks Associations
        logger.info("Step 6: Processing social networks associations...")
        social_associations_df = process_social_networks(processed_data)
        if social_associations_df is None:
            print("\n[WARNING] Social networks processing failed - continuing...")
            social_associations_df = pd.DataFrame()
        
        # Step 7: Apply Site Aggregation (child-to-parent company logic)
        logger.info("Step 7: Applying site aggregation...")
        site_aggregation_df = apply_site_aggregation(processed_data)
        if site_aggregation_df is None:
            print("\n[WARNING] Site aggregation failed - continuing...")
            site_aggregation_df = pd.DataFrame()
        
        # Step 8: Export all enhanced results
        logger.info("Step 8: Exporting enhanced pipeline results...")
        export_success = export_enhanced_pipeline_results(
            processed_data, transformed_deals_df, comm_associations_df, social_associations_df, site_aggregation_df
        )
        
        if not export_success:
            print("\n[ERROR] Pipeline failed: Export failed")
            return False
        
        # Final summary
        end_time = pd.Timestamp.now()
        duration = end_time - start_time
        
        print("\n" + "="*70)
        print("ENHANCED PIPELINE COMPLETED SUCCESSFULLY")
        print("="*70)
        print(f"Duration: {duration}")
        
        # Show results summary
        total_transformed_deals = len(transformed_deals_df)
        total_comm_associations = len(comm_associations_df)
        total_social_associations = len(social_associations_df)
        total_site_records = len(site_aggregation_df)
        
        print(f"Business-Transformed Deals: {total_transformed_deals:,}")
        print(f"Communication Associations: {total_comm_associations:,}")
        print(f"Social Network Associations: {total_social_associations:,}")
        print(f"Site Aggregation Records: {total_site_records:,}")
        
        print("\n[SUCCESS] Enhanced pipeline ready for HubSpot!")
        print("\nGenerated files with business rules applied:")
        print("  -> deals_transformed_tohubspot_success.csv")
        print("  -> communications_associations_success.csv")
        print("  -> social_networks_associations_success.csv")
        print("  -> companies_site_aggregation_success.csv")
        print("  -> companies_enhanced_success.csv")
        print("  -> contacts_enhanced_success.csv")
        
        return True
        
    except Exception as e:
        logger.error(f"Enhanced pipeline failed: {str(e)}")
        print(f"\n[ERROR] Enhanced pipeline crashed: {str(e)}")
        return False

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

def run_hubspot_transformation_pipeline():
    """
    Final HubSpot transformation pipeline using MCP server validation
    Reads success files and creates HubSpot-ready import files
    """
    print("="*70)
    print("HUBSPOT TRANSFORMATION PIPELINE (MCP Server Validated)")
    print("="*70)
    
    start_time = pd.Timestamp.now()
    
    try:
        output_path = config.output_path
        
        # Step 1: Load success files
        logger.info("Step 1: Loading success files...")
        print("\n" + "="*50)
        print("LOADING SUCCESS FILES")
        print("="*50)
        
        success_files = {
            'deals': output_path / "deals_transformed_tohubspot_success.csv",
            'companies': output_path / "companies_enhanced_success.csv", 
            'contacts': output_path / "contacts_enhanced_success.csv",
            'communications': output_path / "communications_associations_success.csv",
            'site_aggregation': output_path / "companies_site_aggregation_success.csv"
        }
        
        loaded_data = {}
        for key, file_path in success_files.items():
            if file_path.exists():
                df = pd.read_csv(file_path)
                loaded_data[key] = df
                print(f"[SUCCESS] {key:15} -> {len(df):8} records loaded")
            else:
                print(f"[WARNING] {key:15} -> File not found: {file_path}")
        
        if len(loaded_data) == 0:
            print("[ERROR] No success files found - run enhanced pipeline first")
            return False
        
        # Step 2: Transform for HubSpot import
        logger.info("Step 2: Transforming for HubSpot import...")
        print("\n" + "="*50)
        print("HUBSPOT TRANSFORMATION (MCP Validated)")
        print("="*50)
        
        hubspot_data = {}
        
        # Transform deals
        if 'deals' in loaded_data:
            hubspot_deals = hubspot_transformation_processor.transform_deals_for_hubspot_import(
                loaded_data['deals']
            )
            hubspot_data['deals'] = hubspot_deals
            print(f"[SUCCESS] Deals HubSpot Transform -> {len(hubspot_deals):8} deals ready")
        
        # Transform companies
        if 'companies' in loaded_data:
            site_agg_df = loaded_data.get('site_aggregation')
            hubspot_companies = hubspot_transformation_processor.transform_companies_for_hubspot_import(
                loaded_data['companies'], site_agg_df
            )
            hubspot_data['companies'] = hubspot_companies
            print(f"[SUCCESS] Companies HubSpot Transform -> {len(hubspot_companies):8} companies ready")
        
        # Transform contacts
        if 'contacts' in loaded_data:
            hubspot_contacts = hubspot_transformation_processor.transform_contacts_for_hubspot_import(
                loaded_data['contacts']
            )
            hubspot_data['contacts'] = hubspot_contacts
            print(f"[SUCCESS] Contacts HubSpot Transform -> {len(hubspot_contacts):8} contacts ready")
        
        # Transform communications
        if 'communications' in loaded_data:
            hubspot_engagements = hubspot_transformation_processor.transform_communications_for_hubspot_import(
                loaded_data['communications']
            )
            hubspot_data['engagements'] = hubspot_engagements
            print(f"[SUCCESS] Engagements HubSpot Transform -> {len(hubspot_engagements):8} engagements ready")
        
        # Step 3: Validate HubSpot readiness
        logger.info("Step 3: Validating HubSpot readiness...")
        print("\n" + "="*50)
        print("HUBSPOT READINESS VALIDATION")
        print("="*50)
        
        if 'deals' in hubspot_data:
            validation = hubspot_transformation_processor.validate_hubspot_readiness(hubspot_data['deals'])
            print(f"[INFO] Validation Status: {validation['overall_status']}")
            print(f"[INFO] Ready Deals: {validation['ready_count']}")
            
            if validation['issues']:
                for issue in validation['issues']:
                    print(f"[ERROR] {issue}")
            
            if validation['warnings']:
                for warning in validation['warnings']:
                    print(f"[WARNING] {warning}")
        
        # Step 4: Export HubSpot-ready files
        logger.info("Step 4: Exporting HubSpot-ready files...")
        print("\n" + "="*50)
        print("HUBSPOT IMPORT FILES EXPORT")
        print("="*50)
        
        export_success = hubspot_transformation_processor.export_hubspot_ready_files(
            hubspot_data.get('deals', pd.DataFrame()),
            hubspot_data.get('companies', pd.DataFrame()),
            hubspot_data.get('contacts', pd.DataFrame()),
            hubspot_data.get('engagements', pd.DataFrame()),
            output_path
        )
        
        if not export_success:
            print("[ERROR] Failed to export HubSpot-ready files")
            return False
        
        print(f"[SUCCESS] HubSpot-ready files exported:")
        print(f"  -> hubspot_deals_import_ready.csv")
        print(f"  -> hubspot_companies_import_ready.csv")
        print(f"  -> hubspot_contacts_import_ready.csv")
        print(f"  -> hubspot_engagements_import_ready.csv")
        print(f"  -> hubspot_import_summary.json")
        
        # Final summary
        end_time = pd.Timestamp.now()
        duration = end_time - start_time
        
        print("\n" + "="*70)
        print("HUBSPOT TRANSFORMATION COMPLETED SUCCESSFULLY")
        print("="*70)
        print(f"Duration: {duration}")
        
        total_records = sum(len(df) for df in hubspot_data.values())
        print(f"Total HubSpot-ready records: {total_records:,}")
        
        print("\n[SUCCESS] Data ready for HubSpot MCP import!")
        print("\nNext steps:")
        print("1. Use HubSpot MCP server to create missing custom properties")
        print("2. Import companies first, then contacts, then deals")
        print("3. Create associations using the import batch IDs")
        print("4. Import engagements last")
        
        return True
        
    except Exception as e:
        logger.error(f"HubSpot transformation pipeline failed: {str(e)}")
        print(f"\n[ERROR] HubSpot transformation pipeline crashed: {str(e)}")
        return False

def test_bronze_extraction_amended():
    """Test Bronze layer data extraction for amended files"""
    logger.info("Testing Bronze layer extraction for amended files...")

    bronze_data = bronze_extractor_amended.extract_all_bronze_amended_data()

    print("\n" + "="*50)
    print("BRONZE LAYER EXTRACTION RESULTS (AMENDED)")
    print("="*50)

    if not bronze_data:
        print("[ERROR] No Bronze amended data extracted")
        return None

    for entity_type, df in bronze_data.items():
        print(f"{entity_type:20} {len(df):8} records  {len(df.columns):3} columns")

    total_records = sum(len(df) for df in bronze_data.values())
    print(f"\nTotal Bronze amended records: {total_records:,}")

    return bronze_data

def apply_business_transformation_amended(processed_data):
    """Apply business transformation rules to amended deals data"""
    logger.info("Applying business transformation rules to amended data...")
    
    print("\n" + "="*50)
    print("BUSINESS TRANSFORMATION (AMENDED)")
    print("="*50)
    
    if not processed_data:
        print("[ERROR] No processed amended data available for transformation")
        return None
    
    companies_df = processed_data.get('companies')
    contacts_df = processed_data.get('persons')  # Use 'persons' key for amended data 
    opportunities_df = processed_data.get('opportunities')
    
    # Debug logging for amended data
    logger.info(f"Amended data available: {list(processed_data.keys())}")
    logger.info(f"Companies: {companies_df is not None} ({len(companies_df) if companies_df is not None else 0} records)")
    logger.info(f"Persons: {contacts_df is not None} ({len(contacts_df) if contacts_df is not None else 0} records)")
    logger.info(f"Opportunities: {opportunities_df is not None} ({len(opportunities_df) if opportunities_df is not None else 0} records)")
    
    if not all([companies_df is not None, contacts_df is not None, opportunities_df is not None]):
        print("[ERROR] Missing core amended data for business transformation")
        missing = []
        if companies_df is None: missing.append('companies')
        if contacts_df is None: missing.append('persons')  
        if opportunities_df is None: missing.append('opportunities')
        print(f"        Missing: {missing}")
        return None
    
    try:
        # Apply business transformation to amended deals
        transformed_deals_df = business_transformation_processor.transform_deals_to_hubspot_format(
            opportunities_df, companies_df, contacts_df
        )
        
        if len(transformed_deals_df) == 0:
            print("[ERROR] Business transformation of amended data produced no results")
            return None
        
        # Generate transformation report
        report = business_transformation_processor.generate_transformation_report(transformed_deals_df)
        
        print(f"[SUCCESS] Amended Business Transform -> {len(transformed_deals_df):8} deals transformed")
        print(f"          Studies Pipeline: {report['transformation_summary']['studies_count']}")
        print(f"          Sales Pipeline: {report['transformation_summary']['opportunities_count']}")
        print(f"          Total Deal Value: ${report['transformation_summary']['total_deal_value']:,.0f}")
        print(f"          Average Certainty: {report['transformation_summary']['average_certainty']:.1f}%")
        
        return transformed_deals_df
        
    except Exception as e:
        print(f"[ERROR] Amended business transformation failed: {str(e)}")
        return None

def create_communication_associations_amended(processed_data, transformed_deals_df):
    """Create communication associations with amended transformed deals"""
    logger.info("Creating communication associations for amended data...")
    
    print("\n" + "="*50)
    print("COMMUNICATION ASSOCIATIONS (AMENDED)")
    print("="*50)
    
    communications_df = processed_data.get('communications')
    companies_df = processed_data.get('companies')
    contacts_df = processed_data.get('contacts')
    
    if not all([communications_df is not None, companies_df is not None, contacts_df is not None]):
        print("[ERROR] Missing data for amended communication associations")
        return None
    
    try:
        # Create communication associations using the transformed deals
        comm_associations_df = business_transformation_processor.create_communication_associations(
            communications_df, transformed_deals_df, companies_df, contacts_df
        )
        
        if len(comm_associations_df) == 0:
            print("[ERROR] Amended communication associations produced no results")
            return None
            
        # Calculate association statistics
        total_comms = len(comm_associations_df)
        deal_associations = len(comm_associations_df[comm_associations_df['deal_association_status'] == 'SUCCESS'])
        company_associations = len(comm_associations_df[comm_associations_df['company_association_status'] == 'SUCCESS'])
        contact_associations = len(comm_associations_df[comm_associations_df['contact_association_status'] == 'SUCCESS'])
        
        print(f"[SUCCESS] Amended Comm Associations -> {total_comms:8} communications processed")
        print(f"          Deal Associations: {deal_associations}")
        print(f"          Company Associations: {company_associations}")
        print(f"          Contact Associations: {contact_associations}")
        
        return comm_associations_df
        
    except Exception as e:
        print(f"[ERROR] Amended communication associations failed: {str(e)}")
        return None

def apply_site_aggregation_amended(processed_data):
    """Apply site aggregation with child-to-parent logic to amended data"""
    logger.info("Applying site aggregation to amended data...")
    
    print("\n" + "="*50)
    print("SITE AGGREGATION (AMENDED)")
    print("="*50)
    
    companies_df = processed_data.get('companies')
    contacts_df = processed_data.get('contacts')
    
    if not all([companies_df is not None, contacts_df is not None]):
        print("[ERROR] Missing companies or contacts data for amended site aggregation")
        return None
    
    try:
        # Apply site aggregation logic to amended data
        site_aggregation_df = site_aggregation_processor.process_site_aggregation(
            companies_df, contacts_df
        )
        
        if len(site_aggregation_df) == 0:
            print("[WARNING] Amended site aggregation produced no multi-site groups")
            return pd.DataFrame()
        
        # Generate aggregation report
        report = site_aggregation_processor.create_site_aggregation_report(site_aggregation_df)
        
        print(f"[SUCCESS] Amended Site Aggregation -> {len(site_aggregation_df):8} records created")
        print(f"          Site Records: {report['summary']['site_records']}")
        print(f"          Standalone Records: {report['summary']['standalone_records']}")
        print(f"          Parent Records: {report['summary']['parent_aggregator_records']}")
        print(f"          Total Contacts: {report['summary']['total_contacts']}")
        
        return site_aggregation_df
        
    except Exception as e:
        print(f"[ERROR] Amended site aggregation failed: {str(e)}")
        return None

def export_amended_results_with_success_suffix(processed_data, transformed_deals_df, comm_associations_df, site_aggregation_df):
    """Export all amended results to CSV files with 'success_amended' suffix"""
    logger.info("Exporting amended results with success_amended suffix...")

    print("\n" + "="*50)
    print("AMENDED EXPORT RESULTS (success_amended suffix)")
    print("="*50)

    output_path = config.output_path
    output_path.mkdir(exist_ok=True)

    exports_completed = 0

    try:
        # Export 1: Transformed deals (amended)
        if transformed_deals_df is not None and len(transformed_deals_df) > 0:
            deals_file = output_path / "deals_transformed_tohubspot_success_amended.csv"
            transformed_deals_df.to_csv(deals_file, index=False)
            print(f"[SUCCESS] Amended Transformed Deals -> deals_transformed_tohubspot_success_amended.csv")
            print(f"          {len(transformed_deals_df):8} deals with business rules applied")
            exports_completed += 1
        
        # Export 2: Communication associations (amended)
        if comm_associations_df is not None and len(comm_associations_df) > 0:
            comm_file = output_path / "communications_associations_success_amended.csv"
            comm_associations_df.to_csv(comm_file, index=False)
            print(f"[SUCCESS] Amended Communication Assoc -> communications_associations_success_amended.csv")
            print(f"          {len(comm_associations_df):8} communications with associations")
            exports_completed += 1
        
        # Export 3: Site aggregation (amended)
        if site_aggregation_df is not None and len(site_aggregation_df) > 0:
            site_file = output_path / "companies_site_aggregation_success_amended.csv"
            site_aggregation_df.to_csv(site_file, index=False)
            print(f"[SUCCESS] Amended Site Aggregation   -> companies_site_aggregation_success_amended.csv")
            print(f"          {len(site_aggregation_df):8} aggregation records")
            exports_completed += 1
        
        # Export 4: Enhanced companies (amended with richer data)
        if processed_data.get('companies') is not None:
            companies_enhanced = processed_data['companies'].copy()
            
            # Add site grouping information if available
            if site_aggregation_df is not None and len(site_aggregation_df) > 0:
                companies_enhanced = companies_enhanced.merge(
                    site_aggregation_df[['company_id', 'parent_company_id', 'has_multiple_sites', 'site_order']],
                    left_on='Comp_CompanyId', right_on='company_id', how='left'
                )
            
            companies_file = output_path / "companies_enhanced_success_amended.csv"
            companies_enhanced.to_csv(companies_file, index=False)
            print(f"[SUCCESS] Amended Enhanced Companies -> companies_enhanced_success_amended.csv")
            print(f"          {len(companies_enhanced):8} companies with enhanced data")
            exports_completed += 1
        
        # Export 5: Enhanced contacts (amended with richer data)
        if processed_data.get('persons') is not None:
            contacts_file = output_path / "contacts_enhanced_success_amended.csv"
            processed_data['persons'].to_csv(contacts_file, index=False)
            print(f"[SUCCESS] Amended Enhanced Contacts  -> contacts_enhanced_success_amended.csv")
            print(f"          {len(processed_data['persons']):8} contacts with enhanced data")
            exports_completed += 1
        
        print(f"\n[SUCCESS] {exports_completed} amended pipeline files exported")
        return exports_completed > 0
        
    except Exception as e:
        print(f"[ERROR] Amended pipeline export failed: {str(e)}")
        return False

def run_enhanced_pipeline_amended():
    """
    Run the enhanced pipeline for amended legacy files
    Same business logic applied to richer data structures
    """
    print("="*70)
    print("IC'ALPS ENHANCED PIPELINE (AMENDED FILES)")
    print("="*70)
    
    start_time = pd.Timestamp.now()
    
    try:
        # Step 1: Validate amended CSV files
        logger.info("Step 1: Validating amended CSV files...")
        if not bronze_extractor_amended.csv_connector.validate_amended_files():
            print("\n[ERROR] Amended pipeline failed: Missing CSV files")
            return False
        
        print("\n[SUCCESS] All amended CSV files validated")
        
        # Step 2: Extract Bronze data from amended files
        logger.info("Step 2: Extracting Bronze layer data from amended files...")
        bronze_data = test_bronze_extraction_amended()
        if not bronze_data:
            print("\n[ERROR] Amended pipeline failed: Bronze extraction failed")
            return False
        
        # Step 3: Process through DuckDB (bypass view creation for amended data)
        logger.info("Step 3: Processing amended data (bypass views)...")
        processed_data = {}
        
        # For amended data, use the bronze data directly since it's already well-structured
        print("\n" + "="*50)
        print("AMENDED DATA PROCESSING (Direct Bronze)")
        print("="*50)
        
        for entity_type, df in bronze_data.items():
            processed_data[entity_type] = df
            print(f"[SUCCESS] {entity_type:15} -> {len(df):8} records processed")
        
        if not processed_data:
            print("\n[ERROR] Amended pipeline failed: No processed data")
            return False
        
        # Step 4: Apply Business Transformation (same logic to amended data)
        logger.info("Step 4: Applying business transformation to amended data...")
        transformed_deals_df = apply_business_transformation_amended(processed_data)
        if transformed_deals_df is None:
            print("\n[ERROR] Amended pipeline failed: Business transformation failed")
            return False
        
        # Step 5: Create Communication Associations (with amended transformed deals)
        logger.info("Step 5: Creating communication associations for amended data...")
        comm_associations_df = create_communication_associations_amended(processed_data, transformed_deals_df)
        if comm_associations_df is None:
            print("\n[WARNING] Amended communication associations failed - continuing...")
            comm_associations_df = pd.DataFrame()
        
        # Step 6: Apply Site Aggregation (enhanced with richer company data)
        logger.info("Step 6: Applying site aggregation to amended data...")
        site_aggregation_df = apply_site_aggregation_amended(processed_data)
        if site_aggregation_df is None:
            print("\n[WARNING] Amended site aggregation failed - continuing...")
            site_aggregation_df = pd.DataFrame()
        
        # Step 7: Export all amended results
        logger.info("Step 7: Exporting amended pipeline results...")
        export_success = export_amended_results_with_success_suffix(
            processed_data, transformed_deals_df, comm_associations_df, site_aggregation_df
        )
        
        if not export_success:
            print("\n[ERROR] Amended pipeline failed: Export failed")
            return False
        
        # Final summary
        end_time = pd.Timestamp.now()
        duration = end_time - start_time
        
        print("\n" + "="*70)
        print("AMENDED PIPELINE COMPLETED SUCCESSFULLY")
        print("="*70)
        print(f"Duration: {duration}")
        
        # Show results summary
        total_transformed_deals = len(transformed_deals_df)
        total_comm_associations = len(comm_associations_df)
        total_site_records = len(site_aggregation_df)
        
        print(f"Amended Business-Transformed Deals: {total_transformed_deals:,}")
        print(f"Amended Communication Associations: {total_comm_associations:,}")
        print(f"Amended Site Aggregation Records: {total_site_records:,}")
        
        print("\n[SUCCESS] Amended pipeline ready for HubSpot!")
        print("\nGenerated files with enhanced data structures:")
        print("  -> deals_transformed_tohubspot_success_amended.csv")
        print("  -> communications_associations_success_amended.csv")
        print("  -> companies_site_aggregation_success_amended.csv")
        print("  -> companies_enhanced_success_amended.csv")
        print("  -> contacts_enhanced_success_amended.csv")
        
        return True
        
    except Exception as e:
        logger.error(f"Amended pipeline failed: {str(e)}")
        print(f"\n[ERROR] Amended pipeline crashed: {str(e)}")
        return False

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='IC\'ALPS Pipeline Runner')
    parser.add_argument('--mode', choices=['test', 'enhanced', 'legacy', 'hubspot', 'amended'], default='enhanced',
                        help='Pipeline mode: test (validation only), enhanced (business transformation), legacy (original), hubspot (final transformation), amended (enhanced pipeline for legacy_amended files)')
    
    args = parser.parse_args()
    
    try:
        if args.mode == 'test':
            print("Running in TEST mode (validation only)...")
            success = run_full_pipeline_test()
        elif args.mode == 'enhanced':
            print("Running in ENHANCED mode (with business transformation)...")
            success = run_enhanced_pipeline()
        elif args.mode == 'hubspot':
            print("Running in HUBSPOT mode (final transformation with MCP validation)...")
            success = run_hubspot_transformation_pipeline()
        elif args.mode == 'amended':
            print("Running in AMENDED mode (enhanced pipeline for legacy_amended files)...")
            success = run_enhanced_pipeline_amended()
        else:  # legacy
            print("Running in LEGACY mode (original pipeline)...")
            success = run_full_pipeline_test()
            
        if success:
            logger.info(f"Pipeline {args.mode} completed successfully")
            sys.exit(0)
        else:
            logger.error(f"Pipeline {args.mode} failed")
            sys.exit(1)

    except Exception as e:
        logger.error(f"Pipeline {args.mode} crashed: {str(e)}")
        print(f"\n[ERROR] Pipeline {args.mode} crashed: {str(e)}")
        sys.exit(1)