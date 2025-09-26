#!/usr/bin/env python3
"""
IC'ALPS HubSpot Transformation Standalone Script
Final transformation step using HubSpot MCP server validation

This script can be run independently to transform success files into HubSpot-ready imports
"""

import sys
import pandas as pd
from pathlib import Path
import logging

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from processors.hubspot_transformation_processor import hubspot_transformation_processor
from config.database_config import config

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('hubspot_transformation.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def main():
    """Main function for standalone HubSpot transformation"""
    
    print("=" * 80)
    print("IC'ALPS HUBSPOT TRANSFORMATION (Standalone)")
    print("=" * 80)
    print("This script transforms your success files into HubSpot-ready import files")
    print("Requires: Enhanced pipeline success files to be already generated")
    print("=" * 80)
    
    try:
        output_path = config.output_path
        
        # Check if success files exist
        required_files = {
            'deals': output_path / "deals_transformed_tohubspot_success.csv",
            'companies': output_path / "companies_enhanced_success.csv",
            'contacts': output_path / "contacts_enhanced_success.csv"
        }
        
        missing_files = []
        for key, file_path in required_files.items():
            if not file_path.exists():
                missing_files.append(str(file_path))
        
        if missing_files:
            print(f"\n[ERROR] Missing required success files:")
            for file in missing_files:
                print(f"  -> {file}")
            print(f"\nRun the enhanced pipeline first:")
            print(f"  python main_pipeline.py --mode enhanced")
            return False
        
        print("\n[SUCCESS] All required success files found")
        
        # Load the data
        logger.info("Loading success files...")
        loaded_data = {}
        
        for key, file_path in required_files.items():
            df = pd.read_csv(file_path)
            loaded_data[key] = df
            print(f"[LOADED] {key:10} -> {len(df):8} records")
        
        # Load optional files
        optional_files = {
            'communications': output_path / "communications_associations_success.csv",
            'site_aggregation': output_path / "companies_site_aggregation_success.csv"
        }
        
        for key, file_path in optional_files.items():
            if file_path.exists():
                df = pd.read_csv(file_path)
                loaded_data[key] = df
                print(f"[LOADED] {key:10} -> {len(df):8} records")
            else:
                print(f"[SKIP]   {key:10} -> File not found (optional)")
        
        # Transform for HubSpot
        logger.info("Starting HubSpot transformation...")
        print(f"\n{'='*50}")
        print("HUBSPOT TRANSFORMATION WITH MCP VALIDATION")
        print(f"{'='*50}")
        
        hubspot_data = {}
        
        # Transform deals
        if 'deals' in loaded_data:
            hubspot_deals = hubspot_transformation_processor.transform_deals_for_hubspot_import(
                loaded_data['deals']
            )
            hubspot_data['deals'] = hubspot_deals
            print(f"[SUCCESS] Deals     -> {len(hubspot_deals):8} HubSpot-ready deals")
        
        # Transform companies  
        if 'companies' in loaded_data:
            site_agg_df = loaded_data.get('site_aggregation')
            hubspot_companies = hubspot_transformation_processor.transform_companies_for_hubspot_import(
                loaded_data['companies'], site_agg_df
            )
            hubspot_data['companies'] = hubspot_companies
            print(f"[SUCCESS] Companies -> {len(hubspot_companies):8} HubSpot-ready companies")
        
        # Transform contacts
        if 'contacts' in loaded_data:
            hubspot_contacts = hubspot_transformation_processor.transform_contacts_for_hubspot_import(
                loaded_data['contacts']
            )
            hubspot_data['contacts'] = hubspot_contacts
            print(f"[SUCCESS] Contacts  -> {len(hubspot_contacts):8} HubSpot-ready contacts")
        
        # Transform communications
        if 'communications' in loaded_data:
            hubspot_engagements = hubspot_transformation_processor.transform_communications_for_hubspot_import(
                loaded_data['communications']
            )
            hubspot_data['engagements'] = hubspot_engagements
            print(f"[SUCCESS] Engagements -> {len(hubspot_engagements):8} HubSpot-ready engagements")
        
        # Validate and export
        logger.info("Validating and exporting HubSpot-ready files...")
        
        export_success = hubspot_transformation_processor.export_hubspot_ready_files(
            hubspot_data.get('deals', pd.DataFrame()),
            hubspot_data.get('companies', pd.DataFrame()),
            hubspot_data.get('contacts', pd.DataFrame()),
            hubspot_data.get('engagements', pd.DataFrame()),
            output_path
        )
        
        if export_success:
            print(f"\n{'='*80}")
            print("HUBSPOT TRANSFORMATION COMPLETED SUCCESSFULLY")
            print(f"{'='*80}")
            
            total_records = sum(len(df) for df in hubspot_data.values())
            print(f"Total HubSpot-ready records: {total_records:,}")
            
            print(f"\n[SUCCESS] Files ready for HubSpot MCP import:")
            print(f"  -> hubspot_deals_import_ready.csv")
            print(f"  -> hubspot_companies_import_ready.csv") 
            print(f"  -> hubspot_contacts_import_ready.csv")
            print(f"  -> hubspot_engagements_import_ready.csv")
            print(f"  -> hubspot_import_summary.json")
            
            print(f"\n📋 Import Order for HubSpot:")
            print(f"  1. Import companies first")
            print(f"  2. Import contacts second")
            print(f"  3. Import deals third")
            print(f"  4. Create associations")
            print(f"  5. Import engagements last")
            
            return True
        else:
            print(f"\n[ERROR] Failed to export HubSpot-ready files")
            return False
        
    except Exception as e:
        logger.error(f"HubSpot transformation failed: {str(e)}")
        print(f"\n[ERROR] HubSpot transformation crashed: {str(e)}")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
