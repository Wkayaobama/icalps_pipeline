#!/usr/bin/env python3
"""
IC'ALPS Pipeline Runner Script
Simple interface to run the complete data pipeline
"""

import subprocess
import sys
from pathlib import Path

def run_pipeline_mode(mode='enhanced'):
    """Run the pipeline in specified mode"""
    
    mode_descriptions = {
        'enhanced': 'ENHANCED (Business Rules → Associations → Site Aggregation)',
        'hubspot': 'HUBSPOT (Final Transformation with MCP Validation)',
        'amended': 'AMENDED (Enhanced Pipeline for legacy_amended files)',
        'test': 'TEST (Validation Only)',
        'legacy': 'LEGACY (Original Pipeline)'
    }
    
    print(f"🚀 Starting IC'ALPS Pipeline in {mode_descriptions.get(mode, mode.upper())} mode...")
    print("=" * 80)
    
    try:
        # Run the main pipeline
        result = subprocess.run([
            sys.executable, 'main_pipeline.py', '--mode', mode
        ], capture_output=False, text=True, cwd=Path(__file__).parent)
        
        if result.returncode == 0:
            print(f"\n✅ Pipeline {mode} completed successfully!")
            
            if mode == 'enhanced':
                print("\n📁 Generated files with business transformation:")
                print("   -> deals_transformed_tohubspot_success.csv")
                print("   -> communications_associations_success.csv")
                print("   -> social_networks_associations_success.csv")
                print("   -> companies_site_aggregation_success.csv")
                print("   -> companies_enhanced_success.csv")
                print("   -> contacts_enhanced_success.csv")
                
                print("\n🔗 Enhanced features:")
                print("   1. Business transformation applied to deals")
                print("   2. Communication associations with transformed deals")
                print("   3. Social networks entity mapping")
                print("   4. Child-to-parent company site aggregation")
                print("   5. Ready for HubSpot upload with success tracking")
            
            elif mode == 'amended':
                print("\n📁 Generated files from enhanced legacy_amended data:")
                print("   -> deals_transformed_tohubspot_success_amended.csv")
                print("   -> communications_associations_success_amended.csv")
                print("   -> companies_site_aggregation_success_amended.csv")
                print("   -> companies_enhanced_success_amended.csv")
                print("   -> contacts_enhanced_success_amended.csv")
                
                print("\n🔗 Enhanced amended features:")
                print("   1. Richer company data (34 columns): phone, email, revenue, employees")
                print("   2. Enhanced contact data (31 columns): title, department, mobile phone")
                print("   3. Improved opportunity data (28 columns): product names, enhanced metadata")
                print("   4. Same business transformation logic applied")
                print("   5. Compatible with existing HubSpot transformation")
            
            elif mode == 'hubspot':
                print("\n📁 Generated HubSpot-ready import files:")
                print("   -> hubspot_deals_import_ready.csv")
                print("   -> hubspot_companies_import_ready.csv")
                print("   -> hubspot_contacts_import_ready.csv")
                print("   -> hubspot_engagements_import_ready.csv")
                print("   -> hubspot_import_summary.json")
                
                print("\n🔗 HubSpot MCP features:")
                print("   1. Pipeline names and stage IDs validated")
                print("   2. Property mappings verified against actual HubSpot")
                print("   3. Import batches prepared with metadata")
                print("   4. Association references ready for linking")
                print("   5. Custom properties list for creation")
            
            elif mode == 'legacy':
                print("\n📁 Generated legacy format files:")
                print("   -> enhanced_companies.csv")
                print("   -> enhanced_opportunities.csv")
                print("   -> enhanced_persons.csv")
            
            return True
        else:
            print(f"\n❌ Pipeline {mode} failed!")
            return False
            
    except Exception as e:
        print(f"\n❌ Error running pipeline: {str(e)}")
        return False

def main():
    """Main function with menu options"""
    
    print("IC'ALPS Data Pipeline Runner")
    print("=" * 40)
    print("Choose pipeline mode:")
    print("1. Enhanced (Business Rules → Associations → Site Aggregation)")
    print("2. HubSpot (Final transformation with MCP validation)")
    print("3. Amended (Enhanced pipeline for legacy_amended files)")
    print("4. Test (Validation and testing only)")
    print("5. Legacy (Original pipeline)")
    print("6. Exit")
    
    while True:
        try:
            choice = input("\nEnter choice (1-6): ").strip()
            
            if choice == '1':
                return run_pipeline_mode('enhanced')
            elif choice == '2':
                return run_pipeline_mode('hubspot')
            elif choice == '3':
                return run_pipeline_mode('amended')
            elif choice == '4':
                return run_pipeline_mode('test')
            elif choice == '5':
                return run_pipeline_mode('legacy')
            elif choice == '6':
                print("Goodbye!")
                return True
            else:
                print("Invalid choice. Please enter 1, 2, 3, 4, 5, or 6.")
                
        except KeyboardInterrupt:
            print("\n\nOperation cancelled by user.")
            return False
        except Exception as e:
            print(f"Error: {str(e)}")
            return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
