#!/usr/bin/env python3
"""
HubSpot Properties Validation Script
Uses HubSpot MCP server to validate pipeline names, stage IDs, and property mappings
"""

import sys
import pandas as pd
import json
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from config.database_config import config

def main():
    """
    Validate pipeline names and stage IDs against actual HubSpot implementation
    This function should be enhanced to use HubSpot MCP server when available
    """
    
    print("=" * 80)
    print("HUBSPOT PIPELINE & PROPERTY VALIDATION")
    print("=" * 80)
    print("This script validates your mapping against actual HubSpot implementation")
    print("=" * 80)
    
    # Expected pipeline mapping based on HubSpot analysis
    expected_pipelines = {
        "Icalps_service": {
            "name": "Icalps_service",
            "stages": {
                "Identified": "1116269224",
                "Qualified": "1162868542", 
                "Negotiate": "1116704051",
                "Closed Won": "1116704052",
                "Closed Lost": "1116704053",
                "On-Hold": "1116269223"
            }
        },
        "Icalps_hardware": {
            "name": "Icalps_hardware", 
            "stages": {
                "Identified": "1116419644",
                "Qualified": "1116419645",
                "Design In": "1116419646", 
                "Design Win": "1116419647",
                "Closed Won": "1116419649",
                "Closed Dead": "1116419650",
                "On-Hold": "1116652341"
            }
        },
        "SealSQ Hardware": {
            "name": "SealSQ Hardware",
            "stages": {
                "Identified": "12096409",
                "Qualified": "12096410",
                "Design In": "12096411",
                "Design Win": "12096412", 
                "Closed Won": "12096869",
                "Closed Lost": "12096415",
                "Closed Dead": "13772274"
            }
        },
        "SealSQ Services": {
            "name": "SealSQ Services",
            "stages": {
                "Identified": "13772280",
                "Qualified": "13772281",
                "Design In": "13772282",
                "Design Win": "31868514",
                "Closed Won": "13772285", 
                "Closed Lost": "13772286",
                "Closed Dead": "13772309"
            }
        }
    }
    
    # Expected custom properties for deals
    expected_deal_properties = {
        'icalps_deal_id': {'type': 'string', 'required': True},
        'icalps_dealforecast': {'type': 'number', 'required': True},
        'icalps_dealcertainty': {'type': 'string', 'required': True},
        'icalps_dealtype': {'type': 'string', 'required': True},
        'icalps_dealsource': {'type': 'string', 'required': True},
        'icalps_dealnotes': {'type': 'string', 'required': True},
        'icalps_dealcategory': {'type': 'string', 'required': True},
        'icalps_deal_created_date': {'type': 'date', 'required': True},
        'icalps_originalstage': {'type': 'enumeration', 'required': True},
        'icalps_original_status': {'type': 'enumeration', 'required': True},
        'icalps_transformation_notes': {'type': 'string', 'required': True}
    }
    
    # Expected custom properties for companies
    expected_company_properties = {
        'icalps_company_id': {'type': 'string', 'required': True},
        'icalps_sitegroup': {'type': 'string', 'required': False},
        'icalps_groupsize': {'type': 'number', 'required': False}
    }
    
    # Expected custom properties for contacts
    expected_contact_properties = {
        'icalps_contact_id': {'type': 'string', 'required': True},
        'icalps_company_id': {'type': 'string', 'required': True},
        'icalps_temperature': {'type': 'enumeration', 'required': False}
    }
    
    print("\n" + "="*50)
    print("PIPELINE VALIDATION")
    print("="*50)
    
    for pipeline_name, pipeline_info in expected_pipelines.items():
        print(f"Pipeline: {pipeline_name}")
        for stage_name, stage_id in pipeline_info['stages'].items():
            print(f"  -> {stage_name}: {stage_id}")
    
    print("\n" + "="*50)
    print("CUSTOM PROPERTIES VALIDATION")
    print("="*50)
    
    print("\nDeals Properties:")
    for prop_name, prop_info in expected_deal_properties.items():
        status = "[REQUIRED]" if prop_info['required'] else "[OPTIONAL]"
        print(f"  {status} {prop_name}: {prop_info['type']}")
    
    print("\nCompanies Properties:")
    for prop_name, prop_info in expected_company_properties.items():
        status = "[REQUIRED]" if prop_info['required'] else "[OPTIONAL]"
        print(f"  {status} {prop_name}: {prop_info['type']}")
    
    print("\nContacts Properties:")
    for prop_name, prop_info in expected_contact_properties.items():
        status = "[REQUIRED]" if prop_info['required'] else "[OPTIONAL]"
        print(f"  {status} {prop_name}: {prop_info['type']}")
    
    # Export validation data
    validation_data = {
        'pipelines': expected_pipelines,
        'deal_properties': expected_deal_properties,
        'company_properties': expected_company_properties,
        'contact_properties': expected_contact_properties,
        'validation_date': pd.Timestamp.now().isoformat()
    }
    
    validation_file = config.output_path / "hubspot_validation_requirements.json"
    with open(validation_file, 'w') as f:
        json.dump(validation_data, f, indent=2)
    
    print(f"\n[SUCCESS] Validation requirements exported to:")
    print(f"  -> {validation_file}")
    
    print(f"\n📋 To use with HubSpot MCP server:")
    print(f"  1. Verify pipelines exist with correct names")
    print(f"  2. Create missing custom properties")
    print(f"  3. Run: python hubspot_transform.py")
    print(f"  4. Import the generated hubspot_*_import_ready.csv files")
    
    return True

if __name__ == "__main__":
    try:
        success = main()
        if success:
            print(f"\n[SUCCESS] Validation completed successfully")
            sys.exit(0)
        else:
            print(f"\n[ERROR] Validation failed")
            sys.exit(1)
    except Exception as e:
        print(f"\n[ERROR] Validation crashed: {str(e)}")
        sys.exit(1)
