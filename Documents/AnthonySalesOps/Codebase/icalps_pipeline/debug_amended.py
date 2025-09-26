#!/usr/bin/env python3
"""
Debug script for amended file processing
"""

import sys
import pandas as pd
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / 'src'))

def test_amended_file_reading():
    """Test reading amended files with various approaches"""
    
    print("=" * 60)
    print("DEBUGGING AMENDED FILE READING")
    print("=" * 60)
    
    files_to_test = {
        'companies': 'input/legacy_amended_companies.csv',
        'contacts': 'input/legacy_amended_contacts.csv', 
        'opportunities': 'input/legacy_amended_opportunities.csv'
    }
    
    for file_type, file_path in files_to_test.items():
        print(f"\nTesting {file_type}: {file_path}")
        
        if not Path(file_path).exists():
            print(f"  [ERROR] File not found: {file_path}")
            continue
        
        # Test different reading approaches
        success = False
        
        # Approach 1: Standard reading
        try:
            df = pd.read_csv(file_path, encoding='utf-8-sig')
            print(f"  [SUCCESS] Standard read: {df.shape}")
            success = True
        except Exception as e:
            print(f"  [FAILED] Standard read: {str(e)[:100]}...")
        
        # Approach 2: Robust reading
        if not success:
            try:
                df = pd.read_csv(file_path, 
                               encoding='utf-8-sig',
                               on_bad_lines='skip',
                               engine='python',
                               dtype=str)
                print(f"  [SUCCESS] Robust read: {df.shape}")
                success = True
            except Exception as e:
                print(f"  [FAILED] Robust read: {str(e)[:100]}...")
        
        # Approach 3: Line-by-line reading  
        if not success:
            try:
                with open(file_path, 'r', encoding='utf-8-sig') as f:
                    lines = f.readlines()
                print(f"  [SUCCESS] Line read: {len(lines)} lines")
                
                # Try to parse first few lines
                for i, line in enumerate(lines[:3]):
                    cols = line.strip().split(',')
                    print(f"    Line {i}: {len(cols)} columns")
                    
            except Exception as e:
                print(f"  [FAILED] Line read: {str(e)[:100]}...")
        
        print(f"  Final status: {'SUCCESS' if success else 'FAILED'}")

def test_amended_extractors():
    """Test the amended extractors directly"""
    
    print("\n" + "=" * 60)
    print("TESTING AMENDED EXTRACTORS")
    print("=" * 60)
    
    try:
        from extractors.bronze_extractor_amended import bronze_extractor_amended
        
        # Test each extractor individually
        extractors = {
            'companies': bronze_extractor_amended.extract_bronze_companies_amended,
            'contacts': bronze_extractor_amended.extract_bronze_contacts_amended,
            'opportunities': bronze_extractor_amended.extract_bronze_opportunities_amended
        }
        
        for extractor_name, extractor_func in extractors.items():
            print(f"\nTesting {extractor_name} extractor...")
            try:
                result = extractor_func()
                if result is not None:
                    print(f"  [SUCCESS] {extractor_name}: {len(result)} records, {len(result.columns)} columns")
                else:
                    print(f"  [FAILED] {extractor_name}: No data returned")
            except Exception as e:
                print(f"  [ERROR] {extractor_name}: {str(e)}")
        
    except Exception as e:
        print(f"[ERROR] Failed to import amended extractors: {str(e)}")

if __name__ == "__main__":
    test_amended_file_reading()
    test_amended_extractors()

