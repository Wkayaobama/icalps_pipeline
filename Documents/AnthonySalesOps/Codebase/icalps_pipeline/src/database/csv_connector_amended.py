"""
Amended CSV Data Connector for IC'ALPS Pipeline
Handles legacy_amended CSV files with enhanced data structures and robust error handling
"""

import pandas as pd
import logging
from pathlib import Path
from typing import Dict, Optional, List
from config.database_config import config

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CSVConnectorAmended:
    """Handles amended CSV file connections with enhanced data structures"""

    def __init__(self):
        self.config = config
        self.data_cache = {}
        
        # Amended file paths
        self.amended_csv_files = {
            'companies': str(self.config.input_path / "legacy_amended_companies.csv"),
            'contacts': str(self.config.input_path / "legacy_amended_contacts.csv"),
            'opportunities': str(self.config.input_path / "legacy_amended_opportunities.csv"),
            # Use original files for these as they don't have amended versions
            'communications': str(self.config.input_path / "Legacy_comm.csv"),
            'social_networks': str(self.config.input_path / "legacy_socialnetworks.csv"),
            'status_combinations': str(self.config.input_path / "combination_set.csv")
        }

    def validate_amended_files(self) -> bool:
        """Validate all required amended CSV files exist"""
        missing_files = []
        
        for key, file_path in self.amended_csv_files.items():
            if not Path(file_path).exists():
                missing_files.append(file_path)
        
        if missing_files:
            logger.error(f"Missing amended CSV files: {missing_files}")
            return False

        logger.info("All amended CSV files validated successfully")
        return True

    def read_amended_csv_file(self, file_key: str) -> Optional[pd.DataFrame]:
        """
        Read amended CSV file with robust error handling
        
        Args:
            file_key: Key from amended_csv_files config
            
        Returns:
            DataFrame or None if error
        """
        try:
            file_path = self.amended_csv_files.get(file_key)
            if not file_path:
                logger.error(f"Unknown amended file key: {file_key}")
                return None

            if not Path(file_path).exists():
                logger.error(f"Amended file not found: {file_path}")
                return None

            # Try multiple encoding strategies for problematic files
            encodings_to_try = ['utf-8-sig', 'utf-8', 'latin1', 'cp1252', 'iso-8859-1']
            
            for encoding in encodings_to_try:
                try:
                    # Read with robust parameters for malformed data
                    df = pd.read_csv(file_path, 
                                   encoding=encoding,
                                   on_bad_lines='skip',        # Skip problematic lines
                                   dtype=str,                  # Read everything as string initially
                                   quoting=1,                  # Handle quotes properly
                                   skipinitialspace=True,      # Skip spaces after delimiter
                                   engine='python')            # Use Python engine for flexibility
                    
                    # Clean column names
                    df.columns = df.columns.str.strip().str.replace('\ufeff', '')
                    
                    logger.info(f"Successfully read amended {file_key}: {len(df)} rows, {len(df.columns)} columns (encoding: {encoding})")
                    return df
                    
                except Exception as e:
                    logger.warning(f"Failed to read {file_key} with encoding {encoding}: {str(e)}")
                    continue
            
            logger.error(f"Failed to read amended {file_key} with all encoding attempts")
            return None

        except Exception as e:
            logger.error(f"Error reading amended {file_key}: {str(e)}")
            return None

    def get_amended_companies_data(self) -> Optional[pd.DataFrame]:
        """Get amended companies data with enhanced structure"""
        try:
            df = self.read_amended_csv_file('companies')
            if df is None:
                return None
            
            # Handle the richer company structure (34 columns)
            # Map to standardized fields for pipeline compatibility
            companies_df = pd.DataFrame()
            
            # Core fields (maintain compatibility)
            companies_df['Comp_CompanyId'] = pd.to_numeric(df['Comp_CompanyId'], errors='coerce')
            companies_df['Comp_Name'] = df['Comp_Name'].fillna('').str.strip()
            companies_df['Comp_Website'] = df['Comp_WebSite'].fillna('').str.strip()
            
            # Enhanced fields from amended structure
            companies_df['Comp_EmailAddress'] = df['Comp_EmailAddress'].fillna('')
            companies_df['Comp_PhoneNumber'] = df['Comp_PhoneFullNumber'].fillna('')
            companies_df['Comp_Type'] = df['Comp_Type'].fillna('')
            companies_df['Comp_Status'] = df['Comp_Status'].fillna('')
            companies_df['Comp_Revenue'] = df['Comp_Revenue'].fillna('')
            companies_df['Comp_Employees'] = df['Comp_Employees'].fillna('')
            companies_df['Comp_Sector'] = df['Comp_Sector'].fillna('')
            companies_df['Comp_Territory'] = df['Comp_Territory'].fillna('')
            companies_df['Comp_Source'] = df['Comp_Source'].fillna('')
            companies_df['Comp_LibraryDir'] = df['Comp_LibraryDir'].fillna('')
            companies_df['Comp_CreatedDate'] = df['Comp_CreatedDate']
            companies_df['Comp_UpdatedDate'] = df['Comp_UpdatedDate']
            
            # Remove deleted records
            if 'Comp_Deleted' in df.columns:
                companies_df = companies_df[df['Comp_Deleted'].isna()]
            
            # Remove duplicates
            companies_df = companies_df.drop_duplicates(subset=['Comp_CompanyId'])
            companies_df = companies_df.dropna(subset=['Comp_CompanyId'])
            
            logger.info(f"Amended companies data: {len(companies_df)} unique companies")
            return companies_df

        except Exception as e:
            logger.error(f"Error processing amended companies data: {str(e)}")
            return None

    def get_amended_contacts_data(self) -> Optional[pd.DataFrame]:
        """Get amended contacts data with enhanced structure"""
        try:
            df = self.read_amended_csv_file('contacts')
            if df is None:
                return None
            
            # Handle the richer contact structure (31 columns)
            contacts_df = pd.DataFrame()
            
            # Core fields (maintain compatibility)
            contacts_df['Pers_PersonId'] = pd.to_numeric(df['Pers_PersonId'], errors='coerce')
            contacts_df['Pers_FirstName'] = df['Pers_FirstName'].fillna('').str.strip()
            contacts_df['Pers_LastName'] = df['Pers_LastName'].fillna('').str.strip()
            contacts_df['Pers_EmailAddress'] = df['Pers_EmailAddress'].fillna('').str.strip()
            contacts_df['Comp_CompanyId'] = pd.to_numeric(df['Comp_CompanyId'], errors='coerce')
            contacts_df['Comp_Name'] = df['Comp_Name'].fillna('').str.strip()
            
            # Enhanced fields from amended structure
            contacts_df['Pers_Title'] = df['Pers_Title'].fillna('')
            contacts_df['Pers_Department'] = df['Pers_Department'].fillna('')
            contacts_df['Pers_Salutation'] = df['Pers_Salutation'].fillna('')
            contacts_df['Pers_MiddleName'] = df['Pers_MiddleName'].fillna('')
            contacts_df['Pers_Gender'] = df['Pers_Gender'].fillna('')
            contacts_df['Pers_Status'] = df['Pers_Status'].fillna('')
            contacts_df['Pers_PhoneNumber'] = df['Pers_PhoneNumber'].fillna('')
            contacts_df['phon_MobileFullNumber'] = df['phon_MobileFullNumber'].fillna('')
            contacts_df['Pers_CreatedDate'] = df['Pers_CreatedDate']
            contacts_df['Pers_UpdatedDate'] = df['Pers_UpdatedDate']
            contacts_df['Pers_LibraryDir'] = df['Pers_LibraryDir'].fillna('')
            
            # Remove deleted records
            if 'Pers_Deleted' in df.columns:
                contacts_df = contacts_df[df['Pers_Deleted'].isna()]
            
            # Remove duplicates and invalid records
            contacts_df = contacts_df.drop_duplicates(subset=['Pers_PersonId'])
            contacts_df = contacts_df.dropna(subset=['Pers_PersonId'])
            
            logger.info(f"Amended contacts data: {len(contacts_df)} unique contacts")
            return contacts_df

        except Exception as e:
            logger.error(f"Error processing amended contacts data: {str(e)}")
            return None

    def get_amended_opportunities_data(self) -> Optional[pd.DataFrame]:
        """Get amended opportunities data with enhanced structure"""
        try:
            df = self.read_amended_csv_file('opportunities')
            if df is None:
                return None
            
            # Handle the richer opportunities structure (28 columns)
            opportunities_df = pd.DataFrame()
            
            # Core fields (maintain compatibility with existing pipeline)
            opportunities_df['Oppo_OpportunityId'] = pd.to_numeric(df['Oppo_OpportunityId'], errors='coerce')
            opportunities_df['Oppo_Description'] = df['Oppo_Description'].fillna('').str.strip()
            opportunities_df['Oppo_PrimaryCompanyId'] = pd.to_numeric(df['Oppo_PrimaryCompanyId'], errors='coerce')
            opportunities_df['Oppo_PrimaryPersonId'] = pd.to_numeric(df['Oppo_PrimaryPersonId'], errors='coerce')
            opportunities_df['Oppo_AssignedUserId'] = pd.to_numeric(df['Oppo_AssignedUserId'], errors='coerce')
            opportunities_df['Oppo_Type'] = df['Oppo_Type'].fillna('').str.strip()
            opportunities_df['Oppo_Product'] = df['Oppo_Product'].fillna('').str.strip()
            opportunities_df['Oppo_Source'] = df['Oppo_Source'].fillna('').str.strip()
            opportunities_df['Oppo_Note'] = df['Oppo_Note'].fillna('').str.strip()
            opportunities_df['Oppo_CustomerRef'] = df['Oppo_CustomerRef'].fillna('').str.strip()
            opportunities_df['Oppo_Status'] = df['Oppo_Status'].fillna('').str.strip()
            opportunities_df['Oppo_Stage'] = df['Oppo_Stage'].fillna('').str.strip()
            
            # Numeric fields
            opportunities_df['Oppo_Forecast'] = pd.to_numeric(df['Oppo_Forecast'], errors='coerce').fillna(0)
            opportunities_df['Oppo_Certainty'] = pd.to_numeric(df['Oppo_Certainty'], errors='coerce').fillna(0)
            opportunities_df['Oppo_Total'] = pd.to_numeric(df['Oppo_Total'], errors='coerce').fillna(0)
            opportunities_df['oppo_cout'] = pd.to_numeric(df['oppo_cout'], errors='coerce').fillna(0)
            
            # Date fields
            opportunities_df['Oppo_CreatedDate'] = df['Oppo_CreatedDate']
            opportunities_df['Oppo_UpdatedDate'] = df['Oppo_UpdatedDate']
            
            # Add missing fields expected by DuckDB processor (compatibility)
            opportunities_df['Oppo_TargetClose'] = df.get('Oppo_TargetClose', df['Oppo_UpdatedDate'])  # Use UpdatedDate as fallback
            opportunities_df['Oppo_Opened'] = df.get('Oppo_Opened', df['Oppo_CreatedDate'])  # Use CreatedDate as fallback  
            opportunities_df['Oppo_Closed'] = df.get('Oppo_Closed', df['Oppo_UpdatedDate'])  # Use UpdatedDate as fallback
            
            # Enhanced fields from amended structure
            opportunities_df['Product_Name'] = df['Product_Name'].fillna('')
            opportunities_df['Oppo_Priority'] = df['Oppo_Priority'].fillna('')
            opportunities_df['Oppo_ChannelId'] = df['Oppo_ChannelId'].fillna('')
            
            # Company and contact info (for associations)
            opportunities_df['Comp_Name'] = df['Comp_Name'].fillna('')
            opportunities_df['Pers_FirstName'] = df['Pers_FirstName'].fillna('')
            opportunities_df['Pers_LastName'] = df['Pers_LastName'].fillna('')
            
            # Remove invalid records
            opportunities_df = opportunities_df.dropna(subset=['Oppo_OpportunityId'])
            
            logger.info(f"Amended opportunities data: {len(opportunities_df)} opportunities")
            return opportunities_df

        except Exception as e:
            logger.error(f"Error processing amended opportunities data: {str(e)}")
            return None

    def get_original_communications_data(self) -> Optional[pd.DataFrame]:
        """Get original communications data (no amended version)"""
        try:
            file_path = self.config.csv_files.get('communications')
            df = pd.read_csv(file_path, encoding='utf-8-sig')
            df.columns = df.columns.str.strip().str.replace('\ufeff', '')
            logger.info(f"Communications data: {len(df)} communications")
            return df
        except Exception as e:
            logger.error(f"Error reading communications data: {str(e)}")
            return None

    def get_original_social_networks_data(self) -> Optional[pd.DataFrame]:
        """Get original social networks data (no amended version)"""
        try:
            file_path = self.config.csv_files.get('social_networks')
            df = pd.read_csv(file_path, encoding='utf-8-sig')
            df.columns = df.columns.str.strip().str.replace('\ufeff', '')
            logger.info(f"Social networks data: {len(df)} social links")
            return df
        except Exception as e:
            logger.error(f"Error reading social networks data: {str(e)}")
            return None

    def get_all_amended_data(self) -> Dict[str, pd.DataFrame]:
        """Get all amended data as dictionary of DataFrames"""
        if not self.validate_amended_files():
            return {}

        data = {
            'companies': self.get_amended_companies_data(),
            'contacts': self.get_amended_contacts_data(),
            'opportunities': self.get_amended_opportunities_data(),
            'communications': self.get_original_communications_data(),
            'social_networks': self.get_original_social_networks_data()
        }

        # Filter out None values
        data = {k: v for k, v in data.items() if v is not None}

        logger.info(f"Successfully loaded {len(data)} amended datasets")
        return data

    def get_amended_data_summary(self) -> Dict[str, Dict[str, int]]:
        """Get summary statistics for all amended datasets"""
        data = self.get_all_amended_data()
        summary = {}

        for key, df in data.items():
            summary[key] = {
                'rows': len(df),
                'columns': len(df.columns),
                'null_values': df.isnull().sum().sum()
            }

        return summary

# Global amended connector instance
csv_connector_amended = CSVConnectorAmended()
