"""
CSV Data Connector for IC'ALPS Pipeline
Handles CSV file reading and initial data validation
"""

import pandas as pd
import logging
from pathlib import Path
from typing import Dict, Optional, List
from config.database_config import config

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CSVConnector:
    """Handles CSV file connections and data reading"""

    def __init__(self):
        self.config = config
        self.data_cache = {}

    def validate_files(self) -> bool:
        """Validate all required CSV files exist"""
        validation_results = self.config.validate_csv_files()
        missing_files = [k for k, v in validation_results.items() if not v]

        if missing_files:
            logger.error(f"Missing CSV files: {missing_files}")
            return False

        logger.info("All CSV files validated successfully")
        return True

    def read_csv_file(self, file_key: str, encoding: str = 'utf-8-sig') -> Optional[pd.DataFrame]:
        """
        Read CSV file and return DataFrame

        Args:
            file_key: Key from csv_files config
            encoding: File encoding (default handles BOM)

        Returns:
            DataFrame or None if error
        """
        try:
            file_path = self.config.csv_files.get(file_key)
            if not file_path:
                logger.error(f"Unknown file key: {file_key}")
                return None

            if not Path(file_path).exists():
                logger.error(f"File not found: {file_path}")
                return None

            # Read CSV with proper encoding to handle BOM
            df = pd.read_csv(file_path, encoding=encoding)

            # Clean column names (remove BOM and extra spaces)
            df.columns = df.columns.str.strip().str.replace('\ufeff', '')

            logger.info(f"Successfully read {file_key}: {len(df)} rows, {len(df.columns)} columns")
            return df

        except Exception as e:
            logger.error(f"Error reading {file_key}: {str(e)}")
            return None

    def get_companies_data(self) -> Optional[pd.DataFrame]:
        """Get companies data with proper column mapping"""
        try:
            file_path = self.config.csv_files.get('companies')
            if not file_path or not Path(file_path).exists():
                logger.error(f"Companies file not found: {file_path}")
                return None

            # Read CSV without headers since first row is data
            df = pd.read_csv(file_path, encoding='utf-8-sig', header=None)
            
            # Assign proper column names based on the 6 columns structure
            expected_columns = ['Comp_CompanyId', 'Comp_Name', 'Comp_Website', 'Comp_Website2', 'Oppo_OpportunityId', 'Oppo_Description']
            
            if len(df.columns) == 6:
                df.columns = expected_columns
            else:
                logger.warning(f"Unexpected number of columns in companies CSV: {len(df.columns)}")
                # Fallback column assignment
                df.columns = [f'col_{i}' for i in range(len(df.columns))]

            # Remove duplicates based on company ID
            df = df.drop_duplicates(subset=['Comp_CompanyId'])
            logger.info(f"Companies data: {len(df)} unique companies")

        except Exception as e:
            logger.error(f"Error reading companies data: {str(e)}")
            return None

        return df

    def get_opportunities_data(self) -> Optional[pd.DataFrame]:
        """Get opportunities data with all columns"""
        df = self.read_csv_file('opportunities')
        if df is not None:
            logger.info(f"Opportunities data: {len(df)} opportunities")
            # Clean NULL values
            df = df.replace('NULL', pd.NA)

        return df

    def get_persons_data(self) -> Optional[pd.DataFrame]:
        """Get persons/contacts data"""
        df = self.read_csv_file('persons')
        if df is not None:
            # Remove duplicates based on person ID
            df = df.drop_duplicates(subset=['Pers_PersonId'])
            logger.info(f"Persons data: {len(df)} unique persons")

        return df

    def get_communications_data(self) -> Optional[pd.DataFrame]:
        """Get communications data"""
        df = self.read_csv_file('communications')
        if df is not None:
            logger.info(f"Communications data: {len(df)} communications")

        return df

    def get_social_networks_data(self) -> Optional[pd.DataFrame]:
        """Get social networks data"""
        df = self.read_csv_file('social_networks')
        if df is not None:
            logger.info(f"Social networks data: {len(df)} social links")

        return df

    def get_status_combinations_data(self) -> Optional[pd.DataFrame]:
        """Get status/stage combinations data"""
        df = self.read_csv_file('status_combinations')
        if df is not None:
            logger.info(f"Status combinations: {len(df)} combinations")

        return df

    def get_all_data(self) -> Dict[str, pd.DataFrame]:
        """Get all data as dictionary of DataFrames"""
        if not self.validate_files():
            return {}

        data = {
            'companies': self.get_companies_data(),
            'opportunities': self.get_opportunities_data(),
            'persons': self.get_persons_data(),
            'communications': self.get_communications_data(),
            'social_networks': self.get_social_networks_data(),
            'status_combinations': self.get_status_combinations_data()
        }

        # Filter out None values
        data = {k: v for k, v in data.items() if v is not None}

        logger.info(f"Successfully loaded {len(data)} datasets")
        return data

    def get_data_summary(self) -> Dict[str, Dict[str, int]]:
        """Get summary statistics for all datasets"""
        data = self.get_all_data()
        summary = {}

        for key, df in data.items():
            summary[key] = {
                'rows': len(df),
                'columns': len(df.columns),
                'null_values': df.isnull().sum().sum()
            }

        return summary

# Global connector instance
csv_connector = CSVConnector()