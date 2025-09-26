"""
Bronze Layer Data Extractor for IC'ALPS Pipeline
Handles CSV extraction and Bronze layer table creation
"""

import pandas as pd
import logging
from typing import Dict, Optional
from database.csv_connector import csv_connector
from config.database_config import config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BronzeExtractor:
    """Extracts CSV data and creates Bronze layer tables with proper naming conventions"""

    def __init__(self):
        self.csv_connector = csv_connector
        self.config = config

    def extract_bronze_companies(self) -> Optional[pd.DataFrame]:
        """Extract and prepare Bronze companies data"""
        try:
            df = self.csv_connector.get_companies_data()
            if df is None:
                return None

            # Add Bronze layer metadata
            df['bronze_extracted_at'] = pd.Timestamp.now()
            df['bronze_source_file'] = 'Legacy_companies.csv'

            # Data quality improvements
            df['Comp_Name'] = df['Comp_Name'].str.strip()
            df['Comp_Website'] = df['Comp_Website'].fillna('').str.strip()

            # Clean website URLs
            mask = (df['Comp_Website'] != '') & (df['Comp_Website'] != 'NULL')
            df.loc[mask, 'Comp_Website'] = df.loc[mask, 'Comp_Website'].apply(self._clean_website_url)

            logger.info(f"Bronze companies extracted: {len(df)} records")
            return df

        except Exception as e:
            logger.error(f"Error extracting Bronze companies: {str(e)}")
            return None

    def extract_bronze_opportunities(self) -> Optional[pd.DataFrame]:
        """Extract and prepare Bronze opportunities data"""
        try:
            df = self.csv_connector.get_opportunities_data()
            if df is None:
                return None

            # Add Bronze layer metadata
            df['bronze_extracted_at'] = pd.Timestamp.now()
            df['bronze_source_file'] = 'Legacy_Opportunities.csv'

            # Data quality improvements
            df['Oppo_Description'] = df['Oppo_Description'].fillna('').str.strip()
            df['Oppo_Type'] = df['Oppo_Type'].fillna('').str.strip()
            df['Oppo_Product'] = df['Oppo_Product'].fillna('').str.strip()

            # Convert numeric fields
            numeric_fields = ['Oppo_Forecast', 'Oppo_Certainty', 'Oppo_Total', 'oppo_cout']
            for field in numeric_fields:
                if field in df.columns:
                    df[field] = pd.to_numeric(df[field], errors='coerce')

            # Convert date fields
            date_fields = ['Oppo_Opened', 'Oppo_Closed', 'Oppo_TargetClose', 'Oppo_CreatedDate', 'Oppo_UpdatedDate']
            for field in date_fields:
                if field in df.columns:
                    df[field] = pd.to_datetime(df[field], errors='coerce')

            logger.info(f"Bronze opportunities extracted: {len(df)} records")
            return df

        except Exception as e:
            logger.error(f"Error extracting Bronze opportunities: {str(e)}")
            return None

    def extract_bronze_persons(self) -> Optional[pd.DataFrame]:
        """Extract and prepare Bronze persons data"""
        try:
            df = self.csv_connector.get_persons_data()
            if df is None:
                return None

            # Add Bronze layer metadata
            df['bronze_extracted_at'] = pd.Timestamp.now()
            df['bronze_source_file'] = 'Legacy_persons.csv'

            # Data quality improvements - handle NaN values properly
            df['Pers_FirstName'] = df['Pers_FirstName'].fillna('').astype(str).str.strip().str.title()
            df['Pers_LastName'] = df['Pers_LastName'].fillna('').astype(str).str.strip().str.title()
            df['Pers_EmailAddress'] = df['Pers_EmailAddress'].fillna('').astype(str).str.strip().str.lower()

            # Validate email addresses
            df['email_valid'] = df['Pers_EmailAddress'].apply(self._validate_email)

            logger.info(f"Bronze persons extracted: {len(df)} records")
            return df

        except Exception as e:
            logger.error(f"Error extracting Bronze persons: {str(e)}")
            return None

    def extract_bronze_communications(self) -> Optional[pd.DataFrame]:
        """Extract and prepare Bronze communications data"""
        try:
            df = self.csv_connector.get_communications_data()
            if df is None:
                return None

            # Add Bronze layer metadata
            df['bronze_extracted_at'] = pd.Timestamp.now()
            df['bronze_source_file'] = 'Legacy_comm.csv'

            # Data quality improvements
            df['Comm_Subject'] = df['Comm_Subject'].fillna('').str.strip()

            # Convert date fields
            if 'Comm_DateTime' in df.columns:
                df['Comm_DateTime'] = pd.to_datetime(df['Comm_DateTime'], errors='coerce')

            # Determine communication type based on subject
            df['comm_type'] = df['Comm_Subject'].apply(self._determine_comm_type)

            logger.info(f"Bronze communications extracted: {len(df)} records")
            return df

        except Exception as e:
            logger.error(f"Error extracting Bronze communications: {str(e)}")
            return None

    def extract_bronze_social_networks(self) -> Optional[pd.DataFrame]:
        """Extract and prepare Bronze social networks data"""
        try:
            df = self.csv_connector.get_social_networks_data()
            if df is None:
                return None

            # Add Bronze layer metadata
            df['bronze_extracted_at'] = pd.Timestamp.now()
            df['bronze_source_file'] = 'legacy_socialnetworks.csv'

            # Clean network links
            df['sone_networklink'] = df['sone_networklink'].fillna('').str.strip()

            # Filter out auto-generated placeholders
            df = df[df['sone_networklink'] != '#AUTO#']

            # Determine social network type
            df['network_type'] = df['sone_networklink'].apply(self._determine_network_type)

            logger.info(f"Bronze social networks extracted: {len(df)} records")
            return df

        except Exception as e:
            logger.error(f"Error extracting Bronze social networks: {str(e)}")
            return None

    def extract_bronze_status_combinations(self) -> Optional[pd.DataFrame]:
        """Extract and prepare Bronze status combinations data"""
        try:
            df = self.csv_connector.get_status_combinations_data()
            if df is None:
                return None

            # Add Bronze layer metadata
            df['bronze_extracted_at'] = pd.Timestamp.now()
            df['bronze_source_file'] = 'combination_set.csv'

            # Clean status and stage values
            df['Oppo_Status'] = df['Oppo_Status'].fillna('').str.strip()
            df['Oppo_Stage'] = df['Oppo_Stage'].fillna('').str.strip()

            logger.info(f"Bronze status combinations extracted: {len(df)} records")
            return df

        except Exception as e:
            logger.error(f"Error extracting Bronze status combinations: {str(e)}")
            return None

    def extract_all_bronze_data(self) -> Dict[str, pd.DataFrame]:
        """Extract all Bronze layer data"""
        bronze_data = {}

        extractors = {
            'companies': self.extract_bronze_companies,
            'opportunities': self.extract_bronze_opportunities,
            'persons': self.extract_bronze_persons,
            'communications': self.extract_bronze_communications,
            'social_networks': self.extract_bronze_social_networks,
            'status_combinations': self.extract_bronze_status_combinations
        }

        for key, extractor_func in extractors.items():
            try:
                data = extractor_func()
                if data is not None:
                    bronze_data[key] = data
                    logger.info(f"Successfully extracted Bronze {key}")
                else:
                    logger.warning(f"Failed to extract Bronze {key}")
            except Exception as e:
                logger.error(f"Error extracting Bronze {key}: {str(e)}")

        logger.info(f"Bronze extraction completed: {len(bronze_data)} datasets extracted")
        return bronze_data

    def _clean_website_url(self, url: str) -> str:
        """Clean and validate website URLs"""
        if not url or url == 'NULL':
            return ''

        url = url.strip()
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url

        return url

    def _validate_email(self, email) -> bool:
        """Basic email validation"""
        # Handle NaN/None values
        if pd.isna(email) or not email:
            return False
            
        # Convert to string to handle any non-string types
        email_str = str(email).strip()
        
        if not email_str or '@' not in email_str:
            return False

        parts = email_str.split('@')
        if len(parts) != 2:
            return False

        local, domain = parts
        if not local or not domain or '.' not in domain:
            return False

        return True

    def _determine_comm_type(self, subject: str) -> str:
        """Determine communication type from subject"""
        if not subject:
            return 'UNKNOWN'

        subject_lower = subject.lower()
        if 'suivi' in subject_lower:
            return 'NOTE'
        elif 'call' in subject_lower or 'appel' in subject_lower:
            return 'CALL'
        elif 'email' in subject_lower or 'mail' in subject_lower:
            return 'EMAIL'
        elif 'meeting' in subject_lower or 'réunion' in subject_lower:
            return 'MEETING'
        else:
            return 'NOTE'

    def _determine_network_type(self, link: str) -> str:
        """Determine social network type from URL"""
        if not link:
            return 'UNKNOWN'

        link_lower = link.lower()
        if 'linkedin.com' in link_lower or 'in/' in link_lower:
            return 'LINKEDIN'
        elif 'company/' in link_lower:
            return 'COMPANY_PAGE'
        elif 'twitter.com' in link_lower:
            return 'TWITTER'
        elif 'facebook.com' in link_lower:
            return 'FACEBOOK'
        else:
            return 'WEBSITE'

# Global extractor instance
bronze_extractor = BronzeExtractor()