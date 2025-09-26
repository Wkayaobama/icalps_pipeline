"""
Amended Bronze Layer Data Extractor for IC'ALPS Pipeline
Handles legacy_amended CSV extraction with enhanced data structures
"""

import pandas as pd
import logging
from typing import Dict, Optional
from database.csv_connector_amended import csv_connector_amended

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BronzeExtractorAmended:
    """Extracts amended CSV data and creates Bronze layer tables with enhanced data structures"""

    def __init__(self):
        self.csv_connector = csv_connector_amended

    def extract_bronze_companies_amended(self) -> Optional[pd.DataFrame]:
        """Extract and prepare Bronze companies data from amended structure"""
        try:
            df = self.csv_connector.get_amended_companies_data()
            if df is None:
                return None

            # Add Bronze layer metadata
            df['bronze_extracted_at'] = pd.Timestamp.now()
            df['bronze_source_file'] = 'legacy_amended_companies.csv'

            # Enhanced data quality improvements
            df['Comp_Name'] = df['Comp_Name'].str.strip()
            df['Comp_Website'] = df['Comp_Website'].fillna('').str.strip()
            
            # Clean website URLs
            mask = (df['Comp_Website'] != '') & (df['Comp_Website'] != 'NULL')
            df.loc[mask, 'Comp_Website'] = df.loc[mask, 'Comp_Website'].apply(self._clean_website_url)
            
            # Enhanced fields processing
            df['Comp_EmailAddress'] = df['Comp_EmailAddress'].str.strip().str.lower()
            df['company_email_valid'] = df['Comp_EmailAddress'].apply(self._validate_email)
            
            # Phone number processing
            df['Comp_PhoneNumber'] = df['Comp_PhoneNumber'].str.strip()
            df['phone_valid'] = df['Comp_PhoneNumber'].apply(self._validate_phone)
            
            # Company size categorization
            df['company_size_category'] = df['Comp_Employees'].apply(self._categorize_company_size)
            
            # Revenue categorization
            df['revenue_category'] = df['Comp_Revenue'].apply(self._categorize_revenue)

            logger.info(f"Bronze amended companies extracted: {len(df)} records")
            return df

        except Exception as e:
            logger.error(f"Error extracting Bronze amended companies: {str(e)}")
            return None

    def extract_bronze_contacts_amended(self) -> Optional[pd.DataFrame]:
        """Extract and prepare Bronze contacts data from amended structure"""
        try:
            df = self.csv_connector.get_amended_contacts_data()
            if df is None:
                return None

            # Add Bronze layer metadata
            df['bronze_extracted_at'] = pd.Timestamp.now()
            df['bronze_source_file'] = 'legacy_amended_contacts.csv'

            # Enhanced data quality improvements
            df['Pers_FirstName'] = df['Pers_FirstName'].fillna('').astype(str).str.strip().str.title()
            df['Pers_LastName'] = df['Pers_LastName'].fillna('').astype(str).str.strip().str.title()
            df['Pers_EmailAddress'] = df['Pers_EmailAddress'].fillna('').astype(str).str.strip().str.lower()

            # Enhanced email validation
            df['email_valid'] = df['Pers_EmailAddress'].apply(self._validate_email)
            
            # Enhanced contact fields processing
            df['Pers_Title'] = df['Pers_Title'].fillna('').str.strip()
            df['Pers_Department'] = df['Pers_Department'].fillna('').str.strip()
            df['contact_seniority'] = df['Pers_Title'].apply(self._determine_seniority)
            
            # Phone processing
            df['primary_phone'] = df['Pers_PhoneNumber'].fillna('')
            df['mobile_phone'] = df['phon_MobileFullNumber'].fillna('')
            df['has_phone'] = (df['primary_phone'] != '') | (df['mobile_phone'] != '')
            
            # Contact quality score
            df['contact_quality_score'] = self._calculate_contact_quality(df)

            logger.info(f"Bronze amended contacts extracted: {len(df)} records")
            return df

        except Exception as e:
            logger.error(f"Error extracting Bronze amended contacts: {str(e)}")
            return None

    def extract_bronze_opportunities_amended(self) -> Optional[pd.DataFrame]:
        """Extract and prepare Bronze opportunities data from amended structure"""
        try:
            df = self.csv_connector.get_amended_opportunities_data()
            if df is None:
                return None

            # Add Bronze layer metadata
            df['bronze_extracted_at'] = pd.Timestamp.now()
            df['bronze_source_file'] = 'legacy_amended_opportunities.csv'

            # Enhanced data quality improvements
            df['Oppo_Description'] = df['Oppo_Description'].fillna('').str.strip()
            df['Oppo_Type'] = df['Oppo_Type'].fillna('').str.strip()
            df['Oppo_Product'] = df['Oppo_Product'].fillna('').str.strip()

            # Enhanced product information
            df['Product_Name'] = df['Product_Name'].fillna('').str.strip()
            df['enhanced_product_info'] = df.apply(self._combine_product_info, axis=1)

            # Convert date fields with robust handling
            date_fields = ['Oppo_CreatedDate', 'Oppo_UpdatedDate']
            for field in date_fields:
                if field in df.columns:
                    df[field] = pd.to_datetime(df[field], errors='coerce')

            # Enhanced deal classification
            df['deal_complexity'] = df['Oppo_Description'].apply(self._assess_deal_complexity)
            df['deal_value_category'] = df['Oppo_Forecast'].apply(self._categorize_deal_value)

            logger.info(f"Bronze amended opportunities extracted: {len(df)} records")
            return df

        except Exception as e:
            logger.error(f"Error extracting Bronze amended opportunities: {str(e)}")
            return None

    def extract_all_bronze_amended_data(self) -> Dict[str, pd.DataFrame]:
        """Extract all Bronze layer amended data"""
        bronze_data = {}

        extractors = {
            'companies': self.extract_bronze_companies_amended,
            'persons': self.extract_bronze_contacts_amended,  # Keep 'persons' name for DuckDB compatibility
            'opportunities': self.extract_bronze_opportunities_amended,
            # Use original extractors for communications and social networks
            'communications': self._extract_original_communications,
            'social_networks': self._extract_original_social_networks
        }

        for key, extractor_func in extractors.items():
            try:
                logger.info(f"Attempting to extract Bronze amended {key}...")
                data = extractor_func()
                if data is not None and len(data) > 0:
                    bronze_data[key] = data
                    logger.info(f"Successfully extracted Bronze amended {key}: {len(data)} records")
                else:
                    logger.warning(f"Failed to extract Bronze amended {key} - no data returned")
            except Exception as e:
                logger.error(f"Error extracting Bronze amended {key}: {str(e)}")
                # Continue with other extractions

        logger.info(f"Bronze amended extraction completed: {len(bronze_data)} datasets extracted")
        
        # Log what was actually extracted
        for key, df in bronze_data.items():
            logger.info(f"  {key}: {len(df)} records, {len(df.columns)} columns")
        
        return bronze_data

    def _extract_original_communications(self) -> Optional[pd.DataFrame]:
        """Extract original communications data (no amended version)"""
        try:
            df = self.csv_connector.get_original_communications_data()
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
            
            # Determine communication type
            df['comm_type'] = df['Comm_Subject'].apply(self._determine_comm_type)
            
            return df
        except Exception as e:
            logger.error(f"Error extracting original communications: {str(e)}")
            return None

    def _extract_original_social_networks(self) -> Optional[pd.DataFrame]:
        """Extract original social networks data (no amended version)"""
        try:
            df = self.csv_connector.get_original_social_networks_data()
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
            
            return df
        except Exception as e:
            logger.error(f"Error extracting original social networks: {str(e)}")
            return None

    # Utility methods
    def _clean_website_url(self, url: str) -> str:
        """Clean and validate website URLs"""
        if not url or url == 'NULL':
            return ''
        url = url.strip()
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        return url

    def _validate_email(self, email) -> bool:
        """Enhanced email validation"""
        if pd.isna(email) or not email:
            return False
        email_str = str(email).strip()
        if not email_str or '@' not in email_str:
            return False
        parts = email_str.split('@')
        if len(parts) != 2:
            return False
        local, domain = parts
        return bool(local and domain and '.' in domain)

    def _validate_phone(self, phone) -> bool:
        """Basic phone validation"""
        if pd.isna(phone) or not phone:
            return False
        phone_str = str(phone).strip()
        return len(phone_str) >= 10 and any(char.isdigit() for char in phone_str)

    def _categorize_company_size(self, employees: str) -> str:
        """Categorize company size"""
        if pd.isna(employees) or not employees:
            return 'Unknown'
        emp_str = str(employees).lower()
        if 'upto20' in emp_str or '1-20' in emp_str:
            return 'Small'
        elif '20-100' in emp_str or '21-100' in emp_str:
            return 'Medium'
        elif '100+' in emp_str or '>100' in emp_str:
            return 'Large'
        return 'Unknown'

    def _categorize_revenue(self, revenue: str) -> str:
        """Categorize company revenue"""
        if pd.isna(revenue) or not revenue:
            return 'Unknown'
        return str(revenue).strip()

    def _determine_seniority(self, title: str) -> str:
        """Determine contact seniority from title"""
        if pd.isna(title) or not title:
            return 'Unknown'
        title_lower = str(title).lower()
        if any(word in title_lower for word in ['ceo', 'president', 'director', 'vp', 'vice president']):
            return 'Executive'
        elif any(word in title_lower for word in ['manager', 'lead', 'head', 'senior']):
            return 'Management'
        elif any(word in title_lower for word in ['engineer', 'developer', 'analyst', 'specialist']):
            return 'Technical'
        return 'Staff'

    def _calculate_contact_quality(self, df: pd.DataFrame) -> pd.Series:
        """Calculate contact quality score based on available information"""
        score = pd.Series(0, index=df.index)
        
        # Email adds 40 points
        score += df['email_valid'].astype(int) * 40
        
        # Phone adds 20 points  
        score += df['has_phone'].astype(int) * 20
        
        # Title adds 20 points
        score += (df['Pers_Title'] != '').astype(int) * 20
        
        # Department adds 10 points
        score += (df['Pers_Department'] != '').astype(int) * 10
        
        # Company association adds 10 points
        score += df['Comp_CompanyId'].notna().astype(int) * 10
        
        return score

    def _combine_product_info(self, row) -> str:
        """Combine product information from multiple fields"""
        product_parts = []
        if row['Oppo_Product'] and row['Oppo_Product'] != '':
            product_parts.append(str(row['Oppo_Product']))
        if row['Product_Name'] and row['Product_Name'] != '':
            product_parts.append(str(row['Product_Name']))
        return ' | '.join(product_parts) if product_parts else ''

    def _assess_deal_complexity(self, description: str) -> str:
        """Assess deal complexity based on description"""
        if pd.isna(description) or not description:
            return 'Unknown'
        
        desc_lower = str(description).lower()
        complexity_indicators = {
            'High': ['asic', 'development', 'design', 'custom', 'complex'],
            'Medium': ['evaluation', 'study', 'analysis', 'consulting'],
            'Low': ['support', 'maintenance', 'standard']
        }
        
        for level, indicators in complexity_indicators.items():
            if any(indicator in desc_lower for indicator in indicators):
                return level
        
        return 'Medium'  # Default

    def _categorize_deal_value(self, forecast) -> str:
        """Categorize deal value"""
        if pd.isna(forecast):
            return 'Unknown'
        
        try:
            value = float(forecast)
            if value == 0:
                return 'No Value'
            elif value < 50000:
                return 'Small (<50K)'
            elif value < 200000:
                return 'Medium (50K-200K)'
            elif value < 500000:
                return 'Large (200K-500K)'
            else:
                return 'Enterprise (>500K)'
        except:
            return 'Unknown'

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

# Global amended extractor instance
bronze_extractor_amended = BronzeExtractorAmended()
