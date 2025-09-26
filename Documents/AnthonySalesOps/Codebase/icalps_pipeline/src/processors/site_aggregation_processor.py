"""
Site Aggregation Processor for IC'ALPS Pipeline
Implements child-to-parent company logic based on domain grouping and name analysis
"""

import pandas as pd
import logging
import re
from typing import Dict, Optional, Tuple, List

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SiteAggregationProcessor:
    """Implements site aggregation logic to group related company sites under parent companies"""

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def process_site_aggregation(self, companies_df: pd.DataFrame, contacts_df: pd.DataFrame) -> pd.DataFrame:
        """
        Process company site aggregation with child-to-parent relationships
        
        Args:
            companies_df: Enhanced companies data
            contacts_df: Enhanced contacts data
            
        Returns:
            DataFrame with site aggregation and parent-child relationships
        """
        try:
            logger.info("Starting site aggregation processing...")
            
            # Step 1: Extract base company names and clean domains
            companies_analysis = self._analyze_companies(companies_df)
            
            # Step 2: Identify domain groups (companies with multiple sites)
            domain_groups = self._identify_domain_groups(companies_analysis)
            
            # Step 3: Create company aggregation with contacts
            aggregation_result = self._create_company_aggregation(companies_analysis, domain_groups, contacts_df)
            
            logger.info(f"Site aggregation completed: {len(aggregation_result)} records created")
            return aggregation_result
            
        except Exception as e:
            logger.error(f"Error in site aggregation processing: {str(e)}")
            return pd.DataFrame()

    def _analyze_companies(self, companies_df: pd.DataFrame) -> pd.DataFrame:
        """Analyze companies to extract base names and clean domains"""
        
        analysis_df = companies_df.copy()
        
        # Extract base company name (everything except last part after final space)
        analysis_df['base_company_name'] = analysis_df['Comp_Name'].apply(self._extract_base_company_name)
        
        # Extract location (last part after final space) 
        analysis_df['location_extracted'] = analysis_df['Comp_Name'].apply(self._extract_location)
        
        # Clean domain from website
        analysis_df['domain_clean'] = analysis_df.get('Comp_Website', '').apply(self._clean_domain)
        
        logger.info(f"Analyzed {len(analysis_df)} companies for site aggregation")
        return analysis_df

    def _extract_base_company_name(self, company_name: str) -> str:
        """Extract base company name (everything except location suffix)"""
        if not company_name or pd.isna(company_name):
            return ""
            
        name = str(company_name).strip()
        
        # Find last space and extract everything before it
        words = name.split()
        if len(words) > 1:
            # Check if last word looks like a location (contains common location indicators)
            last_word = words[-1].lower()
            location_indicators = ['grenoble', 'paris', 'lyon', 'toulouse', 'hq', 'headquarters', 
                                 'usa', 'france', 'germany', 'uk', 'switzerland', 'italy',
                                 'north', 'south', 'east', 'west', 'corp', 'inc', 'ltd', 'sa']
            
            if any(indicator in last_word for indicator in location_indicators):
                return ' '.join(words[:-1])
            else:
                # If last word doesn't look like location, keep full name
                return name
        else:
            return name

    def _extract_location(self, company_name: str) -> str:
        """Extract location from company name (last part after final space)"""
        if not company_name or pd.isna(company_name):
            return "HQ"
            
        name = str(company_name).strip()
        words = name.split()
        
        if len(words) > 1:
            last_word = words[-1]
            # Check if it looks like a location
            location_indicators = ['grenoble', 'paris', 'lyon', 'toulouse', 'hq', 'headquarters',
                                 'usa', 'france', 'germany', 'uk', 'switzerland', 'italy']
            
            if any(indicator in last_word.lower() for indicator in location_indicators):
                return last_word
            else:
                return "HQ"
        else:
            return "HQ"

    def _clean_domain(self, website: str) -> str:
        """Clean website URL to extract domain for grouping"""
        if not website or pd.isna(website) or website in ['', 'NULL', 'NaN']:
            return 'no-domain'
            
        domain = str(website).strip().lower()
        
        # Remove protocol
        domain = re.sub(r'^https?://', '', domain)
        
        # Remove www
        domain = re.sub(r'^www\.', '', domain)
        
        # Remove trailing slash and path
        domain = domain.split('/')[0]
        
        # Remove port numbers
        domain = domain.split(':')[0]
        
        return domain if domain else 'no-domain'

    def _identify_domain_groups(self, companies_analysis: pd.DataFrame) -> pd.DataFrame:
        """Identify companies that should be grouped under parent entities"""
        
        # Group by base company name and domain
        groups = companies_analysis.groupby(['base_company_name', 'domain_clean']).agg({
            'Comp_CompanyId': 'count',
            'Comp_Website': 'first',
            'Comp_Name': 'first'
        }).rename(columns={'Comp_CompanyId': 'site_count'}).reset_index()
        
        # Only keep groups with multiple sites
        multi_site_groups = groups[groups['site_count'] > 1].copy()
        
        if len(multi_site_groups) == 0:
            logger.info("No multi-site companies found")
            return pd.DataFrame()
        
        # Assign new parent IDs (starting from max existing company ID + 1)
        max_company_id = companies_analysis['Comp_CompanyId'].max()
        multi_site_groups['new_parent_id'] = range(max_company_id + 1, max_company_id + 1 + len(multi_site_groups))
        
        # Add flags
        multi_site_groups['has_multiple_sites'] = True
        multi_site_groups['parent_website'] = multi_site_groups['Comp_Website']
        
        logger.info(f"Identified {len(multi_site_groups)} multi-site company groups")
        return multi_site_groups

    def _create_company_aggregation(self, companies_analysis: pd.DataFrame, 
                                  domain_groups: pd.DataFrame, 
                                  contacts_df: pd.DataFrame) -> pd.DataFrame:
        """Create the complete company aggregation with parent-child relationships"""
        
        # Create contact lookup for faster joins
        contact_lookup = contacts_df.groupby('Comp_CompanyId').agg({
            'Pers_PersonId': 'count',
            'Pers_FirstName': lambda x: ', '.join(x.astype(str)) if len(x) > 0 else '',
            'Pers_LastName': lambda x: ', '.join(x.astype(str)) if len(x) > 0 else '',
            'Pers_EmailAddress': lambda x: ', '.join(x.dropna().astype(str)) if len(x.dropna()) > 0 else ''
        }).rename(columns={
            'Pers_PersonId': 'contact_count',
            'Pers_FirstName': 'all_contact_first_names',
            'Pers_LastName': 'all_contact_last_names', 
            'Pers_EmailAddress': 'all_contact_emails'
        }).reset_index()
        
        # Merge domain groups info back to companies
        if len(domain_groups) > 0:
            companies_with_groups = companies_analysis.merge(
                domain_groups[['base_company_name', 'domain_clean', 'new_parent_id', 'has_multiple_sites']],
                on=['base_company_name', 'domain_clean'],
                how='left'
            )
        else:
            companies_with_groups = companies_analysis.copy()
            companies_with_groups['new_parent_id'] = None
            companies_with_groups['has_multiple_sites'] = False
        
        # Merge contact information
        companies_with_contacts = companies_with_groups.merge(
            contact_lookup, on='Comp_CompanyId', how='left'
        )
        
        # Fill missing contact data
        companies_with_contacts['contact_count'] = companies_with_contacts['contact_count'].fillna(0)
        companies_with_contacts['all_contact_first_names'] = companies_with_contacts['all_contact_first_names'].fillna('')
        companies_with_contacts['all_contact_last_names'] = companies_with_contacts['all_contact_last_names'].fillna('')
        companies_with_contacts['all_contact_emails'] = companies_with_contacts['all_contact_emails'].fillna('')
        
        # Create site records (original companies)
        site_records = self._create_site_records(companies_with_contacts)
        
        # Create parent aggregator records
        parent_records = self._create_parent_records(domain_groups, companies_with_contacts)
        
        # Combine all records
        all_records = []
        if len(site_records) > 0:
            all_records.append(site_records)
        if len(parent_records) > 0:
            all_records.append(parent_records)
            
        if all_records:
            result = pd.concat(all_records, ignore_index=True)
        else:
            result = pd.DataFrame()
        
        logger.info(f"Created aggregation with {len(result)} total records")
        return result

    def _create_site_records(self, companies_with_contacts: pd.DataFrame) -> pd.DataFrame:
        """Create site records (original companies) with contact information"""
        
        result = pd.DataFrame()
        
        # Core company information
        result['company_id'] = companies_with_contacts['Comp_CompanyId']
        result['parent_company_id'] = companies_with_contacts['new_parent_id'].fillna(companies_with_contacts['Comp_CompanyId'])
        result['company_name'] = companies_with_contacts['Comp_Name']
        result['base_company_name'] = companies_with_contacts['base_company_name']
        result['location_extracted'] = companies_with_contacts['location_extracted']
        result['domain_clean'] = companies_with_contacts['domain_clean']
        result['company_website'] = companies_with_contacts.get('Comp_Website', '')
        
        # Company classification
        result['record_type'] = companies_with_contacts['new_parent_id'].apply(
            lambda x: 'Site' if pd.notna(x) else 'Standalone'
        )
        result['has_multiple_sites'] = companies_with_contacts['has_multiple_sites'].fillna(False)
        
        # Site ordering within company group
        result['site_order'] = companies_with_contacts.groupby(['base_company_name', 'domain_clean']).cumcount() + 1
        
        # Contact information
        result['contact_count'] = companies_with_contacts['contact_count']
        result['all_contact_names'] = companies_with_contacts.apply(
            lambda row: f"{row['all_contact_first_names']} {row['all_contact_last_names']}".replace('  ', ' ').strip() 
            if row['all_contact_first_names'] or row['all_contact_last_names'] else '', axis=1
        )
        result['all_contact_emails'] = companies_with_contacts['all_contact_emails']
        
        # Primary contact (first contact)
        result['primary_contact_name'] = companies_with_contacts.apply(
            lambda row: f"{row['all_contact_first_names'].split(',')[0].strip()} {row['all_contact_last_names'].split(',')[0].strip()}".strip()
            if row['all_contact_first_names'] and row['all_contact_last_names'] else '', axis=1
        )
        result['primary_contact_email'] = companies_with_contacts['all_contact_emails'].apply(
            lambda x: x.split(',')[0].strip() if x else ''
        )
        
        # Metadata
        result['source_type'] = 'Original'
        result['processed_date'] = pd.Timestamp.now().strftime('%m/%d/%Y %H:%M')
        
        return result

    def _create_parent_records(self, domain_groups: pd.DataFrame, companies_with_contacts: pd.DataFrame) -> pd.DataFrame:
        """Create parent aggregator records for multi-site companies"""
        
        if len(domain_groups) == 0:
            return pd.DataFrame()
        
        result = pd.DataFrame()
        
        # Core parent information
        result['company_id'] = domain_groups['new_parent_id']
        result['parent_company_id'] = None  # Parents have no parent
        result['company_name'] = domain_groups['base_company_name']
        result['base_company_name'] = domain_groups['base_company_name']
        result['location_extracted'] = 'HQ'
        result['domain_clean'] = domain_groups['domain_clean']
        result['company_website'] = domain_groups['parent_website']
        
        # Parent classification
        result['record_type'] = 'Parent_Aggregator'
        result['has_multiple_sites'] = True
        result['site_order'] = 0  # Parents get order 0
        
        # Aggregate contact information from child sites
        for idx, group in domain_groups.iterrows():
            child_companies = companies_with_contacts[
                (companies_with_contacts['base_company_name'] == group['base_company_name']) &
                (companies_with_contacts['domain_clean'] == group['domain_clean'])
            ]
            
            # Aggregate contact counts
            total_contacts = child_companies['contact_count'].sum()
            result.loc[idx, 'contact_count'] = total_contacts
            
            # Aggregate contact names and emails
            all_names = []
            all_emails = []
            
            for _, child in child_companies.iterrows():
                if child['all_contact_first_names']:
                    first_names = str(child['all_contact_first_names']).split(',')
                    last_names = str(child['all_contact_last_names']).split(',')
                    for fn, ln in zip(first_names, last_names):
                        all_names.append(f"{fn.strip()} {ln.strip()}".strip())
                
                if child['all_contact_emails']:
                    emails = str(child['all_contact_emails']).split(',')
                    all_emails.extend([email.strip() for email in emails if email.strip()])
            
            # Remove duplicates while preserving order
            unique_names = list(dict.fromkeys(all_names))
            unique_emails = list(dict.fromkeys(all_emails))
            
            result.loc[idx, 'all_contact_names'] = ', '.join(unique_names)
            result.loc[idx, 'all_contact_emails'] = ', '.join(unique_emails)
            result.loc[idx, 'primary_contact_name'] = unique_names[0] if unique_names else ''
            result.loc[idx, 'primary_contact_email'] = unique_emails[0] if unique_emails else ''
        
        # Metadata
        result['source_type'] = 'Generated'
        result['processed_date'] = pd.Timestamp.now().strftime('%m/%d/%Y %H:%M')
        
        return result

    def _identify_domain_groups(self, companies_analysis: pd.DataFrame) -> pd.DataFrame:
        """Identify companies that should be grouped under parent entities"""
        
        # Group by base company name and domain
        groups = companies_analysis.groupby(['base_company_name', 'domain_clean']).agg({
            'Comp_CompanyId': 'count',
            'Comp_Website': 'first',
            'Comp_Name': 'first'
        }).rename(columns={'Comp_CompanyId': 'site_count'}).reset_index()
        
        # Only keep groups with multiple sites
        multi_site_groups = groups[groups['site_count'] > 1].copy()
        
        if len(multi_site_groups) == 0:
            logger.info("No multi-site companies identified")
            return pd.DataFrame()
        
        # Assign new parent IDs
        max_company_id = companies_analysis['Comp_CompanyId'].max()
        multi_site_groups['new_parent_id'] = range(
            max_company_id + 1, 
            max_company_id + 1 + len(multi_site_groups)
        )
        
        # Add metadata
        multi_site_groups['has_multiple_sites'] = True
        multi_site_groups['parent_website'] = multi_site_groups['Comp_Website']
        
        logger.info(f"Identified {len(multi_site_groups)} multi-site company groups:")
        for _, group in multi_site_groups.iterrows():
            logger.info(f"  {group['base_company_name']}: {group['site_count']} sites")
        
        return multi_site_groups

    def create_site_aggregation_report(self, aggregation_df: pd.DataFrame) -> Dict:
        """Create a comprehensive report of the site aggregation results"""
        
        if len(aggregation_df) == 0:
            return {"error": "No aggregation data available"}
        
        # Calculate statistics
        total_records = len(aggregation_df)
        site_records = len(aggregation_df[aggregation_df['record_type'] == 'Site'])
        standalone_records = len(aggregation_df[aggregation_df['record_type'] == 'Standalone'])
        parent_records = len(aggregation_df[aggregation_df['record_type'] == 'Parent_Aggregator'])
        
        # Contact statistics
        total_contacts = aggregation_df['contact_count'].sum()
        companies_with_contacts = len(aggregation_df[aggregation_df['contact_count'] > 0])
        
        # Multi-site analysis
        multi_site_companies = aggregation_df[aggregation_df['has_multiple_sites'] == True]
        
        report = {
            'summary': {
                'total_records': total_records,
                'site_records': site_records,
                'standalone_records': standalone_records,
                'parent_aggregator_records': parent_records,
                'total_contacts': total_contacts,
                'companies_with_contacts': companies_with_contacts
            },
            'multi_site_analysis': {
                'total_multi_site_groups': len(multi_site_companies),
                'top_multi_site_companies': multi_site_companies.nlargest(5, 'contact_count')[
                    ['base_company_name', 'contact_count', 'location_extracted']
                ].to_dict('records') if len(multi_site_companies) > 0 else []
            },
            'contact_distribution': {
                'companies_with_no_contacts': len(aggregation_df[aggregation_df['contact_count'] == 0]),
                'companies_with_1_contact': len(aggregation_df[aggregation_df['contact_count'] == 1]),
                'companies_with_multiple_contacts': len(aggregation_df[aggregation_df['contact_count'] > 1]),
                'max_contacts_per_company': aggregation_df['contact_count'].max()
            }
        }
        
        return report

# Global processor instance
site_aggregation_processor = SiteAggregationProcessor()
