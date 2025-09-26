"""
Associations Processor for IC'ALPS Pipeline
Handles communications and social networks associations with companies and contacts
"""

import pandas as pd
import logging
from typing import Dict, Optional, Tuple
import re

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AssociationsProcessor:
    """Processes communications and social networks data with proper entity associations"""

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def process_communications_with_associations(self, communications_df: pd.DataFrame, 
                                              companies_df: pd.DataFrame, 
                                              contacts_df: pd.DataFrame) -> pd.DataFrame:
        """
        Process communications data and create associations with companies and contacts
        
        Args:
            communications_df: Raw communications data
            companies_df: Enhanced companies data
            contacts_df: Enhanced contacts data
            
        Returns:
            DataFrame with communication associations
        """
        try:
            logger.info("Processing communications with associations...")
            
            # Create a copy to work with
            comm_df = communications_df.copy()
            
            # Create company lookup
            company_lookup = companies_df.set_index('Comp_CompanyId')['Comp_Name'].to_dict()
            
            # Create contact lookup  
            contact_lookup = contacts_df.set_index('Pers_PersonId')[['Pers_FirstName', 'Pers_LastName', 'Pers_EmailAddress']].to_dict('index')
            
            # Build the associations dataframe
            result = pd.DataFrame()
            
            # Core communication fields
            result['communication_id'] = comm_df['Comm_CommunicationId']
            result['communication_subject'] = comm_df['Comm_Subject'].fillna('')
            result['communication_from'] = comm_df['Comm_From'].fillna('')
            result['communication_to'] = comm_df['Comm_TO'].fillna('')
            result['communication_datetime'] = comm_df['Comm_DateTime']
            result['communication_type'] = comm_df['comm_type']
            
            # Legacy associations
            result['legacy_opportunity_id'] = comm_df['Oppo_OpportunityId'].fillna('')
            result['legacy_person_id'] = comm_df['Pers_PersonId'].fillna('')
            result['legacy_company_id'] = comm_df['Comp_CompanyId'].fillna('')
            
            # Resolve entity names
            result['company_name'] = comm_df['Comp_CompanyId'].map(company_lookup).fillna('Unknown Company')
            
            # Resolve contact names
            result['contact_first_name'] = comm_df['Pers_PersonId'].map(
                lambda x: contact_lookup.get(x, {}).get('Pers_FirstName', '') if pd.notna(x) else ''
            )
            result['contact_last_name'] = comm_df['Pers_PersonId'].map(
                lambda x: contact_lookup.get(x, {}).get('Pers_LastName', '') if pd.notna(x) else ''
            )
            result['contact_email'] = comm_df['Pers_PersonId'].map(
                lambda x: contact_lookup.get(x, {}).get('Pers_EmailAddress', '') if pd.notna(x) else ''
            )
            
            # Association status tracking
            result['company_association_status'] = comm_df['Comp_CompanyId'].apply(
                lambda x: 'SUCCESS' if pd.notna(x) and x in company_lookup else 'NO_COMPANY_FOUND'
            )
            result['contact_association_status'] = comm_df['Pers_PersonId'].apply(
                lambda x: 'SUCCESS' if pd.notna(x) and x in contact_lookup else 'NO_CONTACT_FOUND'
            )
            
            # HubSpot placeholders
            result['hubspot_company_id'] = 'TBD'
            result['hubspot_contact_id'] = 'TBD'
            result['hubspot_deal_id'] = 'TBD'
            
            # Metadata
            result['processed_date'] = pd.Timestamp.now().strftime('%m/%d/%Y %H:%M')
            result['source_file'] = 'Legacy_comm.csv'
            
            logger.info(f"Processed {len(result)} communications with associations")
            return result
            
        except Exception as e:
            logger.error(f"Error processing communications associations: {str(e)}")
            return pd.DataFrame()

    def process_social_networks_with_associations(self, social_df: pd.DataFrame,
                                                companies_df: pd.DataFrame,
                                                contacts_df: pd.DataFrame) -> pd.DataFrame:
        """
        Process social networks data and create associations with companies and contacts
        
        Args:
            social_df: Raw social networks data
            companies_df: Enhanced companies data
            contacts_df: Enhanced contacts data
            
        Returns:
            DataFrame with social network associations
        """
        try:
            logger.info("Processing social networks with associations...")
            
            # Create a copy to work with
            sn_df = social_df.copy()
            
            # Filter out auto-generated placeholders
            sn_df = sn_df[sn_df['sone_networklink'] != '#AUTO#'].copy()
            
            # Create lookup dictionaries
            company_lookup = companies_df.set_index('Comp_CompanyId')['Comp_Name'].to_dict()
            contact_lookup = contacts_df.set_index('Pers_PersonId')[['Pers_FirstName', 'Pers_LastName']].to_dict('index')
            
            # Build the associations dataframe
            result = pd.DataFrame()
            
            # Core social network fields
            result['network_link'] = sn_df['sone_networklink']
            result['network_type'] = sn_df['network_type']
            result['related_table_id'] = sn_df['Related_TableID']  # 5=Company, 13=Person
            result['related_record_id'] = sn_df['Related_RecordID']
            result['entity_caption'] = sn_df['bord_caption']
            
            # Determine entity type
            result['entity_type'] = sn_df['Related_TableID'].map({5: 'Company', 13: 'Person'}).fillna('Unknown')
            
            # Resolve entity information based on table ID
            result['entity_name'] = sn_df.apply(self._resolve_entity_name, axis=1, 
                                              company_lookup=company_lookup, contact_lookup=contact_lookup)
            
            # Association tracking
            result['association_status'] = sn_df.apply(self._check_association_status, axis=1,
                                                     company_lookup=company_lookup, contact_lookup=contact_lookup)
            
            # Legacy reference fields
            result['legacy_company_id'] = sn_df.apply(
                lambda row: row['Related_RecordID'] if row['Related_TableID'] == 5 else '', axis=1
            )
            result['legacy_contact_id'] = sn_df.apply(
                lambda row: row['Related_RecordID'] if row['Related_TableID'] == 13 else '', axis=1
            )
            
            # HubSpot placeholders
            result['hubspot_company_id'] = 'TBD'
            result['hubspot_contact_id'] = 'TBD'
            
            # Determine HubSpot property mapping
            result['hubspot_property_target'] = sn_df.apply(self._determine_hubspot_property, axis=1)
            
            # Metadata
            result['processed_date'] = pd.Timestamp.now().strftime('%m/%d/%Y %H:%M')
            result['source_file'] = 'legacy_socialnetworks.csv'
            
            logger.info(f"Processed {len(result)} social network associations")
            return result
            
        except Exception as e:
            logger.error(f"Error processing social networks associations: {str(e)}")
            return pd.DataFrame()

    def _resolve_entity_name(self, row, company_lookup, contact_lookup):
        """Resolve entity name based on table ID and record ID"""
        if row['Related_TableID'] == 5:  # Company
            return company_lookup.get(row['Related_RecordID'], f"Company_{row['Related_RecordID']}")
        elif row['Related_TableID'] == 13:  # Person
            contact_info = contact_lookup.get(row['Related_RecordID'], {})
            first_name = contact_info.get('Pers_FirstName', '')
            last_name = contact_info.get('Pers_LastName', '')
            if first_name or last_name:
                return f"{first_name} {last_name}".strip()
            else:
                return f"Contact_{row['Related_RecordID']}"
        else:
            return "Unknown Entity"

    def _check_association_status(self, row, company_lookup, contact_lookup):
        """Check if the association can be resolved"""
        if row['Related_TableID'] == 5:  # Company
            return 'SUCCESS' if row['Related_RecordID'] in company_lookup else 'NO_COMPANY_FOUND'
        elif row['Related_TableID'] == 13:  # Person
            return 'SUCCESS' if row['Related_RecordID'] in contact_lookup else 'NO_CONTACT_FOUND'
        else:
            return 'UNKNOWN_ENTITY_TYPE'

    def _determine_hubspot_property(self, row):
        """Determine which HubSpot property should store this social network link"""
        link = str(row['sone_networklink']).lower()
        table_id = row['Related_TableID']
        
        # LinkedIn URL mapping
        if 'in/' in link or 'linkedin.com' in link:
            if table_id == 13:  # Person
                return 'linkedin_bio'
            elif table_id == 5:  # Company  
                return 'linkedin_company_page'
        
        # Company website mapping
        elif 'company/' in link or any(domain in link for domain in ['.com', '.org', '.net', '.fr']):
            return 'website'
            
        # Twitter/X mapping
        elif 'twitter.com' in link or 'x.com' in link:
            if table_id == 13:  # Person
                return 'twitterhandle'
            else:
                return 'twitterhandle'  # Company twitter
                
        # Facebook mapping
        elif 'facebook.com' in link:
            if table_id == 5:  # Company
                return 'facebook_company_page'
            else:
                return 'hs_facebookid'
                
        else:
            return 'website'  # Default fallback

# Global processor instance
associations_processor = AssociationsProcessor()
