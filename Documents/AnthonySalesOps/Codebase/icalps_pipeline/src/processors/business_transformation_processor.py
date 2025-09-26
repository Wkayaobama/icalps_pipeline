"""
Business Transformation Processor for IC'ALPS Pipeline
Applies business process rules to transform deals before site aggregation
Based on deals_transformed_tohubspot.csv logic
"""

import pandas as pd
import logging
from typing import Dict, Optional, Tuple
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BusinessTransformationProcessor:
    """Applies IC'ALPS business process rules to transform deals according to business logic"""

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def transform_deals_to_hubspot_format(self, opportunities_df: pd.DataFrame, 
                                        companies_df: pd.DataFrame,
                                        contacts_df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform deals according to business process rules matching deals_transformed_tohubspot.csv
        
        This is the critical intermediary step that applies business logic before site aggregation
        """
        try:
            logger.info("Starting business transformation to HubSpot format...")
            
            # Create a copy to work with
            deals_df = opportunities_df.copy()
            
            # Apply business transformation rules
            transformed_df = pd.DataFrame()
            
            # Core deal fields with business logic applied
            transformed_df['record_id'] = deals_df['Oppo_OpportunityId']
            transformed_df['deal_name'] = deals_df['Oppo_Description'].fillna('')
            
            # Apply pipeline logic (critical business rule)
            transformed_df['pipeline'] = deals_df['Oppo_Type'].apply(self._determine_pipeline)
            
            # Apply deal stage business logic (status + stage + certainty)
            deal_stages = deals_df.apply(self._transform_deal_stage, axis=1)
            transformed_df['deal_stage'] = deal_stages.apply(lambda x: x[0])
            transformed_df['deal_status'] = deal_stages.apply(lambda x: x[1])
            transformed_df['transformation_notes'] = deal_stages.apply(lambda x: x[2])
            
            # Financial fields with business logic
            transformed_df['deal_amount'] = deals_df['Oppo_Forecast'].fillna(0)
            transformed_df['deal_forecast'] = deals_df['Oppo_Forecast'].fillna(0)
            transformed_df['deal_certainty'] = deals_df['Oppo_Certainty'].fillna(0)
            
            # Deal classification
            transformed_df['deal_type'] = deals_df['Oppo_Type'].fillna('')
            transformed_df['deal_source'] = deals_df['Oppo_Source'].fillna('')
            transformed_df['deal_brand'] = 'ICALPS'  # Default brand
            transformed_df['deal_notes'] = deals_df['Oppo_Note'].fillna('')
            
            # Determine deal category based on type
            transformed_df['deal_category'] = deals_df['Oppo_Type'].apply(
                lambda x: 'Study' if x == 'Preetude' else 'Opportunity'
            )
            
            # Date fields
            transformed_df['deal_created_date'] = deals_df['Oppo_CreatedDate']
            transformed_df['deal_close_date'] = deals_df['Oppo_TargetClose']
            
            # Association fields
            transformed_df['deal_company_id'] = deals_df['Oppo_PrimaryCompanyId']
            transformed_df['deal_contact_id'] = deals_df['Oppo_PrimaryPersonId']
            transformed_df['deal_owner'] = deals_df['Oppo_AssignedUserId'].fillna(27)  # Default owner
            
            # Original tracking fields
            transformed_df['original_stage'] = deals_df['Oppo_Stage'].fillna('')
            transformed_df['original_status'] = deals_df['Oppo_Status'].fillna('')
            
            # Processing metadata
            transformed_df['processing_date'] = datetime.now().strftime('%d/%m/%Y')
            
            logger.info(f"Business transformation completed: {len(transformed_df)} deals transformed")
            return transformed_df
            
        except Exception as e:
            logger.error(f"Error in business transformation: {str(e)}")
            return pd.DataFrame()

    def _determine_pipeline(self, oppo_type: str) -> str:
        """Determine pipeline based on opportunity type (critical business rule)"""
        if pd.isna(oppo_type) or oppo_type == '':
            return 'Sales Pipeline'
        
        oppo_type_clean = str(oppo_type).strip()
        
        if oppo_type_clean == 'Preetude':
            return 'Studies Pipeline'
        else:
            return 'Sales Pipeline'

    def _transform_deal_stage(self, row) -> Tuple[str, str, str]:
        """
        Apply business logic to transform deal stage based on status, stage, and certainty
        Returns: (hubspot_stage, deal_status, transformation_notes)
        """
        oppo_status = str(row.get('Oppo_Status', '')).strip()
        oppo_stage = str(row.get('Oppo_Stage', '')).strip()
        oppo_certainty = row.get('Oppo_Certainty', 0)
        oppo_type = str(row.get('Oppo_Type', '')).strip()
        
        # Convert certainty to numeric
        try:
            certainty = float(oppo_certainty) if pd.notna(oppo_certainty) else 0
        except:
            certainty = 0
        
        # Business Rule 1: Abandoned deals with low certainty go to Closed Dead
        if oppo_status in ['Abandonne', 'Abandonnee'] and certainty <= 10:
            return 'Closed Dead', 'Lost', 'Moved to Closed Dead (abandoned + low certainty)'
        
        # Business Rule 2: Won deals
        if oppo_status == 'Won':
            return 'Closed Won', 'Won', 'Deal successfully closed'
        
        # Business Rule 3: Lost deals  
        if oppo_status in ['Lost', 'NoGo']:
            return 'Closed Lost', 'Lost', 'Deal closed as lost'
        
        # Business Rule 4: Sleap status (keep in pipeline)
        if oppo_status == 'Sleap':
            if oppo_type == 'Preetude':
                return '05-Négociation', 'In Progress', 'Deal on hold (Sleap)'
            else:
                return 'Design Win', 'In Progress', 'Deal on hold (Sleap)'
        
        # Business Rule 5: Active deals - map stage based on pipeline
        if oppo_status == 'In Progress' or oppo_status == '':
            return self._map_active_stage(oppo_stage, oppo_type)
        
        # Default fallback
        return 'Identified', 'In Progress', 'Default mapping applied'

    def _map_active_stage(self, oppo_stage: str, oppo_type: str) -> Tuple[str, str, str]:
        """Map active deal stages based on pipeline type"""
        
        stage_clean = oppo_stage.lower().strip()
        
        if oppo_type == 'Preetude':  # Studies Pipeline
            stage_mapping = {
                'identification': ('01-Identification', 'In Progress', 'Mapped to Studies Pipeline'),
                'qualified': ('02-Qualifiée', 'In Progress', 'Mapped to Studies Pipeline'),
                'evaluation technique': ('03-Evaluation technique', 'In Progress', 'Mapped to Studies Pipeline'),
                'construction propositions': ('04-Construction propositions', 'In Progress', 'Mapped to Studies Pipeline'),
                'construction offre': ('04-Construction propositions', 'In Progress', 'Mapped to Studies Pipeline'),
                'negotiating': ('05-Négociation', 'In Progress', 'Mapped to Studies Pipeline'),
                'negociation': ('05-Négociation', 'In Progress', 'Mapped to Studies Pipeline')
            }
        else:  # Sales Pipeline
            stage_mapping = {
                'identification': ('Identified', 'In Progress', 'Mapped to Sales Pipeline'),
                'qualified': ('Qualified', 'In Progress', 'Mapped to Sales Pipeline'),
                'evaluation technique': ('Design In', 'In Progress', 'Mapped to Sales Pipeline'),
                'construction propositions': ('Negotiate', 'In Progress', 'Mapped to Sales Pipeline'),
                'construction offre': ('Negotiate', 'In Progress', 'Mapped to Sales Pipeline'),
                'negotiating': ('Design Win', 'In Progress', 'Mapped to Sales Pipeline'),
                'negociation': ('Design Win', 'In Progress', 'Mapped to Sales Pipeline')
            }
        
        # Find matching stage
        for key, (stage, status, note) in stage_mapping.items():
            if key in stage_clean:
                return stage, status, note
        
        # Default for unknown stages
        if oppo_type == 'Preetude':
            return '01-Identification', 'In Progress', 'Default Studies Pipeline stage'
        else:
            return 'Identified', 'In Progress', 'Default Sales Pipeline stage'

    def create_communication_associations(self, communications_df: pd.DataFrame,
                                        transformed_deals_df: pd.DataFrame,
                                        companies_df: pd.DataFrame,
                                        contacts_df: pd.DataFrame) -> pd.DataFrame:
        """
        Create communication associations with transformed deals, companies, and contacts
        This is the intermediary step before site aggregation
        """
        try:
            logger.info("Creating communication associations...")
            
            # Create lookup dictionaries for faster joins
            deal_lookup = transformed_deals_df.set_index('record_id').to_dict('index')
            company_lookup = companies_df.set_index('Comp_CompanyId')['Comp_Name'].to_dict()
            contact_lookup = contacts_df.set_index('Pers_PersonId')[['Pers_FirstName', 'Pers_LastName', 'Pers_EmailAddress']].to_dict('index')
            
            # Build communication associations
            comm_associations = pd.DataFrame()
            
            # Core communication fields
            comm_associations['communication_id'] = communications_df['Comm_CommunicationId']
            comm_associations['communication_subject'] = communications_df['Comm_Subject'].fillna('')
            comm_associations['communication_from'] = communications_df['Comm_From'].fillna('')
            comm_associations['communication_to'] = communications_df['Comm_TO'].fillna('')
            comm_associations['communication_datetime'] = communications_df['Comm_DateTime']
            comm_associations['communication_type'] = communications_df['comm_type']
            
            # Legacy associations
            comm_associations['legacy_opportunity_id'] = communications_df['Oppo_OpportunityId'].fillna('')
            comm_associations['legacy_person_id'] = communications_df['Pers_PersonId'].fillna('')
            comm_associations['legacy_company_id'] = communications_df['Comp_CompanyId'].fillna('')
            
            # Resolve transformed deal information
            comm_associations['transformed_deal_name'] = communications_df['Oppo_OpportunityId'].map(
                lambda x: deal_lookup.get(x, {}).get('deal_name', '') if pd.notna(x) else ''
            )
            comm_associations['transformed_deal_pipeline'] = communications_df['Oppo_OpportunityId'].map(
                lambda x: deal_lookup.get(x, {}).get('pipeline', '') if pd.notna(x) else ''
            )
            comm_associations['transformed_deal_stage'] = communications_df['Oppo_OpportunityId'].map(
                lambda x: deal_lookup.get(x, {}).get('deal_stage', '') if pd.notna(x) else ''
            )
            
            # Resolve entity names
            comm_associations['company_name'] = communications_df['Comp_CompanyId'].map(company_lookup).fillna('Unknown Company')
            
            # Resolve contact information
            comm_associations['contact_first_name'] = communications_df['Pers_PersonId'].map(
                lambda x: contact_lookup.get(x, {}).get('Pers_FirstName', '') if pd.notna(x) else ''
            )
            comm_associations['contact_last_name'] = communications_df['Pers_PersonId'].map(
                lambda x: contact_lookup.get(x, {}).get('Pers_LastName', '') if pd.notna(x) else ''
            )
            comm_associations['contact_email'] = communications_df['Pers_PersonId'].map(
                lambda x: contact_lookup.get(x, {}).get('Pers_EmailAddress', '') if pd.notna(x) else ''
            )
            
            # Association status tracking
            comm_associations['deal_association_status'] = communications_df['Oppo_OpportunityId'].apply(
                lambda x: 'SUCCESS' if pd.notna(x) and x in deal_lookup else 'NO_DEAL_FOUND'
            )
            comm_associations['company_association_status'] = communications_df['Comp_CompanyId'].apply(
                lambda x: 'SUCCESS' if pd.notna(x) and x in company_lookup else 'NO_COMPANY_FOUND'
            )
            comm_associations['contact_association_status'] = communications_df['Pers_PersonId'].apply(
                lambda x: 'SUCCESS' if pd.notna(x) and x in contact_lookup else 'NO_CONTACT_FOUND'
            )
            
            # HubSpot placeholders for upload tracking
            comm_associations['hubspot_company_id'] = 'TBD'
            comm_associations['hubspot_contact_id'] = 'TBD'
            comm_associations['hubspot_deal_id'] = 'TBD'
            comm_associations['hubspot_engagement_id'] = 'TBD'
            
            # Processing metadata
            comm_associations['processing_date'] = datetime.now().strftime('%d/%m/%Y')
            comm_associations['source_file'] = 'Legacy_comm.csv'
            
            logger.info(f"Communication associations created: {len(comm_associations)} records")
            return comm_associations
            
        except Exception as e:
            logger.error(f"Error creating communication associations: {str(e)}")
            return pd.DataFrame()

    def generate_transformation_report(self, transformed_deals_df: pd.DataFrame) -> Dict:
        """Generate a report of the business transformation results"""
        
        if len(transformed_deals_df) == 0:
            return {"error": "No transformed deals data available"}
        
        # Pipeline distribution
        pipeline_dist = transformed_deals_df['pipeline'].value_counts().to_dict()
        
        # Stage distribution
        stage_dist = transformed_deals_df['deal_stage'].value_counts().to_dict()
        
        # Status distribution
        status_dist = transformed_deals_df['deal_status'].value_counts().to_dict()
        
        # Financial summary
        total_amount = transformed_deals_df['deal_amount'].sum()
        avg_certainty = transformed_deals_df['deal_certainty'].mean()
        
        # Deal categorization
        studies_count = len(transformed_deals_df[transformed_deals_df['deal_category'] == 'Study'])
        opportunity_count = len(transformed_deals_df[transformed_deals_df['deal_category'] == 'Opportunity'])
        
        report = {
            'transformation_summary': {
                'total_deals_transformed': len(transformed_deals_df),
                'studies_count': studies_count,
                'opportunities_count': opportunity_count,
                'total_deal_value': total_amount,
                'average_certainty': avg_certainty
            },
            'pipeline_distribution': pipeline_dist,
            'stage_distribution': stage_dist,
            'status_distribution': status_dist,
            'business_rules_applied': [
                'Pipeline assignment based on Oppo_Type',
                'Stage transformation based on status + certainty',
                'Abandoned deals with low certainty → Closed Dead',
                'Won/Lost status mapping',
                'Sleap status handling'
            ]
        }
        
        return report

# Global processor instance
business_transformation_processor = BusinessTransformationProcessor()
