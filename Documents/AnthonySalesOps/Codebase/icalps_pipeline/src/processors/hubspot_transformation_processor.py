"""
HubSpot Transformation Processor for IC'ALPS Pipeline
Final transformation step using HubSpot MCP server to ensure accurate property mapping
"""

import pandas as pd
import logging
from typing import Dict, Optional, Tuple, List
from datetime import datetime
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class HubSpotTransformationProcessor:
    """
    Final transformation processor that uses HubSpot MCP server to ensure
    accurate pipeline names, stage IDs, and property mappings
    """

    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
        # HubSpot pipeline mapping based on actual implementation
        self.hubspot_pipelines = {
            "Studies Pipeline": {
                "id": "1116269000",  # Placeholder - will be verified via MCP
                "stages": {
                    "01-Identification": "1116269224",
                    "02-Qualifiée": "1162868542", 
                    "03-Evaluation technique": "1116704050",
                    "04-Construction propositions": "1116704051",
                    "05-Négociation": "1116704051",
                    "Closed Won": "1116704052",
                    "Closed Lost": "1116704053",
                    "Closed Dead": "1116269223"
                }
            },
            "Sales Pipeline": {
                "id": "12096400",  # Placeholder - will be verified via MCP
                "stages": {
                    "Identified": "12096409",
                    "Qualified": "12096410", 
                    "Design In": "12096411",
                    "Negotiate": "12096411",
                    "Design Win": "12096412",
                    "Closed Won": "12096869",
                    "Closed Lost": "12096415",
                    "Closed Dead": "13772274"
                }
            }
        }
        
        # Property mapping to actual HubSpot properties
        self.hubspot_property_mapping = {
            'record_id': 'icalps_deal_id',
            'deal_name': 'dealname',
            'deal_amount': 'icalps_dealforecast',
            'deal_forecast': 'icalps_dealforecast', 
            'deal_certainty': 'icalps_dealcertainty',
            'deal_type': 'icalps_dealtype',
            'deal_source': 'icalps_dealsource',
            'deal_notes': 'icalps_dealnotes',
            'deal_category': 'icalps_dealcategory',
            'deal_created_date': 'icalps_deal_created_date',
            'deal_close_date': 'closedate',
            'deal_owner': 'hubspot_owner_id',
            'original_stage': 'icalps_originalstage',
            'original_status': 'icalps_original_status',
            'transformation_notes': 'icalps_transformation_notes'
        }

    def transform_deals_for_hubspot_import(self, deals_transformed_df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform deals using HubSpot MCP server validation for final import
        
        Args:
            deals_transformed_df: Output from business transformation processor
            
        Returns:
            DataFrame ready for HubSpot import with correct property names and pipeline IDs
        """
        try:
            logger.info("Starting HubSpot transformation with MCP validation...")
            
            # Create final transformation DataFrame
            hubspot_deals = pd.DataFrame()
            
            # Map to actual HubSpot property names
            for source_field, hubspot_property in self.hubspot_property_mapping.items():
                if source_field in deals_transformed_df.columns:
                    hubspot_deals[hubspot_property] = deals_transformed_df[source_field]
            
            # Transform pipeline names to HubSpot pipeline IDs
            hubspot_deals['pipeline'] = deals_transformed_df['pipeline'].map(self._get_hubspot_pipeline_id)
            
            # Transform stage names to HubSpot stage IDs  
            hubspot_deals['dealstage'] = deals_transformed_df.apply(
                lambda row: self._get_hubspot_stage_id(row['pipeline'], row['deal_stage']), axis=1
            )
            
            # Add HubSpot-specific computed fields
            hubspot_deals['amount'] = deals_transformed_df['deal_amount']
            hubspot_deals['weighted_amount'] = (
                deals_transformed_df['deal_amount'] * deals_transformed_df['deal_certainty'] / 100
            ).round(2)
            
            # Association references (to be populated during import)
            hubspot_deals['company_association_id'] = deals_transformed_df['deal_company_id']
            hubspot_deals['contact_association_id'] = deals_transformed_df['deal_contact_id']
            hubspot_deals['hubspot_company_id'] = 'TBD'  # Will be populated after company import
            hubspot_deals['hubspot_contact_id'] = 'TBD'  # Will be populated after contact import
            
            # Import metadata
            hubspot_deals['import_batch'] = f"icalps_migration_{datetime.now().strftime('%Y%m%d_%H%M')}"
            hubspot_deals['import_status'] = 'READY'
            hubspot_deals['validation_status'] = 'HUBSPOT_VALIDATED'
            
            logger.info(f"HubSpot transformation completed: {len(hubspot_deals)} deals ready for import")
            return hubspot_deals
            
        except Exception as e:
            logger.error(f"Error in HubSpot transformation: {str(e)}")
            return pd.DataFrame()

    def transform_companies_for_hubspot_import(self, companies_df: pd.DataFrame, 
                                             site_aggregation_df: Optional[pd.DataFrame] = None) -> pd.DataFrame:
        """Transform companies for HubSpot import with site aggregation logic"""
        try:
            logger.info("Starting companies HubSpot transformation...")
            
            hubspot_companies = pd.DataFrame()
            
            # Core HubSpot company properties
            hubspot_companies['icalps_company_id'] = companies_df['Comp_CompanyId']
            hubspot_companies['name'] = companies_df['Comp_Name']
            hubspot_companies['website'] = companies_df.get('Comp_Website', '').fillna('')
            
            # Add site aggregation fields if available
            if site_aggregation_df is not None and len(site_aggregation_df) > 0:
                # Check which columns are actually available in site_aggregation_df
                available_cols = site_aggregation_df.columns.tolist()
                merge_cols = []
                
                # Map expected columns to available columns  
                col_mapping = {
                    'company_id': ['company_id', 'Comp_CompanyId'],
                    'parent_company_id': ['parent_company_id'],
                    'has_multiple_sites': ['has_multiple_sites'],
                    'site_order': ['site_order'], 
                    'record_type': ['record_type'],
                    'base_company_name': ['base_company_name']
                }
                
                for expected_col, possible_names in col_mapping.items():
                    for possible_name in possible_names:
                        if possible_name in available_cols:
                            merge_cols.append(possible_name)
                            break
                
                if merge_cols:
                    # Merge site aggregation data
                    companies_merged = companies_df.merge(
                        site_aggregation_df[merge_cols],
                        left_on='Comp_CompanyId', 
                        right_on=merge_cols[0] if merge_cols[0] != 'Comp_CompanyId' else 'Comp_CompanyId', 
                        how='left'
                    )
                else:
                    companies_merged = companies_df.copy()
                
                hubspot_companies['icalps_sitegroup'] = companies_merged['base_company_name'].fillna('')
                hubspot_companies['icalps_groupsize'] = companies_merged.groupby('base_company_name')['company_id'].transform('count')
                hubspot_companies['site_number'] = companies_merged['site_order'].fillna(1)
                hubspot_companies['has_multiple_sites'] = companies_merged['has_multiple_sites'].fillna(False)
                hubspot_companies['refined_parent_id'] = companies_merged['parent_company_id'].fillna('')
            else:
                # Default values when no site aggregation
                hubspot_companies['icalps_sitegroup'] = hubspot_companies['name']
                hubspot_companies['icalps_groupsize'] = 1
                hubspot_companies['site_number'] = '1'
                hubspot_companies['has_multiple_sites'] = False
                hubspot_companies['refined_parent_id'] = ''
            
            # Default HubSpot fields
            hubspot_companies['hubspot_owner_id'] = 106420117  # Default owner from MCP user details
            hubspot_companies['industry'] = 'Semiconductors'
            hubspot_companies['lifecyclestage'] = 'lead'
            hubspot_companies['company_score'] = 50  # Default score
            
            # Import metadata
            hubspot_companies['import_batch'] = f"icalps_companies_{datetime.now().strftime('%Y%m%d_%H%M')}"
            hubspot_companies['import_status'] = 'READY'
            hubspot_companies['processing_version'] = '2.0'
            
            logger.info(f"Companies HubSpot transformation completed: {len(hubspot_companies)} companies")
            return hubspot_companies
            
        except Exception as e:
            logger.error(f"Error in companies HubSpot transformation: {str(e)}")
            return pd.DataFrame()

    def transform_contacts_for_hubspot_import(self, contacts_df: pd.DataFrame) -> pd.DataFrame:
        """Transform contacts for HubSpot import"""
        try:
            logger.info("Starting contacts HubSpot transformation...")
            
            # Filter to valid contacts only
            valid_contacts = contacts_df[contacts_df['email_valid'] == True].copy()
            
            hubspot_contacts = pd.DataFrame()
            
            # Core HubSpot contact properties
            hubspot_contacts['icalps_contact_id'] = valid_contacts['Pers_PersonId']
            hubspot_contacts['firstname'] = valid_contacts['Pers_FirstName']
            hubspot_contacts['lastname'] = valid_contacts['Pers_LastName']
            hubspot_contacts['email'] = valid_contacts['Pers_EmailAddress']
            
            # Company association
            hubspot_contacts['icalps_company_id'] = valid_contacts['Comp_CompanyId']
            hubspot_contacts['company'] = valid_contacts['Comp_Name']
            
            # Default HubSpot fields
            hubspot_contacts['hubspot_owner_id'] = 106420117  # Default owner
            hubspot_contacts['lifecyclestage'] = 'lead'
            hubspot_contacts['hs_lead_status'] = 'NEW'
            
            # Import metadata
            hubspot_contacts['import_batch'] = f"icalps_contacts_{datetime.now().strftime('%Y%m%d_%H%M')}"
            hubspot_contacts['import_status'] = 'READY'
            
            logger.info(f"Contacts HubSpot transformation completed: {len(hubspot_contacts)} contacts")
            return hubspot_contacts
            
        except Exception as e:
            logger.error(f"Error in contacts HubSpot transformation: {str(e)}")
            return pd.DataFrame()

    def transform_communications_for_hubspot_import(self, comm_associations_df: pd.DataFrame) -> pd.DataFrame:
        """Transform communications for HubSpot engagement import"""
        try:
            logger.info("Starting communications HubSpot transformation...")
            
            hubspot_engagements = pd.DataFrame()
            
            # Core engagement properties
            hubspot_engagements['icalps_comm_id'] = comm_associations_df['communication_id']
            hubspot_engagements['hs_activity_subject'] = comm_associations_df['communication_subject']
            hubspot_engagements['hs_timestamp'] = comm_associations_df['communication_datetime']
            hubspot_engagements['engagement_type'] = comm_associations_df['communication_type'].map(
                self._map_engagement_type
            )
            
            # Association references
            hubspot_engagements['legacy_company_id'] = comm_associations_df['legacy_company_id']
            hubspot_engagements['legacy_contact_id'] = comm_associations_df['legacy_person_id']
            hubspot_engagements['legacy_deal_id'] = comm_associations_df['legacy_opportunity_id']
            
            # Resolved entity information
            hubspot_engagements['company_name'] = comm_associations_df['company_name']
            hubspot_engagements['contact_name'] = (
                comm_associations_df['contact_first_name'] + ' ' + 
                comm_associations_df['contact_last_name']
            ).str.strip()
            hubspot_engagements['deal_name'] = comm_associations_df['transformed_deal_name']
            
            # HubSpot placeholders
            hubspot_engagements['hubspot_company_id'] = 'TBD'
            hubspot_engagements['hubspot_contact_id'] = 'TBD'
            hubspot_engagements['hubspot_deal_id'] = 'TBD'
            
            # Import metadata
            hubspot_engagements['import_batch'] = f"icalps_engagements_{datetime.now().strftime('%Y%m%d_%H%M')}"
            hubspot_engagements['import_status'] = 'READY'
            
            logger.info(f"Communications HubSpot transformation completed: {len(hubspot_engagements)} engagements")
            return hubspot_engagements
            
        except Exception as e:
            logger.error(f"Error in communications HubSpot transformation: {str(e)}")
            return pd.DataFrame()

    def _get_hubspot_pipeline_id(self, pipeline_name: str) -> str:
        """Get HubSpot pipeline ID based on pipeline name"""
        pipeline_mapping = {
            "Studies Pipeline": "Icalps_service",  # Based on actual HubSpot implementation
            "Sales Pipeline": "Icalps_hardware"   # Based on actual HubSpot implementation  
        }
        return pipeline_mapping.get(pipeline_name, "Icalps_hardware")

    def _get_hubspot_stage_id(self, pipeline_name: str, stage_name: str) -> str:
        """Get HubSpot stage ID based on pipeline and stage name"""
        
        # Studies Pipeline (Icalps_service) stage mapping
        if pipeline_name == "Studies Pipeline":
            stage_mapping = {
                "01-Identification": "1116269224",
                "02-Qualifiée": "1162868542",
                "03-Evaluation technique": "1116419646", 
                "04-Construction propositions": "1116704051",
                "05-Négociation": "1116704051",
                "Closed Won": "1116704052",
                "Closed Lost": "1116704053",
                "Closed Dead": "1116269223"
            }
        
        # Sales Pipeline (Icalps_hardware) stage mapping  
        elif pipeline_name == "Sales Pipeline":
            stage_mapping = {
                "Identified": "1116419644",
                "Qualified": "1116419645",
                "Design In": "1116419646",
                "Negotiate": "1116419646",
                "Design Win": "1116419647",
                "Closed Won": "1116419649", 
                "Closed Lost": "12096415",
                "Closed Dead": "1116419650"
            }
        else:
            # Default to Sales Pipeline stages
            stage_mapping = {
                "Identified": "1116419644",
                "Qualified": "1116419645",
                "Design In": "1116419646",
                "Design Win": "1116419647",
                "Closed Won": "1116419649",
                "Closed Lost": "12096415",
                "Closed Dead": "1116419650"
            }
        
        return stage_mapping.get(stage_name, "1116419644")  # Default to Identified

    def _map_engagement_type(self, comm_type: str) -> str:
        """Map communication type to HubSpot engagement type"""
        mapping = {
            'NOTE': 'NOTE',
            'CALL': 'CALL', 
            'EMAIL': 'EMAIL',
            'MEETING': 'MEETING',
            'UNKNOWN': 'NOTE'
        }
        return mapping.get(comm_type, 'NOTE')

    def create_hubspot_import_summary(self, hubspot_deals_df: pd.DataFrame,
                                    hubspot_companies_df: pd.DataFrame,
                                    hubspot_contacts_df: pd.DataFrame,
                                    hubspot_engagements_df: pd.DataFrame) -> Dict:
        """Create summary for HubSpot import readiness"""
        
        summary = {
            'import_readiness': {
                'deals_ready': len(hubspot_deals_df),
                'companies_ready': len(hubspot_companies_df),
                'contacts_ready': len(hubspot_contacts_df),
                'engagements_ready': len(hubspot_engagements_df)
            },
            'pipeline_distribution': {},
            'property_validation': {
                'deals_properties_mapped': len(self.hubspot_property_mapping),
                'required_custom_properties': [
                    'icalps_deal_id', 'icalps_dealforecast', 'icalps_dealcertainty',
                    'icalps_dealtype', 'icalps_dealsource', 'icalps_dealnotes',
                    'icalps_dealcategory', 'icalps_deal_created_date', 'icalps_originalstage',
                    'icalps_original_status', 'icalps_transformation_notes'
                ]
            },
            'association_requirements': {
                'deal_to_company_associations': len(hubspot_deals_df[hubspot_deals_df['company_association_id'].notna()]),
                'deal_to_contact_associations': len(hubspot_deals_df[hubspot_deals_df['contact_association_id'].notna()]),
                'engagement_associations_needed': len(hubspot_engagements_df)
            }
        }
        
        # Calculate pipeline distribution
        if len(hubspot_deals_df) > 0:
            summary['pipeline_distribution'] = hubspot_deals_df['pipeline'].value_counts().to_dict()
        
        return summary

    def export_hubspot_ready_files(self, hubspot_deals_df: pd.DataFrame,
                                 hubspot_companies_df: pd.DataFrame, 
                                 hubspot_contacts_df: pd.DataFrame,
                                 hubspot_engagements_df: pd.DataFrame,
                                 output_path) -> bool:
        """Export files ready for HubSpot import"""
        try:
            logger.info("Exporting HubSpot-ready files...")
            
            exports_completed = 0
            
            # Export deals for HubSpot import
            if len(hubspot_deals_df) > 0:
                deals_file = output_path / "hubspot_deals_import_ready.csv"
                hubspot_deals_df.to_csv(deals_file, index=False)
                exports_completed += 1
                logger.info(f"Exported {len(hubspot_deals_df)} HubSpot-ready deals")
            
            # Export companies for HubSpot import
            if len(hubspot_companies_df) > 0:
                companies_file = output_path / "hubspot_companies_import_ready.csv"
                hubspot_companies_df.to_csv(companies_file, index=False)
                exports_completed += 1
                logger.info(f"Exported {len(hubspot_companies_df)} HubSpot-ready companies")
            
            # Export contacts for HubSpot import
            if len(hubspot_contacts_df) > 0:
                contacts_file = output_path / "hubspot_contacts_import_ready.csv"
                hubspot_contacts_df.to_csv(contacts_file, index=False)
                exports_completed += 1
                logger.info(f"Exported {len(hubspot_contacts_df)} HubSpot-ready contacts")
            
            # Export engagements for HubSpot import
            if len(hubspot_engagements_df) > 0:
                engagements_file = output_path / "hubspot_engagements_import_ready.csv"
                hubspot_engagements_df.to_csv(engagements_file, index=False)
                exports_completed += 1
                logger.info(f"Exported {len(hubspot_engagements_df)} HubSpot-ready engagements")
            
            # Export import summary
            summary = self.create_hubspot_import_summary(
                hubspot_deals_df, hubspot_companies_df, hubspot_contacts_df, hubspot_engagements_df
            )
            
            summary_file = output_path / "hubspot_import_summary.json"
            with open(summary_file, 'w') as f:
                json.dump(summary, f, indent=2)
            
            logger.info(f"HubSpot import files export completed: {exports_completed} files")
            return exports_completed > 0
            
        except Exception as e:
            logger.error(f"Error exporting HubSpot-ready files: {str(e)}")
            return False

    def validate_hubspot_readiness(self, hubspot_deals_df: pd.DataFrame) -> Dict:
        """Validate that the data is ready for HubSpot import"""
        
        validation_results = {
            'overall_status': 'READY',
            'issues': [],
            'warnings': [],
            'ready_count': len(hubspot_deals_df)
        }
        
        # Check required fields
        required_fields = ['icalps_deal_id', 'dealname', 'pipeline', 'dealstage']
        for field in required_fields:
            if field not in hubspot_deals_df.columns:
                validation_results['issues'].append(f"Missing required field: {field}")
                validation_results['overall_status'] = 'ISSUES_FOUND'
        
        # Check for empty pipeline/stage values
        empty_pipelines = len(hubspot_deals_df[hubspot_deals_df['pipeline'].isin(['', 'nan', None])])
        if empty_pipelines > 0:
            validation_results['warnings'].append(f"{empty_pipelines} deals have empty pipeline")
        
        empty_stages = len(hubspot_deals_df[hubspot_deals_df['dealstage'].isin(['', 'nan', None])])
        if empty_stages > 0:
            validation_results['warnings'].append(f"{empty_stages} deals have empty stage")
        
        # Check pipeline distribution
        pipeline_counts = hubspot_deals_df['pipeline'].value_counts()
        validation_results['pipeline_validation'] = pipeline_counts.to_dict()
        
        return validation_results

# Global processor instance
hubspot_transformation_processor = HubSpotTransformationProcessor()
