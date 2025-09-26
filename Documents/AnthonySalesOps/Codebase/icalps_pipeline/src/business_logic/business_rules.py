"""
Business Rules Engine for IC'ALPS Pipeline
Orchestrates all business logic, validation, and transformation rules
"""

import pandas as pd
import logging
from typing import Dict, List, Tuple, Any, Optional
from business_logic.pipeline_mapper import pipeline_mapper
from business_logic.computed_columns import computed_columns_processor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BusinessRulesEngine:
    """Central engine for applying all business rules and transformations"""

    def __init__(self):
        self.pipeline_mapper = pipeline_mapper
        self.computed_processor = computed_columns_processor
        self.validation_rules = {}
        self.setup_validation_rules()

    def setup_validation_rules(self):
        """Setup data validation rules"""
        self.validation_rules = {
            'companies': {
                'required_fields': ['Comp_CompanyId', 'Comp_Name'],
                'unique_fields': ['Comp_CompanyId'],
                'data_types': {
                    'Comp_CompanyId': 'int',
                    'Comp_Name': 'str'
                }
            },
            'opportunities': {
                'required_fields': ['Oppo_OpportunityId', 'Oppo_PrimaryCompanyId'],
                'unique_fields': ['Oppo_OpportunityId'],
                'data_types': {
                    'Oppo_OpportunityId': 'int',
                    'Oppo_Forecast': 'float',
                    'Oppo_Certainty': 'float'
                },
                'range_checks': {
                    'Oppo_Certainty': (0, 100),
                    'Oppo_Forecast': (0, float('inf'))
                }
            },
            'persons': {
                'required_fields': ['Pers_PersonId', 'Pers_EmailAddress'],
                'unique_fields': ['Pers_PersonId'],
                'data_types': {
                    'Pers_PersonId': 'int'
                },
                'format_checks': {
                    'Pers_EmailAddress': r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
                }
            }
        }

    def validate_data_quality(self, df: pd.DataFrame, entity_type: str) -> Dict[str, Any]:
        """Validate data quality based on business rules"""
        validation_results = {
            'entity_type': entity_type,
            'total_records': len(df),
            'errors': [],
            'warnings': [],
            'quality_score': 1.0
        }

        if entity_type not in self.validation_rules:
            validation_results['warnings'].append(f"No validation rules defined for {entity_type}")
            return validation_results

        rules = self.validation_rules[entity_type]

        # Check required fields
        for field in rules.get('required_fields', []):
            if field not in df.columns:
                validation_results['errors'].append(f"Missing required field: {field}")
                validation_results['quality_score'] -= 0.2
            else:
                null_count = df[field].isnull().sum()
                if null_count > 0:
                    validation_results['warnings'].append(f"Field {field} has {null_count} null values")
                    validation_results['quality_score'] -= 0.1

        # Check unique fields
        for field in rules.get('unique_fields', []):
            if field in df.columns:
                duplicate_count = df[field].duplicated().sum()
                if duplicate_count > 0:
                    validation_results['errors'].append(f"Field {field} has {duplicate_count} duplicates")
                    validation_results['quality_score'] -= 0.3

        # Check data types
        for field, expected_type in rules.get('data_types', {}).items():
            if field in df.columns:
                try:
                    if expected_type == 'int':
                        pd.to_numeric(df[field], errors='raise')
                    elif expected_type == 'float':
                        pd.to_numeric(df[field], errors='raise')
                except:
                    validation_results['warnings'].append(f"Field {field} has invalid {expected_type} values")
                    validation_results['quality_score'] -= 0.05

        # Check range constraints
        for field, (min_val, max_val) in rules.get('range_checks', {}).items():
            if field in df.columns:
                numeric_series = pd.to_numeric(df[field], errors='coerce')
                out_of_range = ((numeric_series < min_val) | (numeric_series > max_val)).sum()
                if out_of_range > 0:
                    validation_results['warnings'].append(f"Field {field} has {out_of_range} values out of range [{min_val}, {max_val}]")
                    validation_results['quality_score'] -= 0.05

        # Ensure quality score doesn't go below 0
        validation_results['quality_score'] = max(0, validation_results['quality_score'])

        return validation_results

    def apply_business_rules_to_opportunities(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply all business rules to opportunities data"""
        try:
            logger.info("Starting business rules application for opportunities")

            # 1. Data quality validation
            validation = self.validate_data_quality(df, 'opportunities')
            if validation['errors']:
                logger.error(f"Data quality errors: {validation['errors']}")

            # 2. Apply pipeline mapping
            df = self.pipeline_mapper.apply_pipeline_mapping(df)

            # 3. Apply computed columns
            df = self.computed_processor.apply_all_computations(df)

            # 4. Apply additional business rules
            df = self._apply_deal_prioritization_rules(df)
            df = self._apply_forecast_adjustment_rules(df)
            df = self._apply_stage_progression_rules(df)

            logger.info(f"Business rules applied successfully to {len(df)} opportunities")
            return df

        except Exception as e:
            logger.error(f"Error applying business rules: {str(e)}")
            return df

    def apply_business_rules_to_companies(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply business rules to companies data"""
        try:
            logger.info("Starting business rules application for companies")

            # Data quality validation
            validation = self.validate_data_quality(df, 'companies')

            # Apply company-specific rules
            df = self._clean_company_data(df)
            df = self._categorize_companies(df)

            logger.info(f"Business rules applied successfully to {len(df)} companies")
            return df

        except Exception as e:
            logger.error(f"Error applying business rules to companies: {str(e)}")
            return df

    def apply_business_rules_to_persons(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply business rules to persons data"""
        try:
            logger.info("Starting business rules application for persons")

            # Data quality validation
            validation = self.validate_data_quality(df, 'persons')

            # Apply person-specific rules
            df = self._clean_person_data(df)
            df = self._enhance_contact_data(df)

            logger.info(f"Business rules applied successfully to {len(df)} persons")
            return df

        except Exception as e:
            logger.error(f"Error applying business rules to persons: {str(e)}")
            return df

    def _apply_deal_prioritization_rules(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply deal prioritization based on business criteria"""
        try:
            # Calculate priority score based on multiple factors
            df['priority_score'] = 0

            # Factor 1: Deal value (normalized)
            if 'Oppo_Forecast' in df.columns:
                max_forecast = df['Oppo_Forecast'].max()
                if max_forecast > 0:
                    df['priority_score'] += (df['Oppo_Forecast'] / max_forecast) * 0.4

            # Factor 2: Certainty level
            if 'Oppo_Certainty' in df.columns:
                df['priority_score'] += (df['Oppo_Certainty'] / 100) * 0.3

            # Factor 3: Deal age (inverse - older deals get lower priority)
            if 'deal_age_days' in df.columns:
                max_age = df['deal_age_days'].max()
                if max_age > 0:
                    df['priority_score'] += (1 - (df['deal_age_days'] / max_age)) * 0.3

            # Convert to priority categories
            df['calculated_priority'] = pd.cut(
                df['priority_score'],
                bins=[0, 0.33, 0.66, 1.0],
                labels=['Low', 'Medium', 'High']
            )

            return df

        except Exception as e:
            logger.error(f"Error in deal prioritization: {str(e)}")
            return df

    def _apply_forecast_adjustment_rules(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply forecast adjustment based on business intelligence"""
        try:
            # Adjust forecast based on stage and historical data
            stage_multipliers = {
                '01-Identification': 0.1,
                'Identified': 0.1,
                '02-Qualifiée': 0.25,
                'Qualified': 0.25,
                '03-Evaluation technique': 0.5,
                'Design in': 0.5,
                '04-Construction propositions': 0.75,
                'Negotiate': 0.75,
                '05-Négociation': 0.9,
                'Design Win': 0.9,
                'closedwon': 1.0,
                'closedlost': 0.0
            }

            if 'hubspot_stage' in df.columns:
                df['stage_multiplier'] = df['hubspot_stage'].map(stage_multipliers).fillna(0.5)
                df['adjusted_forecast'] = df['Oppo_Forecast'] * df['stage_multiplier']

            return df

        except Exception as e:
            logger.error(f"Error in forecast adjustment: {str(e)}")
            return df

    def _apply_stage_progression_rules(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply stage progression validation rules"""
        try:
            # Flag deals that have been in the same stage too long
            if 'stage_duration_days' in df.columns:
                stage_duration_limits = {
                    '01-Identification': 30,
                    'Identified': 30,
                    '02-Qualifiée': 45,
                    'Qualified': 45,
                    '03-Evaluation technique': 60,
                    'Design in': 60,
                    '04-Construction propositions': 45,
                    'Negotiate': 45,
                    '05-Négociation': 30,
                    'Design Win': 30
                }

                df['stage_duration_limit'] = df['hubspot_stage'].map(stage_duration_limits).fillna(60)
                df['stage_overdue'] = df['stage_duration_days'] > df['stage_duration_limit']

            return df

        except Exception as e:
            logger.error(f"Error in stage progression rules: {str(e)}")
            return df

    def _clean_company_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and standardize company data"""
        try:
            # Standardize company names
            df['Comp_Name'] = df['Comp_Name'].str.strip().str.title()

            # Clean and validate websites
            if 'Comp_Website' in df.columns:
                df['Comp_Website'] = df['Comp_Website'].fillna('')
                mask = (df['Comp_Website'] != '') & (df['Comp_Website'] != 'NULL')
                df.loc[mask, 'website_valid'] = df.loc[mask, 'Comp_Website'].str.contains(
                    r'^https?://', case=False, na=False
                )

            return df

        except Exception as e:
            logger.error(f"Error cleaning company data: {str(e)}")
            return df

    def _categorize_companies(self, df: pd.DataFrame) -> pd.DataFrame:
        """Categorize companies based on business criteria"""
        try:
            # Simple company size categorization based on name patterns
            size_indicators = {
                'Enterprise': ['SA', 'Corp', 'Corporation', 'GmbH', 'Ltd', 'Inc'],
                'SME': ['SARL', 'SAS', 'LLC'],
                'Startup': ['Startup', 'Labs', 'Technologies']
            }

            df['company_category'] = 'Unknown'
            for category, indicators in size_indicators.items():
                for indicator in indicators:
                    mask = df['Comp_Name'].str.contains(indicator, case=False, na=False)
                    df.loc[mask, 'company_category'] = category

            return df

        except Exception as e:
            logger.error(f"Error categorizing companies: {str(e)}")
            return df

    def _clean_person_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and standardize person data"""
        try:
            # Standardize names
            df['Pers_FirstName'] = df['Pers_FirstName'].str.strip().str.title()
            df['Pers_LastName'] = df['Pers_LastName'].str.strip().str.title()

            # Clean email addresses
            df['Pers_EmailAddress'] = df['Pers_EmailAddress'].str.strip().str.lower()

            return df

        except Exception as e:
            logger.error(f"Error cleaning person data: {str(e)}")
            return df

    def _enhance_contact_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Enhance contact data with additional insights"""
        try:
            # Extract domain from email for company matching
            if 'Pers_EmailAddress' in df.columns:
                df['email_domain'] = df['Pers_EmailAddress'].str.extract(r'@([^.]+\.[^.]+)')

            # Create full name field
            df['full_name'] = df['Pers_FirstName'] + ' ' + df['Pers_LastName']

            return df

        except Exception as e:
            logger.error(f"Error enhancing contact data: {str(e)}")
            return df

    def generate_business_rules_report(self, processed_data: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
        """Generate comprehensive business rules application report"""
        report = {
            'summary': {},
            'validation_results': {},
            'pipeline_stats': {},
            'computed_metrics': {},
            'recommendations': []
        }

        for entity_type, df in processed_data.items():
            if len(df) > 0:
                # Validation results
                report['validation_results'][entity_type] = self.validate_data_quality(df, entity_type)

                # Entity-specific statistics
                if entity_type == 'opportunities':
                    report['pipeline_stats'] = self.pipeline_mapper.get_pipeline_statistics(df)
                    report['computed_metrics'] = self.computed_processor.get_computation_summary(df)

                # General summary
                report['summary'][entity_type] = {
                    'total_records': len(df),
                    'columns_count': len(df.columns)
                }

        # Generate recommendations
        report['recommendations'] = self._generate_recommendations(report)

        return report

    def _generate_recommendations(self, report: Dict[str, Any]) -> List[str]:
        """Generate recommendations based on business rules analysis"""
        recommendations = []

        # Check data quality issues
        for entity, validation in report.get('validation_results', {}).items():
            if validation.get('quality_score', 1.0) < 0.8:
                recommendations.append(f"Improve data quality for {entity} - current score: {validation['quality_score']:.2f}")

        # Check pipeline distribution
        pipeline_stats = report.get('pipeline_stats', {})
        if 'low_confidence_count' in pipeline_stats and pipeline_stats['low_confidence_count'] > 0:
            recommendations.append(f"Review {pipeline_stats['low_confidence_count']} opportunities with low mapping confidence")

        return recommendations

# Global business rules engine instance
business_rules_engine = BusinessRulesEngine()