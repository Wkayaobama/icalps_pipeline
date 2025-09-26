"""
Pipeline Mapping Business Logic for IC'ALPS
Handles deal stage mapping and pipeline assignment based on business rules
"""

import pandas as pd
import logging
from typing import Dict, Tuple, Optional
from enum import Enum

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PipelineType(Enum):
    """Pipeline types based on opportunity type"""
    STUDIES = "Studies Pipeline"
    SALES = "Sales Pipeline"

class DealStage(Enum):
    """Deal stages for both pipelines"""
    # Studies Pipeline stages
    STUDIES_IDENTIFICATION = "01-Identification"
    STUDIES_QUALIFIED = "02-Qualifiée"
    STUDIES_EVALUATION = "03-Evaluation technique"
    STUDIES_PROPOSITIONS = "04-Construction propositions"
    STUDIES_NEGOTIATION = "05-Négociation"

    # Sales Pipeline stages
    SALES_IDENTIFIED = "Identified"
    SALES_QUALIFIED = "Qualified"
    SALES_DESIGN_IN = "Design in"
    SALES_NEGOTIATE = "Negotiate"
    SALES_DESIGN_WIN = "Design Win"

    # Final stages (both pipelines)
    CLOSED_WON = "closedwon"
    CLOSED_LOST = "closedlost"

class PipelineMapper:
    """Handles pipeline mapping and stage transformation business logic"""

    def __init__(self):
        self.setup_mapping_rules()

    def setup_mapping_rules(self):
        """Set up pipeline and stage mapping rules"""

        # Pipeline assignment rules based on opportunity type
        self.pipeline_rules = {
            'Preetude': PipelineType.STUDIES,
            'FTK': PipelineType.SALES,
            None: PipelineType.SALES,  # Default to Sales pipeline
            '': PipelineType.SALES,
            'NULL': PipelineType.SALES
        }

        # Final stage mapping (double granularity)
        self.final_stage_mapping = {
            'Won': DealStage.CLOSED_WON,
            'Lost': DealStage.CLOSED_LOST,
            'NoGo': DealStage.CLOSED_LOST,      # Not profitable
            'Abandonne': DealStage.CLOSED_LOST, # Project doesn't want ASIC
            'Sleap': None  # Keep in current active stage
        }

        # Active stage mapping for Studies Pipeline
        self.studies_stage_mapping = {
            'Identification': DealStage.STUDIES_IDENTIFICATION,
            'Qualified': DealStage.STUDIES_QUALIFIED,
            'Qualifiée': DealStage.STUDIES_QUALIFIED,
            'Evaluation technique': DealStage.STUDIES_EVALUATION,
            'Construction propositions': DealStage.STUDIES_PROPOSITIONS,
            'Negotiating': DealStage.STUDIES_NEGOTIATION,
            'Négociation': DealStage.STUDIES_NEGOTIATION
        }

        # Active stage mapping for Sales Pipeline
        self.sales_stage_mapping = {
            'Identification': DealStage.SALES_IDENTIFIED,
            'Qualified': DealStage.SALES_QUALIFIED,
            'Qualifiée': DealStage.SALES_QUALIFIED,
            'Evaluation technique': DealStage.SALES_DESIGN_IN,
            'Construction propositions': DealStage.SALES_NEGOTIATE,
            'Negotiating': DealStage.SALES_DESIGN_WIN,
            'Négociation': DealStage.SALES_DESIGN_WIN
        }

        logger.info("Pipeline mapping rules initialized")

    def determine_pipeline(self, oppo_type: str) -> PipelineType:
        """Determine pipeline based on opportunity type"""
        return self.pipeline_rules.get(oppo_type, PipelineType.SALES)

    def map_deal_stage(self, oppo_status: str, oppo_stage: str, oppo_type: str) -> Tuple[str, str]:
        """
        Map deal stage based on status, stage, and type

        Returns:
            Tuple of (pipeline_name, stage_name)
        """
        pipeline = self.determine_pipeline(oppo_type)

        # Handle final stages first (double granularity)
        if oppo_status in self.final_stage_mapping:
            final_stage = self.final_stage_mapping[oppo_status]
            if final_stage is not None:
                return pipeline.value, final_stage.value
            # If final_stage is None (Sleap), continue to active stage mapping

        # Handle active stages
        stage_mapping = (self.studies_stage_mapping if pipeline == PipelineType.STUDIES
                        else self.sales_stage_mapping)

        mapped_stage = stage_mapping.get(oppo_stage)
        if mapped_stage:
            return pipeline.value, mapped_stage.value

        # Default fallback
        default_stage = (DealStage.STUDIES_IDENTIFICATION if pipeline == PipelineType.STUDIES
                        else DealStage.SALES_IDENTIFIED)

        logger.warning(f"No mapping found for status='{oppo_status}', stage='{oppo_stage}', type='{oppo_type}'. Using default.")
        return pipeline.value, default_stage.value

    def apply_pipeline_mapping(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply pipeline mapping to opportunities DataFrame"""
        try:
            df = df.copy()

            # Create new columns for HubSpot mapping
            df['hubspot_pipeline'] = ''
            df['hubspot_stage'] = ''
            df['mapping_confidence'] = 1.0

            for idx, row in df.iterrows():
                oppo_status = str(row.get('Oppo_Status', '')).strip()
                oppo_stage = str(row.get('Oppo_Stage', '')).strip()
                oppo_type = str(row.get('Oppo_Type', '')).strip()

                # Handle NaN and NULL values
                if oppo_status in ['nan', 'None', 'NULL']:
                    oppo_status = ''
                if oppo_stage in ['nan', 'None', 'NULL']:
                    oppo_stage = ''
                if oppo_type in ['nan', 'None', 'NULL']:
                    oppo_type = ''

                pipeline, stage = self.map_deal_stage(oppo_status, oppo_stage, oppo_type)

                df.at[idx, 'hubspot_pipeline'] = pipeline
                df.at[idx, 'hubspot_stage'] = stage

                # Set confidence based on mapping quality
                confidence = self._calculate_mapping_confidence(oppo_status, oppo_stage, oppo_type)
                df.at[idx, 'mapping_confidence'] = confidence

            logger.info(f"Applied pipeline mapping to {len(df)} opportunities")
            return df

        except Exception as e:
            logger.error(f"Error applying pipeline mapping: {str(e)}")
            return df

    def _calculate_mapping_confidence(self, status: str, stage: str, oppo_type: str) -> float:
        """Calculate confidence score for mapping quality"""
        confidence = 1.0

        # Reduce confidence for missing or unclear data
        if not status or status == '':
            confidence -= 0.2
        if not stage or stage == '':
            confidence -= 0.2
        if not oppo_type or oppo_type == '':
            confidence -= 0.1

        # Boost confidence for clear final stages
        if status in ['Won', 'Lost']:
            confidence = min(1.0, confidence + 0.2)

        return max(0.1, confidence)  # Minimum confidence of 0.1

    def get_pipeline_statistics(self, df: pd.DataFrame) -> Dict[str, any]:
        """Get statistics about pipeline distribution"""
        if 'hubspot_pipeline' not in df.columns:
            df = self.apply_pipeline_mapping(df)

        stats = {
            'total_opportunities': len(df),
            'pipeline_distribution': df['hubspot_pipeline'].value_counts().to_dict(),
            'stage_distribution': df['hubspot_stage'].value_counts().to_dict(),
            'average_mapping_confidence': df['mapping_confidence'].mean(),
            'low_confidence_count': len(df[df['mapping_confidence'] < 0.8])
        }

        return stats

    def validate_mapping_rules(self) -> Dict[str, bool]:
        """Validate that all mapping rules are consistent"""
        validation_results = {
            'pipeline_rules_valid': len(self.pipeline_rules) > 0,
            'final_stage_mapping_valid': len(self.final_stage_mapping) > 0,
            'studies_mapping_valid': len(self.studies_stage_mapping) > 0,
            'sales_mapping_valid': len(self.sales_stage_mapping) > 0
        }

        # Check for consistent stage names
        all_studies_stages = set(self.studies_stage_mapping.values())
        all_sales_stages = set(self.sales_stage_mapping.values())

        validation_results['no_stage_overlap'] = len(all_studies_stages & all_sales_stages) == 0

        return validation_results

# Global mapper instance
pipeline_mapper = PipelineMapper()