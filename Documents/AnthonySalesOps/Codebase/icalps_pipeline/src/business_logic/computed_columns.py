"""
Computed Columns Business Logic for IC'ALPS
Handles calculated fields and business rule computations
"""

import pandas as pd
import numpy as np
import logging
from typing import Dict, Optional
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ComputedColumnsProcessor:
    """Handles computed column calculations and business rule implementations"""

    def __init__(self):
        self.risk_thresholds = {
            'high_risk': 30,
            'medium_risk': 70
        }

    def calculate_weighted_forecast(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate Weighted Forecast = Amount × Certainty%"""
        try:
            df = df.copy()

            # Ensure numeric types
            df['Oppo_Forecast'] = pd.to_numeric(df['Oppo_Forecast'], errors='coerce').fillna(0)
            df['Oppo_Certainty'] = pd.to_numeric(df['Oppo_Certainty'], errors='coerce').fillna(0)

            # Calculate weighted forecast
            df['weighted_forecast'] = df['Oppo_Forecast'] * (df['Oppo_Certainty'] / 100)

            logger.info("Calculated weighted forecast for all opportunities")
            return df

        except Exception as e:
            logger.error(f"Error calculating weighted forecast: {str(e)}")
            return df

    def calculate_net_amount(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate Net Amount = Forecast - Cost"""
        try:
            df = df.copy()

            # Ensure numeric types
            df['Oppo_Forecast'] = pd.to_numeric(df['Oppo_Forecast'], errors='coerce').fillna(0)
            df['oppo_cout'] = pd.to_numeric(df['oppo_cout'], errors='coerce').fillna(0)

            # Calculate net amount (profit)
            df['net_amount'] = df['Oppo_Forecast'] - df['oppo_cout']

            logger.info("Calculated net amount for all opportunities")
            return df

        except Exception as e:
            logger.error(f"Error calculating net amount: {str(e)}")
            return df

    def calculate_net_weighted_amount(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate Net Weighted Amount = (Forecast - Cost) × Certainty%"""
        try:
            df = df.copy()

            # Ensure we have net_amount calculated
            if 'net_amount' not in df.columns:
                df = self.calculate_net_amount(df)

            # Ensure numeric types
            df['Oppo_Certainty'] = pd.to_numeric(df['Oppo_Certainty'], errors='coerce').fillna(0)

            # Calculate net weighted amount
            df['net_weighted_amount'] = df['net_amount'] * (df['Oppo_Certainty'] / 100)

            logger.info("Calculated net weighted amount for all opportunities")
            return df

        except Exception as e:
            logger.error(f"Error calculating net weighted amount: {str(e)}")
            return df

    def calculate_deal_age(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate Deal Age = days between today and created date"""
        try:
            df = df.copy()

            # Convert created date to datetime
            df['Oppo_CreatedDate'] = pd.to_datetime(df['Oppo_CreatedDate'], errors='coerce')

            # Calculate deal age in days
            current_date = pd.Timestamp.now()
            df['deal_age_days'] = (current_date - df['Oppo_CreatedDate']).dt.days

            # Handle negative or invalid ages
            df['deal_age_days'] = df['deal_age_days'].clip(lower=0)
            df['deal_age_days'] = df['deal_age_days'].fillna(0)

            logger.info("Calculated deal age for all opportunities")
            return df

        except Exception as e:
            logger.error(f"Error calculating deal age: {str(e)}")
            return df

    def calculate_stage_duration(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate Stage Duration = days between today and last updated"""
        try:
            df = df.copy()

            # Convert updated date to datetime
            df['Oppo_UpdatedDate'] = pd.to_datetime(df['Oppo_UpdatedDate'], errors='coerce')

            # Calculate stage duration in days
            current_date = pd.Timestamp.now()
            df['stage_duration_days'] = (current_date - df['Oppo_UpdatedDate']).dt.days

            # Handle negative or invalid durations
            df['stage_duration_days'] = df['stage_duration_days'].clip(lower=0)
            df['stage_duration_days'] = df['stage_duration_days'].fillna(0)

            logger.info("Calculated stage duration for all opportunities")
            return df

        except Exception as e:
            logger.error(f"Error calculating stage duration: {str(e)}")
            return df

    def assess_risk_level(self, df: pd.DataFrame) -> pd.DataFrame:
        """Assess risk level based on certainty percentage"""
        try:
            df = df.copy()

            # Ensure numeric certainty
            df['Oppo_Certainty'] = pd.to_numeric(df['Oppo_Certainty'], errors='coerce').fillna(0)

            # Define risk assessment function
            def get_risk_level(certainty):
                if certainty < self.risk_thresholds['high_risk']:
                    return 'High Risk'
                elif certainty <= self.risk_thresholds['medium_risk']:
                    return 'Medium Risk'
                else:
                    return 'Low Risk'

            # Apply risk assessment
            df['risk_assessment'] = df['Oppo_Certainty'].apply(get_risk_level)

            logger.info("Calculated risk assessment for all opportunities")
            return df

        except Exception as e:
            logger.error(f"Error calculating risk assessment: {str(e)}")
            return df

    def calculate_roi_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate ROI and margin metrics"""
        try:
            df = df.copy()

            # Ensure we have required calculated fields
            if 'net_amount' not in df.columns:
                df = self.calculate_net_amount(df)

            # Ensure numeric types
            df['Oppo_Forecast'] = pd.to_numeric(df['Oppo_Forecast'], errors='coerce').fillna(0)
            df['oppo_cout'] = pd.to_numeric(df['oppo_cout'], errors='coerce').fillna(0)

            # Calculate margin percentage
            mask = df['Oppo_Forecast'] > 0
            df['margin_percentage'] = 0.0
            df.loc[mask, 'margin_percentage'] = (df.loc[mask, 'net_amount'] / df.loc[mask, 'Oppo_Forecast']) * 100

            # Calculate ROI percentage
            mask = df['oppo_cout'] > 0
            df['roi_percentage'] = 0.0
            df.loc[mask, 'roi_percentage'] = (df.loc[mask, 'net_amount'] / df.loc[mask, 'oppo_cout']) * 100

            logger.info("Calculated ROI metrics for all opportunities")
            return df

        except Exception as e:
            logger.error(f"Error calculating ROI metrics: {str(e)}")
            return df

    def calculate_velocity_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate deal velocity and progression metrics"""
        try:
            df = df.copy()

            # Ensure we have deal age
            if 'deal_age_days' not in df.columns:
                df = self.calculate_deal_age(df)

            # Calculate average days per stage (estimated)
            df['estimated_days_per_stage'] = df['deal_age_days'] / 5  # Assuming 5 stages on average

            # Calculate velocity score (inverse of days per stage, normalized)
            max_days = df['estimated_days_per_stage'].max()
            if max_days > 0:
                df['velocity_score'] = 1 - (df['estimated_days_per_stage'] / max_days)
            else:
                df['velocity_score'] = 1.0

            df['velocity_score'] = df['velocity_score'].clip(0, 1)

            logger.info("Calculated velocity metrics for all opportunities")
            return df

        except Exception as e:
            logger.error(f"Error calculating velocity metrics: {str(e)}")
            return df

    def apply_all_computations(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply all computed column calculations"""
        try:
            logger.info("Starting comprehensive computed columns calculation")

            # Apply all calculations in sequence
            df = self.calculate_weighted_forecast(df)
            df = self.calculate_net_amount(df)
            df = self.calculate_net_weighted_amount(df)
            df = self.calculate_deal_age(df)
            df = self.calculate_stage_duration(df)
            df = self.assess_risk_level(df)
            df = self.calculate_roi_metrics(df)
            df = self.calculate_velocity_metrics(df)

            logger.info(f"Completed all computed columns for {len(df)} opportunities")
            return df

        except Exception as e:
            logger.error(f"Error in comprehensive computation: {str(e)}")
            return df

    def get_computation_summary(self, df: pd.DataFrame) -> Dict[str, any]:
        """Get summary statistics for computed columns"""
        computed_columns = [
            'weighted_forecast', 'net_amount', 'net_weighted_amount',
            'deal_age_days', 'stage_duration_days', 'margin_percentage',
            'roi_percentage', 'velocity_score'
        ]

        summary = {}
        for col in computed_columns:
            if col in df.columns:
                summary[col] = {
                    'mean': df[col].mean(),
                    'median': df[col].median(),
                    'std': df[col].std(),
                    'min': df[col].min(),
                    'max': df[col].max(),
                    'null_count': df[col].isnull().sum()
                }

        # Risk assessment distribution
        if 'risk_assessment' in df.columns:
            summary['risk_distribution'] = df['risk_assessment'].value_counts().to_dict()

        return summary

    def validate_computations(self, df: pd.DataFrame) -> Dict[str, bool]:
        """Validate computed column calculations"""
        validation_results = {}

        # Check for required base columns
        required_columns = ['Oppo_Forecast', 'Oppo_Certainty', 'oppo_cout']
        for col in required_columns:
            validation_results[f'{col}_exists'] = col in df.columns

        # Check computed columns exist and have reasonable values
        if 'weighted_forecast' in df.columns:
            validation_results['weighted_forecast_valid'] = (df['weighted_forecast'] >= 0).all()

        if 'net_amount' in df.columns:
            validation_results['net_amount_calculated'] = df['net_amount'].notna().any()

        if 'risk_assessment' in df.columns:
            valid_risks = {'High Risk', 'Medium Risk', 'Low Risk'}
            validation_results['risk_assessment_valid'] = df['risk_assessment'].isin(valid_risks).all()

        return validation_results

# Global processor instance
computed_columns_processor = ComputedColumnsProcessor()