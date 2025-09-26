"""
XLWings Dashboard Generator for IC'ALPS Pipeline
Creates interactive dashboards and KPI visualizations
"""

import xlwings as xw
from xlwings import script
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import logging
from typing import Dict, Optional, List
import numpy as np

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Styling constants
GRAY_1 = "#CCCCCC"
GRAY_2 = "#657072"
GRAY_3 = "#4A606C"
BLUE_1 = "#1E4E5C"
GREEN_1 = "#28A745"
RED_1 = "#DC3545"
ORANGE_1 = "#FD7E14"
PURPLE_1 = "#6F42C1"

# Dashboard color scheme
COLORS = ['#1E4E5C', '#28A745', '#FD7E14', '#6F42C1', '#DC3545', '#17A2B8']

class DashboardProcessor:
    """Handles dashboard data processing with DuckDB-like operations using pandas"""

    def __init__(self, df: pd.DataFrame):
        self.df = df

    def get_pipeline_distribution(self) -> pd.DataFrame:
        """Get pipeline distribution statistics"""
        if 'hubspot_pipeline' not in self.df.columns:
            return pd.DataFrame({'Pipeline': ['No Data'], 'Count': [0]})

        pipeline_counts = self.df['hubspot_pipeline'].value_counts().reset_index()
        pipeline_counts.columns = ['Pipeline', 'Count']
        return pipeline_counts

    def get_stage_distribution(self) -> pd.DataFrame:
        """Get stage distribution statistics"""
        if 'hubspot_stage' not in self.df.columns:
            return pd.DataFrame({'Stage': ['No Data'], 'Count': [0]})

        stage_counts = self.df['hubspot_stage'].value_counts().reset_index()
        stage_counts.columns = ['Stage', 'Count']
        return stage_counts

    def get_risk_distribution(self) -> pd.DataFrame:
        """Get risk assessment distribution"""
        if 'risk_assessment' not in self.df.columns:
            return pd.DataFrame({'Risk Level': ['No Data'], 'Count': [0]})

        risk_counts = self.df['risk_assessment'].value_counts().reset_index()
        risk_counts.columns = ['Risk Level', 'Count']
        return risk_counts

    def get_kpi_metrics(self) -> Dict[str, float]:
        """Calculate key performance indicators"""
        metrics = {}

        # Basic counts
        metrics['total_deals'] = len(self.df)

        # Pipeline value metrics
        if 'Oppo_Forecast' in self.df.columns:
            numeric_forecast = pd.to_numeric(self.df['Oppo_Forecast'], errors='coerce').fillna(0)
            metrics['total_pipeline_value'] = numeric_forecast.sum()
            metrics['average_deal_size'] = numeric_forecast.mean() if len(numeric_forecast) > 0 else 0

        # Weighted metrics
        if 'weighted_forecast' in self.df.columns:
            numeric_weighted = pd.to_numeric(self.df['weighted_forecast'], errors='coerce').fillna(0)
            metrics['weighted_pipeline_value'] = numeric_weighted.sum()

        # Win rate calculation
        if 'hubspot_stage' in self.df.columns:
            won_deals = len(self.df[self.df['hubspot_stage'] == 'closedwon'])
            lost_deals = len(self.df[self.df['hubspot_stage'] == 'closedlost'])
            total_closed = won_deals + lost_deals
            metrics['win_rate'] = (won_deals / total_closed * 100) if total_closed > 0 else 0

        # Certainty metrics
        if 'Oppo_Certainty' in self.df.columns:
            numeric_certainty = pd.to_numeric(self.df['Oppo_Certainty'], errors='coerce').fillna(0)
            metrics['average_certainty'] = numeric_certainty.mean()

        return metrics

class ChartGenerator:
    """Generates charts for dashboard"""

    def __init__(self):
        plt.style.use('default')
        sns.set_palette(COLORS)

    def create_pipeline_chart(self, data: pd.DataFrame, title: str) -> plt.Figure:
        """Create pipeline distribution chart"""
        fig, ax = plt.subplots(figsize=(10, 6))

        if len(data) > 0 and 'Pipeline' in data.columns:
            bars = ax.bar(data['Pipeline'], data['Count'], color=COLORS[:len(data)])

            # Add value labels on bars
            for bar in bars:
                height = bar.get_height()
                ax.text(bar.get_x() + bar.get_width()/2., height + 0.1,
                       f'{int(height)}', ha='center', va='bottom')

            ax.set_title(title, fontsize=14, fontweight='bold', color=GRAY_3)
            ax.set_xlabel('Pipeline', fontweight='bold')
            ax.set_ylabel('Number of Opportunities', fontweight='bold')

            # Rotate x-axis labels if needed
            plt.xticks(rotation=45, ha='right')

        else:
            ax.text(0.5, 0.5, 'No Data Available', ha='center', va='center',
                   transform=ax.transAxes, fontsize=12)
            ax.set_title(title, fontsize=14, fontweight='bold')

        plt.tight_layout()
        return fig

    def create_stage_chart(self, data: pd.DataFrame, title: str) -> plt.Figure:
        """Create stage distribution chart"""
        fig, ax = plt.subplots(figsize=(12, 8))

        if len(data) > 0 and 'Stage' in data.columns:
            # Horizontal bar chart for better label visibility
            bars = ax.barh(data['Stage'], data['Count'], color=COLORS[:len(data)])

            # Add value labels
            for bar in bars:
                width = bar.get_width()
                ax.text(width + 0.1, bar.get_y() + bar.get_height()/2.,
                       f'{int(width)}', ha='left', va='center')

            ax.set_title(title, fontsize=14, fontweight='bold', color=GRAY_3)
            ax.set_xlabel('Number of Opportunities', fontweight='bold')
            ax.set_ylabel('Deal Stage', fontweight='bold')

        else:
            ax.text(0.5, 0.5, 'No Data Available', ha='center', va='center',
                   transform=ax.transAxes, fontsize=12)
            ax.set_title(title, fontsize=14, fontweight='bold')

        plt.tight_layout()
        return fig

    def create_risk_pie_chart(self, data: pd.DataFrame, title: str) -> plt.Figure:
        """Create risk distribution pie chart"""
        fig, ax = plt.subplots(figsize=(8, 8))

        if len(data) > 0 and 'Risk Level' in data.columns:
            colors = {
                'High Risk': RED_1,
                'Medium Risk': ORANGE_1,
                'Low Risk': GREEN_1
            }

            chart_colors = [colors.get(risk, GRAY_2) for risk in data['Risk Level']]

            wedges, texts, autotexts = ax.pie(data['Count'], labels=data['Risk Level'],
                                             autopct='%1.1f%%', colors=chart_colors,
                                             startangle=90)

            # Enhance text appearance
            for autotext in autotexts:
                autotext.set_color('white')
                autotext.set_fontweight('bold')

            ax.set_title(title, fontsize=14, fontweight='bold', color=GRAY_3)

        else:
            ax.text(0.5, 0.5, 'No Data Available', ha='center', va='center',
                   transform=ax.transAxes, fontsize=12)
            ax.set_title(title, fontsize=14, fontweight='bold')

        plt.tight_layout()
        return fig

    def create_forecast_trend_chart(self, df: pd.DataFrame, title: str) -> plt.Figure:
        """Create forecast trend chart by stage"""
        fig, ax = plt.subplots(figsize=(12, 6))

        if 'hubspot_stage' in df.columns and 'Oppo_Forecast' in df.columns:
            # Group by stage and calculate metrics
            numeric_forecast = pd.to_numeric(df['Oppo_Forecast'], errors='coerce').fillna(0)
            stage_metrics = df.groupby('hubspot_stage').agg({
                'Oppo_Forecast': lambda x: pd.to_numeric(x, errors='coerce').sum()
            }).reset_index()

            stage_metrics.columns = ['Stage', 'Total_Forecast']

            if len(stage_metrics) > 0:
                bars = ax.bar(stage_metrics['Stage'], stage_metrics['Total_Forecast'],
                             color=COLORS[:len(stage_metrics)])

                # Add value labels
                for bar in bars:
                    height = bar.get_height()
                    ax.text(bar.get_x() + bar.get_width()/2., height + height*0.01,
                           f'€{height:,.0f}', ha='center', va='bottom', fontsize=10)

                ax.set_title(title, fontsize=14, fontweight='bold', color=GRAY_3)
                ax.set_xlabel('Deal Stage', fontweight='bold')
                ax.set_ylabel('Total Forecast Value (€)', fontweight='bold')

                # Format y-axis as currency
                ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'€{x:,.0f}'))

                # Rotate x-axis labels
                plt.xticks(rotation=45, ha='right')

            else:
                ax.text(0.5, 0.5, 'No Data Available', ha='center', va='center',
                       transform=ax.transAxes, fontsize=12)

        else:
            ax.text(0.5, 0.5, 'No Data Available', ha='center', va='center',
                   transform=ax.transAxes, fontsize=12)
            ax.set_title(title, fontsize=14, fontweight='bold')

        plt.tight_layout()
        return fig

@script(include=["Enhanced_Opportunities"])
def create_main_dashboard(book: xw.Book):
    """Create main IC'ALPS dashboard"""
    try:
        logger.info("Creating main dashboard...")

        # Get or create dashboard sheet
        try:
            dashboard_sheet = book.sheets["IC_ALPS_Dashboard"]
        except:
            dashboard_sheet = book.sheets.add("IC_ALPS_Dashboard")

        # Clear existing content
        dashboard_sheet.clear()

        # Load enhanced opportunities data
        try:
            enhanced_sheet = book.sheets["Enhanced_Opportunities"]
            data_range = enhanced_sheet.used_range
            if data_range and data_range.shape[0] > 1:
                df = pd.DataFrame(data_range.value[1:], columns=data_range.value[0])
            else:
                df = pd.DataFrame()
        except:
            df = pd.DataFrame()

        # Create dashboard title
        dashboard_sheet.range("A1").value = "IC'ALPS Sales Pipeline Dashboard"
        dashboard_sheet.range("A1").font.size = 20
        dashboard_sheet.range("A1").font.bold = True
        dashboard_sheet.range("A1").font.color = BLUE_1

        dashboard_sheet.range("A2").value = f"Generated: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}"
        dashboard_sheet.range("A2").font.color = GRAY_2

        if len(df) == 0:
            dashboard_sheet.range("A4").value = "No data available for dashboard"
            dashboard_sheet.range("A4").font.color = RED_1
            return

        # Calculate KPIs
        processor = DashboardProcessor(df)
        kpis = processor.get_kpi_metrics()

        # Display KPIs
        kpi_row = 4
        kpi_titles = [
            ("Total Deals", "total_deals", ""),
            ("Pipeline Value", "total_pipeline_value", "€"),
            ("Win Rate", "win_rate", "%"),
            ("Avg Certainty", "average_certainty", "%")
        ]

        dashboard_sheet.range(f"A{kpi_row}").value = "Key Performance Indicators"
        dashboard_sheet.range(f"A{kpi_row}").font.size = 16
        dashboard_sheet.range(f"A{kpi_row}").font.bold = True

        kpi_row += 2
        col = 0
        for title, key, unit in kpi_titles:
            value = kpis.get(key, 0)

            # Title
            cell = dashboard_sheet.range(f"{chr(65 + col)}{kpi_row}")
            cell.value = title
            cell.font.bold = True
            cell.font.color = GRAY_3

            # Value
            cell = dashboard_sheet.range(f"{chr(65 + col)}{kpi_row + 1}")
            if unit == "€":
                cell.value = f"€{value:,.0f}"
            elif unit == "%":
                cell.value = f"{value:.1f}%"
            else:
                cell.value = f"{value:,.0f}"

            cell.font.size = 18
            cell.font.bold = True
            cell.font.color = BLUE_1

            col += 2

        # Generate charts
        chart_generator = ChartGenerator()

        # Pipeline distribution chart
        pipeline_data = processor.get_pipeline_distribution()
        pipeline_fig = chart_generator.create_pipeline_chart(
            pipeline_data, "Pipeline Distribution"
        )

        # Add chart to Excel
        dashboard_sheet.pictures.add(
            pipeline_fig,
            update=True,
            name="Pipeline_Chart",
            anchor=dashboard_sheet[f"A{kpi_row + 4}"]
        )

        # Stage distribution chart
        stage_data = processor.get_stage_distribution()
        stage_fig = chart_generator.create_stage_chart(
            stage_data, "Deal Stage Distribution"
        )

        dashboard_sheet.pictures.add(
            stage_fig,
            update=True,
            name="Stage_Chart",
            anchor=dashboard_sheet[f"I{kpi_row + 4}"]
        )

        # Risk assessment pie chart
        risk_data = processor.get_risk_distribution()
        risk_fig = chart_generator.create_risk_pie_chart(
            risk_data, "Risk Assessment Distribution"
        )

        dashboard_sheet.pictures.add(
            risk_fig,
            update=True,
            name="Risk_Chart",
            anchor=dashboard_sheet[f"A{kpi_row + 25}"]
        )

        # Forecast trend chart
        forecast_fig = chart_generator.create_forecast_trend_chart(
            df, "Pipeline Value by Stage"
        )

        dashboard_sheet.pictures.add(
            forecast_fig,
            update=True,
            name="Forecast_Chart",
            anchor=dashboard_sheet[f"I{kpi_row + 25}"]
        )

        # Close matplotlib figures to free memory
        plt.close(pipeline_fig)
        plt.close(stage_fig)
        plt.close(risk_fig)
        plt.close(forecast_fig)

        logger.info(f"Dashboard created successfully with {len(df)} opportunities")

    except Exception as e:
        logger.error(f"Error creating dashboard: {str(e)}")
        if 'dashboard_sheet' in locals():
            dashboard_sheet.range("A4").value = f"Error creating dashboard: {str(e)}"
            dashboard_sheet.range("A4").font.color = RED_1

@script()
def create_pipeline_analysis(book: xw.Book):
    """Create detailed pipeline analysis"""
    try:
        # Get or create analysis sheet
        try:
            analysis_sheet = book.sheets["Pipeline_Analysis"]
        except:
            analysis_sheet = book.sheets.add("Pipeline_Analysis")

        analysis_sheet.clear()

        # Load data
        try:
            enhanced_sheet = book.sheets["Enhanced_Opportunities"]
            data_range = enhanced_sheet.used_range
            if data_range and data_range.shape[0] > 1:
                df = pd.DataFrame(data_range.value[1:], columns=data_range.value[0])
            else:
                df = pd.DataFrame()
        except:
            df = pd.DataFrame()

        # Title
        analysis_sheet.range("A1").value = "IC'ALPS Pipeline Analysis"
        analysis_sheet.range("A1").font.size = 18
        analysis_sheet.range("A1").font.bold = True
        analysis_sheet.range("A1").font.color = PURPLE_1

        if len(df) == 0:
            analysis_sheet.range("A3").value = "No data available for analysis"
            return

        # Pipeline summary table
        processor = DashboardProcessor(df)

        row = 3
        analysis_sheet.range(f"A{row}").value = "Pipeline Summary"
        analysis_sheet.range(f"A{row}").font.bold = True

        # Pipeline distribution
        pipeline_data = processor.get_pipeline_distribution()
        row += 2
        analysis_sheet.range(f"A{row}").value = "Pipeline"
        analysis_sheet.range(f"B{row}").value = "Opportunities"
        analysis_sheet.range(f"C{row}").value = "Percentage"

        # Make headers bold
        header_range = analysis_sheet.range(f"A{row}:C{row}")
        header_range.font.bold = True
        header_range.color = GRAY_1

        total_opps = pipeline_data['Count'].sum()
        for _, row_data in pipeline_data.iterrows():
            row += 1
            analysis_sheet.range(f"A{row}").value = row_data['Pipeline']
            analysis_sheet.range(f"B{row}").value = row_data['Count']
            percentage = (row_data['Count'] / total_opps * 100) if total_opps > 0 else 0
            analysis_sheet.range(f"C{row}").value = f"{percentage:.1f}%"

        # Stage analysis
        row += 3
        analysis_sheet.range(f"A{row}").value = "Stage Analysis"
        analysis_sheet.range(f"A{row}").font.bold = True

        stage_data = processor.get_stage_distribution()
        row += 2
        analysis_sheet.range(f"A{row}").value = "Stage"
        analysis_sheet.range(f"B{row}").value = "Count"
        analysis_sheet.range(f"C{row}").value = "Percentage"

        # Make headers bold
        header_range = analysis_sheet.range(f"A{row}:C{row}")
        header_range.font.bold = True
        header_range.color = GRAY_1

        total_stages = stage_data['Count'].sum()
        for _, row_data in stage_data.iterrows():
            row += 1
            analysis_sheet.range(f"A{row}").value = row_data['Stage']
            analysis_sheet.range(f"B{row}").value = row_data['Count']
            percentage = (row_data['Count'] / total_stages * 100) if total_stages > 0 else 0
            analysis_sheet.range(f"C{row}").value = f"{percentage:.1f}%"

        logger.info("Pipeline analysis created successfully")

    except Exception as e:
        logger.error(f"Error creating pipeline analysis: {str(e)}")
        if 'analysis_sheet' in locals():
            analysis_sheet.range("A3").value = f"Error: {str(e)}"

@script()
def refresh_all_dashboards(book: xw.Book):
    """Refresh all dashboard components"""
    try:
        logger.info("Refreshing all dashboards...")

        # Refresh main dashboard
        create_main_dashboard(book)

        # Refresh pipeline analysis
        create_pipeline_analysis(book)

        # Create completion notification
        try:
            notification_sheet = book.sheets["Dashboard_Status"]
        except:
            notification_sheet = book.sheets.add("Dashboard_Status")

        notification_sheet.clear()
        notification_sheet.range("A1").value = "Dashboard Refresh Complete"
        notification_sheet.range("A1").font.size = 16
        notification_sheet.range("A1").font.bold = True
        notification_sheet.range("A1").font.color = GREEN_1

        notification_sheet.range("A2").value = f"Refreshed: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}"

        logger.info("All dashboards refreshed successfully")

    except Exception as e:
        logger.error(f"Error refreshing dashboards: {str(e)}")
        if 'notification_sheet' in locals():
            notification_sheet.range("A3").value = f"Error: {str(e)}"
            notification_sheet.range("A3").font.color = RED_1