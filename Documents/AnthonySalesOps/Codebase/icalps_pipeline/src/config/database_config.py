"""
Database Configuration for IC'ALPS Pipeline
Handles CSV input configuration and DuckDB setup
"""

import os
from pathlib import Path
from typing import Dict, Any

class DatabaseConfig:
    """Configuration for database connections and file paths"""

    def __init__(self):
        self.base_path = Path(__file__).parent.parent.parent
        self.input_path = self.base_path / "input"
        self.output_path = self.base_path / "output"
        self.temp_path = self.base_path / "temp"

        # Ensure directories exist
        self.input_path.mkdir(exist_ok=True)
        self.output_path.mkdir(exist_ok=True)
        self.temp_path.mkdir(exist_ok=True)

    @property
    def csv_files(self) -> Dict[str, str]:
        """CSV file mappings for Bronze layer extraction"""
        return {
            'companies': str(self.input_path / "Legacy_companies.csv"),
            'opportunities': str(self.input_path / "Legacy_Opportunities.csv"),
            'persons': str(self.input_path / "Legacy_persons.csv"),
            'communications': str(self.input_path / "Legacy_comm.csv"),
            'social_networks': str(self.input_path / "legacy_socialnetworks.csv"),
            'status_combinations': str(self.input_path / "combination_set.csv"),
            'pipeline_combinations': str(self.input_path / "combination_set._pipeline.csv")
        }

    @property
    def duckdb_config(self) -> Dict[str, Any]:
        """DuckDB configuration settings"""
        return {
            'database_path': str(self.temp_path / "icalps_pipeline.duckdb"),
            'memory_limit': '1GB',
            'threads': 4,
            'temp_directory': str(self.temp_path)
        }

    def get_bronze_table_name(self, entity_type: str) -> str:
        """Generate Bronze layer table names with proper prefix"""
        return f"Bronze_{entity_type}"

    def get_processed_table_name(self, entity_type: str) -> str:
        """Generate processed table names"""
        return f"Processed_{entity_type}"

    def validate_csv_files(self) -> Dict[str, bool]:
        """Validate that all required CSV files exist"""
        validation_results = {}
        for key, file_path in self.csv_files.items():
            validation_results[key] = Path(file_path).exists()
        return validation_results

# Global configuration instance
config = DatabaseConfig()